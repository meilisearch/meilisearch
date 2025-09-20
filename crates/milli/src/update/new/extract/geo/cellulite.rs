use std::cell::RefCell;
use std::fs::File;
use std::io::{BufReader, BufWriter, ErrorKind, Read, Seek as _, Write as _};
use std::mem;
use std::str::FromStr;

use bumpalo::Bump;
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use cellulite::zerometry::ZerometryCodec;
use geo_types::Geometry;
use geojson::GeoJson;
use heed::{BytesEncode, RoTxn};
use zerometry::Zerometry;

use crate::update::new::channel::GeoJsonSender;
use crate::update::new::document::{Document, DocumentContext};
use crate::update::new::indexer::document_changes::Extractor;
use crate::update::new::ref_cell_ext::RefCellExt as _;
use crate::update::new::thread_local::MostlySend;
use crate::update::new::DocumentChange;
use crate::update::GrenadParameters;
use crate::{DocumentId, Index, InternalError, Result, UserError};

pub struct GeoJsonExtractor {
    grenad_parameters: GrenadParameters,
}

impl GeoJsonExtractor {
    pub fn new(
        rtxn: &RoTxn,
        index: &Index,
        grenad_parameters: GrenadParameters,
    ) -> Result<Option<Self>> {
        if index.is_geojson_filtering_enabled(rtxn)? {
            Ok(Some(GeoJsonExtractor { grenad_parameters }))
        } else {
            Ok(None)
        }
    }
}

pub struct GeoJsonExtractorData<'extractor> {
    /// The set of documents ids that were removed. If a document sees its geo
    /// point being updated, we first put it in the deleted and then in the inserted.
    removed: bumpalo::collections::Vec<'extractor, DocumentId>,
    inserted: bumpalo::collections::Vec<'extractor, (DocumentId, &'extractor [u8])>,
    /// Contains a packed list of `ExtractedGeoPoint` of the inserted geo points
    /// data structures if we have spilled to disk.
    spilled_removed: Option<BufWriter<File>>,
    /// Contains a packed list of `ExtractedGeoPoint` of the inserted geo points
    /// data structures if we have spilled to disk.
    spilled_inserted: Option<BufWriter<File>>,
}

impl<'extractor> GeoJsonExtractorData<'extractor> {
    pub fn freeze(self) -> Result<FrozenGeoJsonExtractorData<'extractor>> {
        let GeoJsonExtractorData { removed, inserted, spilled_removed, spilled_inserted } = self;

        Ok(FrozenGeoJsonExtractorData {
            removed: removed.into_bump_slice(),
            inserted: inserted.into_bump_slice(),
            spilled_removed: spilled_removed
                .map(|bw| bw.into_inner().map(BufReader::new).map_err(|iie| iie.into_error()))
                .transpose()?,
            spilled_inserted: spilled_inserted
                .map(|bw| bw.into_inner().map(BufReader::new).map_err(|iie| iie.into_error()))
                .transpose()?,
        })
    }
}

unsafe impl MostlySend for GeoJsonExtractorData<'_> {}

pub struct FrozenGeoJsonExtractorData<'extractor> {
    pub removed: &'extractor [DocumentId],
    pub inserted: &'extractor [(DocumentId, &'extractor [u8])],
    pub spilled_removed: Option<BufReader<File>>,
    pub spilled_inserted: Option<BufReader<File>>,
}

impl FrozenGeoJsonExtractorData<'_> {
    pub fn iter_and_clear_removed(&mut self, channel: GeoJsonSender<'_, '_>) -> Result<()> {
        for docid in mem::take(&mut self.removed) {
            channel.delete_geojson(*docid).unwrap();
        }

        if let Some(mut spilled) = self.spilled_removed.take() {
            spilled.rewind()?;

            loop {
                let docid = match spilled.read_u32::<BigEndian>() {
                    Ok(docid) => docid,
                    Err(e) if e.kind() == ErrorKind::UnexpectedEof => break,
                    Err(e) => return Err(InternalError::SerdeJson(serde_json::Error::io(e)).into()),
                };
                channel.delete_geojson(docid).unwrap();
            }
        }

        Ok(())
    }

    pub fn iter_and_clear_inserted(&mut self, channel: GeoJsonSender<'_, '_>) -> Result<()> {
        for (docid, _buf) in mem::take(&mut self.inserted) {
            channel.send_geojson(*docid, _buf.to_vec()).unwrap();
        }

        if let Some(mut spilled) = self.spilled_inserted.take() {
            spilled.rewind()?;

            loop {
                let docid = match spilled.read_u32::<BigEndian>() {
                    Ok(docid) => docid,
                    Err(e) if e.kind() == ErrorKind::UnexpectedEof => break,
                    Err(e) => return Err(InternalError::SerdeJson(serde_json::Error::io(e)).into()),
                };
                let size = match spilled.read_u32::<BigEndian>() {
                    Ok(size) => size,
                    Err(e) => return Err(InternalError::SerdeJson(serde_json::Error::io(e)).into()),
                };
                let mut buf = vec![0; size as usize];
                spilled
                    .read_exact(&mut buf)
                    .map_err(|e| InternalError::SerdeJson(serde_json::Error::io(e)))?;
                channel.send_geojson(docid, buf).unwrap();
            }
        }

        Ok(())
    }
}

impl<'extractor> Extractor<'extractor> for GeoJsonExtractor {
    type Data = RefCell<GeoJsonExtractorData<'extractor>>;

    fn init_data<'doc>(&'doc self, extractor_alloc: &'extractor Bump) -> Result<Self::Data> {
        Ok(RefCell::new(GeoJsonExtractorData {
            removed: bumpalo::collections::Vec::new_in(extractor_alloc),
            inserted: bumpalo::collections::Vec::new_in(extractor_alloc),
            spilled_inserted: None,
            spilled_removed: None,
        }))
    }

    fn process<'doc>(
        &'doc self,
        changes: impl Iterator<Item = Result<DocumentChange<'doc>>>,
        context: &'doc DocumentContext<'doc, 'extractor, '_, '_, Self::Data>,
    ) -> Result<()> {
        let rtxn = &context.rtxn;
        let index = context.index;
        let max_memory = self.grenad_parameters.max_memory_by_thread();
        let db_fields_ids_map = context.db_fields_ids_map;
        let mut data_ref = context.data.borrow_mut_or_yield();

        for change in changes {
            if data_ref.spilled_removed.is_none()
                && max_memory.is_some_and(|mm| context.extractor_alloc.allocated_bytes() >= mm)
            {
                // We must spill as we allocated too much memory
                data_ref.spilled_removed = tempfile::tempfile().map(BufWriter::new).map(Some)?;
                data_ref.spilled_inserted = tempfile::tempfile().map(BufWriter::new).map(Some)?;
            }

            match change? {
                DocumentChange::Deletion(deletion) => {
                    let docid = deletion.docid();
                    let current = deletion.current(rtxn, index, db_fields_ids_map)?;

                    if let Some(_geojson) = current.geojson_field()? {
                        match &mut data_ref.spilled_removed {
                            Some(file) => {
                                file.write_u32::<BigEndian>(docid)?;
                            }
                            None => {
                                data_ref.removed.push(docid);
                            }
                        }
                    }
                }
                DocumentChange::Update(update) => {
                    let current = update.current(rtxn, index, db_fields_ids_map)?;
                    let docid = update.docid();

                    let current_geo = current.geojson_field()?;

                    let updated_geo =
                        update.merged(rtxn, index, db_fields_ids_map)?.geojson_field()?;

                    if current_geo.map(|c| c.get()) != updated_geo.map(|u| u.get()) {
                        // If the current and new geo points are different it means that
                        // we need to replace the current by the new point and therefore
                        // delete the current point from cellulite.
                        if let Some(_geojson) = current_geo {
                            match &mut data_ref.spilled_removed {
                                Some(file) => {
                                    file.write_u32::<BigEndian>(docid)?;
                                }
                                None => {
                                    data_ref.removed.push(docid);
                                }
                            }
                        }

                        if let Some(geojson) = updated_geo {
                            let geojson =
                                GeoJson::from_str(geojson.get()).map_err(UserError::from)?;
                            let mut geometry =
                                Geometry::try_from(geojson).map_err(UserError::from)?;
                            cellulite::densify_geom(&mut geometry);

                            let buf = ZerometryCodec::bytes_encode(&geometry).unwrap();

                            match &mut data_ref.spilled_inserted {
                                Some(file) => {
                                    file.write_u32::<BigEndian>(docid)?;
                                    file.write_u32::<BigEndian>(buf.len() as u32)?;
                                    file.write_all(&buf)?;
                                }
                                None => {
                                    let mut bvec =
                                        bumpalo::collections::Vec::new_in(context.extractor_alloc);
                                    bvec.extend_from_slice(&buf);
                                    data_ref.inserted.push((docid, bvec.into_bump_slice()));
                                }
                            }
                        }
                    }
                }
                DocumentChange::Insertion(insertion) => {
                    let docid = insertion.docid();
                    let inserted_geo = insertion.inserted().geojson_field()?;

                    if let Some(geojson) = inserted_geo {
                        let geojson = GeoJson::from_str(geojson.get()).map_err(UserError::from)?;
                        let mut geometry = Geometry::try_from(geojson).map_err(UserError::from)?;
                        cellulite::densify_geom(&mut geometry);
                        let mut bytes = Vec::new();
                        Zerometry::write_from_geometry(&mut bytes, &geometry)?;

                        match &mut data_ref.spilled_inserted {
                            Some(file) => {
                                file.write_u32::<BigEndian>(docid)?;
                                file.write_u32::<BigEndian>(bytes.len() as u32)?;
                                file.write_all(&bytes)?;
                            }
                            None => {
                                let mut bvec =
                                    bumpalo::collections::Vec::new_in(context.extractor_alloc);
                                bvec.extend_from_slice(&bytes);
                                data_ref.inserted.push((docid, bvec.into_bump_slice()));
                            }
                        }
                    }
                }
            }
        }

        Ok(())
    }
}
