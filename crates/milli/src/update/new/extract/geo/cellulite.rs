use std::cell::RefCell;
use std::fs::File;
use std::io::{self, BufReader, BufWriter, ErrorKind, Seek as _, Write as _};
use std::str::FromStr;
use std::{iter, mem};

use bumpalo::Bump;
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use geojson::GeoJson;
use heed::RoTxn;

use crate::update::new::document::{Document, DocumentContext};
use crate::update::new::indexer::document_changes::Extractor;
use crate::update::new::ref_cell_ext::RefCellExt as _;
use crate::update::new::thread_local::MostlySend;
use crate::update::new::DocumentChange;
use crate::update::GrenadParameters;
use crate::{DocumentId, Index, Result, UserError};

pub struct GeoJsonExtractor {
    grenad_parameters: GrenadParameters,
}

impl GeoJsonExtractor {
    pub fn new(
        rtxn: &RoTxn,
        index: &Index,
        grenad_parameters: GrenadParameters,
    ) -> Result<Option<Self>> {
        if index.is_geojson_enabled(rtxn)? {
            Ok(Some(GeoJsonExtractor { grenad_parameters }))
        } else {
            Ok(None)
        }
    }
}

pub struct GeoJsonExtractorData<'extractor> {
    /// The set of documents ids that were removed. If a document sees its geo
    /// point being updated, we first put it in the deleted and then in the inserted.
    removed: bumpalo::collections::Vec<'extractor, (DocumentId, GeoJson)>,
    inserted: bumpalo::collections::Vec<'extractor, (DocumentId, GeoJson)>,
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
    pub removed: &'extractor [(DocumentId, GeoJson)],
    pub inserted: &'extractor [(DocumentId, GeoJson)],
    pub spilled_removed: Option<BufReader<File>>,
    pub spilled_inserted: Option<BufReader<File>>,
}

impl FrozenGeoJsonExtractorData<'_> {
    pub fn iter_and_clear_removed(
        &mut self,
    ) -> io::Result<impl IntoIterator<Item = Result<(DocumentId, GeoJson), serde_json::Error>> + '_>
    {
        Ok(mem::take(&mut self.removed)
            .iter()
            .cloned()
            .map(Ok)
            .chain(iterator_over_spilled_geojsons(&mut self.spilled_removed)?))
    }

    pub fn iter_and_clear_inserted(
        &mut self,
    ) -> io::Result<impl IntoIterator<Item = Result<(DocumentId, GeoJson), serde_json::Error>> + '_>
    {
        Ok(mem::take(&mut self.inserted)
            .iter()
            .cloned()
            .map(Ok)
            .chain(iterator_over_spilled_geojsons(&mut self.spilled_inserted)?))
    }
}

fn iterator_over_spilled_geojsons(
    spilled: &mut Option<BufReader<File>>,
) -> io::Result<impl IntoIterator<Item = Result<(DocumentId, GeoJson), serde_json::Error>> + '_> {
    let mut spilled = spilled.take();
    if let Some(spilled) = &mut spilled {
        spilled.rewind()?;
    }

    Ok(iter::from_fn(move || match &mut spilled {
        Some(file) => {
            let docid = match file.read_u32::<BigEndian>() {
                Ok(docid) => docid,
                Err(e) if e.kind() == ErrorKind::UnexpectedEof => return None,
                Err(e) => return Some(Err(serde_json::Error::io(e))),
            };
            match GeoJson::from_reader(file) {
                Ok(geojson) => Some(Ok((docid, geojson))),
                Err(e) if e.is_eof() => None,
                Err(e) => Some(Err(e)),
            }
        }
        None => None,
    }))
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
        context: &'doc DocumentContext<Self::Data>,
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
                    let external_id = deletion.external_document_id();
                    let current = deletion.current(rtxn, index, db_fields_ids_map)?;

                    if let Some(geojson) = current.geojson_field()? {
                        match &mut data_ref.spilled_removed {
                            Some(file) => {
                                file.write_u32::<BigEndian>(docid)?;
                                file.write_all(geojson.get().as_bytes())?;
                            }
                            None => data_ref.removed.push(
                                // TODO: The error type is wrong here. It should be an internal error.
                                (docid, GeoJson::from_str(geojson.get()).map_err(UserError::from)?),
                            ),
                        }
                    }
                }
                DocumentChange::Update(update) => {
                    let current = update.current(rtxn, index, db_fields_ids_map)?;
                    let external_id = update.external_document_id();
                    let docid = update.docid();

                    let current_geo = current.geojson_field()?;

                    let updated_geo =
                        update.merged(rtxn, index, db_fields_ids_map)?.geojson_field()?;

                    if current_geo.map(|c| c.get()) != updated_geo.map(|u| u.get()) {
                        // If the current and new geo points are different it means that
                        // we need to replace the current by the new point and therefore
                        // delete the current point from cellulite.
                        if let Some(geojson) = current_geo {
                            match &mut data_ref.spilled_removed {
                                Some(file) => {
                                    file.write_u32::<BigEndian>(docid)?;
                                    file.write_all(geojson.get().as_bytes())?;
                                }
                                // TODO: Should be an internal error
                                None => data_ref.removed.push((
                                    docid,
                                    GeoJson::from_str(geojson.get()).map_err(UserError::from)?,
                                )),
                            }
                        }

                        if let Some(geojson) = updated_geo {
                            match &mut data_ref.spilled_inserted {
                                Some(file) => {
                                    file.write_u32::<BigEndian>(docid)?;
                                    file.write_all(geojson.get().as_bytes())?;
                                }
                                // TODO: Is the error type correct here? Shouldn't it be an internal error?
                                None => data_ref.inserted.push((
                                    docid,
                                    GeoJson::from_str(geojson.get()).map_err(UserError::from)?,
                                )),
                            }
                        }
                    }
                }
                DocumentChange::Insertion(insertion) => {
                    let external_id = insertion.external_document_id();
                    let docid = insertion.docid();

                    let inserted_geo = insertion.inserted().geojson_field()?;

                    if let Some(geojson) = inserted_geo {
                        match &mut data_ref.spilled_inserted {
                            Some(file) => {
                                file.write_u32::<BigEndian>(docid)?;
                                file.write_all(geojson.get().as_bytes())?;
                            }
                            // TODO: Is the error type correct here? Shouldn't it be an internal error?
                            None => data_ref.inserted.push((
                                docid,
                                GeoJson::from_str(geojson.get()).map_err(UserError::from)?,
                            )),
                        }
                    }
                }
            }
        }

        Ok(())
    }
}
