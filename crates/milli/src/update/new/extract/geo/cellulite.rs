use std::str::FromStr;

use bumpalo::Bump;
use cellulite::zerometry::ZerometryCodec;
use geo_types::Geometry;
use geojson::GeoJson;
use heed::{BytesEncode, RoTxn};
use zerometry::Zerometry;

use crate::update::new::channel::GeoJsonSender;
use crate::update::new::document::{Document, DocumentContext};
use crate::update::new::indexer::document_changes::{Extractor, IndexingContext};
use crate::update::new::indexer::settings_change_extract;
use crate::update::new::indexer::settings_changes::{
    DocumentsIndentifiers, SettingsChangeExtractor,
};
use crate::update::new::steps::IndexingStep;
use crate::update::new::thread_local::{FullySend, ThreadLocal};
use crate::update::new::{DocumentChange, DocumentIdentifiers};
use crate::update::settings::SettingsDelta;
use crate::{Index, Result, UserError};

pub struct GeoJsonExtractor<'a, 'b> {
    sender: GeoJsonSender<'a, 'b>,
}

impl<'a, 'b> GeoJsonExtractor<'a, 'b> {
    pub fn new(rtxn: &RoTxn, index: &Index, sender: GeoJsonSender<'a, 'b>) -> Result<Option<Self>> {
        if index.is_geojson_filtering_enabled(rtxn)? {
            Ok(Some(GeoJsonExtractor { sender }))
        } else {
            Ok(None)
        }
    }

    pub fn run_extraction_from_settings<'fid, 'indexer, 'index, 'extractor, SD, MSP>(
        settings_delta: &SD,
        documents: &'indexer DocumentsIndentifiers<'indexer>,
        indexing_context: IndexingContext<'fid, 'indexer, 'index, MSP>,
        extractor_allocs: &'extractor mut ThreadLocal<FullySend<Bump>>,
        geojson_sender: GeoJsonSender<'_, '_>,
        step: IndexingStep,
    ) -> Result<()>
    where
        SD: SettingsDelta + Sync,
        MSP: Fn() -> bool + Sync,
    {
        let datastore = ThreadLocal::new();
        let extractor_data = GeoJsonSettingsExtractor { settings_delta, geojson_sender };

        settings_change_extract(
            documents,
            &extractor_data,
            indexing_context,
            extractor_allocs,
            &datastore,
            step,
        )
    }

    fn extract_document_from_settings_change<SD>(
        document: DocumentIdentifiers<'_>,
        context: &DocumentContext<()>,
        settings_delta: &SD,
        geojson_sender: &GeoJsonSender<'_, '_>,
    ) -> Result<()>
    where
        SD: SettingsDelta,
    {
        let old_fields_ids_map = settings_delta.old_fields_ids_map();
        let docid = document.docid();

        let current_document = document.current(
            &context.rtxn,
            context.index,
            old_fields_ids_map.as_fields_ids_map(),
        )?;

        if let Some(geojson) = current_document.geojson_field()? {
            let geojson = GeoJson::from_str(geojson.get()).map_err(UserError::from)?;
            let mut geometry = Geometry::try_from(geojson).map_err(UserError::from)?;
            cellulite::densify_geom(&mut geometry);
            let buf = ZerometryCodec::bytes_encode(&geometry).unwrap();
            geojson_sender.insert_geojson(docid, &buf).unwrap();
        }

        Ok(())
    }
}

impl<'extractor> Extractor<'extractor> for GeoJsonExtractor<'_, '_> {
    type Data = ();

    fn init_data<'doc>(&'doc self, _extractor_alloc: &'extractor Bump) -> Result<Self::Data> {
        Ok(())
    }

    fn process<'doc>(
        &'doc self,
        changes: impl Iterator<Item = Result<DocumentChange<'doc>>>,
        context: &'doc DocumentContext<'doc, 'extractor, '_, '_, Self::Data>,
    ) -> Result<()> {
        let rtxn = &context.rtxn;
        let index = context.index;
        let db_fields_ids_map = context.db_fields_ids_map;

        for change in changes {
            match change? {
                DocumentChange::Deletion(deletion) => {
                    let docid = deletion.docid();
                    let current = deletion.current(rtxn, index, db_fields_ids_map)?;

                    if let Some(_geojson) = current.geojson_field()? {
                        self.sender.delete_geojson(docid).unwrap();
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
                            self.sender.delete_geojson(docid).unwrap();
                        }

                        if let Some(geojson) = updated_geo {
                            let geojson =
                                GeoJson::from_str(geojson.get()).map_err(UserError::from)?;
                            let mut geometry =
                                Geometry::try_from(geojson).map_err(UserError::from)?;
                            cellulite::densify_geom(&mut geometry);

                            let buf = ZerometryCodec::bytes_encode(&geometry).unwrap();
                            self.sender.insert_geojson(docid, &buf).unwrap();
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
                        self.sender.insert_geojson(docid, &bytes).unwrap();
                    }
                }
            }
        }

        Ok(())
    }
}

pub struct GeoJsonSettingsExtractor<'a, 'b, SD> {
    settings_delta: &'a SD,
    geojson_sender: GeoJsonSender<'a, 'b>,
}

impl<'extractor, SD: SettingsDelta + Sync> SettingsChangeExtractor<'extractor>
    for GeoJsonSettingsExtractor<'_, '_, SD>
{
    type Data = ();

    fn init_data<'doc>(
        &'doc self,
        _extractor_alloc: &'extractor Bump,
    ) -> crate::Result<Self::Data> {
        Ok(())
    }

    fn process<'doc>(
        &'doc self,
        documents: impl Iterator<Item = crate::Result<DocumentIdentifiers<'doc>>>,
        context: &'doc DocumentContext<Self::Data>,
    ) -> crate::Result<()> {
        for document in documents {
            let document = document?;
            GeoJsonExtractor::extract_document_from_settings_change(
                document,
                context,
                self.settings_delta,
                &self.geojson_sender,
            )?;
        }
        Ok(())
    }
}
