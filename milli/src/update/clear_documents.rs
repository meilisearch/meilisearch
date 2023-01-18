use roaring::RoaringBitmap;
use time::OffsetDateTime;

use crate::facet::FacetType;
use crate::{ExternalDocumentsIds, FieldDistribution, Index, Result};

pub struct ClearDocuments<'t, 'u, 'i> {
    wtxn: &'t mut heed::RwTxn<'i, 'u>,
    index: &'i Index,
}

impl<'t, 'u, 'i> ClearDocuments<'t, 'u, 'i> {
    pub fn new(wtxn: &'t mut heed::RwTxn<'i, 'u>, index: &'i Index) -> ClearDocuments<'t, 'u, 'i> {
        ClearDocuments { wtxn, index }
    }

    pub fn execute(self) -> Result<u64> {
        self.index.set_updated_at(self.wtxn, &OffsetDateTime::now_utc())?;
        let Index {
            env: _env,
            main: _main,
            word_docids,
            exact_word_docids,
            word_prefix_docids,
            exact_word_prefix_docids,
            docid_word_positions,
            word_pair_proximity_docids,
            word_prefix_pair_proximity_docids,
            prefix_word_pair_proximity_docids,
            word_position_docids,
            field_id_word_count_docids,
            word_prefix_position_docids,
            facet_id_f64_docids,
            facet_id_string_docids,
            facet_id_exists_docids,
            field_id_docid_facet_f64s,
            field_id_docid_facet_strings,
            documents,
        } = self.index;

        let empty_roaring = RoaringBitmap::default();

        // We retrieve the number of documents ids that we are deleting.
        let number_of_documents = self.index.number_of_documents(self.wtxn)?;
        let faceted_fields = self.index.faceted_fields_ids(self.wtxn)?;

        // We clean some of the main engine datastructures.
        self.index.put_words_fst(self.wtxn, &fst::Set::default())?;
        self.index.put_words_prefixes_fst(self.wtxn, &fst::Set::default())?;
        self.index.put_external_documents_ids(self.wtxn, &ExternalDocumentsIds::default())?;
        self.index.put_documents_ids(self.wtxn, &empty_roaring)?;
        self.index.put_soft_deleted_documents_ids(self.wtxn, &empty_roaring)?;
        self.index.put_field_distribution(self.wtxn, &FieldDistribution::default())?;
        self.index.delete_geo_rtree(self.wtxn)?;
        self.index.delete_geo_faceted_documents_ids(self.wtxn)?;

        // We clean all the faceted documents ids.
        for field_id in faceted_fields {
            self.index.put_faceted_documents_ids(
                self.wtxn,
                field_id,
                FacetType::Number,
                &empty_roaring,
            )?;
            self.index.put_faceted_documents_ids(
                self.wtxn,
                field_id,
                FacetType::String,
                &empty_roaring,
            )?;
        }

        // Clear the other databases.
        word_docids.clear(self.wtxn)?;
        exact_word_docids.clear(self.wtxn)?;
        word_prefix_docids.clear(self.wtxn)?;
        exact_word_prefix_docids.clear(self.wtxn)?;
        docid_word_positions.clear(self.wtxn)?;
        word_pair_proximity_docids.clear(self.wtxn)?;
        word_prefix_pair_proximity_docids.clear(self.wtxn)?;
        prefix_word_pair_proximity_docids.clear(self.wtxn)?;
        word_position_docids.clear(self.wtxn)?;
        field_id_word_count_docids.clear(self.wtxn)?;
        word_prefix_position_docids.clear(self.wtxn)?;
        facet_id_f64_docids.clear(self.wtxn)?;
        facet_id_exists_docids.clear(self.wtxn)?;
        facet_id_string_docids.clear(self.wtxn)?;
        field_id_docid_facet_f64s.clear(self.wtxn)?;
        field_id_docid_facet_strings.clear(self.wtxn)?;
        documents.clear(self.wtxn)?;

        Ok(number_of_documents)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::tests::TempIndex;

    #[test]
    fn clear_documents() {
        let index = TempIndex::new();

        let mut wtxn = index.write_txn().unwrap();
        index
            .add_documents_using_wtxn(&mut wtxn, documents!([
                { "id": 0, "name": "kevin", "age": 20 },
                { "id": 1, "name": "kevina" },
                { "id": 2, "name": "benoit", "country": "France", "_geo": { "lng": 42, "lat": 35 } }
            ]))
            .unwrap();

        // Clear all documents from the database.
        let builder = ClearDocuments::new(&mut wtxn, &index);
        assert_eq!(builder.execute().unwrap(), 3);
        wtxn.commit().unwrap();

        let rtxn = index.read_txn().unwrap();

        // the value is 7 because there is `[id, name, age, country, _geo, _geo.lng, _geo.lat]`
        assert_eq!(index.fields_ids_map(&rtxn).unwrap().len(), 7);

        assert!(index.words_fst(&rtxn).unwrap().is_empty());
        assert!(index.words_prefixes_fst(&rtxn).unwrap().is_empty());
        assert!(index.external_documents_ids(&rtxn).unwrap().is_empty());
        assert!(index.documents_ids(&rtxn).unwrap().is_empty());
        assert!(index.field_distribution(&rtxn).unwrap().is_empty());
        assert!(index.geo_rtree(&rtxn).unwrap().is_none());
        assert!(index.geo_faceted_documents_ids(&rtxn).unwrap().is_empty());

        assert!(index.word_docids.is_empty(&rtxn).unwrap());
        assert!(index.word_prefix_docids.is_empty(&rtxn).unwrap());
        assert!(index.docid_word_positions.is_empty(&rtxn).unwrap());
        assert!(index.word_pair_proximity_docids.is_empty(&rtxn).unwrap());
        assert!(index.field_id_word_count_docids.is_empty(&rtxn).unwrap());
        assert!(index.word_prefix_pair_proximity_docids.is_empty(&rtxn).unwrap());
        assert!(index.facet_id_f64_docids.is_empty(&rtxn).unwrap());
        assert!(index.facet_id_string_docids.is_empty(&rtxn).unwrap());
        assert!(index.field_id_docid_facet_f64s.is_empty(&rtxn).unwrap());
        assert!(index.field_id_docid_facet_strings.is_empty(&rtxn).unwrap());
        assert!(index.documents.is_empty(&rtxn).unwrap());
    }
}
