use std::collections::HashSet;
use std::ops::Deref;

use big_s::S;
use bumpalo::Bump;
use heed::{EnvOpenOptions, RwTxn};
use maplit::btreemap;
use memmap2::Mmap;
use tempfile::TempDir;

use crate::constants::RESERVED_GEO_FIELD_NAME;
use crate::error::{Error, InternalError};
use crate::index::{DEFAULT_MIN_WORD_LEN_ONE_TYPO, DEFAULT_MIN_WORD_LEN_TWO_TYPOS};
use crate::progress::Progress;
use crate::update::new::indexer;
use crate::update::settings::InnerIndexSettings;
use crate::update::{
    self, IndexDocumentsConfig, IndexDocumentsMethod, IndexerConfig, Setting, Settings,
};
use crate::vector::settings::{EmbedderSource, EmbeddingSettings};
use crate::vector::EmbeddingConfigs;
use crate::{db_snap, obkv_to_json, Filter, FilterableAttributesRule, Index, Search, SearchResult};

pub(crate) struct TempIndex {
    pub inner: Index,
    pub indexer_config: IndexerConfig,
    pub index_documents_config: IndexDocumentsConfig,
    _tempdir: TempDir,
}

impl Deref for TempIndex {
    type Target = Index;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl TempIndex {
    /// Creates a temporary index
    pub fn new_with_map_size(size: usize) -> Self {
        let options = EnvOpenOptions::new();
        let mut options = options.read_txn_without_tls();
        options.map_size(size);
        let _tempdir = TempDir::new_in(".").unwrap();
        let inner = Index::new(options, _tempdir.path(), true).unwrap();
        let indexer_config = IndexerConfig::default();
        let index_documents_config = IndexDocumentsConfig::default();
        Self { inner, indexer_config, index_documents_config, _tempdir }
    }
    /// Creates a temporary index, with a default `4096 * 2000` size. This should be enough for
    /// most tests.
    pub fn new() -> Self {
        Self::new_with_map_size(4096 * 2000)
    }

    pub fn add_documents_using_wtxn<'t>(
        &'t self,
        wtxn: &mut RwTxn<'t>,
        documents: Mmap,
    ) -> Result<(), crate::error::Error> {
        let indexer_config = &self.indexer_config;
        let pool = &indexer_config.thread_pool;

        let rtxn = self.inner.read_txn()?;
        let db_fields_ids_map = self.inner.fields_ids_map(&rtxn)?;
        let mut new_fields_ids_map = db_fields_ids_map.clone();

        let embedders = InnerIndexSettings::from_index(&self.inner, &rtxn, None)?.embedding_configs;
        let mut indexer = indexer::DocumentOperation::new();
        match self.index_documents_config.update_method {
            IndexDocumentsMethod::ReplaceDocuments => {
                indexer.replace_documents(&documents).unwrap()
            }
            IndexDocumentsMethod::UpdateDocuments => indexer.update_documents(&documents).unwrap(),
        }

        let indexer_alloc = Bump::new();
        let (document_changes, operation_stats, primary_key) = indexer.into_changes(
            &indexer_alloc,
            &self.inner,
            &rtxn,
            None,
            &mut new_fields_ids_map,
            &|| false,
            Progress::default(),
        )?;

        if let Some(error) = operation_stats.into_iter().find_map(|stat| stat.error) {
            return Err(error.into());
        }

        pool.install(|| {
            indexer::index(
                wtxn,
                &self.inner,
                &crate::ThreadPoolNoAbortBuilder::new().build().unwrap(),
                indexer_config.grenad_parameters(),
                &db_fields_ids_map,
                new_fields_ids_map,
                primary_key,
                &document_changes,
                embedders,
                &|| false,
                &Progress::default(),
                &Default::default(),
            )
        })
        .unwrap()?;

        Ok(())
    }

    pub fn add_documents(&self, documents: Mmap) -> Result<(), crate::error::Error> {
        let mut wtxn = self.write_txn().unwrap();
        self.add_documents_using_wtxn(&mut wtxn, documents)?;
        wtxn.commit().unwrap();
        Ok(())
    }

    pub fn update_settings(
        &self,
        update: impl Fn(&mut Settings<'_, '_, '_>),
    ) -> Result<(), crate::error::Error> {
        let mut wtxn = self.write_txn().unwrap();
        self.update_settings_using_wtxn(&mut wtxn, update)?;
        wtxn.commit().unwrap();
        Ok(())
    }

    pub fn update_settings_using_wtxn<'t>(
        &'t self,
        wtxn: &mut RwTxn<'t>,
        update: impl Fn(&mut Settings<'_, '_, '_>),
    ) -> Result<(), crate::error::Error> {
        let mut builder = update::Settings::new(wtxn, &self.inner, &self.indexer_config);
        update(&mut builder);
        builder.execute(&|| false, &Progress::default())?;
        Ok(())
    }

    pub fn delete_documents_using_wtxn<'t>(
        &'t self,
        wtxn: &mut RwTxn<'t>,
        external_document_ids: Vec<String>,
    ) -> Result<(), crate::error::Error> {
        let indexer_config = &self.indexer_config;
        let pool = &indexer_config.thread_pool;

        let rtxn = self.inner.read_txn()?;
        let db_fields_ids_map = self.inner.fields_ids_map(&rtxn)?;
        let mut new_fields_ids_map = db_fields_ids_map.clone();

        let embedders = InnerIndexSettings::from_index(&self.inner, &rtxn, None)?.embedding_configs;

        let mut indexer = indexer::DocumentOperation::new();
        let external_document_ids: Vec<_> =
            external_document_ids.iter().map(AsRef::as_ref).collect();
        indexer.delete_documents(external_document_ids.as_slice());

        let indexer_alloc = Bump::new();
        let (document_changes, operation_stats, primary_key) = indexer.into_changes(
            &indexer_alloc,
            &self.inner,
            &rtxn,
            None,
            &mut new_fields_ids_map,
            &|| false,
            Progress::default(),
        )?;

        if let Some(error) = operation_stats.into_iter().find_map(|stat| stat.error) {
            return Err(error.into());
        }

        pool.install(|| {
            indexer::index(
                wtxn,
                &self.inner,
                &crate::ThreadPoolNoAbortBuilder::new().build().unwrap(),
                indexer_config.grenad_parameters(),
                &db_fields_ids_map,
                new_fields_ids_map,
                primary_key,
                &document_changes,
                embedders,
                &|| false,
                &Progress::default(),
                &Default::default(),
            )
        })
        .unwrap()?;

        Ok(())
    }

    pub fn delete_documents(&self, external_document_ids: Vec<String>) {
        let mut wtxn = self.write_txn().unwrap();

        self.delete_documents_using_wtxn(&mut wtxn, external_document_ids).unwrap();

        wtxn.commit().unwrap();
    }

    pub fn delete_document(&self, external_document_id: &str) {
        self.delete_documents(vec![external_document_id.to_string()])
    }
}

#[test]
fn aborting_indexation() {
    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::Ordering::Relaxed;

    let index = TempIndex::new();
    let mut wtxn = index.inner.write_txn().unwrap();
    let should_abort = AtomicBool::new(false);

    let indexer_config = &index.indexer_config;
    let pool = &indexer_config.thread_pool;

    let rtxn = index.inner.read_txn().unwrap();
    let db_fields_ids_map = index.inner.fields_ids_map(&rtxn).unwrap();
    let mut new_fields_ids_map = db_fields_ids_map.clone();

    let embedders = EmbeddingConfigs::default();
    let mut indexer = indexer::DocumentOperation::new();
    let payload = documents!([
        { "id": 1, "name": "kevin" },
        { "id": 2, "name": "bob", "age": 20 },
        { "id": 2, "name": "bob", "age": 20 },
    ]);
    indexer.replace_documents(&payload).unwrap();

    let indexer_alloc = Bump::new();
    let (document_changes, _operation_stats, primary_key) = indexer
        .into_changes(
            &indexer_alloc,
            &index.inner,
            &rtxn,
            None,
            &mut new_fields_ids_map,
            &|| false,
            Progress::default(),
        )
        .unwrap();

    should_abort.store(true, Relaxed);

    let err = pool
        .install(|| {
            indexer::index(
                &mut wtxn,
                &index.inner,
                &crate::ThreadPoolNoAbortBuilder::new().build().unwrap(),
                indexer_config.grenad_parameters(),
                &db_fields_ids_map,
                new_fields_ids_map,
                primary_key,
                &document_changes,
                embedders,
                &|| should_abort.load(Relaxed),
                &Progress::default(),
                &Default::default(),
            )
        })
        .unwrap()
        .unwrap_err();

    assert!(matches!(err, Error::InternalError(InternalError::AbortedIndexation)));
}

#[test]
fn initial_field_distribution() {
    let index = TempIndex::new();
    index
        .add_documents(documents!([
            { "id": 1, "name": "kevin" },
            { "id": 2, "name": "bob", "age": 20 },
            { "id": 2, "name": "bob", "age": 20 },
        ]))
        .unwrap();

    db_snap!(index, field_distribution, @r###"
        age              1      |
        id               2      |
        name             2      |
        "###);

    db_snap!(index, word_docids,
    @r###"
        1                [0, ]
        2                [1, ]
        20               [1, ]
        bob              [1, ]
        kevin            [0, ]
        "###
    );

    // we add all the documents a second time. we are supposed to get the same
    // field_distribution in the end
    index
        .add_documents(documents!([
            { "id": 1, "name": "kevin" },
            { "id": 2, "name": "bob", "age": 20 },
            { "id": 2, "name": "bob", "age": 20 },
        ]))
        .unwrap();

    db_snap!(index, field_distribution,
        @r###"
        age              1      |
        id               2      |
        name             2      |
        "###
    );

    // then we update a document by removing one field and another by adding one field
    index
        .add_documents(documents!([
            { "id": 1, "name": "kevin", "has_dog": true },
            { "id": 2, "name": "bob" }
        ]))
        .unwrap();

    db_snap!(index, field_distribution,
        @r###"
        has_dog          1      |
        id               2      |
        name             2      |
        "###
    );
}

#[test]
fn put_and_retrieve_disable_typo() {
    let index = TempIndex::new();
    let mut txn = index.write_txn().unwrap();
    // default value is true
    assert!(index.authorize_typos(&txn).unwrap());
    // set to false
    index.put_authorize_typos(&mut txn, false).unwrap();
    txn.commit().unwrap();

    let txn = index.read_txn().unwrap();
    assert!(!index.authorize_typos(&txn).unwrap());
}

#[test]
fn set_min_word_len_for_typos() {
    let index = TempIndex::new();
    let mut txn = index.write_txn().unwrap();

    assert_eq!(index.min_word_len_one_typo(&txn).unwrap(), DEFAULT_MIN_WORD_LEN_ONE_TYPO);
    assert_eq!(index.min_word_len_two_typos(&txn).unwrap(), DEFAULT_MIN_WORD_LEN_TWO_TYPOS);

    index.put_min_word_len_one_typo(&mut txn, 3).unwrap();
    index.put_min_word_len_two_typos(&mut txn, 15).unwrap();

    txn.commit().unwrap();

    let txn = index.read_txn().unwrap();
    assert_eq!(index.min_word_len_one_typo(&txn).unwrap(), 3);
    assert_eq!(index.min_word_len_two_typos(&txn).unwrap(), 15);
}

#[test]
fn add_documents_and_set_searchable_fields() {
    let index = TempIndex::new();
    index
        .add_documents(documents!([
            { "id": 1, "doggo": "kevin" },
            { "id": 2, "doggo": { "name": "bob", "age": 20 } },
            { "id": 3, "name": "jean", "age": 25 },
        ]))
        .unwrap();
    index
        .update_settings(|settings| {
            settings.set_searchable_fields(vec![S("doggo"), S("name")]);
        })
        .unwrap();

    // ensure we get the right real searchable fields + user defined searchable fields
    let rtxn = index.read_txn().unwrap();

    let real = index.searchable_fields(&rtxn).unwrap();
    assert_eq!(real, &["doggo", "name", "doggo.name", "doggo.age"]);

    let user_defined = index.user_defined_searchable_fields(&rtxn).unwrap().unwrap();
    assert_eq!(user_defined, &["doggo", "name"]);
}

#[test]
fn set_searchable_fields_and_add_documents() {
    let index = TempIndex::new();

    index
        .update_settings(|settings| {
            settings.set_searchable_fields(vec![S("doggo"), S("name")]);
        })
        .unwrap();

    // ensure we get the right real searchable fields + user defined searchable fields
    let rtxn = index.read_txn().unwrap();

    let real = index.searchable_fields(&rtxn).unwrap();
    assert!(real.is_empty());
    let user_defined = index.user_defined_searchable_fields(&rtxn).unwrap().unwrap();
    assert_eq!(user_defined, &["doggo", "name"]);

    index
        .add_documents(documents!([
            { "id": 1, "doggo": "kevin" },
            { "id": 2, "doggo": { "name": "bob", "age": 20 } },
            { "id": 3, "name": "jean", "age": 25 },
        ]))
        .unwrap();

    // ensure we get the right real searchable fields + user defined searchable fields
    let rtxn = index.read_txn().unwrap();

    let real = index.searchable_fields(&rtxn).unwrap();
    assert_eq!(real, &["doggo", "name", "doggo.name", "doggo.age"]);

    let user_defined = index.user_defined_searchable_fields(&rtxn).unwrap().unwrap();
    assert_eq!(user_defined, &["doggo", "name"]);
}

#[test]
fn test_basic_geo_bounding_box() {
    let index = TempIndex::new();

    index
        .update_settings(|settings| {
            settings.set_filterable_fields(vec![FilterableAttributesRule::Field(
                RESERVED_GEO_FIELD_NAME.to_string(),
            )]);
        })
        .unwrap();
    index
        .add_documents(documents!([
            { "id": 0, RESERVED_GEO_FIELD_NAME: { "lat": "0", "lng": "0" } },
            { "id": 1, RESERVED_GEO_FIELD_NAME: { "lat": 0, "lng": "-175" } },
            { "id": 2, RESERVED_GEO_FIELD_NAME: { "lat": "0", "lng": 175 } },
            { "id": 3, RESERVED_GEO_FIELD_NAME: { "lat": 85, "lng": 0 } },
            { "id": 4, RESERVED_GEO_FIELD_NAME: { "lat": "-85", "lng": "0" } },
        ]))
        .unwrap();

    // ensure we get the right real searchable fields + user defined searchable fields
    let rtxn = index.read_txn().unwrap();
    let mut search = index.search(&rtxn);

    // exact match a document
    let search_result = search
        .filter(Filter::from_str("_geoBoundingBox([0, 0], [0, 0])").unwrap().unwrap())
        .execute()
        .unwrap();
    insta::assert_debug_snapshot!(search_result.candidates, @"RoaringBitmap<[0]>");

    // match a document in the middle of the rectangle
    let search_result = search
        .filter(Filter::from_str("_geoBoundingBox([10, 10], [-10, -10])").unwrap().unwrap())
        .execute()
        .unwrap();
    insta::assert_debug_snapshot!(search_result.candidates, @"RoaringBitmap<[0]>");

    // select everything
    let search_result = search
        .filter(Filter::from_str("_geoBoundingBox([90, 180], [-90, -180])").unwrap().unwrap())
        .execute()
        .unwrap();
    insta::assert_debug_snapshot!(search_result.candidates, @"RoaringBitmap<[0, 1, 2, 3, 4]>");

    // go on the edge of the longitude
    let search_result = search
        .filter(Filter::from_str("_geoBoundingBox([0, -170], [0, 180])").unwrap().unwrap())
        .execute()
        .unwrap();
    insta::assert_debug_snapshot!(search_result.candidates, @"RoaringBitmap<[1]>");

    // go on the other edge of the longitude
    let search_result = search
        .filter(Filter::from_str("_geoBoundingBox([0, -180], [0, 170])").unwrap().unwrap())
        .execute()
        .unwrap();
    insta::assert_debug_snapshot!(search_result.candidates, @"RoaringBitmap<[2]>");

    // wrap around the longitude
    let search_result = search
        .filter(Filter::from_str("_geoBoundingBox([0, -170], [0, 170])").unwrap().unwrap())
        .execute()
        .unwrap();
    insta::assert_debug_snapshot!(search_result.candidates, @"RoaringBitmap<[1, 2]>");

    // go on the edge of the latitude
    let search_result = search
        .filter(Filter::from_str("_geoBoundingBox([90, 0], [80, 0])").unwrap().unwrap())
        .execute()
        .unwrap();
    insta::assert_debug_snapshot!(search_result.candidates, @"RoaringBitmap<[3]>");

    // go on the edge of the latitude
    let search_result = search
        .filter(Filter::from_str("_geoBoundingBox([-80, 0], [-90, 0])").unwrap().unwrap())
        .execute()
        .unwrap();
    insta::assert_debug_snapshot!(search_result.candidates, @"RoaringBitmap<[4]>");

    // the requests that don't make sense

    // try to wrap around the latitude
    let error = search
        .filter(Filter::from_str("_geoBoundingBox([-80, 0], [80, 0])").unwrap().unwrap())
        .execute()
        .unwrap_err();
    insta::assert_snapshot!(
        error,
        @r###"
        The top latitude `-80` is below the bottom latitude `80`.
        32:33 _geoBoundingBox([-80, 0], [80, 0])
        "###
    );

    // send a top latitude lower than the bottow latitude
    let error = search
        .filter(Filter::from_str("_geoBoundingBox([-10, 0], [10, 0])").unwrap().unwrap())
        .execute()
        .unwrap_err();
    insta::assert_snapshot!(
        error,
        @r###"
        The top latitude `-10` is below the bottom latitude `10`.
        32:33 _geoBoundingBox([-10, 0], [10, 0])
        "###
    );
}

#[test]
fn test_contains() {
    let index = TempIndex::new();

    index
        .update_settings(|settings| {
            settings
                .set_filterable_fields(vec![FilterableAttributesRule::Field("doggo".to_string())]);
        })
        .unwrap();
    index
        .add_documents(documents!([
            { "id": 0, "doggo": "kefir" },
            { "id": 1, "doggo": "kefirounet" },
            { "id": 2, "doggo": "kefkef" },
            { "id": 3, "doggo": "fifir" },
            { "id": 4, "doggo": "boubou" },
            { "id": 5 },
        ]))
        .unwrap();

    let rtxn = index.read_txn().unwrap();
    let mut search = index.search(&rtxn);
    let search_result = search
        .filter(Filter::from_str("doggo CONTAINS kefir").unwrap().unwrap())
        .execute()
        .unwrap();
    insta::assert_debug_snapshot!(search_result.candidates, @"RoaringBitmap<[0, 1]>");
    let mut search = index.search(&rtxn);
    let search_result =
        search.filter(Filter::from_str("doggo CONTAINS KEF").unwrap().unwrap()).execute().unwrap();
    insta::assert_debug_snapshot!(search_result.candidates, @"RoaringBitmap<[0, 1, 2]>");
    let mut search = index.search(&rtxn);
    let search_result = search
        .filter(Filter::from_str("doggo NOT CONTAINS fir").unwrap().unwrap())
        .execute()
        .unwrap();
    insta::assert_debug_snapshot!(search_result.candidates, @"RoaringBitmap<[2, 4, 5]>");
}

#[test]
fn replace_documents_external_ids_and_soft_deletion_check() {
    let index = TempIndex::new();

    index
        .update_settings(|settings| {
            settings.set_primary_key("id".to_owned());
            settings
                .set_filterable_fields(vec![FilterableAttributesRule::Field("doggo".to_string())]);
        })
        .unwrap();

    let mut docs = vec![];
    for i in 0..4 {
        docs.push(serde_json::json!(
            { "id": i, "doggo": i }
        ));
    }
    index.add_documents(documents!(docs)).unwrap();

    db_snap!(index, documents_ids, @"[0, 1, 2, 3, ]");
    db_snap!(index, external_documents_ids, 1, @r###"
        docids:
        0                        0
        1                        1
        2                        2
        3                        3
        "###);
    db_snap!(index, facet_id_f64_docids, 1, @r###"
        1   0  0      1  [0, ]
        1   0  1      1  [1, ]
        1   0  2      1  [2, ]
        1   0  3      1  [3, ]
        "###);

    let mut docs = vec![];
    for i in 0..3 {
        docs.push(serde_json::json!(
            { "id": i, "doggo": i + 1 }
        ));
    }
    index.add_documents(documents!(docs)).unwrap();

    db_snap!(index, documents_ids, @"[0, 1, 2, 3, ]");
    db_snap!(index, external_documents_ids, 2, @r###"
        docids:
        0                        0
        1                        1
        2                        2
        3                        3
        "###);
    db_snap!(index, facet_id_f64_docids, 2, @r###"
        1   0  1      1  [0, ]
        1   0  2      1  [1, ]
        1   0  3      1  [2, 3, ]
        "###);

    index
        .add_documents(
            documents!([{ "id": 3, "doggo": 4 }, { "id": 3, "doggo": 5 },{ "id": 3, "doggo": 4 }]),
        )
        .unwrap();

    db_snap!(index, documents_ids, @"[0, 1, 2, 3, ]");
    db_snap!(index, external_documents_ids, 3, @r###"
        docids:
        0                        0
        1                        1
        2                        2
        3                        3
        "###);
    db_snap!(index, facet_id_f64_docids, 3, @r###"
        1   0  1      1  [0, ]
        1   0  2      1  [1, ]
        1   0  3      1  [2, ]
        1   0  4      1  [3, ]
        "###);

    index
        .update_settings(|settings| {
            settings.set_distinct_field("id".to_owned());
        })
        .unwrap();

    db_snap!(index, documents_ids, @"[0, 1, 2, 3, ]");
    db_snap!(index, external_documents_ids, 3, @r###"
        docids:
        0                        0
        1                        1
        2                        2
        3                        3
        "###);
    db_snap!(index, facet_id_f64_docids, 3, @r###"
        0   0  0      1  [0, ]
        0   0  1      1  [1, ]
        0   0  2      1  [2, ]
        0   0  3      1  [3, ]
        1   0  1      1  [0, ]
        1   0  2      1  [1, ]
        1   0  3      1  [2, ]
        1   0  4      1  [3, ]
        "###);
}

#[test]
fn bug_3021_first() {
    // https://github.com/meilisearch/meilisearch/issues/3021
    let mut index = TempIndex::new();
    index.index_documents_config.update_method = IndexDocumentsMethod::ReplaceDocuments;

    index
        .update_settings(|settings| {
            settings.set_primary_key("primary_key".to_owned());
        })
        .unwrap();

    index
        .add_documents(documents!([
            { "primary_key": 38 },
            { "primary_key": 34 }
        ]))
        .unwrap();

    db_snap!(index, documents_ids, @"[0, 1, ]");
    db_snap!(index, external_documents_ids, 1, @r###"
        docids:
        34                       1
        38                       0
        "###);

    index.delete_document("34");

    db_snap!(index, documents_ids, @"[0, ]");
    db_snap!(index, external_documents_ids, 2, @r###"
        docids:
        38                       0
        "###);

    index
        .update_settings(|s| {
            s.set_searchable_fields(vec![]);
        })
        .unwrap();

    // The key point of the test is to verify that the external documents ids
    // do not contain any entry for previously soft-deleted document ids
    db_snap!(index, documents_ids, @"[0, ]");
    db_snap!(index, external_documents_ids, 3, @r###"
        docids:
        38                       0
        "###);

    // So that this document addition works correctly now.
    // It would be wrongly interpreted as a replacement before
    index.add_documents(documents!({ "primary_key": 34 })).unwrap();

    db_snap!(index, documents_ids, @"[0, 1, ]");
    db_snap!(index, external_documents_ids, 4, @r###"
        docids:
        34                       1
        38                       0
        "###);

    // We do the test again, but deleting the document with id 0 instead of id 1 now
    index.delete_document("38");

    db_snap!(index, documents_ids, @"[1, ]");
    db_snap!(index, external_documents_ids, 5, @r###"
        docids:
        34                       1
        "###);

    index
        .update_settings(|s| {
            s.set_searchable_fields(vec!["primary_key".to_owned()]);
        })
        .unwrap();

    db_snap!(index, documents_ids, @"[1, ]");
    db_snap!(index, external_documents_ids, 6, @r###"
        docids:
        34                       1
        "###);

    // And adding lots of documents afterwards instead of just one.
    // These extra subtests don't add much, but it's better than nothing.
    index
        .add_documents(documents!([
            { "primary_key": 38 },
            { "primary_key": 39 },
            { "primary_key": 41 },
            { "primary_key": 40 },
            { "primary_key": 41 },
            { "primary_key": 42 },
        ]))
        .unwrap();

    db_snap!(index, documents_ids, @"[0, 1, 2, 3, 4, 5, ]");
    db_snap!(index, external_documents_ids, 7, @r###"
        docids:
        34                       1
        38                       0
        39                       2
        40                       4
        41                       3
        42                       5
        "###);
}

#[test]
fn simple_delete() {
    let mut index = TempIndex::new();
    index.index_documents_config.update_method = IndexDocumentsMethod::UpdateDocuments;
    index
        .add_documents(documents!([
            { "id": 30 },
            { "id": 34 }
        ]))
        .unwrap();

    db_snap!(index, documents_ids, @"[0, 1, ]");
    db_snap!(index, external_documents_ids, 1, @r###"
        docids:
        30                       0
        34                       1"###);

    index.delete_document("34");

    db_snap!(index, documents_ids, @"[0, ]");
    db_snap!(index, external_documents_ids, 2, @r###"
        docids:
        30                       0
        "###);
}

#[test]
fn bug_3021_second() {
    // https://github.com/meilisearch/meilisearch/issues/3021
    let mut index = TempIndex::new();
    index.index_documents_config.update_method = IndexDocumentsMethod::UpdateDocuments;

    index
        .update_settings(|settings| {
            settings.set_primary_key("primary_key".to_owned());
        })
        .unwrap();

    index
        .add_documents(documents!([
            { "primary_key": 30 },
            { "primary_key": 34 }
        ]))
        .unwrap();

    db_snap!(index, documents_ids, @"[0, 1, ]");
    db_snap!(index, external_documents_ids, 1, @r###"
        docids:
        30                       0
        34                       1
        "###);

    index.delete_document("34");

    db_snap!(index, documents_ids, @"[0, ]");
    db_snap!(index, external_documents_ids, 2, @r###"
        docids:
        30                       0
        "###);

    index
        .update_settings(|s| {
            s.set_searchable_fields(vec![]);
        })
        .unwrap();

    // The key point of the test is to verify that the external documents ids
    // do not contain any entry for previously soft-deleted document ids
    db_snap!(index, documents_ids, @"[0, ]");
    db_snap!(index, external_documents_ids, 3, @r###"
        docids:
        30                       0
        "###);

    // So that when we add a new document
    index.add_documents(documents!({ "primary_key": 35, "b": 2 })).unwrap();

    db_snap!(index, documents_ids, @"[0, 1, ]");
    // The external documents ids don't have several external ids pointing to the same
    // internal document id
    db_snap!(index, external_documents_ids, 4, @r###"
        docids:
        30                       0
        35                       1
        "###);

    // And when we add 34 again, we don't replace document 35
    index.add_documents(documents!({ "primary_key": 34, "a": 1 })).unwrap();

    // And document 35 still exists, is not deleted
    db_snap!(index, documents_ids, @"[0, 1, 2, ]");
    db_snap!(index, external_documents_ids, 5, @r###"
        docids:
        30                       0
        34                       2
        35                       1
        "###);

    let rtxn = index.read_txn().unwrap();
    let (_docid, obkv) = index.documents(&rtxn, [0]).unwrap()[0];
    let json = obkv_to_json(&[0, 1, 2], &index.fields_ids_map(&rtxn).unwrap(), obkv).unwrap();
    insta::assert_debug_snapshot!(json, @r###"
        {
            "primary_key": Number(30),
        }
        "###);

    // Furthermore, when we retrieve document 34, it is not the result of merging 35 with 34
    let (_docid, obkv) = index.documents(&rtxn, [2]).unwrap()[0];
    let json = obkv_to_json(&[0, 1, 2], &index.fields_ids_map(&rtxn).unwrap(), obkv).unwrap();
    insta::assert_debug_snapshot!(json, @r###"
        {
            "primary_key": Number(34),
            "a": Number(1),
        }
        "###);

    drop(rtxn);

    // Add new documents again
    index
        .add_documents(
            documents!([{ "primary_key": 37 }, { "primary_key": 38 }, { "primary_key": 39 }]),
        )
        .unwrap();

    db_snap!(index, documents_ids, @"[0, 1, 2, 3, 4, 5, ]");
    db_snap!(index, external_documents_ids, 6, @r###"
        docids:
        30                       0
        34                       2
        35                       1
        37                       3
        38                       4
        39                       5
        "###);
}

#[test]
fn bug_3021_third() {
    // https://github.com/meilisearch/meilisearch/issues/3021
    let mut index = TempIndex::new();
    index.index_documents_config.update_method = IndexDocumentsMethod::UpdateDocuments;

    index
        .update_settings(|settings| {
            settings.set_primary_key("primary_key".to_owned());
        })
        .unwrap();

    index
        .add_documents(documents!([
            { "primary_key": 3 },
            { "primary_key": 4 },
            { "primary_key": 5 }
        ]))
        .unwrap();

    db_snap!(index, documents_ids, @"[0, 1, 2, ]");
    db_snap!(index, external_documents_ids, 1, @r###"
        docids:
        3                        0
        4                        1
        5                        2
        "###);

    index.delete_document("3");

    db_snap!(index, documents_ids, @"[1, 2, ]");
    db_snap!(index, external_documents_ids, 2, @r###"
        docids:
        4                        1
        5                        2
        "###);

    index.add_documents(documents!([{ "primary_key": "4", "a": 2 }])).unwrap();

    db_snap!(index, documents_ids, @"[1, 2, ]");
    db_snap!(index, external_documents_ids, 2, @r###"
        docids:
        4                        1
        5                        2
        "###);

    index
        .add_documents(documents!([
            { "primary_key": "3" },
        ]))
        .unwrap();

    db_snap!(index, documents_ids, @"[0, 1, 2, ]");
    db_snap!(index, external_documents_ids, 2, @r###"
        docids:
        3                        0
        4                        1
        5                        2
        "###);
}

#[test]
fn bug_3021_fourth() {
    // https://github.com/meilisearch/meilisearch/issues/3021
    let mut index = TempIndex::new();
    index.index_documents_config.update_method = IndexDocumentsMethod::UpdateDocuments;

    index
        .update_settings(|settings| {
            settings.set_primary_key("primary_key".to_owned());
        })
        .unwrap();

    index
        .add_documents(documents!([
            { "primary_key": 11 },
            { "primary_key": 4 },
        ]))
        .unwrap();

    db_snap!(index, documents_ids, @"[0, 1, ]");
    db_snap!(index, external_documents_ids, @r###"
        docids:
        11                       0
        4                        1
        "###);
    db_snap!(index, fields_ids_map, @r###"
        0   primary_key      |
        "###);
    db_snap!(index, searchable_fields, @r###"["primary_key"]"###);
    db_snap!(index, fieldids_weights_map, @r###"
        fid weight
        0   0   |
        "###);

    index
        .add_documents(documents!([
            { "primary_key": 4, "a": 0 },
            { "primary_key": 1 },
        ]))
        .unwrap();

    db_snap!(index, documents_ids, @"[0, 1, 2, ]");
    db_snap!(index, external_documents_ids, @r###"
        docids:
        1                        2
        11                       0
        4                        1
        "###);
    db_snap!(index, fields_ids_map, @r###"
        0   primary_key      |
        1   a                |
        "###);
    db_snap!(index, searchable_fields, @r###"["primary_key", "a"]"###);
    db_snap!(index, fieldids_weights_map, @r###"
        fid weight
        0   0   |
        1   0   |
        "###);

    index.delete_documents(Default::default());

    db_snap!(index, documents_ids, @"[0, 1, 2, ]");
    db_snap!(index, external_documents_ids, @r###"
        docids:
        1                        2
        11                       0
        4                        1
        "###);
    db_snap!(index, fields_ids_map, @r###"
        0   primary_key      |
        1   a                |
        "###);
    db_snap!(index, searchable_fields, @r###"["primary_key", "a"]"###);
    db_snap!(index, fieldids_weights_map, @r###"
        fid weight
        0   0   |
        1   0   |
        "###);

    index
        .add_documents(documents!([
            { "primary_key": 4, "a": 1 },
            { "primary_key": 1, "a": 0 },
        ]))
        .unwrap();

    db_snap!(index, documents_ids, @"[0, 1, 2, ]");
    db_snap!(index, external_documents_ids, @r###"
        docids:
        1                        2
        11                       0
        4                        1
        "###);
    db_snap!(index, fields_ids_map, @r###"
        0   primary_key      |
        1   a                |
        "###);
    db_snap!(index, searchable_fields, @r###"["primary_key", "a"]"###);
    db_snap!(index, fieldids_weights_map, @r###"
        fid weight
        0   0   |
        1   0   |
        "###);

    let rtxn = index.read_txn().unwrap();
    let search = Search::new(&rtxn, &index);
    let SearchResult {
        matching_words: _,
        candidates: _,
        document_scores: _,
        mut documents_ids,
        degraded: _,
        used_negative_operator: _,
    } = search.execute().unwrap();
    let primary_key_id = index.fields_ids_map(&rtxn).unwrap().id("primary_key").unwrap();
    documents_ids.sort_unstable();
    let docs = index.documents(&rtxn, documents_ids).unwrap();
    let mut all_ids = HashSet::new();
    for (_docid, obkv) in docs {
        let id = obkv.get(primary_key_id).unwrap();
        assert!(all_ids.insert(id));
    }
}

#[test]
fn bug_3007() {
    // https://github.com/meilisearch/meilisearch/issues/3007

    use crate::error::{GeoError, UserError};
    let index = TempIndex::new();

    // Given is an index with a geo field NOT contained in the sortable_fields of the settings
    index
        .update_settings(|settings| {
            settings.set_primary_key("id".to_string());
            settings.set_filterable_fields(vec![FilterableAttributesRule::Field(
                RESERVED_GEO_FIELD_NAME.to_string(),
            )]);
        })
        .unwrap();

    // happy path
    index
        .add_documents(documents!({ "id" : 5, RESERVED_GEO_FIELD_NAME: {"lat": 12.0, "lng": 11.0}}))
        .unwrap();

    db_snap!(index, geo_faceted_documents_ids);

    // both are unparseable, we expect GeoError::BadLatitudeAndLongitude
    let err1 = index
            .add_documents(
                documents!({ "id" : 6, RESERVED_GEO_FIELD_NAME: {"lat": "unparseable", "lng": "unparseable"}}),
            )
            .unwrap_err();
    match err1 {
        Error::UserError(UserError::InvalidGeoField(err)) => match *err {
            GeoError::BadLatitudeAndLongitude { .. } => (),
            otherwise => {
                panic!("err1 is not a BadLatitudeAndLongitude error but rather a {otherwise:?}")
            }
        },
        _ => panic!("err1 is not a BadLatitudeAndLongitude error but rather a {err1:?}"),
    }

    db_snap!(index, geo_faceted_documents_ids); // ensure that no more document was inserted
}

#[test]
fn unexpected_extra_fields_in_geo_field() {
    let index = TempIndex::new();

    index
        .update_settings(|settings| {
            settings.set_primary_key("id".to_string());
            settings.set_filterable_fields(vec![FilterableAttributesRule::Field(
                RESERVED_GEO_FIELD_NAME.to_string(),
            )]);
        })
        .unwrap();

    let err = index
            .add_documents(
                documents!({ "id" : "doggo", RESERVED_GEO_FIELD_NAME: { "lat": 1, "lng": 2, "doggo": "are the best" }}),
            )
            .unwrap_err();
    insta::assert_snapshot!(err, @r###"The `_geo` field in the document with the id: `"doggo"` contains the following unexpected fields: `{"doggo":"are the best"}`."###);

    db_snap!(index, geo_faceted_documents_ids); // ensure that no documents were inserted

    // multiple fields and complex values
    let err = index
            .add_documents(
                documents!({ "id" : "doggo", RESERVED_GEO_FIELD_NAME: { "lat": 1, "lng": 2, "doggo": "are the best", "and": { "all": ["cats", { "are": "beautiful" } ] } } }),
            )
            .unwrap_err();
    insta::assert_snapshot!(err, @r###"The `_geo` field in the document with the id: `"doggo"` contains the following unexpected fields: `{"and":{"all":["cats",{"are":"beautiful"}]},"doggo":"are the best"}`."###);

    db_snap!(index, geo_faceted_documents_ids); // ensure that no documents were inserted
}

#[test]
fn swapping_searchable_attributes() {
    // See https://github.com/meilisearch/meilisearch/issues/4484

    let index = TempIndex::new();

    index
        .update_settings(|settings| {
            settings.set_searchable_fields(vec![S("name")]);
            settings
                .set_filterable_fields(vec![FilterableAttributesRule::Field("age".to_string())]);
        })
        .unwrap();

    index
        .add_documents(documents!({ "id": 1, "name": "Many", "age": 28, "realName": "Maxime" }))
        .unwrap();
    db_snap!(index, fields_ids_map, @r###"
        0   id               |
        1   name             |
        2   age              |
        3   realName         |
        "###);
    db_snap!(index, searchable_fields, @r###"["name"]"###);
    db_snap!(index, fieldids_weights_map, @r###"
        fid weight
        1   0   |
        "###);

    index
        .update_settings(|settings| {
            settings.set_searchable_fields(vec![S("name"), S("realName")]);
            settings
                .set_filterable_fields(vec![FilterableAttributesRule::Field("age".to_string())]);
        })
        .unwrap();

    // The order of the field id map shouldn't change
    db_snap!(index, fields_ids_map, @r###"
        0   id               |
        1   name             |
        2   age              |
        3   realName         |
        "###);
    db_snap!(index, searchable_fields, @r###"["name", "realName"]"###);
    db_snap!(index, fieldids_weights_map, @r###"
        fid weight
        1   0   |
        3   1   |
        "###);
}

#[test]
fn attribute_weights_after_swapping_searchable_attributes() {
    // See https://github.com/meilisearch/meilisearch/issues/4484

    let index = TempIndex::new();

    index
        .update_settings(|settings| {
            settings.set_searchable_fields(vec![S("name"), S("beverage")]);
        })
        .unwrap();

    index
        .add_documents(documents!([
            { "id": 0, "name": "kefir", "beverage": "water" },
            { "id": 1, "name": "tamo",  "beverage": "kefir" }
        ]))
        .unwrap();

    let rtxn = index.read_txn().unwrap();
    let mut search = index.search(&rtxn);
    let results = search.query("kefir").execute().unwrap();

    // We should find kefir the dog first
    insta::assert_debug_snapshot!(results.documents_ids, @r###"
        [
            0,
            1,
        ]
        "###);

    index
        .update_settings(|settings| {
            settings.set_searchable_fields(vec![S("beverage"), S("name")]);
        })
        .unwrap();

    let rtxn = index.read_txn().unwrap();
    let mut search = index.search(&rtxn);
    let results = search.query("kefir").execute().unwrap();

    // We should find tamo first
    insta::assert_debug_snapshot!(results.documents_ids, @r###"
        [
            1,
            0,
        ]
        "###);
}

#[test]
fn vectors_are_never_indexed_as_searchable_or_filterable() {
    let index = TempIndex::new();

    index
        .add_documents(documents!([
            { "id": 0, "_vectors": { "doggo": [2345] } },
            { "id": 1, "_vectors": { "doggo": [6789] } },
        ]))
        .unwrap();

    db_snap!(index, fields_ids_map, @r###"
        0   id               |
        1   _vectors         |
        "###);
    db_snap!(index, searchable_fields, @r###"["id"]"###);
    db_snap!(index, fieldids_weights_map, @r###"
        fid weight
        0   0   |
        "###);

    let rtxn = index.read_txn().unwrap();
    let mut search = index.search(&rtxn);
    let results = search.query("2345").execute().unwrap();
    assert!(results.candidates.is_empty());
    drop(rtxn);

    index
        .update_settings(|settings| {
            settings.set_searchable_fields(vec![S("_vectors"), S("_vectors.doggo")]);
            settings.set_filterable_fields(vec![
                FilterableAttributesRule::Field("_vectors".to_string()),
                FilterableAttributesRule::Field("_vectors.doggo".to_string()),
            ]);
        })
        .unwrap();

    db_snap!(index, fields_ids_map, @r###"
        0   id               |
        1   _vectors         |
        "###);
    db_snap!(index, searchable_fields, @"[]");
    db_snap!(index, fieldids_weights_map, @r###"
        fid weight
        "###);

    let rtxn = index.read_txn().unwrap();
    let mut search = index.search(&rtxn);
    let results = search.query("2345").execute().unwrap();
    assert!(results.candidates.is_empty());

    let mut search = index.search(&rtxn);
    let results = search
        .filter(Filter::from_str("_vectors.doggo = 6789").unwrap().unwrap())
        .execute()
        .unwrap();
    assert!(results.candidates.is_empty());

    index
        .update_settings(|settings| {
            settings.set_embedder_settings(btreemap! {
                S("doggo") => Setting::Set(EmbeddingSettings {
                    dimensions: Setting::Set(1),
                    source: Setting::Set(EmbedderSource::UserProvided),
                    ..EmbeddingSettings::default()}),
            });
        })
        .unwrap();

    db_snap!(index, fields_ids_map, @r###"
        0   id               |
        1   _vectors         |
        "###);
    db_snap!(index, searchable_fields, @"[]");
    db_snap!(index, fieldids_weights_map, @r###"
        fid weight
        "###);

    let rtxn = index.read_txn().unwrap();
    let mut search = index.search(&rtxn);
    let results = search.query("2345").execute().unwrap();
    assert!(results.candidates.is_empty());

    let mut search = index.search(&rtxn);
    let results = search
        .filter(Filter::from_str("_vectors.doggo = 6789").unwrap().unwrap())
        .execute()
        .unwrap();
    assert!(results.candidates.is_empty());
}
