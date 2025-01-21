use std::collections::BTreeMap;

use big_s::S;
use insta::assert_json_snapshot;
use meili_snap::{json_string, snapshot};
use meilisearch_types::milli::index::IndexEmbeddingConfig;
use meilisearch_types::milli::update::Setting;
use meilisearch_types::milli::vector::settings::EmbeddingSettings;
use meilisearch_types::milli::{self, obkv_to_json};
use meilisearch_types::settings::{SettingEmbeddingSettings, Settings, Unchecked};
use meilisearch_types::tasks::KindWithContent;
use milli::update::IndexDocumentsMethod::*;

use crate::insta_snapshot::snapshot_index_scheduler;
use crate::test_utils::read_json;
use crate::IndexScheduler;

#[test]
fn import_vectors() {
    let (index_scheduler, mut handle) = IndexScheduler::test(true, vec![]);

    let mut new_settings: Box<Settings<Unchecked>> = Box::default();
    let mut embedders = BTreeMap::default();
    let embedding_settings = milli::vector::settings::EmbeddingSettings {
        source: Setting::Set(milli::vector::settings::EmbedderSource::Rest),
        api_key: Setting::Set(S("My super secret")),
        url: Setting::Set(S("http://localhost:7777")),
        dimensions: Setting::Set(384),
        request: Setting::Set(serde_json::json!("{{text}}")),
        response: Setting::Set(serde_json::json!("{{embedding}}")),
        ..Default::default()
    };
    embedders.insert(
        S("A_fakerest"),
        SettingEmbeddingSettings { inner: Setting::Set(embedding_settings) },
    );

    let embedding_settings = milli::vector::settings::EmbeddingSettings {
        source: Setting::Set(milli::vector::settings::EmbedderSource::HuggingFace),
        model: Setting::Set(S("sentence-transformers/all-MiniLM-L6-v2")),
        revision: Setting::Set(S("e4ce9877abf3edfe10b0d82785e83bdcb973e22e")),
        document_template: Setting::Set(S("{{doc.doggo}} the {{doc.breed}} best doggo")),
        ..Default::default()
    };
    embedders.insert(
        S("B_small_hf"),
        SettingEmbeddingSettings { inner: Setting::Set(embedding_settings) },
    );

    new_settings.embedders = Setting::Set(embedders);

    index_scheduler
        .register(
            KindWithContent::SettingsUpdate {
                index_uid: S("doggos"),
                new_settings,
                is_deletion: false,
                allow_index_creation: true,
            },
            None,
            false,
        )
        .unwrap();

    snapshot!(snapshot_index_scheduler(&index_scheduler), name: "after_registering_settings_task_vectors");

    {
        let rtxn = index_scheduler.read_txn().unwrap();
        let task = index_scheduler.queue.tasks.get_task(&rtxn, 0).unwrap().unwrap();
        let task = meilisearch_types::task_view::TaskView::from_task(&task);
        insta::assert_json_snapshot!(task.details);
    }

    handle.advance_one_successful_batch();
    snapshot!(snapshot_index_scheduler(&index_scheduler), name: "settings_update_processed_vectors");

    {
        let rtxn = index_scheduler.read_txn().unwrap();
        let task = index_scheduler.queue.tasks.get_task(&rtxn, 0).unwrap().unwrap();
        let task = meilisearch_types::task_view::TaskView::from_task(&task);
        insta::assert_json_snapshot!(task.details);
    }

    let (fakerest_name, simple_hf_name, beagle_embed, lab_embed, patou_embed) = {
        let index = index_scheduler.index("doggos").unwrap();
        let rtxn = index.read_txn().unwrap();

        let configs = index.embedding_configs(&rtxn).unwrap();
        // for consistency with the below
        #[allow(clippy::get_first)]
        let IndexEmbeddingConfig { name, config: fakerest_config, user_provided } =
            configs.get(0).unwrap();
        insta::assert_snapshot!(name, @"A_fakerest");
        insta::assert_debug_snapshot!(user_provided, @"RoaringBitmap<[]>");
        insta::assert_json_snapshot!(fakerest_config.embedder_options);
        let fakerest_name = name.clone();

        let IndexEmbeddingConfig { name, config: simple_hf_config, user_provided } =
            configs.get(1).unwrap();
        insta::assert_snapshot!(name, @"B_small_hf");
        insta::assert_debug_snapshot!(user_provided, @"RoaringBitmap<[]>");
        insta::assert_json_snapshot!(simple_hf_config.embedder_options);
        let simple_hf_name = name.clone();

        let configs = index_scheduler.embedders("doggos".to_string(), configs).unwrap();
        let (hf_embedder, _, _) = configs.get(&simple_hf_name).unwrap();
        let beagle_embed = hf_embedder.embed_one(S("Intel the beagle best doggo"), None).unwrap();
        let lab_embed = hf_embedder.embed_one(S("Max the lab best doggo"), None).unwrap();
        let patou_embed = hf_embedder.embed_one(S("kefir the patou best doggo"), None).unwrap();
        (fakerest_name, simple_hf_name, beagle_embed, lab_embed, patou_embed)
    };

    // add one doc, specifying vectors

    let doc = serde_json::json!(
        {
            "id": 0,
            "doggo": "Intel",
            "breed": "beagle",
            "_vectors": {
                &fakerest_name: {
                    // this will never trigger regeneration, which is good because we can't actually generate with
                    // this embedder
                    "regenerate": false,
                    "embeddings": beagle_embed,
                },
                &simple_hf_name: {
                    // this will be regenerated on updates
                    "regenerate": true,
                    "embeddings": lab_embed,
                },
                "noise": [0.1, 0.2, 0.3]
            }
        }
    );

    let (uuid, mut file) = index_scheduler.queue.create_update_file_with_uuid(0u128).unwrap();
    let documents_count = read_json(doc.to_string().as_bytes(), &mut file).unwrap();
    assert_eq!(documents_count, 1);
    file.persist().unwrap();

    index_scheduler
        .register(
            KindWithContent::DocumentAdditionOrUpdate {
                index_uid: S("doggos"),
                primary_key: Some(S("id")),
                method: UpdateDocuments,
                content_file: uuid,
                documents_count,
                allow_index_creation: true,
            },
            None,
            false,
        )
        .unwrap();

    snapshot!(snapshot_index_scheduler(&index_scheduler), name: "after adding Intel");

    handle.advance_one_successful_batch();

    snapshot!(snapshot_index_scheduler(&index_scheduler), name: "adding Intel succeeds");

    // check embeddings
    {
        let index = index_scheduler.index("doggos").unwrap();
        let rtxn = index.read_txn().unwrap();

        // Ensure the document have been inserted into the relevant bitamp
        let configs = index.embedding_configs(&rtxn).unwrap();
        // for consistency with the below
        #[allow(clippy::get_first)]
        let IndexEmbeddingConfig { name, config: _, user_provided: user_defined } =
            configs.get(0).unwrap();
        insta::assert_snapshot!(name, @"A_fakerest");
        insta::assert_debug_snapshot!(user_defined, @"RoaringBitmap<[0]>");

        let IndexEmbeddingConfig { name, config: _, user_provided } = configs.get(1).unwrap();
        insta::assert_snapshot!(name, @"B_small_hf");
        insta::assert_debug_snapshot!(user_provided, @"RoaringBitmap<[]>");

        let embeddings = index.embeddings(&rtxn, 0).unwrap();

        assert_json_snapshot!(embeddings[&simple_hf_name][0] == lab_embed, @"true");
        assert_json_snapshot!(embeddings[&fakerest_name][0] == beagle_embed, @"true");

        let doc = index.documents(&rtxn, std::iter::once(0)).unwrap()[0].1;
        let fields_ids_map = index.fields_ids_map(&rtxn).unwrap();
        let doc = obkv_to_json(
            &[
                fields_ids_map.id("doggo").unwrap(),
                fields_ids_map.id("breed").unwrap(),
                fields_ids_map.id("_vectors").unwrap(),
            ],
            &fields_ids_map,
            doc,
        )
        .unwrap();
        assert_json_snapshot!(doc, {"._vectors.A_fakerest.embeddings" => "[vector]"});
    }

    // update the doc, specifying vectors

    let doc = serde_json::json!(
                {
                    "id": 0,
                    "doggo": "kefir",
                    "breed": "patou",
                }
    );

    let (uuid, mut file) = index_scheduler.queue.create_update_file_with_uuid(1u128).unwrap();
    let documents_count = read_json(doc.to_string().as_bytes(), &mut file).unwrap();
    assert_eq!(documents_count, 1);
    file.persist().unwrap();

    index_scheduler
        .register(
            KindWithContent::DocumentAdditionOrUpdate {
                index_uid: S("doggos"),
                primary_key: None,
                method: UpdateDocuments,
                content_file: uuid,
                documents_count,
                allow_index_creation: true,
            },
            None,
            false,
        )
        .unwrap();

    snapshot!(snapshot_index_scheduler(&index_scheduler), name: "Intel to kefir");

    handle.advance_one_successful_batch();
    snapshot!(snapshot_index_scheduler(&index_scheduler), name: "Intel to kefir succeeds");

    {
        // check embeddings
        {
            let index = index_scheduler.index("doggos").unwrap();
            let rtxn = index.read_txn().unwrap();

            // Ensure the document have been inserted into the relevant bitamp
            let configs = index.embedding_configs(&rtxn).unwrap();
            // for consistency with the below
            #[allow(clippy::get_first)]
            let IndexEmbeddingConfig { name, config: _, user_provided: user_defined } =
                configs.get(0).unwrap();
            insta::assert_snapshot!(name, @"A_fakerest");
            insta::assert_debug_snapshot!(user_defined, @"RoaringBitmap<[0]>");

            let IndexEmbeddingConfig { name, config: _, user_provided } = configs.get(1).unwrap();
            insta::assert_snapshot!(name, @"B_small_hf");
            insta::assert_debug_snapshot!(user_provided, @"RoaringBitmap<[]>");

            let embeddings = index.embeddings(&rtxn, 0).unwrap();

            // automatically changed to patou because set to regenerate
            assert_json_snapshot!(embeddings[&simple_hf_name][0] == patou_embed, @"true");
            // remained beagle
            assert_json_snapshot!(embeddings[&fakerest_name][0] == beagle_embed, @"true");

            let doc = index.documents(&rtxn, std::iter::once(0)).unwrap()[0].1;
            let fields_ids_map = index.fields_ids_map(&rtxn).unwrap();
            let doc = obkv_to_json(
                &[
                    fields_ids_map.id("doggo").unwrap(),
                    fields_ids_map.id("breed").unwrap(),
                    fields_ids_map.id("_vectors").unwrap(),
                ],
                &fields_ids_map,
                doc,
            )
            .unwrap();
            assert_json_snapshot!(doc, {"._vectors.A_fakerest.embeddings" => "[vector]"});
        }
    }
}

#[test]
fn import_vectors_first_and_embedder_later() {
    let (index_scheduler, mut handle) = IndexScheduler::test(true, vec![]);

    let content = serde_json::json!(
        [
            {
                "id": 0,
                "doggo": "kefir",
            },
            {
                "id": 1,
                "doggo": "intel",
                "_vectors": {
                    "my_doggo_embedder": vec![1; 384],
                    "unknown embedder": vec![1, 2, 3],
                }
            },
            {
                "id": 2,
                "doggo": "max",
                "_vectors": {
                    "my_doggo_embedder": {
                        "regenerate": false,
                        "embeddings": vec![2; 384],
                    },
                    "unknown embedder": vec![4, 5],
                },
            },
            {
                "id": 3,
                "doggo": "marcel",
                "_vectors": {
                    "my_doggo_embedder": {
                        "regenerate": true,
                        "embeddings": vec![3; 384],
                    },
                },
            },
            {
                "id": 4,
                "doggo": "sora",
                "_vectors": {
                    "my_doggo_embedder": {
                        "regenerate": true,
                    },
                },
            },
        ]
    );

    let (uuid, mut file) = index_scheduler.queue.create_update_file_with_uuid(0_u128).unwrap();
    let documents_count =
        read_json(serde_json::to_string_pretty(&content).unwrap().as_bytes(), &mut file).unwrap();
    snapshot!(documents_count, @"5");
    file.persist().unwrap();

    index_scheduler
        .register(
            KindWithContent::DocumentAdditionOrUpdate {
                index_uid: S("doggos"),
                primary_key: None,
                method: ReplaceDocuments,
                content_file: uuid,
                documents_count,
                allow_index_creation: true,
            },
            None,
            false,
        )
        .unwrap();
    handle.advance_one_successful_batch();

    let index = index_scheduler.index("doggos").unwrap();
    let rtxn = index.read_txn().unwrap();
    let field_ids_map = index.fields_ids_map(&rtxn).unwrap();
    let field_ids = field_ids_map.ids().collect::<Vec<_>>();
    let documents = index
        .all_documents(&rtxn)
        .unwrap()
        .map(|ret| obkv_to_json(&field_ids, &field_ids_map, ret.unwrap().1).unwrap())
        .collect::<Vec<_>>();
    snapshot!(serde_json::to_string(&documents).unwrap(), name: "documents after initial push");

    let setting = meilisearch_types::settings::Settings::<Unchecked> {
        embedders: Setting::Set(maplit::btreemap! {
            S("my_doggo_embedder") => SettingEmbeddingSettings { inner: Setting::Set(EmbeddingSettings {
                source: Setting::Set(milli::vector::settings::EmbedderSource::HuggingFace),
                model: Setting::Set(S("sentence-transformers/all-MiniLM-L6-v2")),
                revision: Setting::Set(S("e4ce9877abf3edfe10b0d82785e83bdcb973e22e")),
                document_template: Setting::Set(S("{{doc.doggo}}")),
                ..Default::default()
            }) }
        }),
        ..Default::default()
    };
    index_scheduler
        .register(
            KindWithContent::SettingsUpdate {
                index_uid: S("doggos"),
                new_settings: Box::new(setting),
                is_deletion: false,
                allow_index_creation: false,
            },
            None,
            false,
        )
        .unwrap();
    index_scheduler.assert_internally_consistent();
    handle.advance_one_successful_batch();
    index_scheduler.assert_internally_consistent();

    let index = index_scheduler.index("doggos").unwrap();
    let rtxn = index.read_txn().unwrap();
    let field_ids_map = index.fields_ids_map(&rtxn).unwrap();
    let field_ids = field_ids_map.ids().collect::<Vec<_>>();
    let documents = index
        .all_documents(&rtxn)
        .unwrap()
        .map(|ret| obkv_to_json(&field_ids, &field_ids_map, ret.unwrap().1).unwrap())
        .collect::<Vec<_>>();
    // the all the vectors linked to the new specified embedder have been removed
    // Only the unknown embedders stays in the document DB
    snapshot!(serde_json::to_string(&documents).unwrap(), @r###"[{"id":0,"doggo":"kefir"},{"id":1,"doggo":"intel","_vectors":{"unknown embedder":[1.0,2.0,3.0]}},{"id":2,"doggo":"max","_vectors":{"unknown embedder":[4.0,5.0]}},{"id":3,"doggo":"marcel"},{"id":4,"doggo":"sora"}]"###);
    let conf = index.embedding_configs(&rtxn).unwrap();
    // even though we specified the vector for the ID 3, it shouldn't be marked
    // as user provided since we explicitely marked it as NOT user provided.
    snapshot!(format!("{conf:#?}"), @r###"
        [
            IndexEmbeddingConfig {
                name: "my_doggo_embedder",
                config: EmbeddingConfig {
                    embedder_options: HuggingFace(
                        EmbedderOptions {
                            model: "sentence-transformers/all-MiniLM-L6-v2",
                            revision: Some(
                                "e4ce9877abf3edfe10b0d82785e83bdcb973e22e",
                            ),
                            distribution: None,
                        },
                    ),
                    prompt: PromptData {
                        template: "{{doc.doggo}}",
                        max_bytes: Some(
                            400,
                        ),
                    },
                    quantized: None,
                },
                user_provided: RoaringBitmap<[1, 2]>,
            },
        ]
        "###);
    let docid = index.external_documents_ids.get(&rtxn, "0").unwrap().unwrap();
    let embeddings = index.embeddings(&rtxn, docid).unwrap();
    let embedding = &embeddings["my_doggo_embedder"];
    assert!(!embedding.is_empty(), "{embedding:?}");

    // the document with the id 3 should keep its original embedding
    let docid = index.external_documents_ids.get(&rtxn, "3").unwrap().unwrap();
    let embeddings = index.embeddings(&rtxn, docid).unwrap();
    let embeddings = &embeddings["my_doggo_embedder"];

    snapshot!(embeddings.len(), @"1");
    assert!(embeddings[0].iter().all(|i| *i == 3.0), "{:?}", embeddings[0]);

    // If we update marcel it should regenerate its embedding automatically

    let content = serde_json::json!(
        [
            {
                "id": 3,
                "doggo": "marvel",
            },
            {
                "id": 4,
                "doggo": "sorry",
            },
        ]
    );

    let (uuid, mut file) = index_scheduler.queue.create_update_file_with_uuid(1_u128).unwrap();
    let documents_count =
        read_json(serde_json::to_string_pretty(&content).unwrap().as_bytes(), &mut file).unwrap();
    snapshot!(documents_count, @"2");
    file.persist().unwrap();

    index_scheduler
        .register(
            KindWithContent::DocumentAdditionOrUpdate {
                index_uid: S("doggos"),
                primary_key: None,
                method: UpdateDocuments,
                content_file: uuid,
                documents_count,
                allow_index_creation: true,
            },
            None,
            false,
        )
        .unwrap();
    handle.advance_one_successful_batch();

    // the document with the id 3 should have its original embedding updated
    let rtxn = index.read_txn().unwrap();
    let docid = index.external_documents_ids.get(&rtxn, "3").unwrap().unwrap();
    let doc = index.documents(&rtxn, Some(docid)).unwrap()[0];
    let doc = obkv_to_json(&field_ids, &field_ids_map, doc.1).unwrap();
    snapshot!(json_string!(doc), @r###"
        {
          "id": 3,
          "doggo": "marvel"
        }
        "###);

    let embeddings = index.embeddings(&rtxn, docid).unwrap();
    let embedding = &embeddings["my_doggo_embedder"];

    assert!(!embedding.is_empty());
    assert!(!embedding[0].iter().all(|i| *i == 3.0), "{:?}", embedding[0]);

    // the document with the id 4 should generate an embedding
    let docid = index.external_documents_ids.get(&rtxn, "4").unwrap().unwrap();
    let embeddings = index.embeddings(&rtxn, docid).unwrap();
    let embedding = &embeddings["my_doggo_embedder"];

    assert!(!embedding.is_empty());
}

#[test]
fn delete_document_containing_vector() {
    // 1. Add an embedder
    // 2. Push two documents containing a simple vector
    // 3. Delete the first document
    // 4. The user defined roaring bitmap shouldn't contains the id of the first document anymore
    // 5. Clear the index
    // 6. The user defined roaring bitmap shouldn't contains the id of the second document
    let (index_scheduler, mut handle) = IndexScheduler::test(true, vec![]);

    let setting = meilisearch_types::settings::Settings::<Unchecked> {
        embedders: Setting::Set(maplit::btreemap! {
            S("manual") => SettingEmbeddingSettings { inner: Setting::Set(EmbeddingSettings {
                source: Setting::Set(milli::vector::settings::EmbedderSource::UserProvided),
                dimensions: Setting::Set(3),
                ..Default::default()
            }) }
        }),
        ..Default::default()
    };
    index_scheduler
        .register(
            KindWithContent::SettingsUpdate {
                index_uid: S("doggos"),
                new_settings: Box::new(setting),
                is_deletion: false,
                allow_index_creation: true,
            },
            None,
            false,
        )
        .unwrap();
    handle.advance_one_successful_batch();

    let content = serde_json::json!(
        [
            {
                "id": 0,
                "doggo": "kefir",
                "_vectors": {
                    "manual": vec![0, 0, 0],
                }
            },
            {
                "id": 1,
                "doggo": "intel",
                "_vectors": {
                    "manual": vec![1, 1, 1],
                }
            },
        ]
    );

    let (uuid, mut file) = index_scheduler.queue.create_update_file_with_uuid(0_u128).unwrap();
    let documents_count =
        read_json(serde_json::to_string_pretty(&content).unwrap().as_bytes(), &mut file).unwrap();
    snapshot!(documents_count, @"2");
    file.persist().unwrap();

    index_scheduler
        .register(
            KindWithContent::DocumentAdditionOrUpdate {
                index_uid: S("doggos"),
                primary_key: None,
                method: ReplaceDocuments,
                content_file: uuid,
                documents_count,
                allow_index_creation: false,
            },
            None,
            false,
        )
        .unwrap();
    handle.advance_one_successful_batch();

    index_scheduler
        .register(
            KindWithContent::DocumentDeletion {
                index_uid: S("doggos"),
                documents_ids: vec![S("1")],
            },
            None,
            false,
        )
        .unwrap();
    handle.advance_one_successful_batch();

    let index = index_scheduler.index("doggos").unwrap();
    let rtxn = index.read_txn().unwrap();
    let field_ids_map = index.fields_ids_map(&rtxn).unwrap();
    let field_ids = field_ids_map.ids().collect::<Vec<_>>();
    let documents = index
        .all_documents(&rtxn)
        .unwrap()
        .map(|ret| obkv_to_json(&field_ids, &field_ids_map, ret.unwrap().1).unwrap())
        .collect::<Vec<_>>();
    snapshot!(serde_json::to_string(&documents).unwrap(), @r###"[{"id":0,"doggo":"kefir"}]"###);
    let conf = index.embedding_configs(&rtxn).unwrap();
    snapshot!(format!("{conf:#?}"), @r###"
        [
            IndexEmbeddingConfig {
                name: "manual",
                config: EmbeddingConfig {
                    embedder_options: UserProvided(
                        EmbedderOptions {
                            dimensions: 3,
                            distribution: None,
                        },
                    ),
                    prompt: PromptData {
                        template: "{% for field in fields %}{% if field.is_searchable and field.value != nil %}{{ field.name }}: {{ field.value }}\n{% endif %}{% endfor %}",
                        max_bytes: Some(
                            400,
                        ),
                    },
                    quantized: None,
                },
                user_provided: RoaringBitmap<[0]>,
            },
        ]
        "###);
    let docid = index.external_documents_ids.get(&rtxn, "0").unwrap().unwrap();
    let embeddings = index.embeddings(&rtxn, docid).unwrap();
    let embedding = &embeddings["manual"];
    assert!(!embedding.is_empty(), "{embedding:?}");

    index_scheduler
        .register(KindWithContent::DocumentClear { index_uid: S("doggos") }, None, false)
        .unwrap();
    handle.advance_one_successful_batch();

    let index = index_scheduler.index("doggos").unwrap();
    let rtxn = index.read_txn().unwrap();
    let field_ids_map = index.fields_ids_map(&rtxn).unwrap();
    let field_ids = field_ids_map.ids().collect::<Vec<_>>();
    let documents = index
        .all_documents(&rtxn)
        .unwrap()
        .map(|ret| obkv_to_json(&field_ids, &field_ids_map, ret.unwrap().1).unwrap())
        .collect::<Vec<_>>();
    snapshot!(serde_json::to_string(&documents).unwrap(), @"[]");
    let conf = index.embedding_configs(&rtxn).unwrap();
    snapshot!(format!("{conf:#?}"), @r###"
        [
            IndexEmbeddingConfig {
                name: "manual",
                config: EmbeddingConfig {
                    embedder_options: UserProvided(
                        EmbedderOptions {
                            dimensions: 3,
                            distribution: None,
                        },
                    ),
                    prompt: PromptData {
                        template: "{% for field in fields %}{% if field.is_searchable and field.value != nil %}{{ field.name }}: {{ field.value }}\n{% endif %}{% endfor %}",
                        max_bytes: Some(
                            400,
                        ),
                    },
                    quantized: None,
                },
                user_provided: RoaringBitmap<[]>,
            },
        ]
        "###);
}

#[test]
fn delete_embedder_with_user_provided_vectors() {
    // 1. Add two embedders
    // 2. Push two documents containing a simple vector
    // 3. The documents must not contain the vectors after the update as they are in the vectors db
    // 3. Delete the embedders
    // 4. The documents contain the vectors again
    let (index_scheduler, mut handle) = IndexScheduler::test(true, vec![]);

    let setting = meilisearch_types::settings::Settings::<Unchecked> {
        embedders: Setting::Set(maplit::btreemap! {
            S("manual") => SettingEmbeddingSettings { inner: Setting::Set(EmbeddingSettings {
                source: Setting::Set(milli::vector::settings::EmbedderSource::UserProvided),
                dimensions: Setting::Set(3),
                ..Default::default()
            }) },
            S("my_doggo_embedder") => SettingEmbeddingSettings { inner: Setting::Set(EmbeddingSettings {
                source: Setting::Set(milli::vector::settings::EmbedderSource::HuggingFace),
                model: Setting::Set(S("sentence-transformers/all-MiniLM-L6-v2")),
                revision: Setting::Set(S("e4ce9877abf3edfe10b0d82785e83bdcb973e22e")),
                document_template: Setting::Set(S("{{doc.doggo}}")),
                ..Default::default()
            }) },
        }),
        ..Default::default()
    };
    index_scheduler
        .register(
            KindWithContent::SettingsUpdate {
                index_uid: S("doggos"),
                new_settings: Box::new(setting),
                is_deletion: false,
                allow_index_creation: true,
            },
            None,
            false,
        )
        .unwrap();
    handle.advance_one_successful_batch();

    let content = serde_json::json!(
        [
            {
                "id": 0,
                "doggo": "kefir",
                "_vectors": {
                    "manual": vec![0, 0, 0],
                    "my_doggo_embedder": vec![1; 384],
                }
            },
            {
                "id": 1,
                "doggo": "intel",
                "_vectors": {
                    "manual": vec![1, 1, 1],
                }
            },
        ]
    );

    let (uuid, mut file) = index_scheduler.queue.create_update_file_with_uuid(0_u128).unwrap();
    let documents_count =
        read_json(serde_json::to_string_pretty(&content).unwrap().as_bytes(), &mut file).unwrap();
    snapshot!(documents_count, @"2");
    file.persist().unwrap();

    index_scheduler
        .register(
            KindWithContent::DocumentAdditionOrUpdate {
                index_uid: S("doggos"),
                primary_key: None,
                method: ReplaceDocuments,
                content_file: uuid,
                documents_count,
                allow_index_creation: false,
            },
            None,
            false,
        )
        .unwrap();
    handle.advance_one_successful_batch();

    {
        let index = index_scheduler.index("doggos").unwrap();
        let rtxn = index.read_txn().unwrap();
        let field_ids_map = index.fields_ids_map(&rtxn).unwrap();
        let field_ids = field_ids_map.ids().collect::<Vec<_>>();
        let documents = index
            .all_documents(&rtxn)
            .unwrap()
            .map(|ret| obkv_to_json(&field_ids, &field_ids_map, ret.unwrap().1).unwrap())
            .collect::<Vec<_>>();
        snapshot!(serde_json::to_string(&documents).unwrap(), @r###"[{"id":0,"doggo":"kefir"},{"id":1,"doggo":"intel"}]"###);
    }

    {
        let setting = meilisearch_types::settings::Settings::<Unchecked> {
            embedders: Setting::Set(maplit::btreemap! {
                S("manual") => SettingEmbeddingSettings { inner: Setting::Reset },
            }),
            ..Default::default()
        };
        index_scheduler
            .register(
                KindWithContent::SettingsUpdate {
                    index_uid: S("doggos"),
                    new_settings: Box::new(setting),
                    is_deletion: false,
                    allow_index_creation: true,
                },
                None,
                false,
            )
            .unwrap();
        handle.advance_one_successful_batch();
    }

    {
        let index = index_scheduler.index("doggos").unwrap();
        let rtxn = index.read_txn().unwrap();
        let field_ids_map = index.fields_ids_map(&rtxn).unwrap();
        let field_ids = field_ids_map.ids().collect::<Vec<_>>();
        let documents = index
            .all_documents(&rtxn)
            .unwrap()
            .map(|ret| obkv_to_json(&field_ids, &field_ids_map, ret.unwrap().1).unwrap())
            .collect::<Vec<_>>();
        snapshot!(serde_json::to_string(&documents).unwrap(), @r###"[{"id":0,"doggo":"kefir","_vectors":{"manual":{"embeddings":[[0.0,0.0,0.0]],"regenerate":false}}},{"id":1,"doggo":"intel","_vectors":{"manual":{"embeddings":[[1.0,1.0,1.0]],"regenerate":false}}}]"###);
    }

    {
        let setting = meilisearch_types::settings::Settings::<Unchecked> {
            embedders: Setting::Reset,
            ..Default::default()
        };
        index_scheduler
            .register(
                KindWithContent::SettingsUpdate {
                    index_uid: S("doggos"),
                    new_settings: Box::new(setting),
                    is_deletion: false,
                    allow_index_creation: true,
                },
                None,
                false,
            )
            .unwrap();
        handle.advance_one_successful_batch();
    }

    {
        let index = index_scheduler.index("doggos").unwrap();
        let rtxn = index.read_txn().unwrap();
        let field_ids_map = index.fields_ids_map(&rtxn).unwrap();
        let field_ids = field_ids_map.ids().collect::<Vec<_>>();
        let documents = index
            .all_documents(&rtxn)
            .unwrap()
            .map(|ret| obkv_to_json(&field_ids, &field_ids_map, ret.unwrap().1).unwrap())
            .collect::<Vec<_>>();

        // FIXME: redaction
        snapshot!(json_string!(serde_json::to_string(&documents).unwrap(), { "[]._vectors.doggo_embedder.embeddings" => "[vector]" }),  @r###""[{\"id\":0,\"doggo\":\"kefir\",\"_vectors\":{\"manual\":{\"embeddings\":[[0.0,0.0,0.0]],\"regenerate\":false},\"my_doggo_embedder\":{\"embeddings\":[[1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0]],\"regenerate\":false}}},{\"id\":1,\"doggo\":\"intel\",\"_vectors\":{\"manual\":{\"embeddings\":[[1.0,1.0,1.0]],\"regenerate\":false}}}]""###);
    }
}
