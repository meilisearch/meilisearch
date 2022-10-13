use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

mod error;
mod reader;
mod writer;

pub use error::Error;
pub use reader::open;
pub use writer::DumpWriter;

const CURRENT_DUMP_VERSION: Version = Version::V6;

type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Metadata {
    pub dump_version: Version,
    pub db_version: String,
    #[serde(with = "time::serde::rfc3339")]
    pub dump_date: OffsetDateTime,
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct IndexMetadata {
    pub uid: String,
    pub primary_key: Option<String>,
    #[serde(with = "time::serde::rfc3339")]
    pub created_at: OffsetDateTime,
    #[serde(with = "time::serde::rfc3339")]
    pub updated_at: OffsetDateTime,
}

#[derive(Debug, PartialEq, Eq, Deserialize, Serialize)]
pub enum Version {
    V1,
    V2,
    V3,
    V4,
    V5,
    V6,
}

#[cfg(test)]
pub(crate) mod test {
    use std::{
        fs::File,
        io::{Seek, SeekFrom},
        str::FromStr,
    };

    use big_s::S;
    use maplit::btreeset;
    use meilisearch_types::keys::{Action, Key};
    use meilisearch_types::milli::{self, update::Setting};
    use meilisearch_types::settings::{Checked, Settings};
    use meilisearch_types::tasks::{Kind, Status};
    use meilisearch_types::{index_uid::IndexUid, star_or::StarOr};
    use serde_json::{json, Map, Value};
    use time::{macros::datetime, Duration};
    use uuid::Uuid;

    use crate::{
        reader::{self, Document},
        DumpWriter, IndexMetadata, Version,
    };

    pub fn create_test_instance_uid() -> Uuid {
        Uuid::parse_str("9e15e977-f2ae-4761-943f-1eaf75fd736d").unwrap()
    }

    pub fn create_test_index_metadata() -> IndexMetadata {
        IndexMetadata {
            uid: S("doggo"),
            primary_key: None,
            created_at: datetime!(2022-11-20 12:00 UTC),
            updated_at: datetime!(2022-11-21 00:00 UTC),
        }
    }

    pub fn create_test_documents() -> Vec<Map<String, Value>> {
        vec![
            json!({ "id": 1, "race": "golden retriever", "name": "paul", "age": 4 })
                .as_object()
                .unwrap()
                .clone(),
            json!({ "id": 2, "race": "bernese mountain", "name": "tamo", "age": 6 })
                .as_object()
                .unwrap()
                .clone(),
            json!({ "id": 3, "race": "great pyrenees", "name": "patou", "age": 5 })
                .as_object()
                .unwrap()
                .clone(),
        ]
    }

    pub fn create_test_settings() -> Settings<Checked> {
        let settings = Settings {
            displayed_attributes: Setting::Set(vec![S("race"), S("name")]),
            searchable_attributes: Setting::Set(vec![S("name"), S("race")]),
            filterable_attributes: Setting::Set(btreeset! { S("race"), S("age") }),
            sortable_attributes: Setting::Set(btreeset! { S("age") }),
            ranking_rules: Setting::NotSet,
            stop_words: Setting::NotSet,
            synonyms: Setting::NotSet,
            distinct_attribute: Setting::NotSet,
            typo_tolerance: Setting::NotSet,
            faceting: Setting::NotSet,
            pagination: Setting::NotSet,
            _kind: std::marker::PhantomData,
        };
        settings.check()
    }

    pub fn create_test_tasks() -> Vec<(Task, Option<Vec<Document>>)> {
        vec![
            (
                TaskView {
                    uid: 0,
                    index_uid: Some(S("doggos")),
                    status: Status::Succeeded,
                    kind: Kind::DocumentImport {
                        method: milli::update::IndexDocumentsMethod::UpdateDocuments,
                        allow_index_creation: true,
                    },
                    details: todo!(),
                    /*
                    Some(DetailsView::DocumentAddition {
                        received_documents: 10_000,
                        indexed_documents: 3,
                    }),
                    */
                    error: None,
                    duration: Some(Duration::DAY),
                    enqueued_at: datetime!(2022-11-11 0:00 UTC),
                    started_at: Some(datetime!(2022-11-20 0:00 UTC)),
                    finished_at: Some(datetime!(2022-11-21 0:00 UTC)),
                },
                None,
            ),
            (
                TaskView {
                    uid: 1,
                    index_uid: Some(S("doggos")),
                    status: Status::Enqueued,
                    kind: Kind::DocumentImport {
                        method: milli::update::IndexDocumentsMethod::UpdateDocuments,
                        allow_index_creation: true,
                    },
                    details: None,
                    error: None,
                    duration: Some(Duration::DAY),
                    enqueued_at: datetime!(2022-11-11 0:00 UTC),
                    started_at: Some(datetime!(2022-11-20 0:00 UTC)),
                    finished_at: Some(datetime!(2022-11-21 0:00 UTC)),
                },
                Some(vec![
                    json!({ "id": 4, "race": "leonberg" })
                        .as_object()
                        .unwrap()
                        .clone(),
                    json!({ "id": 5, "race": "patou" })
                        .as_object()
                        .unwrap()
                        .clone(),
                ]),
            ),
            (
                TaskView {
                    uid: 5,
                    index_uid: Some(S("doggos")),
                    status: Status::Enqueued,
                    kind: Kind::IndexDeletion,
                    details: None,
                    error: None,
                    duration: None,
                    enqueued_at: datetime!(2022-11-15 0:00 UTC),
                    started_at: None,
                    finished_at: None,
                },
                None,
            ),
        ]
    }

    pub fn create_test_api_keys() -> Vec<Key> {
        vec![
            Key {
                description: Some(S("The main key to manage all the doggos")),
                name: Some(S("doggos_key")),
                uid: Uuid::from_str("9f8a34da-b6b2-42f0-939b-dbd4c3448655").unwrap(),
                actions: vec![Action::DocumentsAll],
                indexes: vec![StarOr::Other(IndexUid::from_str("doggos").unwrap())],
                expires_at: Some(datetime!(4130-03-14 12:21 UTC)),
                created_at: datetime!(1960-11-15 0:00 UTC),
                updated_at: datetime!(2022-11-10 0:00 UTC),
            },
            Key {
                description: Some(S("The master key for everything and even the doggos")),
                name: Some(S("master_key")),
                uid: Uuid::from_str("4622f717-1c00-47bb-a494-39d76a49b591").unwrap(),
                actions: vec![Action::All],
                indexes: vec![StarOr::Star],
                expires_at: None,
                created_at: datetime!(0000-01-01 00:01 UTC),
                updated_at: datetime!(1964-05-04 17:25 UTC),
            },
            Key {
                description: Some(S("The useless key to for nothing nor the doggos")),
                name: Some(S("useless_key")),
                uid: Uuid::from_str("fb80b58b-0a34-412f-8ba7-1ce868f8ac5c").unwrap(),
                actions: vec![],
                indexes: vec![],
                expires_at: None,
                created_at: datetime!(400-02-29 0:00 UTC),
                updated_at: datetime!(1024-02-29 0:00 UTC),
            },
        ]
    }

    pub fn create_test_dump() -> File {
        let instance_uid = create_test_instance_uid();
        let dump = DumpWriter::new(instance_uid.clone()).unwrap();

        // ========== Adding an index
        let documents = create_test_documents();
        let settings = create_test_settings();

        let mut index = dump
            .create_index("doggos", &create_test_index_metadata())
            .unwrap();
        for document in &documents {
            index.push_document(document).unwrap();
        }
        index.settings(&settings).unwrap();

        // ========== pushing the task queue
        let tasks = create_test_tasks();

        let mut task_queue = dump.create_tasks_queue().unwrap();
        for (task, update_file) in &tasks {
            let mut update = task_queue.push_task(task).unwrap();
            if let Some(update_file) = update_file {
                for u in update_file {
                    update.push_document(u).unwrap();
                }
            }
        }

        // ========== pushing the api keys
        let api_keys = create_test_api_keys();

        let mut keys = dump.create_keys().unwrap();
        for key in &api_keys {
            keys.push_key(key).unwrap();
        }

        // create the dump
        let mut file = tempfile::tempfile().unwrap();
        dump.persist_to(&mut file).unwrap();
        file.seek(SeekFrom::Start(0)).unwrap();

        file
    }

    #[test]
    fn test_creating_and_read_dump() {
        let mut file = create_test_dump();
        let mut dump = reader::open(&mut file).unwrap();

        // ==== checking the top level infos
        assert_eq!(dump.version(), Version::V6);
        assert!(dump.date().is_some());
        assert_eq!(
            dump.instance_uid().unwrap().unwrap(),
            create_test_instance_uid()
        );

        // ==== checking the index
        let mut indexes = dump.indexes().unwrap();
        let mut index = indexes.next().unwrap().unwrap();
        assert!(indexes.next().is_none()); // there was only one index in the dump

        for (document, expected) in index.documents().unwrap().zip(create_test_documents()) {
            assert_eq!(document.unwrap(), expected);
        }
        assert_eq!(index.settings().unwrap(), create_test_settings());
        assert_eq!(index.metadata(), &create_test_index_metadata());

        drop(index);
        drop(indexes);

        // ==== checking the task queue
        for (task, expected) in dump.tasks().zip(create_test_tasks()) {
            let (task, content_file) = task.unwrap();
            assert_eq!(task, expected.0);

            if let Some(expected_update) = expected.1 {
                assert!(
                    content_file.is_some(),
                    "A content file was expected for the task {}.",
                    expected.0.uid
                );
                let updates = content_file
                    .unwrap()
                    .collect::<Result<Vec<_>, _>>()
                    .unwrap();
                assert_eq!(updates, expected_update);
            }
        }

        // ==== checking the keys
        for (key, expected) in dump.keys().zip(create_test_api_keys()) {
            assert_eq!(key.unwrap(), expected);
        }
    }
}
