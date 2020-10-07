use std::ops::Deref;
use std::path::PathBuf;
use std::sync::Arc;
use std::collections::{HashSet, BTreeSet, BTreeMap};

use arc_swap::ArcSwap;
use chrono::{DateTime, Utc};
use indexmap::IndexMap;
use log::error;
use meilisearch_core::settings::{SettingsUpdate, Settings, DEFAULT_RANKING_RULES};
use meilisearch_core::update::UpdateStatus;
use meilisearch_core::{update, Database, DatabaseOptions, MainReader, UpdateReader};
use meilisearch_schema::Schema;
use rand::seq::SliceRandom;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::Digest;

use crate::error::{Error, ResponseError};
use crate::index_update_callback;
use crate::option::Opt;

pub type Document = IndexMap<String, Value>;

#[derive(Deserialize, Serialize, Clone, Debug)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct UpdateDocumentsQuery {
    primary_key: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct IndexUpdateResponse {
    pub update_id: u64,
}

impl IndexUpdateResponse {
    pub fn with_id(update_id: u64) -> Self {
        Self { update_id }
    }
}

#[derive(Deserialize)]
pub struct IndexParam {
    pub index_uid: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct IndexResponse {
    pub name: String,
    pub uid: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub primary_key: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct IndexCreateRequest {
    pub name: Option<String>,
    pub uid: Option<String>,
    pub primary_key: Option<String>,
}

#[derive(Clone)]
pub struct Data {
    inner: Arc<DataInner>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct UpdateIndexRequest {
    name: Option<String>,
    primary_key: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct UpdateIndexResponse {
    name: String,
    uid: String,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
    primary_key: Option<String>,
}

impl Data {
    pub fn update_multiple_documents(
        &self,
        index_uid: &str,
        params: UpdateDocumentsQuery,
        body: Vec<Document>,
        is_partial: bool,
    ) -> Result<IndexUpdateResponse, ResponseError> {
        let index = self
            .db
            .load()
            .open_index(index_uid)
            .ok_or(Error::index_not_found(index_uid))?;

        let reader = self.db.load().main_read_txn()?;

        let mut schema = index
            .main
            .schema(&reader)?
            .ok_or(meilisearch_core::Error::SchemaMissing)?;

        if schema.primary_key().is_none() {
            let id = match &params.primary_key {
                Some(id) => id.to_string(),
                None => body
                    .first()
                    .and_then(find_primary_key)
                    .ok_or(meilisearch_core::Error::MissingPrimaryKey)?,
            };

            schema.set_primary_key(&id).map_err(Error::bad_request)?;

            self.db.load().main_write(|w| index.main.put_schema(w, &schema))?;
        }

        let mut document_addition = if is_partial {
            index.documents_partial_addition()
        } else {
            index.documents_addition()
        };

        for document in body {
            document_addition.update_document(document);
        }

        let update_id = self.db.load().update_write(|w| document_addition.finalize(w))?;

        Ok(IndexUpdateResponse::with_id(update_id))
    }

    pub fn delete_documents(
        &self,
        index_uid: &str,
        body: Vec<Value>,
    ) -> Result<IndexUpdateResponse, ResponseError> {
        let index = self
            .db
            .load()
            .open_index(index_uid)
            .ok_or(Error::index_not_found(index_uid))?;

        let mut documents_deletion = index.documents_deletion();

        for document_id in body {
            let document_id = update::value_to_string(&document_id);
            documents_deletion.delete_document_by_external_docid(document_id);
        }

        let update_id = self.db.load().update_write(|w| documents_deletion.finalize(w))?;

        Ok(IndexUpdateResponse::with_id(update_id))
    }

    pub fn clear_all_documents(
        &self,
        index_uid: &str,
    ) -> Result<IndexUpdateResponse, ResponseError> {
        let index = self
            .db
            .load()
            .open_index(index_uid)
            .ok_or(Error::index_not_found(index_uid))?;

        let update_id = self.db.load().update_write(|w| index.clear_all(w))?;

        Ok(IndexUpdateResponse::with_id(update_id))
    }

    pub fn get_all_documents_sync(
        &self,
        reader: &MainReader,
        index_uid: &str,
        offset: usize,
        limit: usize,
        attributes_to_retrieve: Option<&String>
    ) -> Result<Vec<Document>, Error> {
        let index = self
            .db
            .load()
            .open_index(index_uid)
            .ok_or(Error::index_not_found(index_uid))?;

        let documents_ids: Result<BTreeSet<_>, _> = index
            .documents_fields_counts
            .documents_ids(reader)?
            .skip(offset)
            .take(limit)
            .collect();

        let attributes: Option<HashSet<&str>> = attributes_to_retrieve
            .map(|a| a.split(',').collect());

        let mut documents = Vec::new();
        for document_id in documents_ids? {
            if let Ok(Some(document)) =
                index.document::<Document>(reader, attributes.as_ref(), document_id)
            {
                documents.push(document);
            }
        }

        Ok(documents)
    }

    pub fn create_index(
        &self,
        index_info: &IndexCreateRequest,
    ) -> Result<IndexResponse, ResponseError> {
        if let (None, None) = (index_info.name.clone(), index_info.uid.clone()) {
            return Err(Error::bad_request("Index creation must have an uid").into());
        }

        let uid = match &index_info.uid {
            Some(uid) => {
                if uid
                    .chars()
                        .all(|x| x.is_ascii_alphanumeric() || x == '-' || x == '_')
                {
                    uid.to_owned()
                } else {
                    return Err(Error::InvalidIndexUid.into());
                }
            }
            None => loop {
                let uid = generate_uid();
                if self.db.load().open_index(&uid).is_none() {
                    break uid;
                }
            },
        };

        let created_index = self.db.load().create_index(&uid).map_err(|e| match e {
            meilisearch_core::Error::IndexAlreadyExists => e.into(),
            _ => ResponseError::from(Error::create_index(e)),
        })?;

        let index_response = self.db.load().main_write::<_, _, ResponseError>(|mut writer| {
            let name = index_info.name.as_ref().unwrap_or(&uid);
            created_index.main.put_name(&mut writer, name)?;

            let created_at = created_index
                .main
                .created_at(&writer)?
                .ok_or(Error::internal("Impossible to read created at"))?;

            let updated_at = created_index
                .main
                .updated_at(&writer)?
                .ok_or(Error::internal("Impossible to read updated at"))?;

            if let Some(id) = index_info.primary_key.clone() {
                if let Some(mut schema) = created_index.main.schema(&writer)? {
                    schema.set_primary_key(&id).map_err(Error::bad_request)?;
                    created_index.main.put_schema(&mut writer, &schema)?;
                }
            }
            let index_response = IndexResponse {
                name: name.to_string(),
                uid,
                created_at,
                updated_at,
                primary_key: index_info.primary_key.clone(),
            };
            Ok(index_response)
        })?;

        Ok(index_response)
    }


    pub fn list_indexes_sync(&self, reader: &MainReader) -> Result<Vec<IndexResponse>, ResponseError> {
        let mut indexes = Vec::new();

        for index_uid in self.db.load().indexes_uids() {
            let index = self.db.load().open_index(&index_uid);

            match index {
                Some(index) => {
                    let name = index.main.name(reader)?.ok_or(Error::internal(
                            "Impossible to get the name of an index",
                    ))?;
                    let created_at = index
                        .main
                        .created_at(reader)?
                        .ok_or(Error::internal(
                                "Impossible to get the create date of an index",
                        ))?;
                    let updated_at = index
                        .main
                        .updated_at(reader)?
                        .ok_or(Error::internal(
                                "Impossible to get the last update date of an index",
                        ))?;

                    let primary_key = match index.main.schema(reader) {
                        Ok(Some(schema)) => match schema.primary_key() {
                            Some(primary_key) => Some(primary_key.to_owned()),
                            None => None,
                        },
                        _ => None,
                    };

                    let index_response = IndexResponse {
                        name,
                        uid: index_uid,
                        created_at,
                        updated_at,
                        primary_key,
                    };
                    indexes.push(index_response);
                }
                None => error!(
                    "Index {} is referenced in the indexes list but cannot be found",
                    index_uid
                ),
            }
        }

        Ok(indexes)
    }

    pub fn update_index(
        &self,
        index_uid: &str,
        body: IndexCreateRequest,
    ) -> Result<IndexResponse, ResponseError> {
        let index = self
            .db
            .load()
            .open_index(index_uid)
            .ok_or(Error::index_not_found(index_uid))?;

        self.db.load().main_write::<_, _, ResponseError>(|writer| {
            if let Some(name) = &body.name {
                index.main.put_name(writer, name)?;
            }

            if let Some(id) = body.primary_key.clone() {
                if let Some(mut schema) = index.main.schema(writer)? {
                    schema.set_primary_key(&id)?;
                    index.main.put_schema(writer, &schema)?;
                }
            }
            index.main.put_updated_at(writer)?;
            Ok(())
        })?;

        let reader = self.db.load().main_read_txn()?;
        let name = index
            .main
            .name(&reader)?
            .ok_or(Error::internal("Impossible to get the name of an index"))?;
        let created_at = index.main.created_at(&reader)?.ok_or(Error::internal(
                "Impossible to get the create date of an index",
        ))?;
        let updated_at = index.main.updated_at(&reader)?.ok_or(Error::internal(
                "Impossible to get the last update date of an index",
        ))?;

        let primary_key = match index.main.schema(&reader) {
            Ok(Some(schema)) => match schema.primary_key() {
                Some(primary_key) => Some(primary_key.to_owned()),
                None => None,
            },
            _ => None,
        };

        let index_response = IndexResponse {
            name,
            uid: index_uid.into(),
            created_at,
            updated_at,
            primary_key,
        };

        Ok(index_response)
    }

    pub fn delete_index(&self, index_uid: &str) -> Result<(), ResponseError> {
        if self.db.load().delete_index(index_uid)? {
            Ok(())
        } else {
            Err(Error::index_not_found(index_uid).into())
        }
    }

    /// updates all the settings
    pub fn update_settings(
        &self,
        index_uid: &str,
        update: SettingsUpdate,
    ) -> Result<IndexUpdateResponse, ResponseError> {
        let index = self
            .db
            .load()
            .open_index(index_uid)
            .ok_or(Error::index_not_found(index_uid))?;

        let update_id = self.db.load().update_write::<_, _, ResponseError>(|writer| {
            let update_id = index.settings_update(writer, update)?;
            Ok(update_id)
        })?;

        Ok(IndexUpdateResponse::with_id(update_id))
    }

    pub fn get_all_settings_sync(&self, index_uid: &str, reader: &MainReader) -> Result<Settings, Error> {
        let index = self
            .db
            .load()
            .open_index(index_uid)
            .ok_or(Error::index_not_found(index_uid))?;

        let stop_words: BTreeSet<String> = index.main.stop_words(&reader)?.into_iter().collect();

        let synonyms_list = index.main.synonyms(&reader)?;

        let mut synonyms = BTreeMap::new();
        let index_synonyms = &index.synonyms;
        for synonym in synonyms_list {
            let list = index_synonyms.synonyms(&reader, synonym.as_bytes())?;
            synonyms.insert(synonym, list);
        }

        let ranking_rules = index
            .main
            .ranking_rules(&reader)?
            .unwrap_or(DEFAULT_RANKING_RULES.to_vec())
            .into_iter()
            .map(|r| r.to_string())
            .collect();

        let schema = index.main.schema(&reader)?;

        let distinct_attribute = match (index.main.distinct_attribute(&reader)?, &schema) {
            (Some(id), Some(schema)) => schema.name(id).map(str::to_string),
            _ => None,
        };

        let attributes_for_faceting = match (&schema, &index.main.attributes_for_faceting(&reader)?) {
            (Some(schema), Some(attrs)) => attrs
                .iter()
                .filter_map(|&id| schema.name(id))
                .map(str::to_string)
                .collect(),
            _ => vec![],
        };

        let searchable_attributes = schema.as_ref().map(get_indexed_attributes);
        let displayed_attributes = schema.as_ref().map(get_displayed_attributes);

        let settings = Settings {
            ranking_rules: Some(Some(ranking_rules)),
            distinct_attribute: Some(distinct_attribute),
            searchable_attributes: Some(searchable_attributes),
            displayed_attributes: Some(displayed_attributes),
            stop_words: Some(Some(stop_words)),
            synonyms: Some(Some(synonyms)),
            attributes_for_faceting: Some(Some(attributes_for_faceting)),
        };
        Ok(settings)
    }

    pub fn get_all_updates_status_sync(
        &self,
        reader: &UpdateReader,
        index_uid: &str,
    ) -> Result<Vec<UpdateStatus>, Error> {
        let index = self
            .db
            .load()
            .open_index(index_uid)
            .ok_or(Error::index_not_found(index_uid))?;

        Ok(index.all_updates_status(reader)?)
    }
}

fn find_primary_key(document: &IndexMap<String, Value>) -> Option<String> {
    for key in document.keys() {
        if key.to_lowercase().contains("id") {
            return Some(key.to_string());
        }
    }
    None
}

fn generate_uid() -> String {
    let mut rng = rand::thread_rng();
    let sample = b"abcdefghijklmnopqrstuvwxyz0123456789";
    sample
        .choose_multiple(&mut rng, 8)
        .map(|c| *c as char)
        .collect()
}

impl Deref for Data {
    type Target = DataInner;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

#[derive(Clone)]
pub struct DataInner {
    pub db: ArcSwap<Database>,
    pub db_path: String,
    pub backup_folder: PathBuf,
    pub backup_batch_size: usize,
    pub api_keys: ApiKeys,
    pub server_pid: u32,
    pub http_payload_size_limit: usize,
    pub opt: Opt,
}

#[derive(Clone)]
pub struct ApiKeys {
    pub public: Option<String>,
    pub private: Option<String>,
    pub master: Option<String>,
}

impl ApiKeys {
    pub fn generate_missing_api_keys(&mut self) {
        if let Some(master_key) = &self.master {
            if self.private.is_none() {
                let key = format!("{}-private", master_key);
                let sha = sha2::Sha256::digest(key.as_bytes());
                self.private = Some(format!("{:x}", sha));
            }
            if self.public.is_none() {
                let key = format!("{}-public", master_key);
                let sha = sha2::Sha256::digest(key.as_bytes());
                self.public = Some(format!("{:x}", sha));
            }
        }
    }
}

impl Data {
    pub fn new(opt: Opt) -> Result<Data, Box<dyn std::error::Error>> {
        let db_path = opt.db_path.clone();
        let backup_folder = opt.backup_folder.clone();
        let backup_batch_size = opt.backup_batch_size;
        let server_pid = std::process::id();

        let db_opt = DatabaseOptions {
            main_map_size: opt.max_mdb_size,
            update_map_size: opt.max_udb_size,
        };

        let http_payload_size_limit = opt.http_payload_size_limit;

        let db = ArcSwap::from(Arc::new(Database::open_or_create(opt.db_path.clone(), db_opt)?));

        let mut api_keys = ApiKeys {
            master: opt.master_key.clone(),
            private: None,
            public: None,
        };

        api_keys.generate_missing_api_keys();

        let inner_data = DataInner {
            db: db.clone(),
            db_path,
            backup_folder,
            backup_batch_size,
            api_keys,
            server_pid,
            http_payload_size_limit,
            opt,
        };

        let data = Data {
            inner: Arc::new(inner_data),
        };

        let callback_context = data.clone();
        db.load().set_update_callback(Box::new(move |index_uid, status| {
            index_update_callback(&index_uid, &callback_context, status);
        }));

        Ok(data)
    }
}

pub fn get_displayed_attributes(schema: &Schema) -> HashSet<String> {
    if schema.is_displayed_all() {
        ["*"].iter().map(|s| s.to_string()).collect()
    } else {
        schema
            .displayed_name()
            .iter()
            .map(|s| s.to_string())
            .collect()
    }
}

pub fn get_indexed_attributes(schema: &Schema) -> Vec<String> {
    if schema.is_indexed_all() {
        ["*"].iter().map(|s| s.to_string()).collect()
    } else {
        schema
            .indexed_name()
            .iter()
            .map(|s| s.to_string())
            .collect()
    }
}
