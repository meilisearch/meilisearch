use std::ops::Deref;
use std::sync::Arc;

use indexmap::IndexMap;
use meilisearch_core::{update, Database, DatabaseOptions};
use serde::Deserialize;
use serde_json::Value;
use sha2::Digest;

use crate::error::{Error, ResponseError};
use crate::index_update_callback;
use crate::option::Opt;
use crate::routes::{IndexParam, IndexUpdateResponse};

pub type Document = IndexMap<String, Value>;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct UpdateDocumentsQuery {
    primary_key: Option<String>,
}

#[derive(Clone)]
pub struct Data {
    inner: Arc<DataInner>,
}

impl Data {
    pub fn update_multiple_documents(
        &self,
        path: IndexParam,
        params: UpdateDocumentsQuery,
        body: Vec<Document>,
        is_partial: bool,
    ) -> Result<IndexUpdateResponse, ResponseError> {
        let index = self
            .db
            .open_index(&path.index_uid)
            .ok_or(Error::index_not_found(&path.index_uid))?;

        let reader = self.db.main_read_txn()?;

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

            self.db.main_write(|w| index.main.put_schema(w, &schema))?;
        }

        let mut document_addition = if is_partial {
            index.documents_partial_addition()
        } else {
            index.documents_addition()
        };

        for document in body {
            document_addition.update_document(document);
        }

        let update_id = self.db.update_write(|w| document_addition.finalize(w))?;

        Ok(IndexUpdateResponse::with_id(update_id))
    }

    pub fn delete_documents(
        &self,
        path: IndexParam,
        body: Vec<Value>,
    ) -> Result<IndexUpdateResponse, ResponseError> {
        let index = self
            .db
            .open_index(&path.index_uid)
            .ok_or(Error::index_not_found(&path.index_uid))?;

        let mut documents_deletion = index.documents_deletion();

        for document_id in body {
            let document_id = update::value_to_string(&document_id);
            documents_deletion.delete_document_by_external_docid(document_id);
        }

        let update_id = self.db.update_write(|w| documents_deletion.finalize(w))?;

        Ok(IndexUpdateResponse::with_id(update_id))
    }

    pub fn clear_all_documents(
        &self,
        path: IndexParam,
    ) -> Result<IndexUpdateResponse, ResponseError> {
        let index = self
            .db
            .open_index(&path.index_uid)
            .ok_or(Error::index_not_found(&path.index_uid))?;

        let update_id = self.db.update_write(|w| index.clear_all(w))?;

        Ok(IndexUpdateResponse::with_id(update_id))
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

impl Deref for Data {
    type Target = DataInner;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

#[derive(Clone)]
pub struct DataInner {
    pub db: Arc<Database>,
    pub db_path: String,
    pub api_keys: ApiKeys,
    pub server_pid: u32,
    pub http_payload_size_limit: usize,
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
        let server_pid = std::process::id();

        let db_opt = DatabaseOptions {
            main_map_size: opt.max_mdb_size,
            update_map_size: opt.max_udb_size,
        };

        let http_payload_size_limit = opt.http_payload_size_limit;

        let db = Arc::new(Database::open_or_create(opt.db_path, db_opt)?);

        let mut api_keys = ApiKeys {
            master: opt.master_key,
            private: None,
            public: None,
        };

        api_keys.generate_missing_api_keys();

        let inner_data = DataInner {
            db: db.clone(),
            db_path,
            api_keys,
            server_pid,
            http_payload_size_limit,
        };

        let data = Data {
            inner: Arc::new(inner_data),
        };

        let callback_context = data.clone();
        db.set_update_callback(Box::new(move |index_uid, status| {
            index_update_callback(&index_uid, &callback_context, status);
        }));

        Ok(data)
    }
}
