use std::collections::HashMap;
use std::ops::Deref;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use chrono::{DateTime, Utc};
use heed::types::{SerdeBincode, Str};
use log::*;
use meilidb_core::{Database, MResult};
use sysinfo::Pid;

use crate::option::Opt;
use crate::routes::index::index_update_callback;

pub type FreqsMap = HashMap<String, usize>;
type SerdeFreqsMap = SerdeBincode<FreqsMap>;
type SerdeDatetime = SerdeBincode<DateTime<Utc>>;

#[derive(Clone)]
pub struct Data {
    inner: Arc<DataInner>,
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
    pub admin_token: Option<String>,
    pub server_pid: Pid,
    pub accept_updates: Arc<AtomicBool>,
}

impl DataInner {
    pub fn is_indexing(&self, reader: &heed::RoTxn, index: &str) -> MResult<Option<bool>> {
        match self.db.open_index(&index) {
            Some(index) => index.current_update_id(&reader).map(|u| Some(u.is_some())),
            None => Ok(None),
        }
    }

    pub fn last_update(
        &self,
        reader: &heed::RoTxn,
        index_name: &str,
    ) -> MResult<Option<DateTime<Utc>>> {
        let key = format!("last-update-{}", index_name);
        match self
            .db
            .common_store()
            .get::<Str, SerdeDatetime>(&reader, &key)?
        {
            Some(datetime) => Ok(Some(datetime)),
            None => Ok(None),
        }
    }

    pub fn set_last_update(&self, writer: &mut heed::RwTxn, index_name: &str) -> MResult<()> {
        let key = format!("last-update-{}", index_name);
        self.db
            .common_store()
            .put::<Str, SerdeDatetime>(writer, &key, &Utc::now())
            .map_err(Into::into)
    }

    pub fn last_backup(&self, reader: &heed::RoTxn) -> MResult<Option<DateTime<Utc>>> {
        match self
            .db
            .common_store()
            .get::<Str, SerdeDatetime>(&reader, "last-backup")?
        {
            Some(datetime) => Ok(Some(datetime)),
            None => Ok(None),
        }
    }

    pub fn set_last_backup(&self, writer: &mut heed::RwTxn) -> MResult<()> {
        self.db
            .common_store()
            .put::<Str, SerdeDatetime>(writer, "last-backup", &Utc::now())?;

        Ok(())
    }

    pub fn fields_frequency(
        &self,
        reader: &heed::RoTxn,
        index_name: &str,
    ) -> MResult<Option<FreqsMap>> {
        let key = format!("fields-frequency-{}", index_name);
        match self
            .db
            .common_store()
            .get::<Str, SerdeFreqsMap>(&reader, &key)?
        {
            Some(freqs) => Ok(Some(freqs)),
            None => Ok(None),
        }
    }

    pub fn compute_stats(&self, writer: &mut heed::RwTxn, index_name: &str) -> MResult<()> {
        let index = match self.db.open_index(&index_name) {
            Some(index) => index,
            None => {
                error!("Impossible to retrieve index {}", index_name);
                return Ok(());
            }
        };

        let schema = match index.main.schema(&writer)? {
            Some(schema) => schema,
            None => return Ok(()),
        };

        let all_documents_fields = index
            .documents_fields_counts
            .all_documents_fields_counts(&writer)?;

        // count fields frequencies
        let mut fields_frequency = HashMap::<_, usize>::new();
        for result in all_documents_fields {
            let (_, attr, _) = result?;
            *fields_frequency.entry(attr).or_default() += 1;
        }

        // convert attributes to their names
        let frequency: HashMap<_, _> = fields_frequency
            .into_iter()
            .map(|(a, c)| (schema.attribute_name(a).to_owned(), c))
            .collect();

        let key = format!("fields-frequency-{}", index_name);
        self.db
            .common_store()
            .put::<Str, SerdeFreqsMap>(writer, &key, &frequency)?;

        Ok(())
    }

    pub fn stop_accept_updates(&self) {
        self.accept_updates.store(false, Ordering::Relaxed);
    }

    pub fn accept_updates(&self) -> bool {
        self.accept_updates.load(Ordering::Relaxed)
    }
}

impl Data {
    pub fn new(opt: Opt) -> Data {
        let db_path = opt.database_path.clone();
        let admin_token = opt.admin_token.clone();
        let server_pid = sysinfo::get_current_pid().unwrap();

        let db = Arc::new(Database::open_or_create(opt.database_path.clone()).unwrap());
        let accept_updates = Arc::new(AtomicBool::new(true));

        let inner_data = DataInner {
            db: db.clone(),
            db_path,
            admin_token,
            server_pid,
            accept_updates,
        };

        let data = Data {
            inner: Arc::new(inner_data),
        };

        let callback_context = data.clone();
        db.set_update_callback(Box::new(move |index_name, status| {
            index_update_callback(&index_name, &callback_context, status);
        }));

        data
    }
}
