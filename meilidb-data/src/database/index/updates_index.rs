use std::convert::TryInto;
use std::sync::Arc;
use std::thread;

use log::info;
use sled::Event;
use serde::{Serialize, Deserialize};

use super::Error;
use crate::database::{
    DocumentsAddition, DocumentsDeletion, SynonymsAddition, SynonymsDeletion
};

fn event_is_set(event: &Event) -> bool {
    match event {
        Event::Set(_, _) => true,
        _ => false,
    }
}

#[derive(Serialize, Deserialize)]
enum Update {
    DocumentsAddition( () /*DocumentsAddition*/),
    DocumentsDeletion( () /*DocumentsDeletion*/),
    SynonymsAddition( () /*SynonymsAddition*/),
    SynonymsDeletion( () /*SynonymsDeletion*/),
}

#[derive(Clone)]
pub struct UpdatesIndex {
    db: sled::Db,
    updates: Arc<sled::Tree>,
    results: Arc<sled::Tree>,
}

impl UpdatesIndex {
    pub fn new(
        db: sled::Db,
        updates: Arc<sled::Tree>,
        results: Arc<sled::Tree>,
    ) -> UpdatesIndex
    {
        let updates_clone = updates.clone();
        let results_clone = results.clone();
        let _handle = thread::spawn(move || {
            loop {
                let mut subscription = updates_clone.watch_prefix(vec![]);

                while let Some((key, update)) = updates_clone.pop_min().unwrap() {
                    let array = key.as_ref().try_into().unwrap();
                    let id = u64::from_be_bytes(array);

                    match bincode::deserialize(&update).unwrap() {
                        Update::DocumentsAddition(_) => {
                            info!("processing the document addition (update number {})", id);
                            // ...
                        },
                        Update::DocumentsDeletion(_) => {
                            info!("processing the document deletion (update number {})", id);
                            // ...
                        },
                        Update::SynonymsAddition(_) => {
                            info!("processing the synonyms addition (update number {})", id);
                            // ...
                        },
                        Update::SynonymsDeletion(_) => {
                            info!("processing the synonyms deletion (update number {})", id);
                            // ...
                        },
                    }
                }

                // this subscription is just used to block
                // the loop until a new update is inserted
                subscription.filter(event_is_set).next();
            }
        });

        UpdatesIndex { db, updates, results }
    }

    pub fn push_documents_addition(&self, addition: DocumentsAddition) -> Result<u64, Error> {
        let update = bincode::serialize(&())?;
        self.raw_push_update(update)
    }

    pub fn push_documents_deletion(&self, deletion: DocumentsDeletion) -> Result<u64, Error> {
        let update = bincode::serialize(&())?;
        self.raw_push_update(update)
    }

    pub fn push_synonyms_addition(&self, addition: SynonymsAddition) -> Result<u64, Error> {
        let update = bincode::serialize(&())?;
        self.raw_push_update(update)
    }

    pub fn push_synonyms_deletion(&self, deletion: SynonymsDeletion) -> Result<u64, Error> {
        let update = bincode::serialize(&())?;
        self.raw_push_update(update)
    }

    fn raw_push_update(&self, raw_update: Vec<u8>) -> Result<u64, Error> {
        let update_id = self.db.generate_id()?;
        let update_id_array = update_id.to_be_bytes();

        self.updates.insert(update_id_array, raw_update)?;

        Ok(update_id)
    }
}
