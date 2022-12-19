use std::fs::File;
use std::io::{BufReader, Write};
use std::path::Path;

use serde_json::Deserializer;

use crate::{AuthController, HeedAuthStore, Result};

const KEYS_PATH: &str = "keys";

impl AuthController {
    pub fn dump(src: impl AsRef<Path>, dst: impl AsRef<Path>) -> Result<()> {
        let mut store = HeedAuthStore::new(&src)?;

        // do not attempt to close the database on drop!
        store.set_drop_on_close(false);

        let keys_file_path = dst.as_ref().join(KEYS_PATH);

        let keys = store.list_api_keys()?;
        let mut keys_file = File::create(keys_file_path)?;
        for key in keys {
            serde_json::to_writer(&mut keys_file, &key)?;
            keys_file.write_all(b"\n")?;
        }

        Ok(())
    }

    pub fn load_dump(src: impl AsRef<Path>, dst: impl AsRef<Path>) -> Result<()> {
        let store = HeedAuthStore::new(&dst)?;

        let keys_file_path = src.as_ref().join(KEYS_PATH);

        if !keys_file_path.exists() {
            return Ok(());
        }

        let reader = BufReader::new(File::open(&keys_file_path)?);
        for key in Deserializer::from_reader(reader).into_iter() {
            store.put_api_key(key?)?;
        }

        Ok(())
    }
}
