use serde_json::Deserializer;
use serde_json::{Map, Value};
use std::fs;
use std::fs::File;
use std::io::BufReader;
use std::io::Write;
use std::path::Path;
use uuid::Uuid;

use crate::{AuthController, HeedAuthStore, Result};

const KEYS_PATH: &str = "keys";

impl AuthController {
    pub fn dump(src: impl AsRef<Path>, dst: impl AsRef<Path>) -> Result<()> {
        let mut store = HeedAuthStore::new(&src)?;

        // do not attempt to close the database on drop!
        store.set_drop_on_close(false);

        let keys_file_path = dst.as_ref().join(KEYS_PATH);

        let keys = store.list_api_keys()?;
        let mut keys_file = File::create(&keys_file_path)?;
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

    pub fn patch_dump_v4(src: impl AsRef<Path>, dst: impl AsRef<Path>) -> Result<()> {
        let keys_file_src = src.as_ref().join(KEYS_PATH);

        if !keys_file_src.exists() {
            return Ok(());
        }

        fs::create_dir_all(&dst)?;
        let keys_file_dst = dst.as_ref().join(KEYS_PATH);
        let mut writer = File::create(&keys_file_dst)?;

        let reader = BufReader::new(File::open(&keys_file_src)?);
        for key in Deserializer::from_reader(reader).into_iter() {
            let mut key: Map<String, Value> = key?;

            // generate a new uuid v4 and insert it in the key.
            let uid = serde_json::to_value(Uuid::new_v4()).unwrap();
            key.insert("uid".to_string(), uid);

            serde_json::to_writer(&mut writer, &key)?;
            writer.write_all(b"\n")?;
        }

        Ok(())
    }
}
