use serde_json::{Map, Value};
use std::fs;
use std::fs::File;
use std::io::BufRead;
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

        let mut reader = BufReader::new(File::open(&keys_file_path)?).lines();
        while let Some(key) = reader.next().transpose()? {
            let key = serde_json::from_str(&key)?;
            store.put_api_key(key)?;
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

        let mut reader = BufReader::new(File::open(&keys_file_src)?).lines();
        while let Some(key) = reader.next().transpose()? {
            let mut key: Map<String, Value> = serde_json::from_str(&key)?;
            let uid = Uuid::new_v4().to_string();
            key.insert("uid".to_string(), Value::String(uid));
            serde_json::to_writer(&mut writer, &key)?;
            writer.write_all(b"\n")?;
        }

        Ok(())
    }
}
