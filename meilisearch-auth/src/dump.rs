use std::fs::File;
use std::io::BufRead;
use std::io::BufReader;
use std::io::Write;
use std::path::Path;

use crate::{AuthController, HeedAuthStore, Result};

const KEYS_PATH: &str = "keys";

impl AuthController {
    pub fn dump(src: impl AsRef<Path>, dst: impl AsRef<Path>) -> Result<()> {
        let store = HeedAuthStore::new(&src)?;

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

        let mut reader = BufReader::new(File::open(&keys_file_path)?).lines();
        while let Some(key) = reader.next().transpose()? {
            let key = serde_json::from_str(&key)?;
            store.put_api_key(key)?;
        }

        Ok(())
    }
}
