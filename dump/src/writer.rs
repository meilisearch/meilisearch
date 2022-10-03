use std::{
    fs::{self, File},
    io::{Read, Write},
    path::PathBuf,
};

use flate2::{write::GzEncoder, Compression};
use serde::Serialize;
use tempfile::TempDir;
use time::OffsetDateTime;
use uuid::Uuid;

use crate::{Metadata, Result, CURRENT_DUMP_VERSION};

pub struct DumpWriter {
    dir: TempDir,
}

impl DumpWriter {
    pub fn new(instance_uuid: Uuid) -> Result<DumpWriter> {
        let dir = TempDir::new()?;
        fs::write(
            dir.path().join("instance-uid"),
            &instance_uuid.as_hyphenated().to_string(),
        )?;

        let metadata = Metadata {
            dump_version: CURRENT_DUMP_VERSION,
            db_version: env!("CARGO_PKG_VERSION").to_string(),
            dump_date: OffsetDateTime::now_utc(),
        };
        fs::write(
            dir.path().join("metadata.json"),
            serde_json::to_string(&metadata)?,
        )?;

        std::fs::create_dir(&dir.path().join("indexes"))?;

        Ok(DumpWriter { dir })
    }

    pub fn create_index(&self, index_name: &str) -> Result<IndexWriter> {
        IndexWriter::new(self.dir.path().join("indexes").join(index_name))
    }

    pub fn create_keys(&self) -> Result<KeyWriter> {
        KeyWriter::new(self.dir.path().to_path_buf())
    }

    pub fn create_tasks_queue(&self) -> Result<TaskWriter> {
        TaskWriter::new(self.dir.path().join("tasks"))
    }

    pub fn persist_to(self, mut writer: impl Write) -> Result<()> {
        let gz_encoder = GzEncoder::new(&mut writer, Compression::default());
        let mut tar_encoder = tar::Builder::new(gz_encoder);
        tar_encoder.append_dir_all(".", self.dir.path())?;
        let gz_encoder = tar_encoder.into_inner()?;
        gz_encoder.finish()?;
        writer.flush()?;

        Ok(())
    }
}

pub struct KeyWriter {
    file: File,
}

impl KeyWriter {
    pub(crate) fn new(path: PathBuf) -> Result<Self> {
        let file = File::create(path.join("keys.jsonl"))?;
        Ok(KeyWriter { file })
    }

    pub fn push_key(&mut self, key: impl Serialize) -> Result<()> {
        self.file.write_all(&serde_json::to_vec(&key)?)?;
        self.file.write_all(b"\n")?;
        Ok(())
    }
}

pub struct TaskWriter {
    queue: File,
    update_files: PathBuf,
}

impl TaskWriter {
    pub(crate) fn new(path: PathBuf) -> Result<Self> {
        std::fs::create_dir(&path)?;

        let queue = File::create(path.join("queue.jsonl"))?;
        let update_files = path.join("update_files");
        std::fs::create_dir(&update_files)?;

        Ok(TaskWriter {
            queue,
            update_files,
        })
    }

    /// Pushes tasks in the dump.
    /// If the tasks has an associated `update_file` it'll use the `task_id` as its name.
    pub fn push_task(
        &mut self,
        task_id: u32,
        task: impl Serialize,
        update_file: Option<impl Read>,
    ) -> Result<()> {
        self.queue.write_all(&serde_json::to_vec(&task)?)?;
        self.queue.write_all(b"\n")?;
        if let Some(mut update_file) = update_file {
            let mut file = File::create(&self.update_files.join(task_id.to_string()))?;
            std::io::copy(&mut update_file, &mut file)?;
        }
        Ok(())
    }
}

pub struct IndexWriter {
    documents: File,
    settings: File,
}

impl IndexWriter {
    pub(crate) fn new(path: PathBuf) -> Result<Self> {
        std::fs::create_dir(&path)?;

        let documents = File::create(path.join("documents.jsonl"))?;
        let settings = File::create(path.join("settings.json"))?;

        Ok(IndexWriter {
            documents,
            settings,
        })
    }

    pub fn push_document(&mut self, document: impl Serialize) -> Result<()> {
        self.documents.write_all(&serde_json::to_vec(&document)?)?;
        self.documents.write_all(b"\n")?;
        Ok(())
    }

    pub fn settings(mut self, settings: impl Serialize) -> Result<()> {
        self.settings.write_all(&serde_json::to_vec(&settings)?)?;
        Ok(())
    }
}

#[cfg(test)]
pub(crate) mod test {
    use std::{
        fmt::Write,
        io::{Seek, SeekFrom},
        path::Path,
    };

    use flate2::read::GzDecoder;
    use serde_json::json;

    use super::*;

    fn create_directory_hierarchy(dir: &Path) -> String {
        let mut ret = String::new();
        writeln!(ret, ".").unwrap();
        ret.push_str(&_create_directory_hierarchy(dir, 0));
        ret
    }

    fn _create_directory_hierarchy(dir: &Path, depth: usize) -> String {
        let mut ret = String::new();

        // the entries are not guarenteed to be returned in the same order thus we need to sort them.
        let mut entries = fs::read_dir(dir)
            .unwrap()
            .collect::<std::result::Result<Vec<_>, _>>()
            .unwrap();

        // I want the directories first and then sort by name.
        entries.sort_by(|a, b| {
            let (aft, bft) = (a.file_type().unwrap(), b.file_type().unwrap());

            if aft.is_dir() && bft.is_dir() {
                a.file_name().cmp(&b.file_name())
            } else if aft.is_file() {
                std::cmp::Ordering::Greater
            } else if bft.is_file() {
                std::cmp::Ordering::Less
            } else {
                a.file_name().cmp(&b.file_name())
            }
        });

        for (idx, entry) in entries.iter().enumerate() {
            let mut ident = String::new();

            for _ in 0..depth {
                ident.push_str(&"│");
                ident.push_str(&" ".repeat(4));
            }
            if idx == entries.len() - 1 {
                ident.push_str(&"└");
            } else {
                ident.push_str(&"├");
            }
            ident.push_str(&"-".repeat(4));

            let name = entry.file_name().into_string().unwrap();
            let file_type = entry.file_type().unwrap();
            let is_dir = file_type.is_dir().then_some("/").unwrap_or("");

            assert!(!file_type.is_symlink());
            writeln!(ret, "{ident} {name}{is_dir}").unwrap();

            if file_type.is_dir() {
                ret.push_str(&_create_directory_hierarchy(&entry.path(), depth + 1));
            }
        }
        ret
    }

    #[test]
    fn test_creating_dump() {
        let instance_uid = Uuid::parse_str("9e15e977-f2ae-4761-943f-1eaf75fd736d").unwrap();
        let dump = DumpWriter::new(instance_uid.clone()).unwrap();

        // ========== Adding an index
        let documents = [
            json!({ "id": 1, "race": "golden retriever" }),
            json!({ "id": 2, "race": "bernese mountain" }),
            json!({ "id": 3, "race": "great pyrenees" }),
        ];
        let settings = json!({ "the empty setting": [], "the null setting": null, "the string setting": "hello" });
        let mut index = dump.create_index("doggos").unwrap();
        for document in &documents {
            index.push_document(document).unwrap();
        }
        index.settings(&settings).unwrap();

        // ========== pushing the task queue
        let tasks = [
            (0, json!({ "is this a good task": "yes" }), None),
            (
                1,
                json!({ "is this a good boi": "absolutely" }),
                Some(br#"{ "id": 4, "race": "leonberg" }"#),
            ),
            (
                3,
                json!({ "and finally": "one last task with a missing id in the middle" }),
                None,
            ),
        ];

        // ========== pushing the task queue
        let mut task_queue = dump.create_tasks_queue().unwrap();
        for (task_id, task, update_file) in &tasks {
            task_queue
                .push_task(*task_id, task, update_file.map(|c| c.as_slice()))
                .unwrap();
        }

        // ========== pushing the api keys
        let api_keys = [
            json!({ "one api key": 1, "for": "golden retriever" }),
            json!({ "id": 2, "race": "bernese mountain" }),
            json!({ "id": 3, "race": "great pyrenees" }),
        ];
        let mut keys = dump.create_keys().unwrap();
        for key in &api_keys {
            keys.push_key(key).unwrap();
        }

        // create the dump
        let mut file = tempfile::tempfile().unwrap();
        dump.persist_to(&mut file).unwrap();

        // ============ testing we write everything in the correct place.
        file.seek(SeekFrom::Start(0)).unwrap();
        let dump = tempfile::tempdir().unwrap();

        let gz = GzDecoder::new(&mut file);
        let mut tar = tar::Archive::new(gz);
        tar.unpack(dump.path()).unwrap();

        let dump_path = dump.path();

        // ==== checking global file hierarchy (we want to be sure there isn't too many files or too few)
        insta::assert_display_snapshot!(create_directory_hierarchy(dump_path), @r###"
        .
        ├---- indexes/
        │    └---- doggos/
        │    │    ├---- settings.json
        │    │    └---- documents.jsonl
        ├---- tasks/
        │    ├---- update_files/
        │    │    └---- 1
        │    └---- queue.jsonl
        ├---- keys.jsonl
        ├---- metadata.json
        └---- instance-uid
        "###);

        // ==== checking the top level infos

        let metadata = fs::read_to_string(dump_path.join("metadata.json")).unwrap();
        let metadata: Metadata = serde_json::from_str(&metadata).unwrap();
        insta::assert_json_snapshot!(metadata, { ".dumpDate" => "[date]" }, @r###"
        {
          "dumpVersion": "V6",
          "dbVersion": "0.29.0",
          "dumpDate": "[date]"
        }
        "###);

        assert_eq!(
            instance_uid.to_string(),
            fs::read_to_string(dump_path.join("instance-uid")).unwrap()
        );

        // ==== checking the index

        let docs = fs::read_to_string(dump_path.join("indexes/doggos/documents.jsonl")).unwrap();
        for (document, expected) in docs.lines().zip(documents) {
            assert_eq!(document, serde_json::to_string(&expected).unwrap());
        }
        let test_settings =
            fs::read_to_string(dump_path.join("indexes/doggos/settings.json")).unwrap();
        assert_eq!(test_settings, serde_json::to_string(&settings).unwrap());

        // ==== checking the task queue
        let tasks_queue = fs::read_to_string(dump_path.join("tasks/queue.jsonl")).unwrap();
        for (task, expected) in tasks_queue.lines().zip(tasks) {
            assert_eq!(task, serde_json::to_string(&expected.1).unwrap());
            if let Some(expected_update) = expected.2 {
                let path = dump_path.join(format!("tasks/update_files/{}", expected.0));
                println!("trying to open {}", path.display());
                let update = fs::read(path).unwrap();
                assert_eq!(update, expected_update);
            }
        }

        // ==== checking the keys

        let keys = fs::read_to_string(dump_path.join("keys.jsonl")).unwrap();
        for (key, expected) in keys.lines().zip(api_keys) {
            assert_eq!(key, serde_json::to_string(&expected).unwrap());
        }
    }
}
