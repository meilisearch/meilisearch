use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::path::PathBuf;

use flate2::write::GzEncoder;
use flate2::Compression;
use meilisearch_types::batches::Batch;
use meilisearch_types::features::{Network, RuntimeTogglableFeatures};
use meilisearch_types::keys::Key;
use meilisearch_types::settings::{Checked, Settings};
use serde_json::{Map, Value};
use tempfile::TempDir;
use time::OffsetDateTime;
use uuid::Uuid;

use crate::reader::Document;
use crate::{IndexMetadata, Metadata, Result, TaskDump, CURRENT_DUMP_VERSION};

pub struct DumpWriter {
    dir: TempDir,
}

impl DumpWriter {
    pub fn new(instance_uuid: Option<Uuid>) -> Result<DumpWriter> {
        let dir = TempDir::new()?;

        if let Some(instance_uuid) = instance_uuid {
            fs::write(
                dir.path().join("instance_uid.uuid"),
                instance_uuid.as_hyphenated().to_string(),
            )?;
        }

        let metadata = Metadata {
            dump_version: CURRENT_DUMP_VERSION,
            db_version: env!("CARGO_PKG_VERSION").to_string(),
            dump_date: OffsetDateTime::now_utc(),
        };
        fs::write(dir.path().join("metadata.json"), serde_json::to_string(&metadata)?)?;

        std::fs::create_dir(dir.path().join("indexes"))?;

        Ok(DumpWriter { dir })
    }

    pub fn create_index(&self, index_name: &str, metadata: &IndexMetadata) -> Result<IndexWriter> {
        IndexWriter::new(self.dir.path().join("indexes").join(index_name), metadata)
    }

    pub fn create_keys(&self) -> Result<KeyWriter> {
        KeyWriter::new(self.dir.path().to_path_buf())
    }

    pub fn create_tasks_queue(&self) -> Result<TaskWriter> {
        TaskWriter::new(self.dir.path().join("tasks"))
    }

    pub fn create_batches_queue(&self) -> Result<BatchWriter> {
        BatchWriter::new(self.dir.path().join("batches"))
    }

    pub fn create_experimental_features(&self, features: RuntimeTogglableFeatures) -> Result<()> {
        Ok(std::fs::write(
            self.dir.path().join("experimental-features.json"),
            serde_json::to_string(&features)?,
        )?)
    }

    pub fn create_network(&self, network: Network) -> Result<()> {
        Ok(std::fs::write(self.dir.path().join("network.json"), serde_json::to_string(&network)?)?)
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
    keys: BufWriter<File>,
}

impl KeyWriter {
    pub(crate) fn new(path: PathBuf) -> Result<Self> {
        let keys = File::create(path.join("keys.jsonl"))?;
        Ok(KeyWriter { keys: BufWriter::new(keys) })
    }

    pub fn push_key(&mut self, key: &Key) -> Result<()> {
        serde_json::to_writer(&mut self.keys, &key)?;
        self.keys.write_all(b"\n")?;
        Ok(())
    }

    pub fn flush(mut self) -> Result<()> {
        self.keys.flush()?;
        Ok(())
    }
}

pub struct TaskWriter {
    queue: BufWriter<File>,
    update_files: PathBuf,
}

impl TaskWriter {
    pub(crate) fn new(path: PathBuf) -> Result<Self> {
        std::fs::create_dir(&path)?;

        let queue = File::create(path.join("queue.jsonl"))?;
        let update_files = path.join("update_files");
        std::fs::create_dir(&update_files)?;

        Ok(TaskWriter { queue: BufWriter::new(queue), update_files })
    }

    /// Pushes tasks in the dump.
    /// If the tasks has an associated `update_file` it'll use the `task_id` as its name.
    pub fn push_task(&mut self, task: &TaskDump) -> Result<UpdateFile> {
        serde_json::to_writer(&mut self.queue, &task)?;
        self.queue.write_all(b"\n")?;

        Ok(UpdateFile::new(self.update_files.join(format!("{}.jsonl", task.uid))))
    }

    pub fn flush(mut self) -> Result<()> {
        self.queue.flush()?;
        Ok(())
    }
}

pub struct BatchWriter {
    queue: BufWriter<File>,
}

impl BatchWriter {
    pub(crate) fn new(path: PathBuf) -> Result<Self> {
        std::fs::create_dir(&path)?;
        let queue = File::create(path.join("queue.jsonl"))?;
        Ok(BatchWriter { queue: BufWriter::new(queue) })
    }

    /// Pushes batches in the dump.
    pub fn push_batch(&mut self, batch: &Batch) -> Result<()> {
        serde_json::to_writer(&mut self.queue, &batch)?;
        self.queue.write_all(b"\n")?;
        Ok(())
    }

    pub fn flush(mut self) -> Result<()> {
        self.queue.flush()?;
        Ok(())
    }
}

pub struct UpdateFile {
    path: PathBuf,
    writer: Option<BufWriter<File>>,
}

impl UpdateFile {
    pub(crate) fn new(path: PathBuf) -> UpdateFile {
        UpdateFile { path, writer: None }
    }

    pub fn push_document(&mut self, document: &Document) -> Result<()> {
        if let Some(mut writer) = self.writer.as_mut() {
            serde_json::to_writer(&mut writer, &document)?;
            writer.write_all(b"\n")?;
        } else {
            let file = File::create(&self.path).unwrap();
            self.writer = Some(BufWriter::new(file));
            self.push_document(document)?;
        }
        Ok(())
    }

    pub fn flush(self) -> Result<()> {
        if let Some(mut writer) = self.writer {
            writer.flush()?;
        }
        Ok(())
    }
}

pub struct IndexWriter {
    documents: BufWriter<File>,
    settings: File,
}

impl IndexWriter {
    pub(self) fn new(path: PathBuf, metadata: &IndexMetadata) -> Result<Self> {
        std::fs::create_dir(&path)?;

        let metadata_file = File::create(path.join("metadata.json"))?;
        serde_json::to_writer(metadata_file, metadata)?;

        let documents = File::create(path.join("documents.jsonl"))?;
        let settings = File::create(path.join("settings.json"))?;

        Ok(IndexWriter { documents: BufWriter::new(documents), settings })
    }

    pub fn push_document(&mut self, document: &Map<String, Value>) -> Result<()> {
        serde_json::to_writer(&mut self.documents, document)?;
        self.documents.write_all(b"\n")?;
        Ok(())
    }

    pub fn flush(&mut self) -> Result<()> {
        self.documents.flush()?;
        Ok(())
    }

    pub fn settings(mut self, settings: &Settings<Checked>) -> Result<()> {
        self.settings.write_all(&serde_json::to_vec(&settings)?)?;
        Ok(())
    }
}

#[cfg(test)]
pub(crate) mod test {
    use std::fmt::Write;
    use std::io::BufReader;
    use std::path::Path;
    use std::str::FromStr;

    use flate2::bufread::GzDecoder;
    use meili_snap::insta;
    use meilisearch_types::settings::Unchecked;

    use super::*;
    use crate::reader::Document;
    use crate::test::{
        create_test_api_keys, create_test_batches, create_test_documents, create_test_dump,
        create_test_instance_uid, create_test_settings, create_test_tasks,
    };

    fn create_directory_hierarchy(dir: &Path) -> String {
        let mut ret = String::new();
        writeln!(ret, ".").unwrap();
        ret.push_str(&_create_directory_hierarchy(dir, 0));
        ret
    }

    fn _create_directory_hierarchy(dir: &Path, depth: usize) -> String {
        let mut ret = String::new();

        // the entries are not guaranteed to be returned in the same order thus we need to sort them.
        let mut entries =
            fs::read_dir(dir).unwrap().collect::<std::result::Result<Vec<_>, _>>().unwrap();

        // I want the directories first and then sort by name.
        entries.sort_by(|a, b| {
            let (aft, bft) = (a.file_type().unwrap(), b.file_type().unwrap());

            if aft.is_dir() && bft.is_dir() {
                a.file_name().cmp(&b.file_name())
            } else if aft.is_file() && bft.is_dir() {
                std::cmp::Ordering::Greater
            } else if bft.is_file() && aft.is_dir() {
                std::cmp::Ordering::Less
            } else {
                a.file_name().cmp(&b.file_name())
            }
        });

        for (idx, entry) in entries.iter().enumerate() {
            let mut ident = String::new();

            for _ in 0..depth {
                ident.push('│');
                ident.push_str(&" ".repeat(4));
            }
            if idx == entries.len() - 1 {
                ident.push('└');
            } else {
                ident.push('├');
            }
            ident.push_str(&"-".repeat(4));

            let name = entry.file_name().into_string().unwrap();
            let file_type = entry.file_type().unwrap();
            let is_dir = if file_type.is_dir() { "/" } else { "" };

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
        let file = create_test_dump();
        let mut file = BufReader::new(file);

        // ============ ensuring we wrote everything in the correct place.
        let dump = tempfile::tempdir().unwrap();

        let gz = GzDecoder::new(&mut file);
        let mut tar = tar::Archive::new(gz);
        tar.unpack(dump.path()).unwrap();

        let dump_path = dump.path();

        // ==== checking global file hierarchy (we want to be sure there isn't too many files or too few)
        insta::assert_snapshot!(create_directory_hierarchy(dump_path), @r"
        .
        ├---- batches/
        │    └---- queue.jsonl
        ├---- indexes/
        │    └---- doggos/
        │    │    ├---- documents.jsonl
        │    │    ├---- metadata.json
        │    │    └---- settings.json
        ├---- tasks/
        │    ├---- update_files/
        │    │    └---- 1.jsonl
        │    └---- queue.jsonl
        ├---- experimental-features.json
        ├---- instance_uid.uuid
        ├---- keys.jsonl
        ├---- metadata.json
        └---- network.json
        ");

        // ==== checking the top level infos
        let metadata = fs::read_to_string(dump_path.join("metadata.json")).unwrap();
        let metadata: Metadata = serde_json::from_str(&metadata).unwrap();
        insta::assert_json_snapshot!(metadata, { ".dumpDate" => "[date]", ".dbVersion" => "[version]" }, @r###"
        {
          "dumpVersion": "V6",
          "dbVersion": "[version]",
          "dumpDate": "[date]"
        }
        "###);

        let instance_uid = fs::read_to_string(dump_path.join("instance_uid.uuid")).unwrap();
        assert_eq!(Uuid::from_str(&instance_uid).unwrap(), create_test_instance_uid());

        // ==== checking the index
        let docs = fs::read_to_string(dump_path.join("indexes/doggos/documents.jsonl")).unwrap();
        for (document, expected) in docs.lines().zip(create_test_documents()) {
            assert_eq!(serde_json::from_str::<Map<String, Value>>(document).unwrap(), expected);
        }
        let test_settings =
            fs::read_to_string(dump_path.join("indexes/doggos/settings.json")).unwrap();
        assert_eq!(
            serde_json::from_str::<Settings<Unchecked>>(&test_settings).unwrap(),
            create_test_settings().into_unchecked()
        );
        let metadata = fs::read_to_string(dump_path.join("indexes/doggos/metadata.json")).unwrap();
        let metadata: IndexMetadata = serde_json::from_str(&metadata).unwrap();
        insta::assert_json_snapshot!(metadata, { ".createdAt" => "[date]", ".updatedAt" => "[date]" }, @r###"
        {
          "uid": "doggo",
          "primaryKey": null,
          "createdAt": "[date]",
          "updatedAt": "[date]"
        }
        "###);

        // ==== checking the task queue
        let tasks_queue = fs::read_to_string(dump_path.join("tasks/queue.jsonl")).unwrap();
        for (task, expected) in tasks_queue.lines().zip(create_test_tasks()) {
            assert_eq!(serde_json::from_str::<TaskDump>(task).unwrap(), expected.0);

            if let Some(expected_update) = expected.1 {
                let path = dump_path.join(format!("tasks/update_files/{}.jsonl", expected.0.uid));
                println!("trying to open {}", path.display());
                let update = fs::read_to_string(path).unwrap();
                let documents: Vec<Document> =
                    update.lines().map(|line| serde_json::from_str(line).unwrap()).collect();
                assert_eq!(documents, expected_update);
            }
        }

        // ==== checking the batch queue
        let batches_queue = fs::read_to_string(dump_path.join("batches/queue.jsonl")).unwrap();
        for (batch, expected) in batches_queue.lines().zip(create_test_batches()) {
            let mut batch = serde_json::from_str::<Batch>(batch).unwrap();
            if batch.details.settings == Some(Box::new(Settings::<Unchecked>::default())) {
                batch.details.settings = None;
            }
            assert_eq!(batch, expected, "{batch:#?}{expected:#?}");
        }

        // ==== checking the keys
        let keys = fs::read_to_string(dump_path.join("keys.jsonl")).unwrap();
        for (key, expected) in keys.lines().zip(create_test_api_keys()) {
            assert_eq!(serde_json::from_str::<Key>(key).unwrap(), expected);
        }
    }
}
