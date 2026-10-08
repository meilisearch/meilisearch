use std::io::{Read, Write};

use tempfile::TempDir;

use super::*;

#[test]
fn all_uuids() {
    let dir = TempDir::new().unwrap();
    let fs = FileStore::new(dir.path()).unwrap();
    let (uuid, mut file) = fs.new_update().unwrap();
    file.write_all(b"Hello world").unwrap();
    file.persist().unwrap();
    let all_uuids = fs.all_uuids().unwrap().collect::<Result<Vec<_>>>().unwrap();
    assert_eq!(all_uuids, vec![uuid]);

    let (uuid2, file) = fs.new_update().unwrap();
    let all_uuids = fs.all_uuids().unwrap().collect::<Result<Vec<_>>>().unwrap();
    assert_eq!(all_uuids, vec![uuid]);

    file.persist().unwrap();
    let mut all_uuids = fs.all_uuids().unwrap().collect::<Result<Vec<_>>>().unwrap();
    all_uuids.sort();
    let mut expected = vec![uuid, uuid2];
    expected.sort();
    assert_eq!(all_uuids, expected);
}

#[test]
fn file_store_new_creates_directory() {
    let dir = TempDir::new().unwrap();
    let new_path = dir.path().join("new_directory");

    assert!(!new_path.exists());

    let _fs = FileStore::new(&new_path).unwrap();

    assert!(new_path.exists());
    assert!(new_path.is_dir());
}

#[test]
fn new_update_creates_unique_uuids() {
    let dir = TempDir::new().unwrap();
    let fs = FileStore::new(dir.path()).unwrap();

    let (uuid1, _) = fs.new_update().unwrap();
    let (uuid2, _) = fs.new_update().unwrap();

    assert_ne!(uuid1, uuid2);
}

#[test]
fn new_update_with_uuid() {
    let dir = TempDir::new().unwrap();
    let fs = FileStore::new(dir.path()).unwrap();
    let custom_uuid = 12345678901234567890123456789012u128;

    let (uuid, _) = fs.new_update_with_uuid(custom_uuid).unwrap();

    assert_eq!(uuid.as_u128(), custom_uuid);
}

#[test]
fn file_persist_and_retrieve() {
    let dir = TempDir::new().unwrap();
    let fs = FileStore::new(dir.path()).unwrap();
    let test_data = b"Test file content";

    let (uuid, mut file) = fs.new_update().unwrap();
    file.write_all(test_data).unwrap();
    file.persist().unwrap();

    let retrieved_file = fs.get_update(uuid).unwrap();
    let mut content = Vec::new();
    retrieved_file.take(1000).read_to_end(&mut content).unwrap();
    assert_eq!(content, test_data);
}

#[test]
fn file_persist_dry_file() {
    let dry_file = File::dry_file().unwrap();
    let result = dry_file.persist().unwrap();

    assert!(result.is_none());
}

#[test]
fn file_write_dry_file() {
    let mut dry_file = File::dry_file().unwrap();
    let test_data = b"Test data";

    let bytes_written = dry_file.write(test_data).unwrap();
    assert_eq!(bytes_written, test_data.len());

    dry_file.flush().unwrap();
}

#[test]
fn compute_size() {
    let dir = TempDir::new().unwrap();
    let fs = FileStore::new(dir.path()).unwrap();
    let test_data = b"Test content for size calculation";

    let (uuid, mut file) = fs.new_update().unwrap();
    file.write_all(test_data).unwrap();
    file.persist().unwrap();

    let size = fs.compute_size(uuid).unwrap();
    assert_eq!(size, test_data.len() as u64);
}

#[test]
fn compute_total_size() {
    let dir = TempDir::new().unwrap();
    let fs = FileStore::new(dir.path()).unwrap();

    let initial_size = fs.compute_total_size().unwrap();
    assert_eq!(initial_size, 0);

    let (_uuid1, mut file1) = fs.new_update().unwrap();
    file1.write_all(b"First file content").unwrap();
    file1.persist().unwrap();

    let size_after_first = fs.compute_total_size().unwrap();
    assert!(size_after_first > 0);

    let (_uuid2, mut file2) = fs.new_update().unwrap();
    file2.write_all(b"Second file content").unwrap();
    file2.persist().unwrap();

    let final_size = fs.compute_total_size().unwrap();
    assert!(final_size > size_after_first);
}

#[test]
fn delete_file() {
    let dir = TempDir::new().unwrap();
    let fs = FileStore::new(dir.path()).unwrap();
    let test_data = b"File to be deleted";

    let (uuid, mut file) = fs.new_update().unwrap();
    file.write_all(test_data).unwrap();
    file.persist().unwrap();

    assert!(fs.get_update(uuid).is_ok());

    fs.delete(uuid).unwrap();

    assert!(fs.get_update(uuid).is_err());
}

#[test]
fn get_update_path() {
    let dir = TempDir::new().unwrap();
    let fs = FileStore::new(dir.path()).unwrap();
    let uuid = Uuid::new_v4();

    let path = fs.get_update_path(uuid);
    let expected_path = dir.path().join(uuid.to_string());

    assert_eq!(path, expected_path);
}

#[test]
fn snapshot() {
    let dir = TempDir::new().unwrap();
    let fs = FileStore::new(dir.path()).unwrap();
    let test_data = b"Snapshot test data";

    let (uuid, mut file) = fs.new_update().unwrap();
    file.write_all(test_data).unwrap();
    file.persist().unwrap();

    let snapshot_dir = TempDir::new().unwrap();
    fs.snapshot(uuid, snapshot_dir.path()).unwrap();

    let snapshot_path = snapshot_dir.path().join("updates/updates_files").join(uuid.to_string());
    assert!(snapshot_path.exists());

    let mut content = Vec::new();
    std::fs::File::open(snapshot_path).unwrap().read_to_end(&mut content).unwrap();
    assert_eq!(content, test_data);
}

#[test]
fn file_from_parts_and_into_parts() {
    let path = PathBuf::from("/test/path");
    let file = NamedTempFile::new().unwrap();

    let file_struct = File::from_parts(path.clone(), Some(file));
    let (returned_path, returned_file) = file_struct.into_parts();

    assert_eq!(returned_path, path);
    assert!(returned_file.is_some());
}

#[test]
fn all_uuids_ignores_hidden_files() {
    let dir = TempDir::new().unwrap();
    let fs = FileStore::new(dir.path()).unwrap();

    let (uuid, mut file) = fs.new_update().unwrap();
    file.write_all(b"Normal file").unwrap();
    file.persist().unwrap();

    let hidden_path = dir.path().join(".hidden_file");
    std::fs::write(hidden_path, b"Hidden content").unwrap();

    let all_uuids = fs.all_uuids().unwrap().collect::<Result<Vec<_>>>().unwrap();

    assert_eq!(all_uuids.len(), 1);
    assert_eq!(all_uuids[0], uuid);
}
