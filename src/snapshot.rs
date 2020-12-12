use crate::Data;
use crate::error::Error;
use crate::helpers::compression;

use log::error;
use std::fs::create_dir_all;
use std::path::Path;
use std::thread;
use std::time::{Duration};
use tempfile::TempDir;

pub fn load_snapshot(
    db_path: &str,
    snapshot_path: &Path,
    ignore_snapshot_if_db_exists: bool,
    ignore_missing_snapshot: bool
) -> Result<(), Error> {
    let db_path = Path::new(db_path);

    if !db_path.exists() && snapshot_path.exists() {
        compression::from_tar_gz(snapshot_path, db_path)
    } else if db_path.exists() && !ignore_snapshot_if_db_exists {
        Err(Error::Internal(format!("database already exists at {:?}, try to delete it or rename it", db_path.canonicalize().unwrap_or(db_path.into()))))
    } else if !snapshot_path.exists() && !ignore_missing_snapshot {
        Err(Error::Internal(format!("snapshot doesn't exist at {:?}", snapshot_path.canonicalize().unwrap_or(snapshot_path.into()))))
    } else {
        Ok(())
    }
}

pub fn create_snapshot(data: &Data, snapshot_path: &Path) -> Result<(), Error> {
    let tmp_dir = TempDir::new()?;

    data.db.copy_and_compact_to_path(tmp_dir.path())?;

    compression::to_tar_gz(tmp_dir.path(), snapshot_path).map_err(|e| Error::Internal(format!("something went wrong during snapshot compression: {}", e)))
}

pub fn schedule_snapshot(data: Data, snapshot_dir: &Path, time_gap_s: u64) -> Result<(), Error> {
    if snapshot_dir.file_name().is_none() { 
        return Err(Error::Internal("invalid snapshot file path".to_string()));
    }
    let db_name = Path::new(&data.db_path).file_name().ok_or_else(|| Error::Internal("invalid database name".to_string()))?;
    create_dir_all(snapshot_dir)?;
    let snapshot_path = snapshot_dir.join(format!("{}.snapshot", db_name.to_str().unwrap_or("data.ms")));
    
    thread::spawn(move || loop { 
        if let Err(e) = create_snapshot(&data, &snapshot_path) {
            error!("Unsuccessful snapshot creation: {}", e);
        }
        thread::sleep(Duration::from_secs(time_gap_s));
    });

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::prelude::*;
    use std::fs;

    #[test]
    fn test_pack_unpack() {
        let tempdir = TempDir::new().unwrap();

        let test_dir = tempdir.path();
        let src_dir = test_dir.join("src");
        let dest_dir = test_dir.join("complex/destination/path/");
        let archive_path = test_dir.join("archive.snapshot");

        let file_1_relative = Path::new("file1.txt");
        let subdir_relative = Path::new("subdir/");
        let file_2_relative = Path::new("subdir/file2.txt");
        
        create_dir_all(src_dir.join(subdir_relative)).unwrap();
        fs::File::create(src_dir.join(file_1_relative)).unwrap().write_all(b"Hello_file_1").unwrap();
        fs::File::create(src_dir.join(file_2_relative)).unwrap().write_all(b"Hello_file_2").unwrap();

        
        assert!(compression::to_tar_gz(&src_dir, &archive_path).is_ok());
        assert!(archive_path.exists());
        assert!(load_snapshot(&dest_dir.to_str().unwrap(), &archive_path, false, false).is_ok());

        assert!(dest_dir.exists());
        assert!(dest_dir.join(file_1_relative).exists());
        assert!(dest_dir.join(subdir_relative).exists());
        assert!(dest_dir.join(file_2_relative).exists());

        let contents = fs::read_to_string(dest_dir.join(file_1_relative)).unwrap();
        assert_eq!(contents, "Hello_file_1");
    
        let contents = fs::read_to_string(dest_dir.join(file_2_relative)).unwrap();
        assert_eq!(contents, "Hello_file_2");
    }
}
