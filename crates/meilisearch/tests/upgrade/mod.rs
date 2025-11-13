mod v1_12;

use std::path::Path;
use std::{fs, io};

use meili_snap::snapshot;
use meilisearch::Opt;

use crate::common::{default_settings, Server};

fn copy_dir_all(src: impl AsRef<Path>, dst: impl AsRef<Path>) -> io::Result<()> {
    fs::create_dir_all(&dst)?;
    for entry in fs::read_dir(src)? {
        let entry = entry?;
        let ty = entry.file_type()?;
        if ty.is_dir() {
            copy_dir_all(entry.path(), dst.as_ref().join(entry.file_name()))?;
        } else {
            fs::copy(entry.path(), dst.as_ref().join(entry.file_name()))?;
        }
    }
    Ok(())
}

#[actix_rt::test]
async fn malformed_version_file() {
    let temp = tempfile::tempdir().unwrap();
    let default_settings = default_settings(temp.path());
    let db_path = default_settings.db_path.clone();
    std::fs::create_dir_all(&db_path).unwrap();
    std::fs::write(db_path.join("VERSION"), "kefir").unwrap();
    let options = Opt { experimental_dumpless_upgrade: true, ..default_settings };
    let err = Server::new_with_options(options).await.map(|_| ()).unwrap_err();
    snapshot!(err, @"Version file is corrupted and thus Meilisearch is unable to determine the version of the database. The version contains 1 parts instead of 3 (major, minor and patch)");
}

#[actix_rt::test]
async fn version_too_old() {
    let temp = tempfile::tempdir().unwrap();
    let default_settings = default_settings(temp.path());
    let db_path = default_settings.db_path.clone();
    std::fs::create_dir_all(&db_path).unwrap();
    std::fs::write(db_path.join("VERSION"), "1.11.9999").unwrap();
    let options = Opt { experimental_dumpless_upgrade: true, ..default_settings };
    let err = Server::new_with_options(options).await.map(|_| ()).unwrap_err();
    snapshot!(err, @"Database version 1.11.9999 is too old for the experimental dumpless upgrade feature. Please generate a dump using the v1.11.9999 and import it in the v1.26.0");
}

#[actix_rt::test]
async fn version_requires_downgrade() {
    let temp = tempfile::tempdir().unwrap();
    let default_settings = default_settings(temp.path());
    let db_path = default_settings.db_path.clone();
    std::fs::create_dir_all(&db_path).unwrap();
    let major = meilisearch_types::versioning::VERSION_MAJOR;
    let minor = meilisearch_types::versioning::VERSION_MINOR;
    let patch = meilisearch_types::versioning::VERSION_PATCH + 1;
    std::fs::write(db_path.join("VERSION"), format!("{major}.{minor}.{patch}")).unwrap();
    let options = Opt { experimental_dumpless_upgrade: true, ..default_settings };
    let err = Server::new_with_options(options).await.map(|_| ()).unwrap_err();
    snapshot!(err, @"Database version 1.26.1 is higher than the Meilisearch version 1.26.0. Downgrade is not supported");
}

#[actix_rt::test]
async fn upgrade_to_the_current_version() {
    let temp = tempfile::tempdir().unwrap();
    let server = Server::new_with_options(Opt {
        experimental_dumpless_upgrade: true,
        ..default_settings(temp.path())
    })
    .await
    .unwrap();
    // The upgrade tasks should NOT be spawned => task queue is empty
    let (tasks, _status) = server.tasks().await;
    snapshot!(tasks, @r#"
    {
      "results": [],
      "total": 0,
      "limit": 20,
      "from": null,
      "next": null
    }
    "#);
}
