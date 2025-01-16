use meili_snap::snapshot;
use meilisearch::Opt;

use crate::common::{default_settings, Server};

#[actix_rt::test]
async fn malformed_version_file() {
    let temp = tempfile::tempdir().unwrap();
    let default_settings = default_settings(temp.path());
    let db_path = default_settings.db_path.clone();
    std::fs::create_dir_all(&db_path).unwrap();
    std::fs::write(db_path.join("VERSION"), "kefir").unwrap();
    let options = Opt { experimental_dumpless_upgrade: true, ..default_settings };
    let err = Server::new_with_options(options).await.map(|_| ()).unwrap_err();
    snapshot!(err, @"Version file is corrupted and thus Meilisearch is unable to determine the version of the database.");
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
    snapshot!(err, @"Database version 1.11.9999 is too old for the experimental dumpless upgrade feature. Please generate a dump using the v1.11.9999 and imports it in the v1.12.2");
}

#[actix_rt::test]
async fn version_requires_downgrade() {
    let temp = tempfile::tempdir().unwrap();
    let default_settings = default_settings(temp.path());
    let db_path = default_settings.db_path.clone();
    std::fs::create_dir_all(&db_path).unwrap();
    let major = meilisearch_types::versioning::VERSION_MAJOR;
    let minor = meilisearch_types::versioning::VERSION_MINOR;
    let patch = meilisearch_types::versioning::VERSION_PATCH.parse::<u32>().unwrap() + 1;
    std::fs::write(db_path.join("VERSION"), format!("{major}.{minor}.{patch}")).unwrap();
    let options = Opt { experimental_dumpless_upgrade: true, ..default_settings };
    let err = Server::new_with_options(options).await.map(|_| ()).unwrap_err();
    snapshot!(err, @"Database version 1.12.3 is higher than the binary version 1.12.2. Downgrade is not supported");
}
