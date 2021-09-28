use std::time::Duration;

use crate::common::server::default_settings;
use crate::common::GetAllDocumentsOptions;
use crate::common::Server;
use tokio::time::sleep;

use meilisearch_http::Opt;

#[actix_rt::test]
async fn perform_snapshot() {
    let temp = tempfile::tempdir().unwrap();
    let snapshot_dir = tempfile::tempdir().unwrap();

    let options = Opt {
        snapshot_dir: snapshot_dir.path().to_owned(),
        snapshot_interval_sec: 1,
        schedule_snapshot: true,
        ..default_settings(temp.path())
    };

    let server = Server::new_with_options(options).await;
    let index = server.index("test");
    index.load_test_set().await;

    let (response, _) = index
        .get_all_documents(GetAllDocumentsOptions::default())
        .await;

    sleep(Duration::from_secs(2)).await;

    let temp = tempfile::tempdir().unwrap();

    let snapshot_path = snapshot_dir
        .path()
        .to_owned()
        .join("db.snapshot".to_string());

    let options = Opt {
        import_snapshot: Some(snapshot_path),
        ..default_settings(temp.path())
    };

    let server = Server::new_with_options(options).await;
    let index = server.index("test");

    let (response_from_snapshot, _) = index
        .get_all_documents(GetAllDocumentsOptions::default())
        .await;

    assert_eq!(response, response_from_snapshot);
}
