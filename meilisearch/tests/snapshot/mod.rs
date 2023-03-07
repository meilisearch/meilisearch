use std::time::Duration;

use actix_rt::time::sleep;
use meilisearch::option::ScheduleSnapshot;
use meilisearch::Opt;

use crate::common::server::default_settings;
use crate::common::{GetAllDocumentsOptions, Server};

macro_rules! verify_snapshot {
    (
        $orig:expr,
        $snapshot: expr,
        |$server:ident| =>
        $($e:expr,)+) => {
            use std::sync::Arc;
            let snapshot = Arc::new($snapshot);
            let orig = Arc::new($orig);
            $(
                {
                    let test= |$server: Arc<Server>| async move {
                        $e.await
                    };
                    let (snapshot, _) = test(snapshot.clone()).await;
                    let (orig, _) = test(orig.clone()).await;
                    assert_eq!(snapshot, orig, "Got \n{}\nWhile expecting:\n{}", serde_json::to_string_pretty(&snapshot).unwrap(), serde_json::to_string_pretty(&orig).unwrap());
                }
            )*
    };
}

#[actix_rt::test]
async fn perform_snapshot() {
    let temp = tempfile::tempdir().unwrap();
    let snapshot_dir = tempfile::tempdir().unwrap();

    let options = Opt {
        snapshot_dir: snapshot_dir.path().to_owned(),
        schedule_snapshot: ScheduleSnapshot::Enabled(2),
        ..default_settings(temp.path())
    };

    let server = Server::new_with_options(options).await.unwrap();

    let index = server.index("test");
    index
        .update_settings(serde_json::json! ({
        "searchableAttributes": [],
        }))
        .await;

    index.load_test_set().await;

    server.index("test1").create(Some("prim")).await;

    index.wait_task(2).await;

    sleep(Duration::from_secs(2)).await;

    let temp = tempfile::tempdir().unwrap();

    let snapshot_path = snapshot_dir.path().to_owned().join("db.snapshot");
    #[cfg_attr(windows, allow(unused))]
    let snapshot_meta = std::fs::metadata(&snapshot_path).unwrap();

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = snapshot_meta.permissions().mode();
        //                                                    rwxrwxrwx
        meili_snap::snapshot!(format!("{:b}", mode), @"1000000100100100");
    }

    let options = Opt { import_snapshot: Some(snapshot_path), ..default_settings(temp.path()) };

    let snapshot_server = Server::new_with_options(options).await.unwrap();

    verify_snapshot!(server, snapshot_server, |server| =>
        server.list_indexes(None, None),
        // for some reason the db sizes differ. this may be due to the compaction options we have
        // set when performing the snapshot
        //server.stats(),

        // The original instance contains the snapshotCreation task, while the snapshotted-instance does not. For this reason we need to compare the task queue **after** the task 4
        server.tasks_filter("?from=2"),

        server.index("test").get_all_documents(GetAllDocumentsOptions::default()),
        server.index("test").settings(),
        server.index("test1").get_all_documents(GetAllDocumentsOptions::default()),
        server.index("test1").settings(),
    );
}
