use std::time::Duration;

use meili_snap::{json_string, snapshot};
use meilisearch::option::ScheduleSnapshot;
use meilisearch::Opt;

use crate::common::server::default_settings;
use crate::common::{GetAllDocumentsOptions, Server};
use crate::json;

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
#[cfg_attr(target_os = "windows", ignore)]
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
        .update_settings(json! ({
        "searchableAttributes": [],
        }))
        .await;

    index.load_test_set().await;

    let (task, code) = server.index("test1").create(Some("prim")).await;
    meili_snap::snapshot!(code, @"202 Accepted");

    index.wait_task(task.uid()).await.succeeded();

    // wait for the _next task_ to process, aka the snapshot that should be enqueued at some point

    println!("waited for the next task to finish");
    let now = std::time::Instant::now();
    let next_task = task.uid() + 1;
    loop {
        let (value, code) = index.get_task(next_task).await;
        if code != 404 && value["status"].as_str() == Some("succeeded") {
            break;
        }

        if now.elapsed() > Duration::from_secs(30) {
            panic!("The snapshot didn't schedule in 30s even though it was supposed to be scheduled every 2s: {}",
                serde_json::to_string_pretty(&value).unwrap()
            );
        }
    }

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

#[actix_rt::test]
async fn perform_on_demand_snapshot() {
    let temp = tempfile::tempdir().unwrap();
    let snapshot_dir = tempfile::tempdir().unwrap();

    let options =
        Opt { snapshot_dir: snapshot_dir.path().to_owned(), ..default_settings(temp.path()) };

    let server = Server::new_with_options(options).await.unwrap();

    let index = server.index("catto");
    index
        .update_settings(json! ({
        "searchableAttributes": [],
        }))
        .await;

    index.load_test_set().await;

    let (task, _status_code) = server.index("doggo").create(Some("bone")).await;
    index.wait_task(task.uid()).await.succeeded();

    let (task, _status_code) = server.index("doggo").create(Some("bone")).await;
    index.wait_task(task.uid()).await.failed();

    let (task, code) = server.create_snapshot().await;
    snapshot!(code, @"202 Accepted");
    snapshot!(json_string!(task, { ".enqueuedAt" => "[date]" }), @r###"
    {
      "taskUid": 4,
      "indexUid": null,
      "status": "enqueued",
      "type": "snapshotCreation",
      "enqueuedAt": "[date]"
    }
    "###);
    let task = index.wait_task(task.uid()).await;
    snapshot!(json_string!(task, { ".enqueuedAt" => "[date]", ".startedAt" => "[date]", ".finishedAt" => "[date]", ".duration" => "[duration]" }), @r###"
    {
      "uid": 4,
      "batchUid": 4,
      "indexUid": null,
      "status": "succeeded",
      "type": "snapshotCreation",
      "canceledBy": null,
      "error": null,
      "duration": "[duration]",
      "enqueuedAt": "[date]",
      "startedAt": "[date]",
      "finishedAt": "[date]"
    }
    "###);

    let temp = tempfile::tempdir().unwrap();

    let snapshots: Vec<String> = std::fs::read_dir(&snapshot_dir)
        .unwrap()
        .map(|entry| entry.unwrap().path().file_name().unwrap().to_str().unwrap().to_string())
        .collect();
    meili_snap::snapshot!(format!("{snapshots:?}"), @r###"["db.snapshot"]"###);

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

        server.index("catto").get_all_documents(GetAllDocumentsOptions::default()),
        server.index("catto").settings(),
        server.index("doggo").get_all_documents(GetAllDocumentsOptions::default()),
        server.index("doggo").settings(),
    );
}
