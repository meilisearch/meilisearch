mod v1_12;

use std::path::Path;
use std::{fs, io};

use meili_snap::snapshot;
use meilisearch::Opt;

use crate::common::{Server, Value, default_settings};
use crate::json;

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
    let err = Server::new_with_options(options).await.map(|_| ()).unwrap_err().to_string();

    let major = meilisearch_types::versioning::VERSION_MAJOR;
    let minor = meilisearch_types::versioning::VERSION_MINOR;
    let patch = meilisearch_types::versioning::VERSION_PATCH;

    let current_version = format!("{major}.{minor}.{patch}");
    let err = err.replace(&current_version, "[current version]");

    snapshot!(err, @"Database version 1.11.9999 is too old for the experimental dumpless upgrade feature. Please generate a dump using the v1.11.9999 and import it in the v[current version]");
}

#[actix_rt::test]
async fn version_requires_downgrade() {
    let temp = tempfile::tempdir().unwrap();
    let default_settings = default_settings(temp.path());
    let db_path = default_settings.db_path.clone();
    std::fs::create_dir_all(&db_path).unwrap();
    let major = meilisearch_types::versioning::VERSION_MAJOR;
    let minor = meilisearch_types::versioning::VERSION_MINOR;
    let mut patch = meilisearch_types::versioning::VERSION_PATCH;

    let current_version = format!("{major}.{minor}.{patch}");
    patch += 1;
    let future_version = format!("{major}.{minor}.{patch}");

    std::fs::write(db_path.join("VERSION"), &future_version).unwrap();
    let options = Opt { experimental_dumpless_upgrade: true, ..default_settings };
    let err = Server::new_with_options(options).await.map(|_| ()).unwrap_err();

    let err = err.to_string();
    let err = err.replace(&current_version, "[current version]");
    let err = err.replace(&future_version, "[future version]");

    snapshot!(err, @"Database version [future version] is higher than the Meilisearch version [current version]. Downgrade is not supported");
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

/// Exercises the dumpless-upgrade orchestration end-to-end on a database that was
/// populated by the *current* binary. We boot a server, write an index + settings +
/// documents, then copy the resulting database to a fresh location and overwrite the
/// VERSION file with `1.12.0`. Re-booting with `--experimental-dumpless-upgrade`
/// should:
///   1. rewrite the VERSION file to the current binary version,
///   2. auto-enqueue an `upgradeDatabase` task (`from = 1.12.0`) that succeeds,
///   3. preserve the user's pre-existing index, settings, documents and task uids.
///
/// `1.12.0` is the only `from` value where `update_version_file_for_dumpless_upgrade`
/// also syncs the index-scheduler's internal version DB; any later `from` would leave
/// the internal version equal to the current binary, and `upgrade_index_scheduler`
/// would short-circuit before enqueuing the upgrade task. So picking 1.12.0 here is
/// what actually triggers the migration pipeline.
#[actix_rt::test]
async fn dumpless_upgrade_from_v1_12_with_local_db() {
    let temp_source = tempfile::tempdir().unwrap();
    let source_settings = default_settings(temp_source.path());
    let source_db_path = source_settings.db_path.clone();

    // Phase 1: populate a fresh DB with the current binary.
    {
        let server = Server::new_with_options(source_settings).await.unwrap();
        let index = server.index("kefir");

        let (task, _) = index
            .update_settings(json!({
                "searchableAttributes": ["name", "description"],
                "filterableAttributes": ["age"],
                "sortableAttributes": ["age"],
            }))
            .await;
        server.wait_task(task.uid()).await.succeeded();

        let (task, _) = index
            .add_documents(
                json!([
                    { "id": 1, "name": "kefir", "age": 6, "description": "the patou" },
                    { "id": 2, "name": "echo", "age": 4, "description": "the labrador" },
                ]),
                Some("id"),
            )
            .await;
        server.wait_task(task.uid()).await.succeeded();
    }

    // Phase 2: clone the populated DB into a fresh location and pretend it was
    // written by v1.12.0. Copying into a new tempdir avoids re-opening LMDB on the
    // same path within the same process.
    let temp_target = tempfile::tempdir().unwrap();
    let target_settings =
        Opt { experimental_dumpless_upgrade: true, ..default_settings(temp_target.path()) };
    let target_db_path = target_settings.db_path.clone();
    copy_dir_all(&source_db_path, &target_db_path).unwrap();
    fs::write(target_db_path.join("VERSION"), "1.12.0").unwrap();

    // Phase 3: boot the second server with dumpless upgrade enabled.
    let server = Server::new_with_options(target_settings).await.unwrap();

    // The VERSION file on disk must have been rewritten to the current binary version.
    let bumped_version = fs::read_to_string(target_db_path.join("VERSION")).unwrap();
    let expected_version = format!(
        "{}.{}.{}",
        meilisearch_types::versioning::VERSION_MAJOR,
        meilisearch_types::versioning::VERSION_MINOR,
        meilisearch_types::versioning::VERSION_PATCH,
    );
    assert_eq!(bumped_version, expected_version);

    // The `upgradeDatabase` task must have been auto-enqueued; wait for it to finish.
    let (tasks, _) = server.tasks_filter("types=upgradeDatabase&limit=1").await;
    let upgrade_uid = Value(tasks["results"][0].clone()).uid();
    let upgrade_task = server.wait_task(upgrade_uid).await.succeeded();
    assert_eq!(upgrade_task["type"], "upgradeDatabase");
    assert_eq!(upgrade_task["status"], "succeeded");
    assert_eq!(upgrade_task["details"]["upgradeFrom"], "v1.12.0");
    assert_eq!(upgrade_task["details"]["upgradeTo"], format!("v{expected_version}"));
    assert!(upgrade_task["error"].is_null());

    // The user's index, settings and documents must survive the upgrade.
    let index = server.index("kefir");
    let (settings, _) = index.settings().await;
    assert_eq!(settings["searchableAttributes"], json!(["name", "description"]));
    assert_eq!(settings["filterableAttributes"], json!(["age"]));
    assert_eq!(settings["sortableAttributes"], json!(["age"]));

    let (results, _) =
        index.search_post(json!({ "sort": ["age:asc"], "filter": "age >= 5" })).await;
    snapshot!(results["estimatedTotalHits"], @"1");
    let hits = results["hits"].as_array().unwrap();
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0]["id"], 1);
    assert_eq!(hits[0]["name"], "kefir");

    // The pre-existing task uids (settings update and document addition) must still
    // be retrievable from the queue.
    let (settings_task, _) = server.get_task(0).await;
    assert_eq!(settings_task["type"], "settingsUpdate");
    assert_eq!(settings_task["status"], "succeeded");
    let (docs_task, _) = server.get_task(1).await;
    assert_eq!(docs_task["type"], "documentAdditionOrUpdate");
    assert_eq!(docs_task["status"], "succeeded");
}
