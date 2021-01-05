use assert_json_diff::{assert_json_eq, assert_json_include};
use meilisearch_http::helpers::compression;
use serde_json::{json, Value};
use std::fs::File;
use std::path::Path;
use std::thread;
use std::time::Duration;
use tempfile::TempDir;

#[macro_use] mod common;

async fn trigger_and_wait_dump(server: &mut common::Server) -> String {
    let (value, status_code) = server.trigger_dump().await;

    assert_eq!(status_code, 202);

    let dump_uid = value["uid"].as_str().unwrap().to_string();

    for _ in 0..20 as u8 {
        let (value, status_code) = server.get_dump_status(&dump_uid).await;
    
        assert_eq!(status_code, 200);
        assert_ne!(value["status"].as_str(), Some("dump_process_failed"));
        
        if value["status"].as_str() == Some("done") { return dump_uid }
        thread::sleep(Duration::from_millis(100));
    }

    unreachable!("dump creation runned out of time")
}

fn current_db_version() -> (String, String, String) {
    let current_version_major = env!("CARGO_PKG_VERSION_MAJOR").to_string();
    let current_version_minor = env!("CARGO_PKG_VERSION_MINOR").to_string();
    let current_version_patch = env!("CARGO_PKG_VERSION_PATCH").to_string();

    (current_version_major, current_version_minor, current_version_patch)
}

fn current_dump_version() -> String {
    "V1".into()
}

fn read_all_jsonline<R: std::io::Read>(r: R) -> Value {
    let deserializer = serde_json::Deserializer::from_reader(r); let iterator = deserializer.into_iter::<serde_json::Value>();

    json!(iterator.map(|v| v.unwrap()).collect::<Vec<Value>>())
}

#[actix_rt::test]
async fn trigger_dump_should_return_ok() {
    let server = common::Server::test_server().await;

    let (_, status_code) = server.trigger_dump().await;

    assert_eq!(status_code, 202);
}

#[actix_rt::test]
async fn trigger_dump_twice_should_return_conflict() {
    let server = common::Server::test_server().await;

    let expected = json!({
        "message": "Another dump is already in progress",
        "errorCode": "dump_already_in_progress",
        "errorType": "invalid_request_error",
        "errorLink": "https://docs.meilisearch.com/errors#dump_already_in_progress"
    });

    let (_, status_code) = server.trigger_dump().await;

    assert_eq!(status_code, 202);

    let (value, status_code) = server.trigger_dump().await;

    
    assert_json_eq!(expected, value, ordered: false);
    assert_eq!(status_code, 409);
}

#[actix_rt::test]
async fn trigger_dump_concurently_should_return_conflict() {
    let server = common::Server::test_server().await;

    let expected = json!({
        "message": "Another dump is already in progress",
        "errorCode": "dump_already_in_progress",
        "errorType": "invalid_request_error",
        "errorLink": "https://docs.meilisearch.com/errors#dump_already_in_progress"
    });

    let ((_value_1, _status_code_1), (value_2, status_code_2)) = futures::join!(server.trigger_dump(), server.trigger_dump());
    
    assert_json_eq!(expected, value_2, ordered: false);
    assert_eq!(status_code_2, 409);
}

#[actix_rt::test]
async fn get_dump_status_early_should_return_in_progress() {
    let mut server = common::Server::test_server().await;
    


    let (value, status_code) = server.trigger_dump().await;

    assert_eq!(status_code, 202);

    let dump_uid = value["uid"].as_str().unwrap().to_string();

    let (value, status_code) = server.get_dump_status(&dump_uid).await;

    let expected = json!({
        "uid": dump_uid,
        "status": "in_progress"
    });

    assert_eq!(status_code, 200);

    assert_json_eq!(expected, value, ordered: false);
}

#[actix_rt::test]
async fn get_dump_status_should_return_done() {
    let mut server = common::Server::test_server().await;


    let (value, status_code) = server.trigger_dump().await;

    assert_eq!(status_code, 202);

    let dump_uid = value["uid"].as_str().unwrap().to_string();
    
    let expected = json!({
        "uid": dump_uid.clone(),
        "status": "done"
    });

    thread::sleep(Duration::from_secs(1)); // wait dump until process end

    let (value, status_code) = server.get_dump_status(&dump_uid).await;

    assert_eq!(status_code, 200);

    assert_json_eq!(expected, value, ordered: false);
}

#[actix_rt::test]
async fn get_dump_status_should_return_error_provoking_it() {
    let mut server = common::Server::test_server().await;


    let (value, status_code) = server.trigger_dump().await;

    // removing destination directory provoking `No such file or directory` error
    std::fs::remove_dir(server.data().dumps_dir.clone()).unwrap();

    assert_eq!(status_code, 202);

    let dump_uid = value["uid"].as_str().unwrap().to_string();
    
    let expected = json!({
        "uid": dump_uid.clone(),
        "status": "failed",
        "message": "Dump process failed: compressing dump; No such file or directory (os error 2)",
        "errorCode": "dump_process_failed",
        "errorType": "internal_error",
        "errorLink": "https://docs.meilisearch.com/errors#dump_process_failed"
    });

    thread::sleep(Duration::from_secs(1)); // wait dump until process end

    let (value, status_code) = server.get_dump_status(&dump_uid).await;

    assert_eq!(status_code, 200);

    assert_json_eq!(expected, value, ordered: false);
}

#[actix_rt::test]
async fn dump_metadata_should_be_valid() {
    let mut server = common::Server::test_server().await;
    
    let body = json!({
        "uid": "test2",
        "primaryKey": "test2_id",
    });

    server.create_index(body).await;
    
    let uid = trigger_and_wait_dump(&mut server).await;

    let dumps_dir = Path::new(&server.data().dumps_dir);
    let tmp_dir = TempDir::new().unwrap();
    let tmp_dir_path = tmp_dir.path();

    compression::from_tar_gz(&dumps_dir.join(&format!("{}.dump", uid)), tmp_dir_path).unwrap();

    let file = File::open(tmp_dir_path.join("metadata.json")).unwrap();
    let mut metadata: serde_json::Value = serde_json::from_reader(file).unwrap();

     // fields are randomly ordered
     metadata.get_mut("indexes").unwrap()
        .as_array_mut().unwrap()
        .sort_by(|a, b| 
            a.get("uid").unwrap().as_str().cmp(&b.get("uid").unwrap().as_str())
        );

    let (major, minor, patch) = current_db_version();

    let expected = json!({
        "indexes": [{
                "uid": "test",
                "primaryKey": "id",
            }, {
                "uid": "test2",
                "primaryKey": "test2_id",
            }
        ],
        "dbVersion": format!("{}.{}.{}", major, minor, patch),
        "dumpVersion": current_dump_version()
    });

    assert_json_include!(expected: expected, actual: metadata);
}

#[actix_rt::test]
async fn dump_gzip_should_have_been_created() {
    let mut server = common::Server::test_server().await;
    

    let dump_uid = trigger_and_wait_dump(&mut server).await;
    let dumps_dir = Path::new(&server.data().dumps_dir);

    let compressed_path = dumps_dir.join(format!("{}.dump", dump_uid));
    assert!(File::open(compressed_path).is_ok());
}

#[actix_rt::test]
async fn dump_index_settings_should_be_valid() {
    let mut server = common::Server::test_server().await;

    let expected = json!({
        "rankingRules": [
            "typo",
            "words",
            "proximity",
            "attribute",
            "wordsPosition",
            "exactness"
        ],
        "distinctAttribute": "email",
        "searchableAttributes": [
            "balance",
            "picture",
            "age",
            "color",
            "name",
            "gender",
            "email",
            "phone",
            "address",
            "about",
            "registered",
            "latitude",
            "longitude",
            "tags"
        ],
        "displayedAttributes": [
            "id",
            "isActive",
            "balance",
            "picture",
            "age",
            "color",
            "name",
            "gender",
            "email",
            "phone",
            "address",
            "about",
            "registered",
            "latitude",
            "longitude",
            "tags"
        ],
        "stopWords": [
            "in",
            "ad"
        ],
        "synonyms": {
            "wolverine": ["xmen", "logan"],
            "logan": ["wolverine", "xmen"]
        },
        "attributesForFaceting": [
            "gender",
            "color",
            "tags"
        ]
    });

    server.update_all_settings(expected.clone()).await;

    let uid = trigger_and_wait_dump(&mut server).await;

    let dumps_dir = Path::new(&server.data().dumps_dir);
    let tmp_dir = TempDir::new().unwrap();
    let tmp_dir_path = tmp_dir.path();

    compression::from_tar_gz(&dumps_dir.join(&format!("{}.dump", uid)), tmp_dir_path).unwrap();

    let file = File::open(tmp_dir_path.join("test").join("settings.json")).unwrap();
    let settings: serde_json::Value = serde_json::from_reader(file).unwrap();

    assert_json_eq!(expected, settings, ordered: false);
}

#[actix_rt::test]
async fn dump_index_documents_should_be_valid() {
    let mut server = common::Server::test_server().await;

    let dataset = include_bytes!("assets/dumps/v1/test/documents.jsonl");
    let mut slice: &[u8] = dataset;

    let expected: Value = read_all_jsonline(&mut slice);

    let uid = trigger_and_wait_dump(&mut server).await;

    let dumps_dir = Path::new(&server.data().dumps_dir);
    let tmp_dir = TempDir::new().unwrap();
    let tmp_dir_path = tmp_dir.path();

    compression::from_tar_gz(&dumps_dir.join(&format!("{}.dump", uid)), tmp_dir_path).unwrap();

    let file = File::open(tmp_dir_path.join("test").join("documents.jsonl")).unwrap();
    let documents = read_all_jsonline(file);

    assert_json_eq!(expected, documents, ordered: false);
}

#[actix_rt::test]
async fn dump_index_updates_should_be_valid() {
    let mut server = common::Server::test_server().await;

    let dataset = include_bytes!("assets/dumps/v1/test/updates.jsonl");
    let mut slice: &[u8] = dataset;

    let expected: Value = read_all_jsonline(&mut slice);

    let uid = trigger_and_wait_dump(&mut server).await;

    let dumps_dir = Path::new(&server.data().dumps_dir);
    let tmp_dir = TempDir::new().unwrap();
    let tmp_dir_path = tmp_dir.path();

    compression::from_tar_gz(&dumps_dir.join(&format!("{}.dump", uid)), tmp_dir_path).unwrap();

    let file = File::open(tmp_dir_path.join("test").join("updates.jsonl")).unwrap();
    let updates = read_all_jsonline(file);

    eprintln!("{}\n", updates);
    eprintln!("{}", expected);
    assert_json_include!(expected: expected, actual: updates);
}
 
#[actix_rt::test]
async fn get_unexisting_dump_status_should_return_not_found() {
    let mut server = common::Server::test_server().await;

    let (_, status_code) = server.get_dump_status("4242").await;

    assert_eq!(status_code, 404);
}
