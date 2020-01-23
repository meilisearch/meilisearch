use async_std::io::prelude::*;
use async_std::task::block_on;
use http_service::Body;
use serde_json::json;
use serde_json::Value;
use std::convert::Into;

mod common;

#[test]
fn create_index_with_name() {
    let mut server = common::setup_server().unwrap();

    // 1 - Create a new index
    // Index with only a name "movies"
    // POST: /indexes

    let body = json!({
        "name": "movies",
    })
    .to_string()
    .into_bytes();

    let req = http::Request::post("/indexes")
        .body(Body::from(body))
        .unwrap();
    let res = server.simulate(req).unwrap();
    assert_eq!(res.status(), 201);

    let mut buf = Vec::new();
    block_on(res.into_body().read_to_end(&mut buf)).unwrap();
    let res1_value: Value = serde_json::from_slice(&buf).unwrap();

    assert_eq!(res1_value.as_object().unwrap().len(), 4);
    let r1_name = res1_value["name"].as_str().unwrap();
    let r1_uid = res1_value["uid"].as_str().unwrap();
    let r1_created_at = res1_value["createdAt"].as_str().unwrap();
    let r1_updated_at = res1_value["updatedAt"].as_str().unwrap();

    assert_eq!(r1_name, "movies");
    assert_eq!(r1_uid.len(), 8);
    assert_eq!(r1_created_at.len(), 27);
    assert_eq!(r1_updated_at.len(), 27);

    // 2 - Check the list of indexes
    // Must have 1 index with the exact same content that the request 1
    // GET: /indexes

    let req = http::Request::get("/indexes").body(Body::empty()).unwrap();
    let res = server.simulate(req).unwrap();
    assert_eq!(res.status(), 200);

    let mut buf = Vec::new();
    block_on(res.into_body().read_to_end(&mut buf)).unwrap();
    let res2_value: Value = serde_json::from_slice(&buf).unwrap();

    assert_eq!(res2_value.as_array().unwrap().len(), 1);
    assert_eq!(res2_value[0].as_object().unwrap().len(), 4);
    let r2_name = res2_value[0]["name"].as_str().unwrap();
    let r2_uid = res2_value[0]["uid"].as_str().unwrap();
    let r2_created_at = res2_value[0]["createdAt"].as_str().unwrap();
    let r2_updated_at = res2_value[0]["updatedAt"].as_str().unwrap();

    assert_eq!(r2_name, r1_name);
    assert_eq!(r2_uid.len(), r1_uid.len());
    assert_eq!(r2_created_at.len(), r1_created_at.len());
    assert_eq!(r2_updated_at.len(), r1_updated_at.len());
}

#[test]
fn create_index_with_uid() {
    let mut server = common::setup_server().unwrap();

    // 1 - Create a new index
    // Index with only an uid "movies"
    // POST: /indexes

    let body = json!({
        "uid": "movies",
    })
    .to_string()
    .into_bytes();

    let req = http::Request::post("/indexes")
        .body(Body::from(body))
        .unwrap();
    let res = server.simulate(req).unwrap();
    assert_eq!(res.status(), 201);

    let mut buf = Vec::new();
    block_on(res.into_body().read_to_end(&mut buf)).unwrap();
    let res1_value: Value = serde_json::from_slice(&buf).unwrap();

    assert_eq!(res1_value.as_object().unwrap().len(), 4);
    let r1_name = res1_value["name"].as_str().unwrap();
    let r1_uid = res1_value["uid"].as_str().unwrap();
    let r1_created_at = res1_value["createdAt"].as_str().unwrap();
    let r1_updated_at = res1_value["updatedAt"].as_str().unwrap();

    assert_eq!(r1_name, "movies");
    assert_eq!(r1_uid, "movies");
    assert_eq!(r1_created_at.len(), 27);
    assert_eq!(r1_updated_at.len(), 27);

    // 2 - Check the list of indexes
    // Must have 1 index with the exact same content that the request 1
    // GET: /indexes

    let req = http::Request::get("/indexes").body(Body::empty()).unwrap();
    let res = server.simulate(req).unwrap();
    assert_eq!(res.status(), 200);

    let mut buf = Vec::new();
    block_on(res.into_body().read_to_end(&mut buf)).unwrap();
    let res2_value: Value = serde_json::from_slice(&buf).unwrap();

    assert_eq!(res2_value.as_array().unwrap().len(), 1);
    assert_eq!(res2_value[0].as_object().unwrap().len(), 4);
    let r2_name = res2_value[0]["name"].as_str().unwrap();
    let r2_uid = res2_value[0]["uid"].as_str().unwrap();
    let r2_created_at = res2_value[0]["createdAt"].as_str().unwrap();
    let r2_updated_at = res2_value[0]["updatedAt"].as_str().unwrap();

    assert_eq!(r2_name, r1_name);
    assert_eq!(r2_uid, r1_uid);
    assert_eq!(r2_created_at.len(), r1_created_at.len());
    assert_eq!(r2_updated_at.len(), r1_updated_at.len());
}

#[test]
fn create_index_with_name_and_uid() {
    let mut server = common::setup_server().unwrap();

    // 1 - Create a new index
    // Index with a name "Films" and an uid "fn_movies"
    // POST: /indexes

    let body = json!({
        "name": "Films",
        "uid": "fr_movies",
    })
    .to_string()
    .into_bytes();

    let req = http::Request::post("/indexes")
        .body(Body::from(body))
        .unwrap();
    let res = server.simulate(req).unwrap();
    assert_eq!(res.status(), 201);

    let mut buf = Vec::new();
    block_on(res.into_body().read_to_end(&mut buf)).unwrap();
    let res1_value: Value = serde_json::from_slice(&buf).unwrap();

    assert_eq!(res1_value.as_object().unwrap().len(), 4);
    let r1_name = res1_value["name"].as_str().unwrap();
    let r1_uid = res1_value["uid"].as_str().unwrap();
    let r1_created_at = res1_value["createdAt"].as_str().unwrap();
    let r1_updated_at = res1_value["updatedAt"].as_str().unwrap();

    assert_eq!(r1_name, "Films");
    assert_eq!(r1_uid, "fr_movies");
    assert_eq!(r1_created_at.len(), 27);
    assert_eq!(r1_updated_at.len(), 27);

    // 2 - Check the list of indexes
    // Must have 1 index with the exact same content that the request 1
    // GET: /indexes

    let req = http::Request::get("/indexes").body(Body::empty()).unwrap();
    let res = server.simulate(req).unwrap();
    assert_eq!(res.status(), 200);

    let mut buf = Vec::new();
    block_on(res.into_body().read_to_end(&mut buf)).unwrap();
    let res2_value: Value = serde_json::from_slice(&buf).unwrap();

    assert_eq!(res2_value.as_array().unwrap().len(), 1);
    assert_eq!(res2_value[0].as_object().unwrap().len(), 4);
    let r2_name = res2_value[0]["name"].as_str().unwrap();
    let r2_uid = res2_value[0]["uid"].as_str().unwrap();
    let r2_created_at = res2_value[0]["createdAt"].as_str().unwrap();
    let r2_updated_at = res2_value[0]["updatedAt"].as_str().unwrap();

    assert_eq!(r2_name, r1_name);
    assert_eq!(r2_uid, r1_uid);
    assert_eq!(r2_created_at.len(), r1_created_at.len());
    assert_eq!(r2_updated_at.len(), r1_updated_at.len());
}

#[test]
fn rename_index() {
    let mut server = common::setup_server().unwrap();
    // 1 - Create a new index
    // Index with only a name "movies"
    // POST: /indexes

    let body = json!({
        "name": "movies",
    })
    .to_string()
    .into_bytes();

    let req = http::Request::post("/indexes")
        .body(Body::from(body))
        .unwrap();
    let res = server.simulate(req).unwrap();
    assert_eq!(res.status(), 201);

    let mut buf = Vec::new();
    block_on(res.into_body().read_to_end(&mut buf)).unwrap();
    let res1_value: Value = serde_json::from_slice(&buf).unwrap();

    assert_eq!(res1_value.as_object().unwrap().len(), 4);
    let r1_name = res1_value["name"].as_str().unwrap();
    let r1_uid = res1_value["uid"].as_str().unwrap();
    let r1_created_at = res1_value["createdAt"].as_str().unwrap();
    let r1_updated_at = res1_value["updatedAt"].as_str().unwrap();

    assert_eq!(r1_name, "movies");
    assert_eq!(r1_uid.len(), 8);
    assert_eq!(r1_created_at.len(), 27);
    assert_eq!(r1_updated_at.len(), 27);

    // 2 - Update an index name
    // Update "movies" to "TV Shows"
    // PUT: /indexes/:uid

    let body = json!({
        "name": "TV Shows",
    })
    .to_string()
    .into_bytes();

    let req = http::Request::put(format!("/indexes/{}", r1_uid))
        .body(Body::from(body))
        .unwrap();
    let res = server.simulate(req).unwrap();
    assert_eq!(res.status(), 200);

    let mut buf = Vec::new();
    block_on(res.into_body().read_to_end(&mut buf)).unwrap();
    let res2_value: Value = serde_json::from_slice(&buf).unwrap();

    assert_eq!(res2_value.as_object().unwrap().len(), 4);
    let r2_name = res2_value["name"].as_str().unwrap();
    let r2_uid = res2_value["uid"].as_str().unwrap();
    let r2_created_at = res2_value["createdAt"].as_str().unwrap();
    let r2_updated_at = res2_value["updatedAt"].as_str().unwrap();

    assert_eq!(r2_name, "TV Shows");
    assert_eq!(r2_uid, r1_uid);
    assert_eq!(r2_created_at, r1_created_at);
    assert_eq!(r2_updated_at.len(), 27);

    // 3 - Check the list of indexes
    // Must have 1 index with the exact same content that the request 2
    // GET: /indexes

    let req = http::Request::get("/indexes").body(Body::empty()).unwrap();
    let res = server.simulate(req).unwrap();
    assert_eq!(res.status(), 200);

    let mut buf = Vec::new();
    block_on(res.into_body().read_to_end(&mut buf)).unwrap();
    let res3_value: Value = serde_json::from_slice(&buf).unwrap();

    assert_eq!(res3_value.as_array().unwrap().len(), 1);
    assert_eq!(res3_value[0].as_object().unwrap().len(), 4);
    let r3_name = res3_value[0]["name"].as_str().unwrap();
    let r3_uid = res3_value[0]["uid"].as_str().unwrap();
    let r3_created_at = res3_value[0]["createdAt"].as_str().unwrap();
    let r3_updated_at = res3_value[0]["updatedAt"].as_str().unwrap();

    assert_eq!(r3_name, r2_name);
    assert_eq!(r3_uid.len(), r1_uid.len());
    assert_eq!(r3_created_at.len(), r1_created_at.len());
    assert_eq!(r3_updated_at.len(), r2_updated_at.len());
}

#[test]
fn delete_index_and_recreate_it() {
    let mut server = common::setup_server().unwrap();

    // 1 - Create a new index
    // Index with only a name "movies"
    // POST: /indexes

    let body = json!({
        "name": "movies",
    })
    .to_string()
    .into_bytes();

    let req = http::Request::post("/indexes")
        .body(Body::from(body))
        .unwrap();
    let res = server.simulate(req).unwrap();
    assert_eq!(res.status(), 201);

    let mut buf = Vec::new();
    block_on(res.into_body().read_to_end(&mut buf)).unwrap();
    let res1_value: Value = serde_json::from_slice(&buf).unwrap();

    assert_eq!(res1_value.as_object().unwrap().len(), 4);
    let r1_name = res1_value["name"].as_str().unwrap();
    let r1_uid = res1_value["uid"].as_str().unwrap();
    let r1_created_at = res1_value["createdAt"].as_str().unwrap();
    let r1_updated_at = res1_value["updatedAt"].as_str().unwrap();

    assert_eq!(r1_name, "movies");
    assert_eq!(r1_uid.len(), 8);
    assert_eq!(r1_created_at.len(), 27);
    assert_eq!(r1_updated_at.len(), 27);

    // 2 - Check the list of indexes
    // Must have 1 index with the exact same content that the request 1
    // GET: /indexes

    let req = http::Request::get("/indexes").body(Body::empty()).unwrap();
    let res = server.simulate(req).unwrap();
    assert_eq!(res.status(), 200);

    let mut buf = Vec::new();
    block_on(res.into_body().read_to_end(&mut buf)).unwrap();
    let res2_value: Value = serde_json::from_slice(&buf).unwrap();

    assert_eq!(res2_value.as_array().unwrap().len(), 1);
    assert_eq!(res2_value[0].as_object().unwrap().len(), 4);
    let r2_name = res2_value[0]["name"].as_str().unwrap();
    let r2_uid = res2_value[0]["uid"].as_str().unwrap();
    let r2_created_at = res2_value[0]["createdAt"].as_str().unwrap();
    let r2_updated_at = res2_value[0]["updatedAt"].as_str().unwrap();

    assert_eq!(r2_name, r1_name);
    assert_eq!(r2_uid.len(), r1_uid.len());
    assert_eq!(r2_created_at.len(), r1_created_at.len());
    assert_eq!(r2_updated_at.len(), r1_updated_at.len());

    // 3- Delete an index
    // Update "movies" to "TV Shows"
    // DELETE: /indexes/:uid

    let req = http::Request::delete(format!("/indexes/{}", r1_uid))
        .body(Body::empty())
        .unwrap();
    let res = server.simulate(req).unwrap();
    assert_eq!(res.status(), 204);

    let mut buf = Vec::new();
    block_on(res.into_body().read_to_end(&mut buf)).unwrap();
    assert_eq!(buf.len(), 0);

    // 4 - Check the list of indexes
    // Must have 0 index
    // GET: /indexes

    let req = http::Request::get("/indexes").body(Body::empty()).unwrap();
    let res = server.simulate(req).unwrap();
    assert_eq!(res.status(), 200);

    let mut buf = Vec::new();
    block_on(res.into_body().read_to_end(&mut buf)).unwrap();
    let res2_value: Value = serde_json::from_slice(&buf).unwrap();

    assert_eq!(res2_value.as_array().unwrap().len(), 0);

    // 5 - Create a new index
    // Index with only a name "movies"
    // POST: /indexes

    let body = json!({
        "name": "movies",
    })
    .to_string()
    .into_bytes();

    let req = http::Request::post("/indexes")
        .body(Body::from(body))
        .unwrap();
    let res = server.simulate(req).unwrap();
    assert_eq!(res.status(), 201);

    let mut buf = Vec::new();
    block_on(res.into_body().read_to_end(&mut buf)).unwrap();
    let res1_value: Value = serde_json::from_slice(&buf).unwrap();

    assert_eq!(res1_value.as_object().unwrap().len(), 4);
    let r1_name = res1_value["name"].as_str().unwrap();
    let r1_uid = res1_value["uid"].as_str().unwrap();
    let r1_created_at = res1_value["createdAt"].as_str().unwrap();
    let r1_updated_at = res1_value["updatedAt"].as_str().unwrap();

    assert_eq!(r1_name, "movies");
    assert_eq!(r1_uid.len(), 8);
    assert_eq!(r1_created_at.len(), 27);
    assert_eq!(r1_updated_at.len(), 27);

    // 6 - Check the list of indexes
    // Must have 1 index with the exact same content that the request 1
    // GET: /indexes

    let req = http::Request::get("/indexes").body(Body::empty()).unwrap();
    let res = server.simulate(req).unwrap();
    assert_eq!(res.status(), 200);

    let mut buf = Vec::new();
    block_on(res.into_body().read_to_end(&mut buf)).unwrap();
    let res2_value: Value = serde_json::from_slice(&buf).unwrap();

    assert_eq!(res2_value.as_array().unwrap().len(), 1);
    assert_eq!(res2_value[0].as_object().unwrap().len(), 4);
    let r2_name = res2_value[0]["name"].as_str().unwrap();
    let r2_uid = res2_value[0]["uid"].as_str().unwrap();
    let r2_created_at = res2_value[0]["createdAt"].as_str().unwrap();
    let r2_updated_at = res2_value[0]["updatedAt"].as_str().unwrap();

    assert_eq!(r2_name, r1_name);
    assert_eq!(r2_uid.len(), r1_uid.len());
    assert_eq!(r2_created_at.len(), r1_created_at.len());
    assert_eq!(r2_updated_at.len(), r1_updated_at.len());
}

#[test]
fn check_multiples_indexes() {
    let mut server = common::setup_server().unwrap();

    // 1 - Create a new index
    // Index with only a name "movies"
    // POST: /indexes

    let body = json!({
        "name": "movies",
    })
    .to_string()
    .into_bytes();

    let req = http::Request::post("/indexes")
        .body(Body::from(body))
        .unwrap();
    let res = server.simulate(req).unwrap();
    assert_eq!(res.status(), 201);

    let mut buf = Vec::new();
    block_on(res.into_body().read_to_end(&mut buf)).unwrap();
    let res1_value: Value = serde_json::from_slice(&buf).unwrap();

    assert_eq!(res1_value.as_object().unwrap().len(), 4);
    let r1_name = res1_value["name"].as_str().unwrap();
    let r1_uid = res1_value["uid"].as_str().unwrap();
    let r1_created_at = res1_value["createdAt"].as_str().unwrap();
    let r1_updated_at = res1_value["updatedAt"].as_str().unwrap();

    assert_eq!(r1_name, "movies");
    assert_eq!(r1_uid.len(), 8);
    assert_eq!(r1_created_at.len(), 27);
    assert_eq!(r1_updated_at.len(), 27);

    // 2 - Check the list of indexes
    // Must have 1 index with the exact same content that the request 1
    // GET: /indexes

    let req = http::Request::get("/indexes").body(Body::empty()).unwrap();
    let res = server.simulate(req).unwrap();
    assert_eq!(res.status(), 200);

    let mut buf = Vec::new();
    block_on(res.into_body().read_to_end(&mut buf)).unwrap();
    let res2_value: Value = serde_json::from_slice(&buf).unwrap();

    assert_eq!(res2_value.as_array().unwrap().len(), 1);
    assert_eq!(res2_value[0].as_object().unwrap().len(), 4);
    let r2_0_name = res2_value[0]["name"].as_str().unwrap();
    let r2_0_uid = res2_value[0]["uid"].as_str().unwrap();
    let r2_0_created_at = res2_value[0]["createdAt"].as_str().unwrap();
    let r2_0_updated_at = res2_value[0]["updatedAt"].as_str().unwrap();

    assert_eq!(r2_0_name, r1_name);
    assert_eq!(r2_0_uid.len(), r1_uid.len());
    assert_eq!(r2_0_created_at.len(), r1_created_at.len());
    assert_eq!(r2_0_updated_at.len(), r1_updated_at.len());

    // 3 - Create a new index
    // Index with only a name "films"
    // POST: /indexes

    let body = json!({
        "name": "films",
    })
    .to_string()
    .into_bytes();

    let req = http::Request::post("/indexes")
        .body(Body::from(body))
        .unwrap();
    let res = server.simulate(req).unwrap();
    assert_eq!(res.status(), 201);

    let mut buf = Vec::new();
    block_on(res.into_body().read_to_end(&mut buf)).unwrap();
    let res3_value: Value = serde_json::from_slice(&buf).unwrap();

    assert_eq!(res3_value.as_object().unwrap().len(), 4);
    let r3_name = res3_value["name"].as_str().unwrap();
    let r3_uid = res3_value["uid"].as_str().unwrap();
    let r3_created_at = res3_value["createdAt"].as_str().unwrap();
    let r3_updated_at = res3_value["updatedAt"].as_str().unwrap();

    assert_eq!(r3_name, "films");
    assert_eq!(r3_uid.len(), 8);
    assert_eq!(r3_created_at.len(), 27);
    assert_eq!(r3_updated_at.len(), 27);

    // 4 - Check the list of indexes
    // Must have 2 index with the exact same content that the request 1 and 3
    // GET: /indexes

    let req = http::Request::get("/indexes").body(Body::empty()).unwrap();
    let res = server.simulate(req).unwrap();
    assert_eq!(res.status(), 200);

    let mut buf = Vec::new();
    block_on(res.into_body().read_to_end(&mut buf)).unwrap();
    let res4_value: Value = serde_json::from_slice(&buf).unwrap();

    assert_eq!(res4_value.as_array().unwrap().len(), 2);

    assert_eq!(res4_value[0].as_object().unwrap().len(), 4);
    let r4_0_name = res4_value[0]["name"].as_str().unwrap();
    let r4_0_uid = res4_value[0]["uid"].as_str().unwrap();
    let r4_0_created_at = res4_value[0]["createdAt"].as_str().unwrap();
    let r4_0_updated_at = res4_value[0]["updatedAt"].as_str().unwrap();

    assert_eq!(res4_value[1].as_object().unwrap().len(), 4);
    let r4_1_name = res4_value[1]["name"].as_str().unwrap();
    let r4_1_uid = res4_value[1]["uid"].as_str().unwrap();
    let r4_1_created_at = res4_value[1]["createdAt"].as_str().unwrap();
    let r4_1_updated_at = res4_value[1]["updatedAt"].as_str().unwrap();

    if r4_0_name == r1_name {
        assert_eq!(r4_0_name, r1_name);
        assert_eq!(r4_0_uid.len(), r1_uid.len());
        assert_eq!(r4_0_created_at.len(), r1_created_at.len());
        assert_eq!(r4_0_updated_at.len(), r1_updated_at.len());
    } else {
        assert_eq!(r4_0_name, r3_name);
        assert_eq!(r4_0_uid.len(), r3_uid.len());
        assert_eq!(r4_0_created_at.len(), r3_created_at.len());
        assert_eq!(r4_0_updated_at.len(), r3_updated_at.len());
    }

    if r4_1_name == r1_name {
        assert_eq!(r4_1_name, r1_name);
        assert_eq!(r4_1_uid.len(), r1_uid.len());
        assert_eq!(r4_1_created_at.len(), r1_created_at.len());
        assert_eq!(r4_1_updated_at.len(), r1_updated_at.len());
    } else {
        assert_eq!(r4_1_name, r3_name);
        assert_eq!(r4_1_uid.len(), r3_uid.len());
        assert_eq!(r4_1_created_at.len(), r3_created_at.len());
        assert_eq!(r4_1_updated_at.len(), r3_updated_at.len());
    }
}

#[test]
fn create_index_failed() {
    let mut server = common::setup_server().unwrap();

    // 1 - Push index creation with empty body
    // POST: /indexes

    let req = http::Request::post("/indexes").body(Body::empty()).unwrap();
    let res = server.simulate(req).unwrap();
    assert_eq!(res.status(), 400);

    let mut buf = Vec::new();
    block_on(res.into_body().read_to_end(&mut buf)).unwrap();
    let res_value: Value = serde_json::from_slice(&buf).unwrap();

    let message = res_value["message"].as_str().unwrap();
    assert_eq!(res_value.as_object().unwrap().len(), 1);
    assert_eq!(message, "invalid data");

    // 2 - Push index creation with empty json body
    // POST: /indexes

    let body = json!({}).to_string().into_bytes();

    let req = http::Request::post("/indexes")
        .body(Body::from(body))
        .unwrap();
    let res = server.simulate(req).unwrap();
    assert_eq!(res.status(), 400);

    let mut buf = Vec::new();
    block_on(res.into_body().read_to_end(&mut buf)).unwrap();
    let res_value: Value = serde_json::from_slice(&buf).unwrap();

    let message = res_value["message"].as_str().unwrap();
    assert_eq!(res_value.as_object().unwrap().len(), 1);
    assert_eq!(message, "Index creation must have an uid");

    // 3 - Create a index with extra data
    // POST: /indexes

    let body = json!({
        "name": "movies",
        "active": true
    })
    .to_string()
    .into_bytes();

    let req = http::Request::post("/indexes")
        .body(Body::from(body))
        .unwrap();
    let res = server.simulate(req).unwrap();
    assert_eq!(res.status(), 400);

    let mut buf = Vec::new();
    block_on(res.into_body().read_to_end(&mut buf)).unwrap();
    let res_value: Value = serde_json::from_slice(&buf).unwrap();

    let message = res_value["message"].as_str().unwrap();
    assert_eq!(res_value.as_object().unwrap().len(), 1);
    assert_eq!(message, "invalid data");

    // 3 - Create a index with wrong data type
    // POST: /indexes

    let body = json!({
        "name": "movies",
        "uid": 0
    })
    .to_string()
    .into_bytes();

    let req = http::Request::post("/indexes")
        .body(Body::from(body))
        .unwrap();
    let res = server.simulate(req).unwrap();
    assert_eq!(res.status(), 400);

    let mut buf = Vec::new();
    block_on(res.into_body().read_to_end(&mut buf)).unwrap();
    let res_value: Value = serde_json::from_slice(&buf).unwrap();

    let message = res_value["message"].as_str().unwrap();
    assert_eq!(res_value.as_object().unwrap().len(), 1);
    assert_eq!(message, "invalid data");
}
