#![allow(dead_code)]

use http::StatusCode;
use serde_json::Value;
use std::time::Duration;

use async_std::io::prelude::*;
use async_std::task::{block_on, sleep};
use http_service::Body;
use http_service_mock::{make_server, TestBackend};
use meilisearch_http::data::Data;
use meilisearch_http::option::Opt;
use meilisearch_http::routes;
use serde_json::json;
use tempdir::TempDir;
use tide::server::Service;

pub struct Server {
    uid: String,
    mock: TestBackend<Service<Data>>,
}

impl Server {
    pub fn with_uid(uid: &str) -> Server {
        let tmp_dir = TempDir::new("meilisearch").unwrap();

        let opt = Opt {
            db_path: tmp_dir.path().to_str().unwrap().to_string(),
            http_addr: "127.0.0.1:7700".to_owned(),
            master_key: None,
            env: "development".to_owned(),
            no_analytics: true,
        };

        let data = Data::new(opt.clone());
        let mut app = tide::with_state(data);
        routes::load_routes(&mut app);
        let http_server = app.into_http_service();
        let mock = make_server(http_server).unwrap();

        Server {
            uid: uid.to_string(),
            mock,
        }
    }

    fn wait_update_id(&mut self, update_id: u64) {
        loop {
            let req = http::Request::get(format!("/indexes/{}/updates/{}", self.uid, update_id))
                .body(Body::empty())
                .unwrap();

            let res = self.mock.simulate(req).unwrap();
            assert_eq!(res.status(), 200);

            let mut buf = Vec::new();
            block_on(res.into_body().read_to_end(&mut buf)).unwrap();
            let response: Value = serde_json::from_slice(&buf).unwrap();

            if response["status"] == "processed" || response["status"] == "error" {
                eprintln!("{:#?}", response);
                return;
            }
            block_on(sleep(Duration::from_secs(1)));
        }
    }

    // // Global Http request GET/POST/DELETE async or sync

    fn get_request(&mut self, url: &str) -> (Value, StatusCode) {
        eprintln!("get_request: {}", url);
        let req = http::Request::get(url)
            .body(Body::empty())
            .unwrap();
        let res = self.mock.simulate(req).unwrap();
        let status_code = res.status().clone();

        let mut buf = Vec::new();
        block_on(res.into_body().read_to_end(&mut buf)).unwrap();
        let response = serde_json::from_slice(&buf).unwrap_or_default();
        (response, status_code)
    }

    fn post_request(&mut self, url: &str, body: Value) -> (Value, StatusCode) {
        eprintln!("post_request: {}", url);
        let body_bytes = body.to_string().into_bytes();

        let req = http::Request::post(url)
            .body(Body::from(body_bytes))
            .unwrap();
        let res = self.mock.simulate(req).unwrap();
        let status_code = res.status().clone();

        let mut buf = Vec::new();
        block_on(res.into_body().read_to_end(&mut buf)).unwrap();
        let response = serde_json::from_slice(&buf).unwrap_or_default();
        (response, status_code)
    }

    fn post_request_async(&mut self, url: &str, body: Value)  -> (Value, StatusCode) {
        eprintln!("post_request_async: {}", url);
        let (response, status_code) = self.post_request(url, body);
        assert_eq!(status_code, 202);
        assert!(response["updateId"].as_u64().is_some());
        self.wait_update_id(response["updateId"].as_u64().unwrap());
        (response, status_code)
    }

    fn put_request(&mut self, url: &str, body: Value) -> (Value, StatusCode) {
        eprintln!("put_request: {}", url);
        let body_bytes = body.to_string().into_bytes();

        let req = http::Request::put(url)
            .body(Body::from(body_bytes))
            .unwrap();
        let res = self.mock.simulate(req).unwrap();
        let status_code = res.status().clone();

        let mut buf = Vec::new();
        block_on(res.into_body().read_to_end(&mut buf)).unwrap();
        let response = serde_json::from_slice(&buf).unwrap_or_default();
        (response, status_code)
    }

    fn put_request_async(&mut self, url: &str, body: Value) -> (Value, StatusCode) {
        eprintln!("put_request_async: {}", url);
        let (response, status_code) = self.put_request(url, body);
        assert!(response["updateId"].as_u64().is_some());
        assert_eq!(status_code, 202);
        self.wait_update_id(response["updateId"].as_u64().unwrap());
        (response, status_code)
    }

    fn delete_request(&mut self, url: &str) -> (Value, StatusCode) {
        eprintln!("delete_request: {}", url);
        let req = http::Request::delete(url)
            .body(Body::empty())
            .unwrap();
        let res = self.mock.simulate(req).unwrap();
        let status_code = res.status().clone();

        let mut buf = Vec::new();
        block_on(res.into_body().read_to_end(&mut buf)).unwrap();
        let response = serde_json::from_slice(&buf).unwrap_or_default();
        (response, status_code)
    }

    fn delete_request_async(&mut self, url: &str) -> (Value, StatusCode) {
        eprintln!("delete_request_async: {}", url);
        let (response, status_code) = self.delete_request(url);
        assert!(response["updateId"].as_u64().is_some());
        assert_eq!(status_code, 202);
        self.wait_update_id(response["updateId"].as_u64().unwrap());
        (response, status_code)
    }


    // // All Routes

    pub fn list_indexes(&mut self) -> (Value, StatusCode) {
        self.get_request("/indexes")
    }

    pub fn create_index(&mut self, body: Value) -> (Value, StatusCode) {
        self.post_request("/indexes", body)
    }

    pub fn search_multi_index(&mut self, query: &str) -> (Value, StatusCode) {
        let url = format!("/indexes/search?{}", query);
        self.get_request(&url)
    }

    pub fn get_index(&mut self) -> (Value, StatusCode) {
        let url = format!("/indexes/{}", self.uid);
        self.get_request(&url)
    }

    pub fn update_index(&mut self, body: Value) -> (Value, StatusCode) {
        let url = format!("/indexes/{}", self.uid);
        self.put_request(&url, body)
    }

    pub fn delete_index(&mut self) -> (Value, StatusCode) {
        let url = format!("/indexes/{}", self.uid);
        self.delete_request(&url)
    }

    pub fn search(&mut self, query: &str) -> (Value, StatusCode) {
        let url = format!("/indexes/{}/search?{}", self.uid, query);
        self.get_request(&url)
    }

    pub fn get_all_updates_status(&mut self) -> (Value, StatusCode) {
        let url = format!("/indexes/{}/updates", self.uid);
        self.get_request(&url)
    }

    pub fn get_update_status(&mut self, update_id: u64) -> (Value, StatusCode) {
        let url = format!("/indexes/{}/updates/{}", self.uid, update_id);
        self.get_request(&url)
    }

    pub fn get_all_documents(&mut self) -> (Value, StatusCode) {
        let url = format!("/indexes/{}/documents", self.uid);
        self.get_request(&url)
    }

    pub fn add_or_replace_multiple_documents(&mut self, body: Value) {
        let url = format!("/indexes/{}/documents", self.uid);
        self.post_request_async(&url, body);
    }

    pub fn add_or_update_multiple_documents(&mut self, body: Value) {
        let url = format!("/indexes/{}/documents", self.uid);
        self.put_request_async(&url, body);
    }

    pub fn clear_all_documents(&mut self) {
        let url = format!("/indexes/{}/documents", self.uid);
        self.delete_request_async(&url);
    }

    pub fn get_document(&mut self, document_id: impl ToString) -> (Value, StatusCode) {
        let url = format!("/indexes/{}/documents/{}", self.uid, document_id.to_string());
        self.get_request(&url)
    }

    pub fn delete_document(&mut self, document_id: impl ToString) -> (Value, StatusCode) {
        let url = format!("/indexes/{}/documents/{}", self.uid, document_id.to_string());
        self.delete_request_async(&url)
    }

    pub fn delete_multiple_documents(&mut self, body: Value) {
        let url = format!("/indexes/{}/documents/delete-batch", self.uid);
        self.post_request_async(&url, body);
    }

    pub fn get_all_settings(&mut self) -> (Value, StatusCode) {
        let url = format!("/indexes/{}/settings", self.uid);
        self.get_request(&url)
    }

    pub fn update_all_settings(&mut self, body: Value) {
        let url = format!("/indexes/{}/settings", self.uid);
        self.post_request_async(&url, body);
    }

    pub fn delete_all_settings(&mut self) -> (Value, StatusCode) {
        let url = format!("/indexes/{}/settings", self.uid);
        self.delete_request_async(&url)
    }

    pub fn get_ranking_rules(&mut self) -> (Value, StatusCode) {
        let url = format!("/indexes/{}/settings/ranking-rules", self.uid);
        self.get_request(&url)
    }

    pub fn update_ranking_rules(&mut self, body: Value) {
        let url = format!("/indexes/{}/settings/ranking-rules", self.uid);
        self.post_request_async(&url, body);
    }

    pub fn update_ranking_rules_sync(&mut self, body: Value) -> (Value, StatusCode) {
        let url = format!("/indexes/{}/settings/ranking-rules", self.uid);
        self.post_request(&url, body)
    }

    pub fn delete_ranking_rules(&mut self) -> (Value, StatusCode) {
        let url = format!("/indexes/{}/settings/ranking-rules", self.uid);
        self.delete_request_async(&url)
    }

    pub fn get_distinct_attribute(&mut self) -> (Value, StatusCode) {
        let url = format!("/indexes/{}/settings/distinct-attribute", self.uid);
        self.get_request(&url)
    }

    pub fn update_distinct_attribute(&mut self, body: Value) {
        let url = format!("/indexes/{}/settings/distinct-attribute", self.uid);
        self.post_request_async(&url, body);
    }

    pub fn delete_distinct_attribute(&mut self) -> (Value, StatusCode) {
        let url = format!("/indexes/{}/settings/distinct-attribute", self.uid);
        self.delete_request_async(&url)
    }

    pub fn get_primary_key(&mut self) -> (Value, StatusCode) {
        let url = format!("/indexes/{}/settings/primary_key", self.uid);
        self.get_request(&url)
    }

    pub fn get_searchable_attributes(&mut self) -> (Value, StatusCode) {
        let url = format!("/indexes/{}/settings/searchable-attributes", self.uid);
        self.get_request(&url)
    }

    pub fn update_searchable_attributes(&mut self, body: Value) {
        let url = format!("/indexes/{}/settings/searchable-attributes", self.uid);
        self.post_request_async(&url, body);
    }

    pub fn delete_searchable_attributes(&mut self) -> (Value, StatusCode) {
        let url = format!("/indexes/{}/settings/searchable-attributes", self.uid);
        self.delete_request_async(&url)
    }

    pub fn get_displayed_attributes(&mut self) -> (Value, StatusCode) {
        let url = format!("/indexes/{}/settings/displayed-attributes", self.uid);
        self.get_request(&url)
    }

    pub fn update_displayed_attributes(&mut self, body: Value) {
        let url = format!("/indexes/{}/settings/displayed-attributes", self.uid);
        self.post_request_async(&url, body);
    }

    pub fn delete_displayed_attributes(&mut self) -> (Value, StatusCode) {
        let url = format!("/indexes/{}/settings/displayed-attributes", self.uid);
        self.delete_request_async(&url)
    }

    pub fn get_accept_new_fields(&mut self) -> (Value, StatusCode) {
        let url = format!("/indexes/{}/settings/accept-new-fields", self.uid);
        self.get_request(&url)
    }

    pub fn update_accept_new_fields(&mut self, body: Value) {
        let url = format!("/indexes/{}/settings/accept-new-fields", self.uid);
        self.post_request_async(&url, body);
    }

    pub fn get_synonyms(&mut self) -> (Value, StatusCode) {
        let url = format!("/indexes/{}/settings/synonyms", self.uid);
        self.get_request(&url)
    }

    pub fn update_synonyms(&mut self, body: Value) {
        let url = format!("/indexes/{}/settings/synonyms", self.uid);
        self.post_request_async(&url, body);
    }

    pub fn delete_synonyms(&mut self) -> (Value, StatusCode) {
        let url = format!("/indexes/{}/settings/synonyms", self.uid);
        self.delete_request_async(&url)
    }

    pub fn get_stop_words(&mut self) -> (Value, StatusCode) {
        let url = format!("/indexes/{}/settings/stop-words", self.uid);
        self.get_request(&url)
    }

    pub fn update_stop_words(&mut self, body: Value) {
        let url = format!("/indexes/{}/settings/stop-words", self.uid);
        self.post_request_async(&url, body);
    }

    pub fn delete_stop_words(&mut self) -> (Value, StatusCode) {
        let url = format!("/indexes/{}/settings/stop-words", self.uid);
        self.delete_request_async(&url)
    }

    pub fn get_index_stats(&mut self) -> (Value, StatusCode) {
        let url = format!("/indexes/{}/stats", self.uid);
        self.get_request(&url)
    }

    pub fn list_keys(&mut self) -> (Value, StatusCode) {
        self.get_request("/keys")
    }

    pub fn get_health(&mut self) -> (Value, StatusCode) {
        self.get_request("/health")
    }

    pub fn update_health(&mut self, body: Value) -> (Value, StatusCode) {
        self.put_request("/health", body)
    }

    pub fn get_version(&mut self) -> (Value, StatusCode) {
        self.get_request("/version")
    }

    pub fn get_sys_info(&mut self) -> (Value, StatusCode) {
        self.get_request("/sys-info")
    }

    pub fn get_sys_info_pretty(&mut self) -> (Value, StatusCode) {
        self.get_request("/sys-info/pretty")
    }

    // Populate routes

    pub fn populate_movies(&mut self) {
        let body = json!({
            "uid": "movies",
            "primaryKey": "id",
        });
        self.create_index(body);

        let body = json!({
            "rankingRules": [
                "typo",
                "words",
                "proximity",
                "attribute",
                "wordsPosition",
                "desc(popularity)",
                "exactness",
                "desc(vote_average)",
            ],
            "searchableAttributes": [
                "title",
                "tagline",
                "overview",
                "cast",
                "director",
                "producer",
                "production_companies",
                "genres",
            ],
            "displayedAttributes": [
                "title",
                "director",
                "producer",
                "tagline",
                "genres",
                "id",
                "overview",
                "vote_count",
                "vote_average",
                "poster_path",
                "popularity",
            ],
            "acceptNewFields": false,
        });

        self.update_all_settings(body);

        let dataset = include_bytes!("assets/movies.json");

        let body: Value = serde_json::from_slice(dataset).unwrap();

        self.add_or_replace_multiple_documents(body);
    }

}
