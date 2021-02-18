use tempdir::TempDir;
use byte_unit::{Byte, ByteUnit};

use meilisearch_http::data::Data;
use meilisearch_http::option::{Opt, IndexerOpts};

use super::index::Index;
use super::service::Service;

pub struct Server {
    service: Service,
}

impl Server {
    pub async fn new() -> Self {
        let tmp_dir = TempDir::new("meilisearch").unwrap();

        let opt = Opt {
            db_path: tmp_dir.path().join("db"),
            dumps_dir: tmp_dir.path().join("dump"),
            dump_batch_size: 16,
            http_addr: "127.0.0.1:7700".to_owned(),
            master_key: None,
            env: "development".to_owned(),
            no_analytics: true,
            max_mdb_size: Byte::from_unit(4.0, ByteUnit::GiB).unwrap(),
            max_udb_size: Byte::from_unit(4.0, ByteUnit::GiB).unwrap(),
            http_payload_size_limit: Byte::from_unit(10.0, ByteUnit::MiB).unwrap(),
            ssl_cert_path: None,
            ssl_key_path: None,
            ssl_auth_path: None,
            ssl_ocsp_path: None,
            ssl_require_auth: false,
            ssl_resumption: false,
            ssl_tickets: false,
            import_snapshot: None,
            ignore_missing_snapshot: false,
            ignore_snapshot_if_db_exists: false,
            snapshot_dir: ".".into(),
            schedule_snapshot: false,
            snapshot_interval_sec: None,
            import_dump: None,
            indexer_options: IndexerOpts::default(),
        };

        let data = Data::new(opt).unwrap();
        let service = Service(data);

        Server {
            service,
        }
    }

    //pub async fn test_server() -> Self {
        //let mut server = Self::new();

        //let body = json!({
            //"uid": "test",
            //"primaryKey": "id",
        //});

        //server.create_index(body).await;

        //let body = json!({
            ////"rankingRules": [
                ////"typo",
                ////"words",
                ////"proximity",
                ////"attribute",
                ////"wordsPosition",
                ////"exactness",
            ////],
            //"searchableAttributes": [
                //"balance",
                //"picture",
                //"age",
                //"color",
                //"name",
                //"gender",
                //"email",
                //"phone",
                //"address",
                //"about",
                //"registered",
                //"latitude",
                //"longitude",
                //"tags",
            //],
            //"displayedAttributes": [
                //"id",
                //"isActive",
                //"balance",
                //"picture",
                //"age",
                //"color",
                //"name",
                //"gender",
                //"email",
                //"phone",
                //"address",
                //"about",
                //"registered",
                //"latitude",
                //"longitude",
                //"tags",
            //],
        //});

        //server.update_all_settings(body).await;

        //let dataset = include_bytes!("../assets/test_set.json");

        //let body: Value = serde_json::from_slice(dataset).unwrap();

        //server.add_or_replace_multiple_documents(body).await;
        //server
    //}

    //pub fn data(&self) -> &Data {
        //&self.data
    //}

    //pub async fn wait_update_id(&mut self, update_id: u64) {
        //// try 10 times to get status, or panic to not wait forever
        //for _ in 0..10 {
            //let (response, status_code) = self.get_update_status(update_id).await;
            //assert_eq!(status_code, 200);

            //if response["status"] == "processed" || response["status"] == "failed" {
                //// eprintln!("{:#?}", response);
                //return;
            //}

            //delay_for(Duration::from_secs(1)).await;
        //}
        //panic!("Timeout waiting for update id");
    //}

    // Global Http request GET/POST/DELETE async or sync

    //pub async fn post_request_async(&mut self, url: &str, body: Value) -> (Value, StatusCode) {
        //eprintln!("post_request_async: {}", url);

        //let (response, status_code) = self.post_request(url, body).await;
        //eprintln!("response: {}", response);
        //assert!(response["updateId"].as_u64().is_some());
        //self.wait_update_id(response["updateId"].as_u64().unwrap())
            //.await;
        //(response, status_code)
    //}


    //pub async fn put_request_async(&mut self, url: &str, body: Value) -> (Value, StatusCode) {
        //eprintln!("put_request_async: {}", url);

        //let (response, status_code) = self.put_request(url, body).await;
        //assert!(response["updateId"].as_u64().is_some());
        //assert_eq!(status_code, 202);
        //self.wait_update_id(response["updateId"].as_u64().unwrap())
            //.await;
        //(response, status_code)
    //}


    //pub async fn delete_request_async(&mut self, url: &str) -> (Value, StatusCode) {
        //eprintln!("delete_request_async: {}", url);

        //let (response, status_code) = self.delete_request(url).await;
        //assert!(response["updateId"].as_u64().is_some());
        //assert_eq!(status_code, 202);
        //self.wait_update_id(response["updateId"].as_u64().unwrap())
            //.await;
        //(response, status_code)
    //}

    // All Routes

    //pub async fn list_indexes(&mut self) -> (Value, StatusCode) {
        //self.get_request("/indexes").await
    //}

    /// Returns a view to an index. There is no guarantee that the index exists.
    pub fn index<'a>(&'a self, uid: impl AsRef<str>) -> Index<'a> {
        Index {
            uid: uid.as_ref().to_string(),
            service: &self.service,
        }
    }
    //pub async fn search_multi_index(&mut self, query: &str) -> (Value, StatusCode) {
        //let url = format!("/indexes/search?{}", query);
        //self.get_request(&url).await
    //}

    //pub async fn get_index(&mut self) -> (Value, StatusCode) {
        //let url = format!("/indexes/{}", self.uid);
        //self.get_request(&url).await
    //}

    //pub async fn update_index(&mut self, body: Value) -> (Value, StatusCode) {
        //let url = format!("/indexes/{}", self.uid);
        //self.put_request(&url, body).await
    //}

    //pub async fn delete_index(&mut self) -> (Value, StatusCode) {
        //let url = format!("/indexes/{}", self.uid);
        //self.delete_request(&url).await
    //}

    //pub async fn search_get(&mut self, query: &str) -> (Value, StatusCode) {
        //let url = format!("/indexes/{}/search?{}", self.uid, query);
        //self.get_request(&url).await
    //}

    //pub async fn search_post(&mut self, body: Value) -> (Value, StatusCode) {
        //let url = format!("/indexes/{}/search", self.uid);
        //self.post_request(&url, body).await
    //}

    //pub async fn get_all_updates_status(&mut self) -> (Value, StatusCode) {
        //let url = format!("/indexes/{}/updates", self.uid);
        //self.get_request(&url).await
    //}

    //pub async fn get_update_status(&mut self, update_id: u64) -> (Value, StatusCode) {
        //let url = format!("/indexes/{}/updates/{}", self.uid, update_id);
        //self.get_request(&url).await
    //}

    //pub async fn get_all_documents(&mut self) -> (Value, StatusCode) {
        //let url = format!("/indexes/{}/documents", self.uid);
        //self.get_request(&url).await
    //}

    //pub async fn add_or_replace_multiple_documents(&mut self, body: Value) {
        //let url = format!("/indexes/{}/documents", self.uid);
        //self.post_request_async(&url, body).await;
    //}

    //pub async fn add_or_replace_multiple_documents_sync(
        //&mut self,
        //body: Value,
    //) -> (Value, StatusCode) {
        //let url = format!("/indexes/{}/documents", self.uid);
        //self.post_request(&url, body).await
    //}

    //pub async fn add_or_update_multiple_documents(&mut self, body: Value) {
        //let url = format!("/indexes/{}/documents", self.uid);
        //self.put_request_async(&url, body).await;
    //}

    //pub async fn clear_all_documents(&mut self) {
        //let url = format!("/indexes/{}/documents", self.uid);
        //self.delete_request_async(&url).await;
    //}

    //pub async fn get_document(&mut self, document_id: impl ToString) -> (Value, StatusCode) {
        //let url = format!(
            //"/indexes/{}/documents/{}",
            //self.uid,
            //document_id.to_string()
        //);
        //self.get_request(&url).await
    //}

    //pub async fn delete_document(&mut self, document_id: impl ToString) -> (Value, StatusCode) {
        //let url = format!(
            //"/indexes/{}/documents/{}",
            //self.uid,
            //document_id.to_string()
        //);
        //self.delete_request_async(&url).await
    //}

    //pub async fn delete_multiple_documents(&mut self, body: Value) -> (Value, StatusCode) {
        //let url = format!("/indexes/{}/documents/delete-batch", self.uid);
        //self.post_request_async(&url, body).await
    //}

    //pub async fn get_all_settings(&mut self) -> (Value, StatusCode) {
        //let url = format!("/indexes/{}/settings", self.uid);
        //self.get_request(&url).await
    //}

    //pub async fn update_all_settings(&mut self, body: Value) {
        //let url = format!("/indexes/{}/settings", self.uid);
        //self.post_request_async(&url, body).await;
    //}

    //pub async fn update_all_settings_sync(&mut self, body: Value) -> (Value, StatusCode) {
        //let url = format!("/indexes/{}/settings", self.uid);
        //self.post_request(&url, body).await
    //}

    //pub async fn delete_all_settings(&mut self) -> (Value, StatusCode) {
        //let url = format!("/indexes/{}/settings", self.uid);
        //self.delete_request_async(&url).await
    //}

    //pub async fn get_ranking_rules(&mut self) -> (Value, StatusCode) {
        //let url = format!("/indexes/{}/settings/ranking-rules", self.uid);
        //self.get_request(&url).await
    //}

    //pub async fn update_ranking_rules(&mut self, body: Value) {
        //let url = format!("/indexes/{}/settings/ranking-rules", self.uid);
        //self.post_request_async(&url, body).await;
    //}

    //pub async fn update_ranking_rules_sync(&mut self, body: Value) -> (Value, StatusCode) {
        //let url = format!("/indexes/{}/settings/ranking-rules", self.uid);
        //self.post_request(&url, body).await
    //}

    //pub async fn delete_ranking_rules(&mut self) -> (Value, StatusCode) {
        //let url = format!("/indexes/{}/settings/ranking-rules", self.uid);
        //self.delete_request_async(&url).await
    //}

    //pub async fn get_distinct_attribute(&mut self) -> (Value, StatusCode) {
        //let url = format!("/indexes/{}/settings/distinct-attribute", self.uid);
        //self.get_request(&url).await
    //}

    //pub async fn update_distinct_attribute(&mut self, body: Value) {
        //let url = format!("/indexes/{}/settings/distinct-attribute", self.uid);
        //self.post_request_async(&url, body).await;
    //}

    //pub async fn update_distinct_attribute_sync(&mut self, body: Value) -> (Value, StatusCode) {
        //let url = format!("/indexes/{}/settings/distinct-attribute", self.uid);
        //self.post_request(&url, body).await
    //}

    //pub async fn delete_distinct_attribute(&mut self) -> (Value, StatusCode) {
        //let url = format!("/indexes/{}/settings/distinct-attribute", self.uid);
        //self.delete_request_async(&url).await
    //}

    //pub async fn get_primary_key(&mut self) -> (Value, StatusCode) {
        //let url = format!("/indexes/{}/settings/primary_key", self.uid);
        //self.get_request(&url).await
    //}

    //pub async fn get_searchable_attributes(&mut self) -> (Value, StatusCode) {
        //let url = format!("/indexes/{}/settings/searchable-attributes", self.uid);
        //self.get_request(&url).await
    //}

    //pub async fn update_searchable_attributes(&mut self, body: Value) {
        //let url = format!("/indexes/{}/settings/searchable-attributes", self.uid);
        //self.post_request_async(&url, body).await;
    //}

    //pub async fn update_searchable_attributes_sync(&mut self, body: Value) -> (Value, StatusCode) {
        //let url = format!("/indexes/{}/settings/searchable-attributes", self.uid);
        //self.post_request(&url, body).await
    //}

    //pub async fn delete_searchable_attributes(&mut self) -> (Value, StatusCode) {
        //let url = format!("/indexes/{}/settings/searchable-attributes", self.uid);
        //self.delete_request_async(&url).await
    //}

    //pub async fn get_displayed_attributes(&mut self) -> (Value, StatusCode) {
        //let url = format!("/indexes/{}/settings/displayed-attributes", self.uid);
        //self.get_request(&url).await
    //}

    //pub async fn update_displayed_attributes(&mut self, body: Value) {
        //let url = format!("/indexes/{}/settings/displayed-attributes", self.uid);
        //self.post_request_async(&url, body).await;
    //}

    //pub async fn update_displayed_attributes_sync(&mut self, body: Value) -> (Value, StatusCode) {
        //let url = format!("/indexes/{}/settings/displayed-attributes", self.uid);
        //self.post_request(&url, body).await
    //}

    //pub async fn delete_displayed_attributes(&mut self) -> (Value, StatusCode) {
        //let url = format!("/indexes/{}/settings/displayed-attributes", self.uid);
        //self.delete_request_async(&url).await
    //}

    //pub async fn get_attributes_for_faceting(&mut self) -> (Value, StatusCode) {
        //let url = format!("/indexes/{}/settings/attributes-for-faceting", self.uid);
        //self.get_request(&url).await
    //}

    //pub async fn update_attributes_for_faceting(&mut self, body: Value) {
        //let url = format!("/indexes/{}/settings/attributes-for-faceting", self.uid);
        //self.post_request_async(&url, body).await;
    //}

    //pub async fn update_attributes_for_faceting_sync(
        //&mut self,
        //body: Value,
    //) -> (Value, StatusCode) {
        //let url = format!("/indexes/{}/settings/attributes-for-faceting", self.uid);
        //self.post_request(&url, body).await
    //}

    //pub async fn delete_attributes_for_faceting(&mut self) -> (Value, StatusCode) {
        //let url = format!("/indexes/{}/settings/attributes-for-faceting", self.uid);
        //self.delete_request_async(&url).await
    //}

    //pub async fn get_synonyms(&mut self) -> (Value, StatusCode) {
        //let url = format!("/indexes/{}/settings/synonyms", self.uid);
        //self.get_request(&url).await
    //}

    //pub async fn update_synonyms(&mut self, body: Value) {
        //let url = format!("/indexes/{}/settings/synonyms", self.uid);
        //self.post_request_async(&url, body).await;
    //}

    //pub async fn update_synonyms_sync(&mut self, body: Value) -> (Value, StatusCode) {
        //let url = format!("/indexes/{}/settings/synonyms", self.uid);
        //self.post_request(&url, body).await
    //}

    //pub async fn delete_synonyms(&mut self) -> (Value, StatusCode) {
        //let url = format!("/indexes/{}/settings/synonyms", self.uid);
        //self.delete_request_async(&url).await
    //}

    //pub async fn get_stop_words(&mut self) -> (Value, StatusCode) {
        //let url = format!("/indexes/{}/settings/stop-words", self.uid);
        //self.get_request(&url).await
    //}

    //pub async fn update_stop_words(&mut self, body: Value) {
        //let url = format!("/indexes/{}/settings/stop-words", self.uid);
        //self.post_request_async(&url, body).await;
    //}

    //pub async fn update_stop_words_sync(&mut self, body: Value) -> (Value, StatusCode) {
        //let url = format!("/indexes/{}/settings/stop-words", self.uid);
        //self.post_request(&url, body).await
    //}

    //pub async fn delete_stop_words(&mut self) -> (Value, StatusCode) {
        //let url = format!("/indexes/{}/settings/stop-words", self.uid);
        //self.delete_request_async(&url).await
    //}

    //pub async fn get_index_stats(&mut self) -> (Value, StatusCode) {
        //let url = format!("/indexes/{}/stats", self.uid);
        //self.get_request(&url).await
    //}

    //pub async fn list_keys(&mut self) -> (Value, StatusCode) {
        //self.get_request("/keys").await
    //}

    //pub async fn get_health(&mut self) -> (Value, StatusCode) {
        //self.get_request("/health").await
    //}

    //pub async fn update_health(&mut self, body: Value) -> (Value, StatusCode) {
        //self.put_request("/health", body).await
    //}

    //pub async fn get_version(&mut self) -> (Value, StatusCode) {
        //self.get_request("/version").await
    //}

    //pub async fn get_sys_info(&mut self) -> (Value, StatusCode) {
        //self.get_request("/sys-info").await
    //}

    //pub async fn get_sys_info_pretty(&mut self) -> (Value, StatusCode) {
        //self.get_request("/sys-info/pretty").await
    //}

    //pub async fn trigger_dump(&self) -> (Value, StatusCode) {
        //self.post_request("/dumps", Value::Null).await
    //}

    //pub async fn get_dump_status(&mut self, dump_uid: &str) -> (Value, StatusCode) {
        //let url = format!("/dumps/{}/status", dump_uid);
        //self.get_request(&url).await
    //}

    //pub async fn trigger_dump_importation(&mut self, dump_uid: &str) -> (Value, StatusCode) {
        //let url = format!("/dumps/{}/import", dump_uid);
        //self.get_request(&url).await
    //}
}
