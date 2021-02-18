use actix_web::http::StatusCode;
use serde_json::{json, Value};

use super::service::Service;

pub struct Index<'a> {
    pub uid: String,
    pub service: &'a Service,
}

impl Index<'_> {
    pub async fn get(&self) -> (Value, StatusCode) {
        let url = format!("/indexes/{}", self.uid);
        self.service.get(url).await
    }

    pub async fn create<'a>(
        &'a self,
        primary_key: Option<&str>,
    ) -> (Value, StatusCode) {
        let body = json!({
            "uid": self.uid,
            "primaryKey": primary_key,
        });
        self.service.post("/indexes", body).await
    }

    pub async fn update(&self, primary_key: Option<&str>) -> (Value, StatusCode) {
        let body = json!({
            "primaryKey": primary_key,
        });
        let url = format!("/indexes/{}", self.uid);

        self.service.put(url, body).await
    }
}
