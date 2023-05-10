use std::fmt;
use std::time::Duration;

use actix_http::StatusCode;
use actix_rt::time::sleep;
use meili_snap::json_string;
use serde_json::Value;

use super::service::Service;

#[must_use = "You must use the task you send. Either `wait_for_completion` or drop it."]
pub struct Task<'a> {
    service: &'a Service,
    code: StatusCode,
    value: Value,
}

impl<'a> Task<'a> {
    pub fn new(service: &'a Service, code: StatusCode, value: Value) -> Self {
        Task { service, code, value }
    }

    pub async fn wait_for_completion(self) -> FinishedTask {
        let id = self.value["taskUid"].as_u64().expect(&format!(
            "Tried to wait for completion on a failed task\n{}\n{}",
            self.code,
            json_string!(self.value)
        ));
        let url = format!("/tasks/{}", id);
        for _ in 0..100 {
            let (response, status_code) = self.service.get(&url).await;
            assert_eq!(200, status_code, "response: {}", response);

            if response["status"] == "succeeded" || response["status"] == "failed" {
                return FinishedTask(response);
            }

            // wait 0.5 second.
            sleep(Duration::from_millis(500)).await;
        }
        panic!("Timeout waiting for update id");
    }
}

impl fmt::Display for Task<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "{}", self.code)?;
        writeln!(f, "{}", json_string!(self.value, { ".enqueuedAt" => "[date]" }))
    }
}

pub struct FinishedTask(pub Value);

impl fmt::Display for FinishedTask {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(
            f,
            "{}",
            json_string!(self.0, { ".enqueuedAt" => "[date]", ".startedAt" => "[date]", ".finishedAt" => "[date]", ".duration" => "[duration]" })
        )
    }
}
