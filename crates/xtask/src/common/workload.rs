use std::fmt;

use serde::{Deserialize, Serialize};

use crate::bench::BenchWorkload;
use crate::relevancy::RelevancyWorkload;
use crate::test::TestWorkload;

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "type")]
#[serde(rename_all = "camelCase")]
pub enum Workload {
    Bench(BenchWorkload),
    Test(TestWorkload),
    Relevancy(RelevancyWorkload),
}

#[derive(Clone, Copy, Serialize, Deserialize, Debug, Default)]
#[serde(rename_all = "camelCase")]
pub enum EngineType {
    #[default]
    Meilisearch,
    ElasticSearch,
}

impl EngineType {
    pub fn base_url(self) -> String {
        match self {
            EngineType::Meilisearch => "http://127.0.0.1:7700".into(),
            EngineType::ElasticSearch => "http://127.0.0.1:9200".into(),
        }
    }
}

impl fmt::Display for EngineType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            EngineType::Meilisearch => f.write_str("Meilisearch"),
            EngineType::ElasticSearch => f.write_str("Elastic Search"),
        }
    }
}
