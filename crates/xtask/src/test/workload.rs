use serde::Deserialize;
use std::collections::BTreeMap;

use crate::common::assets::Asset;

/// A test workload.
/// Not to be confused with [a bench workload](crate::bench::workload::Workload).
#[derive(Deserialize)]
pub struct TestWorkload {
    pub name: String,
    pub assets: BTreeMap<String, Asset>,
}
