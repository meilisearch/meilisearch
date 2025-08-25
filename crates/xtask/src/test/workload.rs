use std::collections::BTreeMap;
use serde::Deserialize;

use crate::common::assets::Asset;

/// A test workload.
/// Not to be confused with [a bench workload](crate::bench::workload::Workload).
#[derive(Deserialize)]
pub struct Workload {
    pub name: String,
    pub assets: BTreeMap<String, Asset>,
}
