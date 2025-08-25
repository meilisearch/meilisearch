use serde::Deserialize;

use crate::{bench::BenchWorkload, test::TestWorkload};

#[derive(Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "camelCase")]
pub enum Workload {
    Bench(BenchWorkload),
    Test(TestWorkload),
}
