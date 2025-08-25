use serde::Deserialize;

use crate::{bench::BenchWorkload, test::TestWorkload};

#[derive(Deserialize)]
#[serde(tag = "type")]
pub enum Workload {
    Bench(BenchWorkload),
    Test(TestWorkload),
}
