use serde::{Deserialize, Serialize};

use crate::{bench::BenchWorkload, test::TestWorkload};

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "type")]
#[serde(rename_all = "camelCase")]
pub enum Workload {
    Bench(BenchWorkload),
    Test(TestWorkload),
}
