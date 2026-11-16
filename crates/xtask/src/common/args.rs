use std::{collections::HashMap, path::PathBuf};

use anyhow::Context as _;
use clap::Parser;

pub fn default_asset_folder() -> String {
    "./bench/assets/".into()
}

pub fn default_log_filter() -> String {
    "info".into()
}

#[derive(Parser, Debug, Clone)]
pub struct CommonArgs {
    /// Filename of the workload file, pass multiple filenames
    /// to run multiple workloads in the specified order.
    ///
    /// For benches, each workload run will get its own report file.
    #[arg(value_name = "WORKLOAD_FILE", last = false)]
    pub workload_file: Vec<PathBuf>,

    /// Directory to store the remote assets.
    #[arg(long, default_value_t = default_asset_folder())]
    pub asset_folder: String,

    /// Log directives
    #[arg(short, long, default_value_t = default_log_filter())]
    pub log_filter: String,

    /// Authentication bearer for fetching assets
    #[arg(long)]
    pub assets_key: Option<String>,

    /// The maximum time in seconds we allow for fetching the task queue before timing out.
    #[arg(long, default_value_t = 60)]
    pub tasks_queue_timeout_secs: u64,

    /// Reuse an existing DB instead of deleting and creating a temporary one.
    ///
    /// This option can break the reentrancy of workloads, so use it with caution.
    #[arg(long, value_name = "DB_PATH")]
    pub reuse_db: Option<PathBuf>,

    /// A list of env variables to hydrate in bodies and expected answers. Variables declared
    /// in bodies or expected answers must be prefixed by a dollar ($) sign.
    #[arg(long)]
    pub env_variables: Vec<String>,
}

pub fn fetch_env_variables(env_keys: Vec<String>) -> anyhow::Result<HashMap<String, String>> {
    env_keys
        .into_iter()
        .map(|env_key| {
            Ok((
                env_key.clone(),
                std::env::var(&env_key)
                    .with_context(|| format!("Cannot find value for the `{env_key}` variable"))?,
            ))
        })
        .collect()
}
