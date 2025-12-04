use clap::Parser;
use std::path::PathBuf;

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
}
