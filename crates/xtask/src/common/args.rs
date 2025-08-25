use clap::Parser;

pub fn default_asset_folder() -> String {
    "./bench/assets/".into()
}

pub fn default_log_filter() -> String {
    "info".into()
}

#[derive(Parser, Debug, Clone)]
pub struct CommonArgs {
    /// Directory to store the remote assets.
    #[arg(long, default_value_t = default_asset_folder())]
    pub asset_folder: String,

    /// Log directives
    #[arg(short, long, default_value_t = default_log_filter())]
    pub log_filter: String,

    /// Authentication bearer for fetching assets
    #[arg(long)]
    pub assets_key: Option<String>,
}
