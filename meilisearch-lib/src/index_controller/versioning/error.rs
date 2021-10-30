#[derive(thiserror::Error, Debug)]
pub enum VersionFileError {
    #[error("Version file is missing or the previous MeiliSearch engine version was below 0.24.0")]
    MissingVersionFile,
    #[error("Version file is empty")]
    EmptyVersionFile,
    #[error("Version file is malformed")]
    MalformedVersionFile,
    #[error(
        "Expected MeiliSearch engine version: {major}.{minor}.{patch}, current engine version: {}",
        env!("CARGO_PKG_VERSION").to_string()
    )]
    VersionMismatch {
        major: String,
        minor: String,
        patch: String,
    },
}
