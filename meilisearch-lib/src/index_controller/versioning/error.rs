#[derive(thiserror::Error, Debug)]
pub enum VersionFileError {
    #[error("Version file is missing or the previous MeiliSearch engine version was below 0.24.0. Use a dump to update MeiliSearch.")]
    MissingVersionFile,
    #[error("Version file is corrupted and thus MeiliSearch is unable to determine the version of the database.")]
    MalformedVersionFile,
    #[error(
        "Expected MeiliSearch engine version: {major}.{minor}.{patch}, current engine version: {}. To update MeiliSearch use a dump.",
        env!("CARGO_PKG_VERSION").to_string()
    )]
    VersionMismatch {
        major: String,
        minor: String,
        patch: String,
    },
}
