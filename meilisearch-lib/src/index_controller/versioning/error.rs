#[derive(thiserror::Error, Debug)]
pub enum VersionFileError {
    #[error(
        "Meilisearch (v{}) failed to infer the version of the database. Please consider using a dump to load your data.",
        env!("CARGO_PKG_VERSION").to_string()
    )]
    MissingVersionFile,
    #[error("Version file is corrupted and thus Meilisearch is unable to determine the version of the database.")]
    MalformedVersionFile,
    #[error(
        "Expected Meilisearch engine version: {major}.{minor}.{patch}, current engine version: {}. To update Meilisearch use a dump.",
        env!("CARGO_PKG_VERSION").to_string()
    )]
    VersionMismatch {
        major: String,
        minor: String,
        patch: String,
    },
}
