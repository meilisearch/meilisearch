#[derive(Debug)]
pub enum Error {
    Json(serde_json::Error),
}

impl std::error::Error for Error {}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("error de/serializing trace entry:")?;
        match self {
            Error::Json(error) => std::fmt::Display::fmt(&error, f),
        }
    }
}

impl From<serde_json::Error> for Error {
    fn from(value: serde_json::Error) -> Self {
        Self::Json(value)
    }
}
