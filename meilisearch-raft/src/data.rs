use actix_raft::{AppData, AppDataResponse};
use serde::{Serialize, Deserialize};

/// The application's data type.
///
/// Enum types are recommended as typically there will be different types of data mutating
/// requests which will be submitted by application clients.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Data {
    // Your data variants go here.
}

/// The application's data response types.
///
/// Enum types are recommended as typically there will be multiple response types which can be
/// returned from the storage layer.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DataResponse {
    // Your response variants go here.
}

/// This also has a `'static` lifetime constraint, so no `&` references at this time.
/// The new futures & async/await should help out with this quite a lot, so
/// hopefully this constraint will be removed in actix as well.
impl AppData for Data {}

/// This also has a `'static` lifetime constraint, so no `&` references at this time.
impl AppDataResponse for DataResponse {}
