/// A trait for declaring routes in actix.
///
/// Generate a definition with `#[routes::routes]`.
///
/// Requires that `utoipa::OpenApi` be implemented
#[diagnostic::on_unimplemented(message = "use #[routes::routes] to implement this trait")]
pub trait Routes: utoipa::OpenApi {
    /// Configure the routes for this API.
    fn configure(cfg: &mut actix_web::web::ServiceConfig);
}

pub use routes_macros::{path, request, routes};

#[diagnostic::on_unimplemented(
    message = "use #[routes::path] on the handler function to implement this trait"
)]
pub trait Path: utoipa::Path {
    fn implemented() {}
}

#[diagnostic::on_unimplemented(message = "add request_body parameter to the route handler")]
pub trait PathWithBody {
    fn implemented() {}
}

#[diagnostic::on_unimplemented(message = "use #[routes::request] to implement this trait")]
pub trait RequestBody {
    fn implemented() {}
}

// base implementers

// primitives
impl RequestBody for String {}
impl RequestBody for bool {}
impl RequestBody for u8 {}
impl RequestBody for u16 {}
impl RequestBody for u32 {}
impl RequestBody for u64 {}
impl RequestBody for usize {}
impl RequestBody for f32 {}
impl RequestBody for f64 {}

// std
impl<T: RequestBody> RequestBody for Vec<T> {}
impl<T: RequestBody> RequestBody for Option<T> {}
impl<K: RequestBody> RequestBody for std::collections::BTreeSet<K> {}
impl<K: RequestBody> RequestBody for std::collections::HashSet<K> {}
impl<K: RequestBody, V: RequestBody> RequestBody for std::collections::BTreeMap<K, V> {}

// uuid
impl RequestBody for uuid::Uuid {}
// time
impl RequestBody for time::OffsetDateTime {}
// serde_json
impl RequestBody for serde_json::Value {}
