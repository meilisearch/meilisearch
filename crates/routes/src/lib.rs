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

pub use routes_macros::{path, routes};

#[diagnostic::on_unimplemented(
    message = "use #[routes::path] on the handler function to implement this trait"
)]
pub trait Path: utoipa::Path {
    fn implemented() {}
}
