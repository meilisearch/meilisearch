mod path;
mod routes;

use path::try_path;
use proc_macro::TokenStream;
use routes::try_routes;

/// Configure routes and implement `utoipa::OpenApi` and `routes::Routes` for this struct.
///
/// # Example
///
/// ```rust,ignore
/// #[routes::routes(
///     tag = "Category of the route for the reference API",
///     routes(
///         "" => get(get_root),
///         "/users" => [get(get_users), post(create_user)],
///         "/settings" => sub(settings::SettingsApi),
///     ),
///     // other parameters...
/// )]
/// struct MyApi;
/// ```
///
/// # Parameters
///
/// ## `routes`
///
/// A list of routes to configure.
///
/// Each route is composed of a literal indicating the path of the route, **relative** to where this API is mounted.
/// Then, the `=>` token.
/// Then, one or multiple methods in the form `method(handler)`,
/// where `method` can be `post`, `get`, `patch`, `delete`, `put` or `sub`, and `handler` a function annotated with `#[routes::path]`.
///
/// The special method `sub` can be used only once, and should refer to a struct that is also annotated with `#[routes::routes]`.
///
/// ## `tag`
///
/// The category applied to all routes defined by the API, unless they use the `override_tag` parameter in their `#[routes::path]` macro.
///
/// This category is important as it builds the list of sections available in the online reference.
///
/// ## `paths` and `nest`
///
/// These parameters are generated using the `routes` attribute and cannot be passed directly.
///
/// ## Other attributes
///
/// Any other parameter is proxied to `utoipa::OpenApi` (example: `tags`, `modifiers`, `servers`, `components`, etc.)
#[proc_macro_attribute]
pub fn routes(attr: TokenStream, item: TokenStream) -> TokenStream {
    match try_routes(attr, item) {
        Ok(stream) => stream,
        Err(diag) => diag.emit_as_item_tokens().into(),
    }
}

/// Configure a route handler, wrapping `utoipa::path` and implementing `routes::Path` on a helper structure.
///
/// Applying this macro to a handler is required to use it in a `routes` parameter of the `routes::routes` macro.
///
/// # Example
///
/// ```rust,ignore
/// #[routes::path(
///     override_tag = "If present, replaces the tag defined by the routes::routes macro",
///     security(("Bearer" = ["action.create", "action.*", "*"])),
///     // other parameters...
/// )]
/// pub async fn my_handler(/* ... */) {}
/// ```
///
/// # Parameters
///
/// ## `security`
///
/// `security` is proxied to `utoipa::path`. Contrary to `utoipa::path`, it is mandatory to pass it,
/// even if empty. This enforces that we don't forget it.
///
/// ## `override_tag`
///
/// When present, overrides the tag defined by the API that calls the handler.
///
/// ## `request_body`
///
/// We only support the parenthesized version of `request_body`, in other words:
///
/// - ✅ `request_body(content = MyType<WithGenerics>)`
/// - ❌ `request_body = MyType<WithGenerics>`
///
/// ## method and `path`
///
/// - The method (`get`, `post`, ...) and `path` parameters are recovered
///   from the API that calls the handler and so cannot be passed directly to this macro
///
/// ## Other parameters
///
/// Other parameters are proxied as-is to `utoipa::path`
#[proc_macro_attribute]
pub fn path(attr: TokenStream, item: TokenStream) -> TokenStream {
    match try_path(attr, item) {
        Ok(stream) => stream,
        Err(diag) => diag.emit_as_item_tokens().into(),
    }
}
