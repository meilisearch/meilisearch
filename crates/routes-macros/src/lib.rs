mod path;
mod request;
mod routes;

use path::try_path;
use proc_macro::TokenStream;
use request::try_request;
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
/// `request_body` is proxied to `utoipa::path`, and is also mandatory for handlers with methods POST, PUT or PATCH.
///
/// The targeted type must implement the `routes::RequestBody` trait (see [`request`] for a macro to implement this trait).
///
/// For handlers with methods POST, PUT, or PATCH **that don't have a body**, use `no_request_body`.
/// Note that bodiless handlers with these methods are discouraged, as a body cannot be added later in a backward-compatible fashion.
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

/// Configure a type to be used as a Meilisearch request type, wrapping deserr, serde and utoipa ToSchema.
///
/// Applying this macro to the struct or enum used as the `request_body` type for PATCH/PUT/POST handlers is mandatory.
///
/// By default, this macro has the following effects:
///
/// - Derives deserr with `error = DeserrJsonError`, `deny_unknown_fields` and `rename_all = camelCase`
/// - Derives ToSchema with `rename_all = "camelCase"`
///
/// Additionally, each parameter must specify its error code, whether it is default or required
///
/// # Item parameters
///
/// ## `allow_unknown_fields`
///
/// Allow unknown fields for `deserr` and `serde`. The default is to deny.
///
/// ## `deny_unknown_fields`
///
/// Deny unknown fields for `deserr` and `serde`. Default.
///
/// ## `proxied`
///
/// Specifies that this type is proxied, for instance for sharding.
/// This will make the type `serde::Serialize`, so that sharding code can send these types easily.
///
/// Additionally, a `serde` attribute will be applied to the item, with `rename_all = "camelCase"` and `deny_unknown_fields`.
///
/// ## `db`
///
/// Specifies that this type is stored in DB.
/// This will make the type `serde::Serialize` and `serde::Deserialize`. It will also propagate the `rename_all` but **not**
/// the `deny_unknown_fields`, to preserve backward compatibility.
///
/// ## `response`
///
/// Specifies that this type is also used in responses.
/// This will make the type `serde::Serialize` so that the type can be returned as a response. It will also propagate the casing to serde.
///
/// ## `setting`
///
/// Specifies that this type is part of the settings.
/// This will make the type `serde::Serialize` and `serde::Deserialize`, because settings type are stored in DB and proxied.
///
/// Implies `db`, `response` and `proxied`
///
/// ## `override_error`
///
/// Specifies the type of the deserr Error. If not specified, defaults to `DeserrJsonError` (which must be in scope).
///
/// ## `no_error`
///
/// Specifies that there is no deserr Error type.
///
/// ## `where_predicate`
///
/// See deserr's `where_predicate`
///
/// ## `try_from`
///
/// See deserr's try_from. The attribute **is not** applied to serde.
///
/// ## `validate``
///
/// See deserr's `validate`
///
/// ## `serde_bound`
///
/// See serde's `bound`
///
/// # Variant parameters
///
/// ## `rename`
///
/// Rename the variant. Applies to serde, deserr and schema.
///
/// # Field parameters
///
/// ## `default`
///
/// Forwarded to `deserr`. If provided, then the field is optional and assumes its default value, or a specified default value according
/// to the syntax supported by `deserr`.
///
/// Exactly one of `default` or `required` must be present for each field.
///
/// ## `required`
///
/// If provided, then the field is mandatory and assumes no default value.
///
/// ## `schema_default`
///
/// Specifies a default value for use by `schema`. Will be interpreted exactly as `default` in the `schema` attribute.
/// Implies `default`.
///
/// ## `skip`
///
/// Skips the attribute for serde, deserr and ignore the attribute in schema.
///
/// ## `rename`
///
/// Changes the attribute's name for serde, deserr and schema.
///
/// ## `example`
///
/// See schema's `example`
///
/// ## `schema_type`
///
/// Propagates the type to schema's `value_type`, and requires the RequestBody trait on the target
/// of `schema_type` rather than on the natural type of the field;
///
/// ## `serde_with`
///
/// See serde's `with`
///
/// ## `inline`
///
/// See schema's `inline`
///
/// ## `error`
///
/// See deserr's `error`
///
/// ## `missing_field_error`
///
/// See deserr's `missing_field_error`
///
/// ## `try_from`
///
/// See deserr's `try_from`. **Not** propagated to `serde`.
///
/// ## `nullable`
///
/// See schema's `nullable`.
///
/// ## `skip_serializing_if`
///
/// See serde's `skip_serializing_if`.
#[proc_macro_attribute]
pub fn request(attr: TokenStream, item: TokenStream) -> TokenStream {
    match try_request(attr, item) {
        Ok(stream) => stream,
        Err(diag) => diag.emit_as_item_tokens().into(),
    }
}
