use std::fmt::Write as _;

use proc_macro::TokenStream;
use proc_macro2_diagnostics::{Diagnostic, SpanDiagnosticExt};
use quote::quote;
use syn::parse::{Parse, Parser};
use syn::spanned::Spanned as _;
use syn::{bracketed, parenthesized, Ident, ItemStruct, LitStr, Path, Token};

pub(crate) fn try_routes(attr: TokenStream, item: TokenStream) -> Result<TokenStream, Diagnostic> {
    let attr = proc_macro2::TokenStream::from(attr);

    let mut routes = Vec::new();
    let mut proxy_attrs: Vec<proc_macro2::TokenStream> = Vec::new();
    let mut tag: Option<LitStr> = None;

    if attr.is_empty() {
        return Err(attr.span().error("Attribute list cannot be empty"));
    }

    let attr_parser = syn::meta::parser(|attr_arg| {
        if attr_arg.path.is_ident("routes") {
            let content;
            syn::parenthesized!(content in attr_arg.input);
            routes = content.parse_terminated(Route::parse, Token![,])?.into_iter().collect();

            Ok(())
        } else if attr_arg.path.is_ident("tag") {
            tag = Some(attr_arg.value()?.parse()?);

            Ok(())
        } else if attr_arg.path.is_ident("paths") {
            // removed
            Err(attr_arg
                .path
                .span()
                .error("Unsupported parameter `paths`. Use a method in `routes`")
                .into())
        } else if attr_arg.path.is_ident("nest") {
            // removed
            Err(attr_arg
                .path
                .span()
                .error("Unsupported parameter `nest`. Use a `sub` method in `routes`")
                .into())
        } else {
            // proxy as-is to utoipa
            let path = &attr_arg.path;
            // ident ()
            let content;
            parenthesized!(content in attr_arg.input);
            let content: proc_macro2::TokenStream = content.parse()?;

            proxy_attrs.push(quote! { #path(#content) });
            Ok(())
        }
    });

    attr_parser.parse2(attr)?;

    let item: ItemStruct = syn::parse(item)?;
    let struct_name = &item.ident;

    let Some(tag) = tag else {
        return Err(struct_name.span().error("the attribute `tag` is required."));
    };

    let configs: Result<Vec<proc_macro2::TokenStream>, Diagnostic> = routes.iter().map(|route|
{
        let Route { path , get, post, patch, put, delete,sub } = route;

        let tokens = if let Some(sub) = sub {
            quote! {
                actix_web::web::scope(#path).configure(<#sub as routes::Routes>::configure)
            }
        } else {

        let mut tokens = quote! {
            actix_web::web::resource(#path)
        };

        if let Some(get) = get {
            tokens = quote! { #tokens.route(web::get().to(crate::extractors::sequential_extractor::SeqHandler(#get)))};
        }
                if let Some(post) = post {
            tokens = quote! { #tokens.route(web::post().to(crate::extractors::sequential_extractor::SeqHandler(#post)))};
        }
                if let Some(put) = put {
            tokens = quote! { #tokens.route(web::put().to(crate::extractors::sequential_extractor::SeqHandler(#put)))};
        }
                if let Some(patch) = patch {
            tokens = quote! { #tokens.route(web::patch().to(crate::extractors::sequential_extractor::SeqHandler(#patch)))};
        }
                if let Some(delete) = delete {
            tokens = quote! { #tokens.route(web::delete().to(crate::extractors::sequential_extractor::SeqHandler(#delete)))};
        }

        tokens
    };

        Ok(quote! {
            cfg.service(
                #tokens
            );
        })}
    ).collect();

    let configs = configs?;

    let impl_trait = quote! {
        impl routes::Routes for #struct_name {
            fn configure(cfg: &mut actix_web::web::ServiceConfig) {
                #(#configs)*
            }
        }
    };

    let local_path_it = routes
        .iter()
        .filter(|route| route.sub.is_none())
        .flat_map(|route| {
            let Route { path: _, get, post, patch, put, delete, sub: _ } = route;
            get.iter().chain(post.iter()).chain(patch.iter()).chain(put.iter()).chain(delete.iter())
        })
        .map(localized_handler);

    let nest_it = routes.iter().filter_map(|route| {
        let sub = route.sub.as_ref()?;
        let path = &route.path;
        Some(quote! { (path = #path, api = #sub)})
    });

    let local_struct_it = routes
        .iter()
        .filter(|route| route.sub.is_none())
        .flat_map(|route| route.iter_path_handler_method())
        .map(|(path, handler, method)| define_local_handler(path, handler, method, tag.clone()));

    let expanded = quote! {
        #[derive(utoipa::OpenApi)]
        #[openapi(
            paths(#(#local_path_it),*),
            nest(#(#nest_it),*),
            #(#proxy_attrs),*
        )]
        #item

        #impl_trait

        #(#local_struct_it)*
    };

    Ok(TokenStream::from(expanded))
}

fn define_local_handler(
    path: LitStr,
    handler: &Path,
    method: Ident,
    tag: LitStr,
) -> proc_macro2::TokenStream {
    let local_handler = localized_handler(handler);
    let local_handler = quote::format_ident!("__path_{local_handler}");

    let mut struct_handler = handler.clone();

    if let Some(last_ident) = struct_handler.segments.last_mut() {
        last_ident.ident = quote::format_ident!("__path_{}", last_ident.ident);
    }

    quote! {
        struct #local_handler;
        impl<'t> utoipa::__dev::Tags<'t> for #local_handler {
            fn tags() -> Vec<&'t str> {
                let tags = #struct_handler::tags();

                if tags.is_empty() {
                    vec![#tag]
                } else {
                    tags
                }
            }
        }

        impl utoipa::Path for #local_handler {
            fn path() -> String {
                <#struct_handler as routes::Path>::implemented();
                #path.to_string()
            }
            fn methods() -> Vec<utoipa::openapi::path::HttpMethod> {
                [utoipa::openapi::path::HttpMethod::#method].into()
            }
            fn operation() -> utoipa::openapi::path::Operation {
                #struct_handler::operation()
            }
        }

        impl utoipa::__dev::SchemaReferences for #local_handler {
            fn schemas(
                schemas: &mut Vec<(String, utoipa::openapi::RefOr<utoipa::openapi::schema::Schema>)>,
            ) {
                #struct_handler::schemas(schemas)
            }
        }
    }
}

fn localized_handler(handler: &Path) -> Ident {
    let ident = if let Some(ident) = handler.get_ident() {
        format!("__local__handler_{ident}")
    } else {
        let mut ident = "__local".to_string();
        let mut peek = handler.segments.iter().peekable();
        while let Some(component) = peek.next() {
            if peek.peek().is_some() {
                ident.push_str("__mod_");
            } else {
                ident.push_str("__handler_");
            }
            let _ = write!(&mut ident, "{}", component.ident);
        }
        ident
    };
    Ident::new(&ident, handler.span())
}

struct Route {
    path: LitStr,
    get: Option<Path>,
    post: Option<Path>,
    patch: Option<Path>,
    put: Option<Path>,
    delete: Option<Path>,
    sub: Option<Path>,
}

struct RouteComponent {
    method: Ident,
    handler: Path,
}

impl syn::parse::Parse for RouteComponent {
    fn parse(input: syn::parse::ParseStream) -> syn::Result<Self> {
        let method = input.parse()?;
        let content;
        syn::parenthesized!(content in input);
        let handler = content.parse()?;
        Ok(Self { method, handler })
    }
}

impl Route {
    fn new(path: LitStr) -> Self {
        Route { path, get: None, post: None, patch: None, put: None, delete: None, sub: None }
    }

    fn apply_component(&mut self, component: RouteComponent) -> syn::Result<()> {
        let Route { path: _, get, post, patch, put, delete, sub } = self;
        let RouteComponent { method, handler: new_handler } = component;
        let handler = match &method {
            ident if ident == "get" => get,
            ident if ident == "post" => post,
            ident if ident == "patch" => patch,
            ident if ident == "put" => put,
            ident if ident == "delete" => delete,
            ident if ident == "sub" => sub,
            unknown_method => return Err(unknown_method.span().error("Unknown method. Supported methods `get`, `post`, `patch`, `put`, `delete` or `sub` for delegation").into()),
        };

        if let Some(handler) = handler.replace(new_handler.clone()) {
            return Err(new_handler
                .span()
                .error("Duplicate method")
                .span_note(handler.span(), "Previous handler defined here")
                .into());
        }
        Ok(())
    }

    fn check(&self) -> Result<(), Diagnostic> {
        match self {
            Route {path, get: None, post: None, patch: None, put:None, delete:None, sub:None } =>
                Err(path.span().error("Missing method. At least one method is required. Supported methods `get`, `post`, `put`, `patch`, `delete`, or `sub` for delegation")),
            Route {path:_,sub: Some(_), get: None, post:None, patch:None, put:None, delete:None} => Ok(()),
            Route { path:_,sub: None, get:_,post:_,patch:_,put:_,delete:_}=>Ok(()),
            Route {path,sub:Some(_), get:_, post:_, patch:_, put:_, delete:_}=>Err(path.span().error(
                "`sub` and another method are defined. When `sub` is defined, do not define another method"))
        }
    }

    /// Iterates other all (path, handler, method) triples for this route.
    ///
    /// The method is returned as an utoipa-compatible ident.
    fn iter_path_handler_method(&self) -> impl Iterator<Item = (LitStr, &Path, Ident)> {
        let Route { path, get, post, patch, put, delete, sub: _ } = self;
        get.iter()
            .map(|handler| (path.clone(), handler, Ident::new("Get", handler.span())))
            .chain(
                post.iter()
                    .map(|handler| (path.clone(), handler, Ident::new("Post", handler.span()))),
            )
            .chain(
                patch
                    .iter()
                    .map(|handler| (path.clone(), handler, Ident::new("Patch", handler.span()))),
            )
            .chain(
                put.iter()
                    .map(|handler| (path.clone(), handler, Ident::new("Put", handler.span()))),
            )
            .chain(
                delete
                    .iter()
                    .map(|handler| (path.clone(), handler, Ident::new("Delete", handler.span()))),
            )
    }
}

impl syn::parse::Parse for Route {
    fn parse(input: syn::parse::ParseStream) -> syn::Result<Self> {
        let path = input.parse()?;

        let _fat_arrow: syn::token::FatArrow = input.parse()?;

        let mut route = Route::new(path);

        if input.peek(Ident) {
            let route_component = input.parse()?;
            route.apply_component(route_component)?;
            return Ok(route);
        }
        let content;
        bracketed!(content in input);

        for route_component in content.parse_terminated(RouteComponent::parse, Token![,])? {
            route.apply_component(route_component)?;
        }

        route.check()?;

        Ok(route)
    }
}
