use proc_macro::TokenStream;
use proc_macro2_diagnostics::{Diagnostic, SpanDiagnosticExt};
use quote::quote;
use syn::parse::{Parse as _, Parser};
use syn::spanned::Spanned as _;
use syn::{parenthesized, ItemFn, LitStr, Path, Token};

pub(crate) fn try_path(attr: TokenStream, item: TokenStream) -> Result<TokenStream, Diagnostic> {
    let attr = proc_macro2::TokenStream::from(attr);

    // proxy attributes to utoipa
    let mut proxy_attrs: Vec<proc_macro2::TokenStream> = Vec::new();
    let mut has_security = false;
    let mut override_tag: Option<LitStr> = None;
    let mut request_body: Option<syn::Type> = None;
    let mut has_no_request_body = false;

    if attr.is_empty() {
        return Err(attr.span().error("Attribute list cannot be empty"));
    }

    let attr_parser = syn::meta::parser(|attr_arg| {
        let path = &attr_arg.path;
        if path.is_ident("tag") || path.is_ident("tags") {
            Err(attr_arg
                .path
                .span()
                .error("Unsupported parameter `tag` and `tags`. Use `override_tag` if you don't want to inherit the tag from the API declaration.")
                .into())
        } else if path.is_ident("path") {
            Err(attr_arg
                .path
                .span()
                .error("Unsupported parameter `path`. the path is already specified in the API declaration.")
                .into())
        } else if is_method(path) {
            Err(attr_arg
                .path
                .span()
                .error("Unsupported method parameter. the method is already specified in the API declaration.")
                .into())
        } else if path.is_ident("override_tag") {
            override_tag = Some(attr_arg.value()?.parse()?);

            Ok(())
        } else if path.is_ident("no_request_body") {
            has_no_request_body = true;

            Ok(())
        } else {
            if path.is_ident("security") {
                has_security = true;
            }
            // proxy as-is to utoipa
            let path = &path;

            // we need to parse the right-hand of the attribute to set the parser at the right location.
            // for utoipa::path, we have several possible syntaxes:
            // - ident ()
            // - ident = ".."
            // - ident = expr
            // - request_body special syntax. We'll support `request_body = Type` only, as it is the only one supported in Meilisearch

            let lookahead = attr_arg.input.lookahead1();
            let tokens = if lookahead.peek(syn::token::Paren) {
                // ident ()
                let content;
                parenthesized!(content in attr_arg.input);
                let quote = if path.is_ident("request_body") {
                    let request_body_args: Vec<RequestBodyArg> = content
                        .parse_terminated(RequestBodyArg::parse, Token![,])?
                        .into_iter()
                        .collect();

                    for content in request_body_args.iter() {
                        if let RequestBodyArg::Content { ident: _, eq: _, right_hand } = content {
                            request_body.replace(right_hand.clone());
                        }
                    }

                    quote! { #path(#(#request_body_args),*) }
                } else {
                    let content: proc_macro2::TokenStream = content.parse()?;

                    quote! { #path(#content) }
                };
                quote
            } else if lookahead.peek(Token![=]) {
                // ident = expr
                attr_arg.input.parse::<Token![=]>()?;

                if path.is_ident("request_body") {
                    let right_hand: syn::Type = attr_arg.input.parse()?;
                    request_body.replace(right_hand.clone());

                    quote! {#path = #right_hand }
                } else {
                    let right_hand: syn::Expr = attr_arg.input.parse()?;
                    quote! { #path = #right_hand }
                }
            } else {
                return Err(lookahead.error());
            };
            proxy_attrs.push(tokens);
            Ok(())
        }
    });

    attr_parser.parse2(attr)?;

    let item: ItemFn = syn::parse(item)?;
    if !has_security {
        return Err(item.span().error("the attribute `security` is required."));
    }
    let fun_name = &item.sig.ident;
    let struct_name = quote::format_ident!("__path_{fun_name}");

    let path_with_body = if let Some(request_body) = request_body {
        quote! {
            impl routes::PathWithBody for #struct_name {
                fn implemented() {
                    <#request_body as routes::RequestBody>::implemented();
                }
            }
        }
    } else if has_no_request_body {
        quote! {
            impl routes::PathWithBody for #struct_name {}
        }
    } else {
        quote! {}
    };

    let expanded = quote! {
        #[utoipa::path(
            post,
            path = "",
            tags = [#override_tag],
            #(#proxy_attrs),*
        )]
        #item

        impl routes::Path for #struct_name {}
        #path_with_body
    };

    Ok(TokenStream::from(expanded))
}

enum RequestBodyArg {
    Content { ident: syn::Ident, eq: syn::token::Eq, right_hand: syn::Type },
    Other { ident: syn::Ident, eq: syn::token::Eq, right_hand: proc_macro2::TokenStream },
}

impl quote::ToTokens for RequestBodyArg {
    fn to_tokens(&self, tokens: &mut proc_macro2::TokenStream) {
        match self {
            RequestBodyArg::Content { ident, eq, right_hand } => {
                ident.to_tokens(tokens);
                eq.to_tokens(tokens);
                right_hand.to_tokens(tokens);
            }
            RequestBodyArg::Other { ident, eq, right_hand } => {
                ident.to_tokens(tokens);
                eq.to_tokens(tokens);
                right_hand.to_tokens(tokens);
            }
        }
    }
}

impl syn::parse::Parse for RequestBodyArg {
    fn parse(input: syn::parse::ParseStream) -> syn::Result<Self> {
        let ident: syn::Ident = input.parse()?;
        let eq = input.parse()?;

        Ok(if ident == "content" {
            let right_hand = input.parse()?;
            Self::Content { ident, eq, right_hand }
        } else {
            let right_hand = input.parse()?;
            Self::Other { ident, eq, right_hand }
        })
    }
}

fn is_method(path: &Path) -> bool {
    path.is_ident("get")
        || path.is_ident("post")
        || path.is_ident("patch")
        || path.is_ident("put")
        || path.is_ident("delete")
        || path.is_ident("head")
        || path.is_ident("options")
        || path.is_ident("trace")
        || path.is_ident("method")
}
