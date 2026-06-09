use proc_macro::TokenStream;
use proc_macro2_diagnostics::{Diagnostic, SpanDiagnosticExt};
use quote::quote;
use syn::parse::Parser;
use syn::spanned::Spanned as _;
use syn::{parenthesized, parse_quote, Token};

pub(crate) fn try_request(attr: TokenStream, item: TokenStream) -> Result<TokenStream, Diagnostic> {
    let item_attr = RequestItemAttr::from_token_stream(attr)?;
    let mut item: syn::Item = syn::parse(item)?;
    let (item_name, item_generics, tys) = match &mut item {
        syn::Item::Enum(item) => parse_enum(item, item_attr)?,
        syn::Item::Struct(item) => parse_struct(item, item_attr)?,
        item => {
            return Err(item
                .span()
                .error("#[routes::request] is only supported on enums and structs"))
        }
    };

    let tys_constraints: Vec<_> =
        tys.into_iter().map(|ty| quote!(<#ty as routes::RequestBody>::implemented();)).collect();

    let (impl_generics, ty_generics, where_clause) = item_generics.split_for_impl();
    let expanded = quote! {
        impl #impl_generics routes::RequestBody for #item_name #ty_generics #where_clause {
            fn implemented() {
                #(#tys_constraints)*
            }
        }


        #item

    };

    Ok(TokenStream::from(expanded))
}

fn parse_struct(
    item: &mut syn::ItemStruct,
    mut item_attr: RequestItemAttr,
) -> Result<(&syn::Ident, &syn::Generics, Vec<syn::Type>), Diagnostic> {
    let item_attrs = &mut item.attrs;

    handle_derive(item_attrs, &item_attr)?;

    let fields = &mut item.fields;

    handle_item_attrs(item_attrs, &mut item_attr, matches!(fields, syn::Fields::Named(_)))?;

    let tys = match fields {
        syn::Fields::Named(fields_named) => handle_named_fields(fields_named, &item_attr)?,
        syn::Fields::Unnamed(fields_unnamed) => {
            fields_unnamed.unnamed.iter().map(|field| field.ty.clone()).collect()
        }
        syn::Fields::Unit => {
            vec![]
        }
    };

    Ok((&item.ident, &item.generics, tys))
}

fn parse_enum(
    item: &mut syn::ItemEnum,
    mut item_attr: RequestItemAttr,
) -> Result<(&syn::Ident, &syn::Generics, Vec<syn::Type>), Diagnostic> {
    handle_derive(&mut item.attrs, &item_attr)?;
    handle_item_attrs(&mut item.attrs, &mut item_attr, true /* always named variants */)?;

    let mut tys = Vec::new();

    for variant in &mut item.variants {
        handle_variant_attrs(variant, &item_attr)?;

        match &mut variant.fields {
            syn::Fields::Named(fields_named) => {
                tys.append(&mut handle_named_fields(fields_named, &item_attr)?);
            }
            syn::Fields::Unnamed(fields_unnamed) => {
                tys.extend(fields_unnamed.unnamed.iter().map(|field| field.ty.clone()));
            }
            syn::Fields::Unit => (),
        }
    }

    Ok((&item.ident, &item.generics, tys))
}

fn handle_derive(
    item_attrs: &mut Vec<syn::Attribute>,
    item_attr: &RequestItemAttr,
) -> Result<(), Diagnostic> {
    let derive_attr = item_attrs.iter_mut().find(|attr| attr.path().is_ident("derive"));
    let mut derives: Vec<syn::Path> = Vec::new();
    derives.push(syn::parse_quote!(deserr::Deserr));
    derives.push(syn::parse_quote!(utoipa::ToSchema));
    if item_attr.uses.needs_serialize() {
        derives.push(syn::parse_quote!(serde::Serialize));
    }
    if item_attr.uses.needs_deserialize() {
        derives.push(syn::parse_quote!(serde::Deserialize));
    }

    if let Some(derive_attr) = derive_attr {
        derive_attr.parse_nested_meta(|meta| {
            derives.push(meta.path);
            Ok(())
        })?;

        *derive_attr = syn::parse_quote!(#[derive(#(#derives),*)])
    } else {
        let attr = syn::parse_quote!(#[derive(#(#derives),*)]);

        item_attrs.push(attr);
    };
    Ok(())
}

fn handle_item_attrs(
    item_attrs: &mut Vec<syn::Attribute>,
    item_attr: &mut RequestItemAttr,
    has_named_fields: bool,
) -> Result<(), Diagnostic> {
    for item_attr in item_attrs.iter() {
        let Some(ident) = item_attr.path().get_ident() else {
            continue;
        };
        if ident == "serde" || ident == "deserr" || ident == "schema" {
            return Err(ident.span().error(
                "Unsupported item attribute, pass parameters inside of #[routes::request]",
            ));
        }
    }

    let RequestItemAttr { unknown_fields_policy, error, deserr_attrs, uses, serde_attrs } =
        item_attr;

    serde_attrs.push(quote!(rename_all = "camelCase"));

    let deserr_attr = {
        match unknown_fields_policy {
            UnknownFieldPolicy::Deny(Some(path)) => {
                deserr_attrs.push(quote!(deny_unknown_fields = #path));
                if uses.must_propagate_deny_unknown_fields() {
                    serde_attrs.push(quote!(deny_unknown_fields));
                }
            }
            UnknownFieldPolicy::Deny(None) => {
                deserr_attrs.push(quote!(deny_unknown_fields));

                if uses.must_propagate_deny_unknown_fields() {
                    serde_attrs.push(quote!(deny_unknown_fields));
                }
            }
            UnknownFieldPolicy::Allow => (),
        };

        if let Some(error) = error {
            deserr_attrs.push(quote!(error = #error));
        }
        syn::parse_quote!(#[deserr(rename_all = camelCase, #(#deserr_attrs),*)])
    };
    item_attrs.push(deserr_attr);
    if has_named_fields {
        item_attrs.push(parse_quote!(#[schema(rename_all = "camelCase")]));
    }

    if uses.needs_serde() {
        let attr = syn::parse_quote!(#[serde(#(#serde_attrs),*)]);
        item_attrs.push(attr);
    }
    Ok(())
}

fn handle_variant_attrs(
    variant: &mut syn::Variant,
    item_attr: &RequestItemAttr,
) -> Result<(), Diagnostic> {
    let mut request_attr: Option<syn::Result<RequestVariantAttr>> = None;

    for attr in &variant.attrs {
        let Some(ident) = attr.path().get_ident() else {
            continue;
        };
        if ident == "serde" || ident == "deserr" || ident == "schema" {
            return Err(ident.span().error(
                "Unsupported item attribute, pass parameters inside of #[routes::request]",
            ));
        }
    }

    variant.attrs.retain_mut(|attr| {
        let is_request = attr.path().is_ident("request");
        if !is_request {
            return true;
        }

        let attr_span = attr.span();

        if request_attr.replace(RequestVariantAttr::from_attr(attr)).is_some() {
            request_attr = Some(Err(attr_span.error("Duplicate #[request] attribute").into()));
        }

        false
    });

    let Some(RequestVariantAttr { deserr_attrs, schema_attrs, serde_attrs }) =
        request_attr.transpose()?
    else {
        return Ok(());
    };

    if !schema_attrs.is_empty() {
        variant.attrs.push(parse_quote!(#[schema(#(#schema_attrs),*)]));
    }
    if !deserr_attrs.is_empty() {
        variant.attrs.push(parse_quote!(#[deserr(#(#deserr_attrs),*)]));
    }

    if item_attr.uses.needs_serde() && !serde_attrs.is_empty() {
        variant.attrs.push(parse_quote!(#[serde(#(#serde_attrs),*)]));
    }

    Ok(())
}

fn handle_named_fields(
    fields_named: &mut syn::FieldsNamed,
    item_attr: &RequestItemAttr,
) -> Result<Vec<syn::Type>, Diagnostic> {
    let mut tys = Vec::new();
    for field in &mut fields_named.named {
        let mut request_attr: Option<syn::Result<RequestFieldAttr>> = None;

        for attr in &field.attrs {
            let Some(ident) = attr.path().get_ident() else {
                continue;
            };
            if ident == "serde" || ident == "deserr" || ident == "schema" {
                return Err(ident.span().error(
                    "Unsupported item attribute, pass parameters inside of #[routes::request]",
                ));
            }
        }

        field.attrs.retain_mut(|attr| {
            let is_request = attr.path().is_ident("request");
            if !is_request {
                return true;
            }

            let attr_span = attr.span();

            if request_attr.replace(RequestFieldAttr::from_attr(attr, &field.ty)).is_some() {
                request_attr = Some(Err(attr_span.error("Duplicate #[request] attribute").into()));
            }

            false
        });

        let RequestFieldAttr {
            mut deserr_attrs,
            mut schema_attrs,
            mut serde_attrs,
            default_or_required,
            ty,
        } = request_attr
            .transpose()?
            .ok_or_else(|| field.span().error("Field is missing #[request] attribute"))?;

        if let Some(ty) = ty {
            tys.push(ty);
        }

        match default_or_required {
            DefaultOrRequired::Default { deserr, schema } => {
                let deserr_default = if let Some(deserr) = deserr {
                    if item_attr.uses.needs_deserialize() {
                        return Err(deserr.span().error("#[routes::request] does not support `default = value` when Deserialize is required"));
                    }
                    quote!(default = #deserr)
                } else {
                    quote!(default)
                };

                deserr_attrs.push(deserr_default);

                let schema_default = if let Some(schema) = schema {
                    quote!(required = false, default = #schema)
                } else {
                    quote!(required = false, default)
                };
                schema_attrs.push(schema_default);

                serde_attrs.push(quote!(default));
            }
            DefaultOrRequired::Required => {
                schema_attrs.push(quote!(required = true));
            }
            DefaultOrRequired::Skip => {
                schema_attrs.push(quote!(ignore));
                deserr_attrs.push(quote!(skip));
                serde_attrs.push(quote!(skip));
            }
        }
        if !schema_attrs.is_empty() {
            field.attrs.push(parse_quote!(#[schema(#(#schema_attrs),*)]));
        }
        if !deserr_attrs.is_empty() {
            field.attrs.push(parse_quote!(#[deserr(#(#deserr_attrs),*)]));
        }

        if item_attr.uses.needs_serde() && !serde_attrs.is_empty() {
            field.attrs.push(parse_quote!(#[serde(#(#serde_attrs),*)]));
        }
    }
    Ok(tys)
}

#[allow(clippy::large_enum_variant)]
enum DefaultOrRequired {
    Default { deserr: Option<syn::Expr>, schema: Option<syn::Expr> },
    Required,
    Skip,
}

struct RequestFieldAttr {
    pub deserr_attrs: Vec<proc_macro2::TokenStream>,
    pub schema_attrs: Vec<proc_macro2::TokenStream>,
    pub serde_attrs: Vec<proc_macro2::TokenStream>,
    pub default_or_required: DefaultOrRequired,
    pub ty: Option<syn::Type>,
}

struct RequestVariantAttr {
    pub deserr_attrs: Vec<proc_macro2::TokenStream>,
    pub schema_attrs: Vec<proc_macro2::TokenStream>,
    pub serde_attrs: Vec<proc_macro2::TokenStream>,
}

impl RequestVariantAttr {
    fn from_attr(attr: &mut syn::Attribute) -> syn::Result<Self> {
        let mut deserr_attrs = Vec::new();
        let mut schema_attrs = Vec::new();
        let mut serde_attrs = Vec::new();

        attr.parse_nested_meta(|meta| {
            let param_name = meta
                .path
                .get_ident()
                .ok_or_else(|| meta.path.span().error("unsupported path parameter."))?;

            match param_name {
                ident if ident == "rename" => {
                    let eq: Token![=] = meta.input.parse()?;
                    let lit: syn::LitStr = meta.input.parse()?;
                    schema_attrs.push(quote!(rename #eq #lit));
                    deserr_attrs.push(quote!(rename #eq #lit));
                    serde_attrs.push(quote!(rename #eq #lit));
                }

                _ => {
                    return Err(meta
                        .path
                        .span()
                        .error("unsupported parameter. Supported parameters are: `rename`")
                        .into());
                }
            };
            Ok(())
        })?;

        Ok(Self { deserr_attrs, schema_attrs, serde_attrs })
    }
}

#[derive(Default)]
struct Uses {
    /// type is proxied to other instances (sharding)
    pub proxied: bool,
    /// type is used as a setting type
    pub setting: bool,
    /// type is also used as a response
    pub response: bool,
    /// type is stored in DB
    pub db: bool,
}

impl Uses {
    pub fn needs_serialize(&self) -> bool {
        self.proxied || self.setting || self.response || self.db
    }

    pub fn needs_deserialize(&self) -> bool {
        self.setting || self.db
    }

    pub fn needs_serde(&self) -> bool {
        self.needs_deserialize() || self.needs_serialize()
    }

    pub fn must_propagate_deny_unknown_fields(&self) -> bool {
        // we want leniency from type that we deserialize from db
        !(self.setting || self.db)
    }
}

impl RequestFieldAttr {
    fn from_attr(attr: &mut syn::Attribute, field_type: &syn::Type) -> syn::Result<Self> {
        let mut deserr_attrs = Vec::new();
        let mut schema_attrs = Vec::new();
        let mut serde_attrs = Vec::new();
        let mut res_default_or_required = None;
        let mut ty = Some(field_type.clone());

        attr.parse_nested_meta(|meta| {
            let param_name = meta
                .path
                .get_ident()
                .ok_or_else(|| meta.path.span().error("unsupported path parameter."))?;

            match param_name {
                ident if ident == "default" => {
                    let deserr_default = if meta.input.peek(Token![=]) {
                            meta.value()?;
                            let e: syn::Expr = meta.input.parse()?;
                            Some(e)
                        } else {
                            None
                        };

                    res_default_or_required = match res_default_or_required.take() {
                        Some(DefaultOrRequired::Default { deserr:_, schema }) => Some(DefaultOrRequired::Default { deserr: deserr_default, schema }),
                        Some(DefaultOrRequired::Required | DefaultOrRequired::Skip) => return Err(ident.span().error("option conflicting with `required` or `skip`").into()),
                        None => Some(DefaultOrRequired::Default { deserr: deserr_default, schema: None }),
                    };

                }
                ident if ident == "schema_default" => {
                    let _eq: Token![=] = meta.input.parse()?;
                    let schema_default: syn::Expr = meta.input.parse()?;
                    res_default_or_required = match res_default_or_required.take() {
                        Some(DefaultOrRequired::Default { deserr, schema:_ }) => Some(DefaultOrRequired::Default { deserr, schema: Some(schema_default) }),
                        Some(DefaultOrRequired::Required | DefaultOrRequired::Skip) => return Err(ident.span().error("option conflicting with `required` or `skip`").into()),
                        None => Some(DefaultOrRequired::Default { deserr: None, schema: Some(schema_default) }),
                    };
                }
                ident if ident == "required" => {
                    res_default_or_required = Some(DefaultOrRequired::Required);
                }
                ident if ident == "skip" => {
                    res_default_or_required = Some(DefaultOrRequired::Skip);
                    ty = None;
                }
                ident if ident == "rename" => {
                    let eq: Token![=] = meta.input.parse()?;
                    let lit: syn::LitStr = meta.input.parse()?;
                    schema_attrs.push(quote!(rename #eq #lit));
                    deserr_attrs.push(quote!(rename #eq #lit));
                    serde_attrs.push(quote!(rename #eq #lit));
                }
                ident if ident == "example" => {
                    let eq: Token![=] = meta.input.parse()?;
                    let expr: syn::Expr = meta.input.parse()?;
                    schema_attrs.push(quote!(example #eq #expr));
                }
                ident if ident == "schema_type" => {
                    let eq: Token![=] = meta.input.parse()?;
                    let typ : syn::Type = meta.input.parse()?;
                    if &typ == field_type {
                        return Err(ident.span()
                        .error("redundant `schema_type` attribute.")
                        .span_error(field_type.span(), "type is identical to this field's type")
                        .error("remove the redudant attribute").into());
                    }
                    schema_attrs.push(quote!(value_type #eq #typ));
                    ty = Some(typ);
                }
                ident if ident == "serde_with" => {
                    let eq: Token![=] = meta.input.parse()?;
                    let lit: syn::LitStr = meta.input.parse()?;
                    serde_attrs.push(quote!(with #eq #lit))
                }
                ident if ident == "inline" => {
                    schema_attrs.push(quote!(inline));
                }
                ident if ident == "error" => {
                    let eq: Token![=] = meta.input.parse()?;
                    let typ : syn::Type = meta.input.parse()?;
                    deserr_attrs.push(quote!(error #eq #typ));
                }
                ident if ident == "missing_field_error" => {
                    let eq: Token![=] = meta.input.parse()?;
                    let typ : syn::Type = meta.input.parse()?;
                    deserr_attrs.push(quote!(missing_field_error #eq #typ));
                }
                ident if ident == "try_from" => {
                    let content;
                    parenthesized!(content in meta.input);
                    let param_type: syn::Type = content.parse()?;
                    let eq: Token![=] = meta.input.parse()?;
                    let path: syn::Path = meta.input.parse()?;
                    let arrow: Token![->] = meta.input.parse()?;
                    let return_type: syn::Type= meta.input.parse()?;
                    deserr_attrs.push(quote!(try_from(#param_type) #eq #path #arrow #return_type));
                }
                ident if ident == "nullable" => {
                    let eq: Token![=] = meta.input.parse()?;
                    let val : syn::LitBool = meta.input.parse()?;
                    schema_attrs.push(quote!(nullable #eq #val));
                }
                ident if ident == "skip_serializing_if" => {
                    let eq: Token![=] = meta.input.parse()?;
                    let val : syn::LitStr = meta.input.parse()?;
                    serde_attrs.push(quote!(skip_serializing_if #eq #val));
                }

                _ => {
                    return Err(meta
                    .path
                    .span()
                    .error(
                        "unsupported parameter. Supported parameters are: `default`, `schema_default`, `required`, `skip`, `rename`, `example`, `schema_type`, `inline`, `error`, `missing_field_error`, `try_from`, `nullable`, `skip_serializing_if`, `serde_with`",
                    )
                    .into());
                }
            };
            Ok(())
        })?;

        let default_or_required = res_default_or_required
            .ok_or_else(|| attr.span().error("`required` or `default` is mandatory"))?;

        Ok(Self { deserr_attrs, schema_attrs, serde_attrs, default_or_required, ty })
    }
}

struct RequestItemAttr {
    pub unknown_fields_policy: UnknownFieldPolicy,
    pub error: Option<syn::Type>,
    pub deserr_attrs: Vec<proc_macro2::TokenStream>,
    pub serde_attrs: Vec<proc_macro2::TokenStream>,
    pub uses: Uses,
}

enum UnknownFieldPolicy {
    Deny(Option<syn::Path>),
    Allow,
}

impl Default for RequestItemAttr {
    fn default() -> Self {
        Self {
            unknown_fields_policy: UnknownFieldPolicy::Deny(None),
            error: Some(parse_quote!(DeserrJsonError)),
            deserr_attrs: Default::default(),
            serde_attrs: Default::default(),
            uses: Default::default(),
        }
    }
}

impl RequestItemAttr {
    fn from_token_stream(tokens: TokenStream) -> syn::Result<Self> {
        let mut unknown_fields_policy = UnknownFieldPolicy::Deny(None);
        let mut error = Some(parse_quote!(DeserrJsonError));
        let mut deserr_attrs = Vec::new();
        let mut serde_attrs = Vec::new();
        let mut uses = Default::default();
        let parser = syn::meta::parser(|meta| {
            parse_request_item_attr_arg(
                &mut unknown_fields_policy,
                &mut error,
                &mut deserr_attrs,
                &mut serde_attrs,
                &mut uses,
                meta,
            )
        });

        parser.parse(tokens)?;

        Ok(Self { unknown_fields_policy, error, deserr_attrs, uses, serde_attrs })
    }
}

fn parse_request_item_attr_arg(
    unknown_fields_policy: &mut UnknownFieldPolicy,
    error: &mut Option<syn::Type>,
    deserr_attrs: &mut Vec<proc_macro2::TokenStream>,
    serde_attrs: &mut Vec<proc_macro2::TokenStream>,
    uses: &mut Uses,
    meta: syn::meta::ParseNestedMeta<'_>,
) -> Result<(), syn::Error> {
    let param_name = meta
        .path
        .get_ident()
        .ok_or_else(|| meta.path.span().error("unsupported path parameter."))?;
    match param_name {
        ident if ident == "allow_unknown_fields" => {
            *unknown_fields_policy = UnknownFieldPolicy::Allow;
        }
        ident if ident == "deny_unknown_fields" => {
            *unknown_fields_policy = if meta.input.peek(Token![=]) {
                let _: Token![=] = meta.input.parse()?;
                let path: syn::Path = meta.input.parse()?;
                UnknownFieldPolicy::Deny(Some(path))
            } else {
                UnknownFieldPolicy::Deny(None)
            };
        }
        ident if ident == "override_error" => {
            let _: Token![=] = meta.input.parse()?;
            let typ: syn::Type = meta.input.parse()?;
            *error = Some(typ);
        }
        ident if ident == "no_error" => {
            *error = None;
        }
        ident if ident == "where_predicate" => {
            let eq: Token![=] = meta.input.parse()?;
            let bound: syn::GenericParam = meta.input.parse()?;
            deserr_attrs.push(quote!(where_predicate #eq #bound));
        }
        ident if ident == "try_from" => {
            let content;
            parenthesized!(content in meta.input);
            let param_type: syn::Type = content.parse()?;
            let eq: Token![=] = meta.input.parse()?;
            let path: syn::Path = meta.input.parse()?;
            let arrow: Token![->] = meta.input.parse()?;
            let return_type: syn::Type = meta.input.parse()?;
            deserr_attrs.push(quote!(try_from(#param_type) #eq #path #arrow #return_type));
        }
        ident if ident == "proxied" => {
            uses.proxied = true;
        }
        ident if ident == "setting" => {
            uses.setting = true;
        }
        ident if ident == "response" => {
            uses.response = true;
        }
        ident if ident == "db" => {
            uses.db = true;
        }
        ident if ident == "validate" => {
            let eq: Token![=] = meta.input.parse()?;
            let path: syn::Path = meta.input.parse()?;
            let arrow: Token![->] = meta.input.parse()?;
            let return_type: syn::Type = meta.input.parse()?;
            deserr_attrs.push(quote!(validate #eq #path #arrow #return_type));
        }
        ident if ident == "serde_bound" => {
            if meta.input.peek(Token![=]) {
                let eq: Token![=] = meta.input.parse()?;
                let path_str: syn::LitStr = meta.input.parse()?;
                serde_attrs.push(quote!(bound #eq #path_str));
            } else {
                let content;
                parenthesized!(content in meta.input);
                let content: proc_macro2::TokenStream = content.parse()?;

                serde_attrs.push(quote! {bound(#content)});
            }
        }
        ident => {
            return Err(ident
            .span()
            .error(
                "unsupported parameter. Supported parameters are: `allow_unknown_fields`, `deny_unknown_fields`, `override_error`, `no_error`, `where_predicate`, `try_from`, `proxied`, `response`, `db`, `validate`, `setting`, `serde_bound`",
            )
            .into());
        }
    }
    Ok(())
}
