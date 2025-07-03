//! Exposes types to manipulate JSON values
//!
//! - [`JsonTemplate`]: renders JSON values by rendering its strings as [`Template`]s.
//! - [`InjectableValue`]: Describes a JSON value containing placeholders,
//!   then allows to inject values instead of the placeholder to produce new concrete JSON values,
//!   or extract sub-values at the placeholder location from concrete JSON values.
//!
//! The module also exposes foundational types to work with JSON paths:
//!
//! - [`ValuePath`] is made of [`PathComponent`]s to indicate the location of a sub-value inside of a JSON value.
//! - [`inject_value`] is a primitive that replaces the sub-value at the described location by an injected value.

#![warn(rustdoc::broken_intra_doc_links)]
#![warn(missing_docs)]

use bumpalo::Bump;
use liquid::{Parser, Template};
use serde_json::{Map, Value};

use crate::prompt::ParseableDocument;
use crate::update::new::document::Document;

mod injectable_value;

pub use injectable_value::InjectableValue;

/// Represents a JSON [`Value`] where each string is rendered as a [`Template`].
#[derive(Debug)]
pub struct JsonTemplate {
    value: Value,
    templates: Vec<TemplateAtPath>,
}

impl Clone for JsonTemplate {
    fn clone(&self) -> Self {
        Self::new(self.value.clone()).unwrap()
    }
}

struct TemplateAtPath {
    template: Template,
    path: ValuePath,
}

impl std::fmt::Debug for TemplateAtPath {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TemplateAtPath")
            .field("template", &&"template")
            .field("path", &self.path)
            .finish()
    }
}

/// Error that can occur either when parsing the templates in the value, or when trying to render them.
#[derive(Debug)]
pub struct Error {
    template_error: liquid::Error,
    path: ValuePath,
}

impl Error {
    /// Produces an error message when the error happened at rendering time.
    pub fn rendering_error(&self, root: &str) -> String {
        format!(
            "in `{}`, error while rendering template: {}",
            path_with_root(root, self.path.iter()),
            &self.template_error
        )
    }

    /// Produces an error message when the error happened at parsing time.
    pub fn parsing(&self, root: &str) -> String {
        format!(
            "in `{}`, error while parsing template: {}",
            path_with_root(root, self.path.iter()),
            &self.template_error
        )
    }
}

impl JsonTemplate {
    /// Creates a new `JsonTemplate` by parsing all strings inside the value as templates.
    ///
    /// # Error
    ///
    ///  - If any of the strings contains a template that cannot be parsed.
    pub fn new(value: Value) -> Result<Self, Error> {
        let templates = build_templates(&value)?;
        Ok(Self { value, templates })
    }

    /// Renders this value by replacing all its strings with the rendered version of the template they represent from the given context.
    ///
    /// # Error
    ///
    /// - If any of the strings contains a template that cannot be rendered with the given context.
    pub fn render(&self, context: &dyn liquid::ObjectView) -> Result<Value, Error> {
        let mut rendered = self.value.clone();
        for TemplateAtPath { template, path } in &self.templates {
            let injected_value =
                template.render(context).map_err(|err| error_with_path(err, path.clone()))?;
            inject_value(&mut rendered, path, Value::String(injected_value));
        }
        Ok(rendered)
    }

    /// Renders this value by replacing all its strings with the rendered version of the template they represent from the contents of the given document.
    ///
    /// # Error
    ///
    /// - If any of the strings contains a template that cannot be rendered with the given document.
    pub fn render_document<'a, 'doc, D: Document<'a> + std::fmt::Debug>(
        &self,
        document: D,
        doc_alloc: &'doc Bump,
    ) -> Result<Value, Error> {
        let document = ParseableDocument::new(document, doc_alloc);
        let context = crate::prompt::Context::without_fields(&document);
        self.render(&context)
    }

    /// Renders this value by replacing all its strings with the rendered version of the template they represent from the contents of the search query.
    ///
    /// # Error
    ///
    /// - If any of the strings contains a template that cannot be rendered from the contents of the search query
    pub fn render_search(&self, q: Option<&str>, media: Option<&Value>) -> Result<Value, Error> {
        let search_data = match (q, media) {
            (None, None) => liquid::object!({}),
            (None, Some(media)) => liquid::object!({ "media": media }),
            (Some(q), None) => liquid::object!({"q": q}),
            (Some(q), Some(media)) => liquid::object!({"q": q, "media": media}),
        };
        self.render(&search_data)
    }

    /// The JSON value representing the underlying template
    pub fn template(&self) -> &Value {
        &self.value
    }
}

fn build_templates(value: &Value) -> Result<Vec<TemplateAtPath>, Error> {
    let mut current_path = ValuePath::new();
    let mut templates = Vec::new();
    let compiler = liquid::ParserBuilder::with_stdlib().build().unwrap();
    parse_value(value, &mut current_path, &mut templates, &compiler)?;
    Ok(templates)
}

fn error_with_path(template_error: liquid::Error, path: ValuePath) -> Error {
    Error { template_error, path }
}

fn parse_value(
    value: &Value,
    current_path: &mut ValuePath,
    templates: &mut Vec<TemplateAtPath>,
    compiler: &Parser,
) -> Result<(), Error> {
    match value {
        Value::String(template) => {
            let template = compiler
                .parse(template)
                .map_err(|err| error_with_path(err, current_path.clone()))?;
            templates.push(TemplateAtPath { template, path: current_path.clone() });
        }
        Value::Array(values) => {
            parse_array(values, current_path, templates, compiler)?;
        }
        Value::Object(map) => {
            parse_object(map, current_path, templates, compiler)?;
        }
        _ => {}
    }
    Ok(())
}

fn parse_object(
    map: &Map<String, Value>,
    current_path: &mut ValuePath,
    templates: &mut Vec<TemplateAtPath>,
    compiler: &Parser,
) -> Result<(), Error> {
    for (key, value) in map {
        current_path.push(PathComponent::MapKey(key.clone()));
        parse_value(value, current_path, templates, compiler)?;
        current_path.pop();
    }
    Ok(())
}

fn parse_array(
    values: &[Value],
    current_path: &mut ValuePath,
    templates: &mut Vec<TemplateAtPath>,
    compiler: &Parser,
) -> Result<(), Error> {
    for (index, value) in values.iter().enumerate() {
        current_path.push(PathComponent::ArrayIndex(index));
        parse_value(value, current_path, templates, compiler)?;
        current_path.pop();
    }
    Ok(())
}

/// A list of [`PathComponent`]s describing a path to a value inside a JSON value.
///
/// The empty list refers to the root value.
pub type ValuePath = Vec<PathComponent>;

/// Component of a path to a Value
#[derive(Debug, Clone)]
pub enum PathComponent {
    /// A key inside of an object
    MapKey(String),
    /// An index inside of an array
    ArrayIndex(usize),
}

impl PartialEq for PathComponent {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::MapKey(l0), Self::MapKey(r0)) => l0 == r0,
            (Self::ArrayIndex(l0), Self::ArrayIndex(r0)) => l0 == r0,
            _ => false,
        }
    }
}

impl Eq for PathComponent {}

/// Builds a string representation of a path, preprending the name of the root value.
pub fn path_with_root<'a>(
    root: &str,
    path: impl IntoIterator<Item = &'a PathComponent> + 'a,
) -> String {
    use std::fmt::Write as _;
    let mut res = format!("`{root}");
    for component in path.into_iter() {
        match component {
            PathComponent::MapKey(key) => {
                let _ = write!(&mut res, ".{key}");
            }
            PathComponent::ArrayIndex(index) => {
                let _ = write!(&mut res, "[{index}]");
            }
        }
    }
    res.push('`');
    res
}

/// Modifies `rendered` to replace the sub-value at the `injection_path` location by the `injected_value`.
///
/// # Panics
///
/// - if the provided `injection_path` cannot be traversed in `rendered`.
pub fn inject_value(
    rendered: &mut Value,
    injection_path: &Vec<PathComponent>,
    injected_value: Value,
) {
    let mut current_value = rendered;
    for injection_component in injection_path {
        current_value = match injection_component {
            PathComponent::MapKey(key) => current_value.get_mut(key).unwrap(),
            PathComponent::ArrayIndex(index) => current_value.get_mut(index).unwrap(),
        }
    }
    *current_value = injected_value;
}

fn format_value(value: &Value) -> String {
    match value {
        Value::Array(array) => format!("an array of size {}", array.len()),
        Value::Object(object) => {
            format!("an object with {} field(s)", object.len())
        }
        value => value.to_string(),
    }
}
