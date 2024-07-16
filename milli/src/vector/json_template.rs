//! Module to manipulate JSON templates.
//!
//! This module allows two main operations:
//! 1. Render JSON values from a template and a context value.
//! 2. Retrieve data from a template and JSON values.

#![warn(rustdoc::broken_intra_doc_links)]
#![warn(missing_docs)]

use serde::Deserialize;
use serde_json::{Map, Value};

type ValuePath = Vec<PathComponent>;

/// Encapsulates a JSON template and allows injecting and extracting values from it.
#[derive(Debug)]
pub struct ValueTemplate {
    template: Value,
    value_kind: ValueKind,
}

#[derive(Debug)]
enum ValueKind {
    Single(ValuePath),
    Array(ArrayPath),
}

#[derive(Debug)]
struct ArrayPath {
    repeated_value: Value,
    path_to_array: ValuePath,
    value_path_in_array: ValuePath,
}

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

/// Error that occurs when no few value was provided to a template for injection.
#[derive(Debug)]
pub struct MissingValue;

/// Error that occurs when trying to parse a template in [`ValueTemplate::new`]
#[derive(Debug)]
pub enum TemplateParsingError {
    /// A repeat string appears inside a repeated value
    NestedRepeatString(ValuePath),
    /// A repeat string appears outside of an array
    RepeatStringNotInArray(ValuePath),
    /// A repeat string appears in an array, but not in the second position
    BadIndexForRepeatString(ValuePath, usize),
    /// A repeated value lacks a placeholder
    MissingPlaceholderInRepeatedValue(ValuePath),
    /// Multiple repeat string appear in the template
    MultipleRepeatString(ValuePath, ValuePath),
    /// Multiple placeholder strings appear in the template
    MultiplePlaceholderString(ValuePath, ValuePath),
    /// No placeholder string appear in the template
    MissingPlaceholderString,
    /// A placeholder appears both inside a repeated value and outside of it
    BothArrayAndSingle {
        /// Path to the single value
        single_path: ValuePath,
        /// Path to the array of repeated values
        path_to_array: ValuePath,
        /// Path to placeholder inside each repeated value, starting from the array
        array_to_placeholder: ValuePath,
    },
}

impl TemplateParsingError {
    /// Produce an error message from the error kind, the name of the root object, the placeholder string and the repeat string
    pub fn error_message(&self, root: &str, placeholder: &str, repeat: &str) -> String {
        match self {
            TemplateParsingError::NestedRepeatString(path) => {
                format!(
                    r#"in {}: "{repeat}" appears nested inside of a value that is itself repeated"#,
                    path_with_root(root, path)
                )
            }
            TemplateParsingError::RepeatStringNotInArray(path) => format!(
                r#"in {}: "{repeat}" appears outside of an array"#,
                path_with_root(root, path)
            ),
            TemplateParsingError::BadIndexForRepeatString(path, index) => format!(
                r#"in {}: "{repeat}" expected at position #1, but found at position #{index}"#,
                path_with_root(root, path)
            ),
            TemplateParsingError::MissingPlaceholderInRepeatedValue(path) => format!(
                r#"in {}: Expected "{placeholder}" inside of the repeated value"#,
                path_with_root(root, path)
            ),
            TemplateParsingError::MultipleRepeatString(current, previous) => format!(
                r#"in {}: Found "{repeat}", but it was already present in {}"#,
                path_with_root(root, current),
                path_with_root(root, previous)
            ),
            TemplateParsingError::MultiplePlaceholderString(current, previous) => format!(
                r#"in {}: Found "{placeholder}", but it was already present in {}"#,
                path_with_root(root, current),
                path_with_root(root, previous)
            ),
            TemplateParsingError::MissingPlaceholderString => {
                format!(r#"in `{root}`: "{placeholder}" not found"#)
            }
            TemplateParsingError::BothArrayAndSingle {
                single_path,
                path_to_array,
                array_to_placeholder,
            } => {
                let path_to_first_repeated = path_to_array
                    .iter()
                    .chain(std::iter::once(&PathComponent::ArrayIndex(0)))
                    .chain(array_to_placeholder.iter());
                format!(
                    r#"in {}: Found "{placeholder}", but it was already present in {} (repeated)"#,
                    path_with_root(root, single_path),
                    path_with_root(root, path_to_first_repeated)
                )
            }
        }
    }

    fn prepend_path(self, mut prepended_path: ValuePath) -> Self {
        match self {
            TemplateParsingError::NestedRepeatString(mut path) => {
                prepended_path.append(&mut path);
                TemplateParsingError::NestedRepeatString(prepended_path)
            }
            TemplateParsingError::RepeatStringNotInArray(mut path) => {
                prepended_path.append(&mut path);
                TemplateParsingError::RepeatStringNotInArray(prepended_path)
            }
            TemplateParsingError::BadIndexForRepeatString(mut path, index) => {
                prepended_path.append(&mut path);
                TemplateParsingError::BadIndexForRepeatString(prepended_path, index)
            }
            TemplateParsingError::MissingPlaceholderInRepeatedValue(mut path) => {
                prepended_path.append(&mut path);
                TemplateParsingError::MissingPlaceholderInRepeatedValue(prepended_path)
            }
            TemplateParsingError::MultipleRepeatString(mut path, older_path) => {
                let older_prepended_path =
                    prepended_path.iter().cloned().chain(older_path).collect();
                prepended_path.append(&mut path);
                TemplateParsingError::MultipleRepeatString(prepended_path, older_prepended_path)
            }
            TemplateParsingError::MultiplePlaceholderString(mut path, older_path) => {
                let older_prepended_path =
                    prepended_path.iter().cloned().chain(older_path).collect();
                prepended_path.append(&mut path);
                TemplateParsingError::MultiplePlaceholderString(
                    prepended_path,
                    older_prepended_path,
                )
            }
            TemplateParsingError::MissingPlaceholderString => {
                TemplateParsingError::MissingPlaceholderString
            }
            TemplateParsingError::BothArrayAndSingle {
                single_path,
                mut path_to_array,
                array_to_placeholder,
            } => {
                // note, this case is not super logical, but is also likely to be dead code
                let single_prepended_path =
                    prepended_path.iter().cloned().chain(single_path).collect();
                prepended_path.append(&mut path_to_array);
                // we don't prepend the array_to_placeholder path as it is the array path that is prepended
                TemplateParsingError::BothArrayAndSingle {
                    single_path: single_prepended_path,
                    path_to_array: prepended_path,
                    array_to_placeholder,
                }
            }
        }
    }
}

/// Error that occurs when [`ValueTemplate::extract`] fails.
#[derive(Debug)]
pub struct ExtractionError {
    /// The cause of the failure
    pub kind: ExtractionErrorKind,
    /// The context where the failure happened: the operation that failed
    pub context: ExtractionErrorContext,
}

impl ExtractionError {
    /// Produce an error message from the error, the name of the root object, the placeholder string and the expected value type
    pub fn error_message(
        &self,
        root: &str,
        placeholder: &str,
        expected_value_type: &str,
    ) -> String {
        let context = match &self.context {
            ExtractionErrorContext::ExtractingSingleValue => {
                format!(r#"extracting a single "{placeholder}""#)
            }
            ExtractionErrorContext::FindingPathToArray => {
                format!(r#"extracting the array of "{placeholder}"s"#)
            }
            ExtractionErrorContext::ExtractingArrayItem(index) => {
                format!(r#"extracting item #{index} from the array of "{placeholder}"s"#)
            }
        };
        match &self.kind {
            ExtractionErrorKind::MissingPathComponent { missing_index, path, key_suggestion } => {
                let last_named_object = last_named_object(root, path.iter().take(*missing_index));
                format!(
                    "in {}, while {context}, configuration expects {}, which is missing in response{}",
                    path_with_root(root, path.iter().take(*missing_index)),
                    missing_component(path.get(*missing_index)),
                    match key_suggestion {
                        Some(key_suggestion) => format!("\n  - Hint: {last_named_object} has key `{key_suggestion}`, did you mean {} in embedder configuration?",
                        path_with_root(root, path.iter().take(*missing_index).chain(std::iter::once(&PathComponent::MapKey(key_suggestion.to_owned()))))),
                        None => "".to_owned(),
                    }
                )
            }
            ExtractionErrorKind::WrongPathComponent { wrong_component, index, path } => {
                let last_named_object = last_named_object(root, path.iter().take(*index));
                format!(
                    "in {}, while {context}, configuration expects {last_named_object} to be {} but server sent {wrong_component}",
                    path_with_root(root, path.iter().take(*index)),
                    expected_component(path.get(*index))
                )
            }
            ExtractionErrorKind::DeserializationError { error, path } => {
                let last_named_object = last_named_object(root, path);
                format!(
                    "in {}, while {context}, expected {last_named_object} to be {expected_value_type}, but failed to parse server response:\n  - {error}",
                    path_with_root(root, path)
                )
            }
        }
    }
}

fn missing_component(component: Option<&PathComponent>) -> String {
    match component {
        Some(PathComponent::ArrayIndex(index)) => {
            format!(r#"item #{index}"#)
        }
        Some(PathComponent::MapKey(key)) => {
            format!(r#"key "{key}""#)
        }
        None => "unknown".to_string(),
    }
}

fn expected_component(component: Option<&PathComponent>) -> String {
    match component {
        Some(PathComponent::ArrayIndex(index)) => {
            format!(r#"an array with at least {} item(s)"#, index.saturating_add(1))
        }
        Some(PathComponent::MapKey(key)) => {
            format!("an object with key `{}`", key)
        }
        None => "unknown".to_string(),
    }
}

fn last_named_object<'a>(
    root: &'a str,
    path: impl IntoIterator<Item = &'a PathComponent> + 'a,
) -> LastNamedObject<'a> {
    let mut last_named_object = LastNamedObject::Object { name: root };
    for component in path.into_iter() {
        last_named_object = match (component, last_named_object) {
            (PathComponent::MapKey(name), _) => LastNamedObject::Object { name },
            (PathComponent::ArrayIndex(index), LastNamedObject::Object { name }) => {
                LastNamedObject::ArrayInsideObject { object_name: name, index: *index }
            }
            (
                PathComponent::ArrayIndex(index),
                LastNamedObject::ArrayInsideObject { object_name, index: _ },
            ) => LastNamedObject::NestedArrayInsideObject {
                object_name,
                index: *index,
                nesting_level: 0,
            },
            (
                PathComponent::ArrayIndex(index),
                LastNamedObject::NestedArrayInsideObject { object_name, index: _, nesting_level },
            ) => LastNamedObject::NestedArrayInsideObject {
                object_name,
                index: *index,
                nesting_level: nesting_level.saturating_add(1),
            },
        }
    }
    last_named_object
}

impl<'a> std::fmt::Display for LastNamedObject<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LastNamedObject::Object { name } => write!(f, "`{name}`"),
            LastNamedObject::ArrayInsideObject { object_name, index } => {
                write!(f, "item #{index} inside `{object_name}`")
            }
            LastNamedObject::NestedArrayInsideObject { object_name, index, nesting_level } => {
                if *nesting_level == 0 {
                    write!(f, "item #{index} inside nested array in `{object_name}`")
                } else {
                    write!(f, "item #{index} inside nested array ({} levels of nesting) in `{object_name}`", nesting_level + 1)
                }
            }
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum LastNamedObject<'a> {
    Object { name: &'a str },
    ArrayInsideObject { object_name: &'a str, index: usize },
    NestedArrayInsideObject { object_name: &'a str, index: usize, nesting_level: usize },
}

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

/// Context where an extraction failure happened
///
/// The operation that failed
#[derive(Debug, Clone, Copy)]
pub enum ExtractionErrorContext {
    /// Failure happened while extracting a value at a single location
    ExtractingSingleValue,
    /// Failure happened while extracting an array of values
    FindingPathToArray,
    /// Failure happened while extracting a value inside of an array
    ExtractingArrayItem(usize),
}

/// Kind of errors that can happen during extraction
#[derive(Debug)]
pub enum ExtractionErrorKind {
    /// An expected path component is missing
    MissingPathComponent {
        /// Index of the missing component in the path
        missing_index: usize,
        /// Path where a component is missing
        path: ValuePath,
        /// Possible matching key in object
        key_suggestion: Option<String>,
    },
    /// An expected path component cannot be found because its container is the wrong type
    WrongPathComponent {
        /// String representation of the wrong component
        wrong_component: String,
        /// Index of the wrong component in the path
        index: usize,
        /// Path where a component has the wrong type
        path: ValuePath,
    },
    /// Could not deserialize an extracted value to its requested type
    DeserializationError {
        /// inner deserialization error
        error: serde_json::Error,
        /// path to extracted value
        path: ValuePath,
    },
}

enum ArrayParsingContext<'a> {
    Nested,
    NotNested(&'a mut Option<ArrayPath>),
}

impl ValueTemplate {
    /// Prepare a template for injection or extraction.
    ///
    /// # Parameters
    ///
    /// - `template`: JSON value that acts a template. Its placeholder values will be replaced by actual values during injection,
    ///   and actual values will be recovered from their location during extraction.
    /// - `placeholder_string`: Value that a JSON string should assume to act as a placeholder value that can be injected into or
    ///   extracted from.
    /// - `repeat_string`: Sentinel value that can be placed as the second value in an array to indicate that the first value can be repeated
    ///   any number of times. The first value should contain exactly one placeholder string.
    ///
    /// # Errors
    ///
    /// - [`TemplateParsingError`]: refer to the documentation of this type
    pub fn new(
        template: Value,
        placeholder_string: &str,
        repeat_string: &str,
    ) -> Result<Self, TemplateParsingError> {
        let mut value_path = None;
        let mut array_path = None;
        let mut current_path = Vec::new();
        Self::parse_value(
            &template,
            placeholder_string,
            repeat_string,
            &mut value_path,
            &mut ArrayParsingContext::NotNested(&mut array_path),
            &mut current_path,
        )?;

        let value_kind = match (array_path, value_path) {
            (None, None) => return Err(TemplateParsingError::MissingPlaceholderString),
            (None, Some(value_path)) => ValueKind::Single(value_path),
            (Some(array_path), None) => ValueKind::Array(array_path),
            (Some(array_path), Some(value_path)) => {
                return Err(TemplateParsingError::BothArrayAndSingle {
                    single_path: value_path,
                    path_to_array: array_path.path_to_array,
                    array_to_placeholder: array_path.value_path_in_array,
                })
            }
        };

        Ok(Self { template, value_kind })
    }

    /// Whether there is a placeholder that can be repeated.
    ///
    /// - During injection, all values are injected in the array placeholder,
    /// - During extraction, all repeatable placeholders are extracted from the array.
    pub fn has_array_value(&self) -> bool {
        matches!(self.value_kind, ValueKind::Array(_))
    }

    /// Render a value from the template and context values.
    ///
    /// # Error
    ///
    /// - [`MissingValue`]: if the number of injected values is 0.
    pub fn inject(&self, values: impl IntoIterator<Item = Value>) -> Result<Value, MissingValue> {
        let mut rendered = self.template.clone();
        let mut values = values.into_iter();

        match &self.value_kind {
            ValueKind::Single(injection_path) => {
                let Some(injected_value) = values.next() else { return Err(MissingValue) };
                inject_value(&mut rendered, injection_path, injected_value);
            }
            ValueKind::Array(ArrayPath { repeated_value, path_to_array, value_path_in_array }) => {
                // 1. build the array of repeated values
                let mut array = Vec::new();
                for injected_value in values {
                    let mut repeated_value = repeated_value.clone();
                    inject_value(&mut repeated_value, value_path_in_array, injected_value);
                    array.push(repeated_value);
                }

                if array.is_empty() {
                    return Err(MissingValue);
                }
                // 2. inject at the injection point in the rendered value
                inject_value(&mut rendered, path_to_array, Value::Array(array));
            }
        }

        Ok(rendered)
    }

    /// Extract sub values from the template and a value.
    ///
    /// # Errors
    ///
    /// - if a single placeholder is missing.
    /// - if there is no value corresponding to an array placeholder
    /// - if the value corresponding to an array placeholder is not an array
    pub fn extract<T>(&self, mut value: Value) -> Result<Vec<T>, ExtractionError>
    where
        T: for<'de> Deserialize<'de>,
    {
        Ok(match &self.value_kind {
            ValueKind::Single(extraction_path) => {
                let extracted_value =
                    extract_value(extraction_path, &mut value).with_context(|kind| {
                        ExtractionError {
                            kind,
                            context: ExtractionErrorContext::ExtractingSingleValue,
                        }
                    })?;
                vec![extracted_value]
            }
            ValueKind::Array(ArrayPath {
                repeated_value: _,
                path_to_array,
                value_path_in_array,
            }) => {
                // get the array
                let array = extract_value(path_to_array, &mut value).with_context(|kind| {
                    ExtractionError { kind, context: ExtractionErrorContext::FindingPathToArray }
                })?;
                let array = match array {
                    Value::Array(array) => array,
                    not_array => {
                        let mut path = path_to_array.clone();
                        path.push(PathComponent::ArrayIndex(0));
                        return Err(ExtractionError {
                            kind: ExtractionErrorKind::WrongPathComponent {
                                wrong_component: format_value(&not_array),
                                index: path_to_array.len(),
                                path,
                            },
                            context: ExtractionErrorContext::FindingPathToArray,
                        });
                    }
                };
                let mut extracted_values = Vec::with_capacity(array.len());

                for (index, mut item) in array.into_iter().enumerate() {
                    let extracted_value = extract_value(value_path_in_array, &mut item)
                        .with_context(|kind| ExtractionError {
                            kind,
                            context: ExtractionErrorContext::ExtractingArrayItem(index),
                        })?;
                    extracted_values.push(extracted_value);
                }

                extracted_values
            }
        })
    }

    fn parse_array(
        array: &[Value],
        placeholder_string: &str,
        repeat_string: &str,
        value_path: &mut Option<ValuePath>,
        mut array_path: &mut ArrayParsingContext,
        current_path: &mut ValuePath,
    ) -> Result<(), TemplateParsingError> {
        // two modes for parsing array.
        match array {
            // 1. array contains a repeat string in second position
            [first, second, rest @ ..] if second == repeat_string => {
                let ArrayParsingContext::NotNested(array_path) = &mut array_path else {
                    return Err(TemplateParsingError::NestedRepeatString(current_path.clone()));
                };
                if let Some(array_path) = array_path {
                    return Err(TemplateParsingError::MultipleRepeatString(
                        current_path.clone(),
                        array_path.path_to_array.clone(),
                    ));
                }
                if first == repeat_string {
                    return Err(TemplateParsingError::BadIndexForRepeatString(
                        current_path.clone(),
                        0,
                    ));
                }
                if let Some(position) = rest.iter().position(|value| value == repeat_string) {
                    let position = position + 2;
                    return Err(TemplateParsingError::BadIndexForRepeatString(
                        current_path.clone(),
                        position,
                    ));
                }

                let value_path_in_array = {
                    let mut value_path = None;
                    let mut current_path_in_array = Vec::new();

                    Self::parse_value(
                        first,
                        placeholder_string,
                        repeat_string,
                        &mut value_path,
                        &mut ArrayParsingContext::Nested,
                        &mut current_path_in_array,
                    )
                    .map_err(|error| error.prepend_path(current_path.to_vec()))?;

                    value_path.ok_or_else(|| {
                        let mut repeated_value_path = current_path.clone();
                        repeated_value_path.push(PathComponent::ArrayIndex(0));
                        TemplateParsingError::MissingPlaceholderInRepeatedValue(repeated_value_path)
                    })?
                };
                **array_path = Some(ArrayPath {
                    repeated_value: first.to_owned(),
                    path_to_array: current_path.clone(),
                    value_path_in_array,
                });
            }
            // 2. array does not contain a repeat string
            array => {
                if let Some(position) = array.iter().position(|value| value == repeat_string) {
                    return Err(TemplateParsingError::BadIndexForRepeatString(
                        current_path.clone(),
                        position,
                    ));
                }
                for (index, value) in array.iter().enumerate() {
                    current_path.push(PathComponent::ArrayIndex(index));
                    Self::parse_value(
                        value,
                        placeholder_string,
                        repeat_string,
                        value_path,
                        array_path,
                        current_path,
                    )?;
                    current_path.pop();
                }
            }
        }
        Ok(())
    }

    fn parse_object(
        object: &Map<String, Value>,
        placeholder_string: &str,
        repeat_string: &str,
        value_path: &mut Option<ValuePath>,
        array_path: &mut ArrayParsingContext,
        current_path: &mut ValuePath,
    ) -> Result<(), TemplateParsingError> {
        for (key, value) in object.iter() {
            current_path.push(PathComponent::MapKey(key.to_owned()));
            Self::parse_value(
                value,
                placeholder_string,
                repeat_string,
                value_path,
                array_path,
                current_path,
            )?;
            current_path.pop();
        }
        Ok(())
    }

    fn parse_value(
        value: &Value,
        placeholder_string: &str,
        repeat_string: &str,
        value_path: &mut Option<ValuePath>,
        array_path: &mut ArrayParsingContext,
        current_path: &mut ValuePath,
    ) -> Result<(), TemplateParsingError> {
        match value {
            Value::String(str) => {
                if placeholder_string == str {
                    if let Some(value_path) = value_path {
                        return Err(TemplateParsingError::MultiplePlaceholderString(
                            current_path.clone(),
                            value_path.clone(),
                        ));
                    }

                    *value_path = Some(current_path.clone());
                }
                if repeat_string == str {
                    return Err(TemplateParsingError::RepeatStringNotInArray(current_path.clone()));
                }
            }
            Value::Null | Value::Bool(_) | Value::Number(_) => {}
            Value::Array(array) => Self::parse_array(
                array,
                placeholder_string,
                repeat_string,
                value_path,
                array_path,
                current_path,
            )?,
            Value::Object(object) => Self::parse_object(
                object,
                placeholder_string,
                repeat_string,
                value_path,
                array_path,
                current_path,
            )?,
        }
        Ok(())
    }
}

fn inject_value(rendered: &mut Value, injection_path: &Vec<PathComponent>, injected_value: Value) {
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

fn extract_value<T>(
    extraction_path: &[PathComponent],
    initial_value: &mut Value,
) -> Result<T, ExtractionErrorKind>
where
    T: for<'de> Deserialize<'de>,
{
    let mut current_value = initial_value;
    for (path_index, extraction_component) in extraction_path.iter().enumerate() {
        current_value = {
            match extraction_component {
                PathComponent::MapKey(key) => {
                    if !current_value.is_object() {
                        return Err(ExtractionErrorKind::WrongPathComponent {
                            wrong_component: format_value(current_value),
                            index: path_index,
                            path: extraction_path.to_vec(),
                        });
                    }
                    if let Some(object) = current_value.as_object_mut() {
                        if !object.contains_key(key) {
                            let typos =
                                levenshtein_automata::LevenshteinAutomatonBuilder::new(2, true)
                                    .build_dfa(key);
                            let mut key_suggestion = None;
                            'check_typos: for (key, _) in object.iter() {
                                match typos.eval(key) {
                                    levenshtein_automata::Distance::Exact(0) => { /* ??? */ }
                                    levenshtein_automata::Distance::Exact(_) => {
                                        key_suggestion = Some(key.to_owned());
                                        break 'check_typos;
                                    }
                                    levenshtein_automata::Distance::AtLeast(_) => continue,
                                }
                            }
                            return Err(ExtractionErrorKind::MissingPathComponent {
                                missing_index: path_index,
                                path: extraction_path.to_vec(),
                                key_suggestion,
                            });
                        }
                        if let Some(value) = object.get_mut(key) {
                            value
                        } else {
                            // borrow checking limit: the borrow checker cannot be convinced that `object` is no longer mutably borrowed on the
                            // `else` branch of the `if let`, so we cannot return MissingPathComponent here.
                            // As a workaround, we checked that the object does not contain the key above, making this `else` unreachable.
                            unreachable!()
                        }
                    } else {
                        // borrow checking limit: the borrow checker cannot be convinced that `current_value` is no longer mutably borrowed
                        // on the `else` branch of the `if let`, so we cannot return WrongPathComponent here.
                        // As a workaround, we checked that the value was not a map above, making this `else` unreachable.
                        unreachable!()
                    }
                }
                PathComponent::ArrayIndex(index) => {
                    if !current_value.is_array() {
                        return Err(ExtractionErrorKind::WrongPathComponent {
                            wrong_component: format_value(current_value),
                            index: path_index,
                            path: extraction_path.to_vec(),
                        });
                    }
                    match current_value.get_mut(index) {
                        Some(value) => value,
                        None => {
                            return Err(ExtractionErrorKind::MissingPathComponent {
                                missing_index: path_index,
                                path: extraction_path.to_vec(),
                                key_suggestion: None,
                            });
                        }
                    }
                }
            }
        };
    }
    serde_json::from_value(current_value.take()).map_err(|error| {
        ExtractionErrorKind::DeserializationError { error, path: extraction_path.to_vec() }
    })
}

trait ExtractionResultErrorContext<T> {
    fn with_context<F>(self, f: F) -> Result<T, ExtractionError>
    where
        F: FnOnce(ExtractionErrorKind) -> ExtractionError;
}

impl<T> ExtractionResultErrorContext<T> for Result<T, ExtractionErrorKind> {
    fn with_context<F>(self, f: F) -> Result<T, ExtractionError>
    where
        F: FnOnce(ExtractionErrorKind) -> ExtractionError,
    {
        match self {
            Ok(t) => Ok(t),
            Err(kind) => Err(f(kind)),
        }
    }
}

#[cfg(test)]
mod test {
    use serde_json::{json, Value};

    use super::{PathComponent, TemplateParsingError, ValueTemplate};

    fn new_template(template: Value) -> Result<ValueTemplate, TemplateParsingError> {
        ValueTemplate::new(template, "{{text}}", "{{..}}")
    }

    #[test]
    fn empty_template() {
        let template = json!({
            "toto": "no template at all",
            "titi": ["this", "will", "not", "work"],
            "tutu": null
        });

        let error = new_template(template.clone()).unwrap_err();
        assert!(matches!(error, TemplateParsingError::MissingPlaceholderString))
    }

    #[test]
    fn single_template() {
        let template = json!({
            "toto": "text",
            "titi": ["this", "will", "still", "{{text}}"],
            "tutu": null
        });

        let basic = new_template(template.clone()).unwrap();

        assert!(!basic.has_array_value());

        assert_eq!(
            basic.inject(vec!["work".into(), Value::Null, "test".into()]).unwrap(),
            json!({
                "toto": "text",
                "titi": ["this", "will", "still", "work"],
                "tutu": null
            })
        );
    }

    #[test]
    fn too_many_placeholders() {
        let template = json!({
            "toto": "{{text}}",
            "titi": ["this", "will", "still", "{{text}}"],
            "tutu": "text"
        });

        match new_template(template.clone()) {
            Err(TemplateParsingError::MultiplePlaceholderString(left, right)) => {
                assert_eq!(
                    left,
                    vec![PathComponent::MapKey("titi".into()), PathComponent::ArrayIndex(3)]
                );

                assert_eq!(right, vec![PathComponent::MapKey("toto".into())])
            }
            _ => panic!("should error"),
        }
    }

    #[test]
    fn dynamic_template() {
        let template = json!({
            "toto": "text",
            "titi": [{
                "type": "text",
                "data": "{{text}}"
            }, "{{..}}"],
            "tutu": null
        });

        let basic = new_template(template.clone()).unwrap();

        assert!(basic.has_array_value());

        let injected_values = vec![
            "work".into(),
            Value::Null,
            42.into(),
            "test".into(),
            "tata".into(),
            "titi".into(),
            "tutu".into(),
        ];

        let rendered = basic.inject(injected_values.clone()).unwrap();

        assert_eq!(
            rendered,
            json!({
                "toto": "text",
                "titi": [
                    {
                        "type": "text",
                        "data": "work"
                    },
                    {
                        "type": "text",
                        "data": Value::Null
                    },
                    {
                        "type": "text",
                        "data": 42
                    },
                    {
                        "type": "text",
                        "data": "test"
                    },
                    {
                        "type": "text",
                        "data": "tata"
                    },
                    {
                        "type": "text",
                        "data": "titi"
                    },
                    {
                        "type": "text",
                        "data": "tutu"
                    }
                ],
                "tutu": null
            })
        );

        let extracted_values: Vec<Value> = basic.extract(rendered).unwrap();
        assert_eq!(extracted_values, injected_values);
    }
}
