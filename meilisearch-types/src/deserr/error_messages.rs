/*!
This module implements the error messages of deserialization errors.

We try to:
1. Give a human-readable description of where the error originated.
2. Use the correct terms depending on the format of the request (json/query param)
3. Categorise the type of the error (e.g. missing field, wrong value type, unexpected error, etc.)
 */
use deserr::{ErrorKind, IntoValue, ValueKind, ValuePointerRef};

use super::{DeserrJsonError, DeserrQueryParamError};
use crate::error::{Code, ErrorCode};

/// Return a description of the given location in a Json, preceded by the given article.
/// e.g. `at .key1[8].key2`. If the location is the origin, the given article will not be
/// included in the description.
pub fn location_json_description(location: ValuePointerRef, article: &str) -> String {
    fn rec(location: ValuePointerRef) -> String {
        match location {
            ValuePointerRef::Origin => String::new(),
            ValuePointerRef::Key { key, prev } => rec(*prev) + "." + key,
            ValuePointerRef::Index { index, prev } => format!("{}[{index}]", rec(*prev)),
        }
    }
    match location {
        ValuePointerRef::Origin => String::new(),
        _ => {
            format!("{article} `{}`", rec(location))
        }
    }
}

/// Return a description of the list of value kinds for a Json payload.
fn value_kinds_description_json(kinds: &[ValueKind]) -> String {
    // Rank each value kind so that they can be sorted (and deduplicated)
    // Having a predictable order helps with pattern matching
    fn order(kind: &ValueKind) -> u8 {
        match kind {
            ValueKind::Null => 0,
            ValueKind::Boolean => 1,
            ValueKind::Integer => 2,
            ValueKind::NegativeInteger => 3,
            ValueKind::Float => 4,
            ValueKind::String => 5,
            ValueKind::Sequence => 6,
            ValueKind::Map => 7,
        }
    }
    // Return a description of a single value kind, preceded by an article
    fn single_description(kind: &ValueKind) -> &'static str {
        match kind {
            ValueKind::Null => "null",
            ValueKind::Boolean => "a boolean",
            ValueKind::Integer => "a positive integer",
            ValueKind::NegativeInteger => "an integer",
            ValueKind::Float => "a number",
            ValueKind::String => "a string",
            ValueKind::Sequence => "an array",
            ValueKind::Map => "an object",
        }
    }

    fn description_rec(kinds: &[ValueKind], count_items: &mut usize, message: &mut String) {
        let (msg_part, rest): (_, &[ValueKind]) = match kinds {
            [] => (String::new(), &[]),
            [ValueKind::Integer | ValueKind::NegativeInteger, ValueKind::Float, rest @ ..] => {
                ("a number".to_owned(), rest)
            }
            [ValueKind::Integer, ValueKind::NegativeInteger, ValueKind::Float, rest @ ..] => {
                ("a number".to_owned(), rest)
            }
            [ValueKind::Integer, ValueKind::NegativeInteger, rest @ ..] => {
                ("an integer".to_owned(), rest)
            }
            [a] => (single_description(a).to_owned(), &[]),
            [a, rest @ ..] => (single_description(a).to_owned(), rest),
        };

        if rest.is_empty() {
            if *count_items == 0 {
                message.push_str(&msg_part);
            } else if *count_items == 1 {
                message.push_str(&format!(" or {msg_part}"));
            } else {
                message.push_str(&format!(", or {msg_part}"));
            }
        } else {
            if *count_items == 0 {
                message.push_str(&msg_part);
            } else {
                message.push_str(&format!(", {msg_part}"));
            }

            *count_items += 1;
            description_rec(rest, count_items, message);
        }
    }

    let mut kinds = kinds.to_owned();
    kinds.sort_by_key(order);
    kinds.dedup();

    if kinds.is_empty() {
        // Should not happen ideally
        "a different value".to_owned()
    } else {
        let mut message = String::new();
        description_rec(kinds.as_slice(), &mut 0, &mut message);
        message
    }
}

/// Return the JSON string of the value preceded by a description of its kind
fn value_description_with_kind_json(v: &serde_json::Value) -> String {
    match v.kind() {
        ValueKind::Null => "null".to_owned(),
        kind => {
            format!(
                "{}: `{}`",
                value_kinds_description_json(&[kind]),
                serde_json::to_string(v).unwrap()
            )
        }
    }
}

impl<C: Default + ErrorCode> deserr::DeserializeError for DeserrJsonError<C> {
    fn error<V: IntoValue>(
        _self_: Option<Self>,
        error: deserr::ErrorKind<V>,
        location: ValuePointerRef,
    ) -> Result<Self, Self> {
        let mut message = String::new();

        message.push_str(&match error {
            ErrorKind::IncorrectValueKind { actual, accepted } => {
                let expected = value_kinds_description_json(accepted);
                let received = value_description_with_kind_json(&serde_json::Value::from(actual));

                let location = location_json_description(location, " at");

                format!("Invalid value type{location}: expected {expected}, but found {received}")
            }
            ErrorKind::MissingField { field } => {
                let location = location_json_description(location, " inside");
                format!("Missing field `{field}`{location}")
            }
            ErrorKind::UnknownKey { key, accepted } => {
                let location = location_json_description(location, " inside");
                format!(
                    "Unknown field `{}`{location}: expected one of {}",
                    key,
                    accepted
                        .iter()
                        .map(|accepted| format!("`{}`", accepted))
                        .collect::<Vec<String>>()
                        .join(", ")
                )
            }
            ErrorKind::UnknownValue { value, accepted } => {
                let location = location_json_description(location, " at");
                format!(
                    "Unknown value `{}`{location}: expected one of {}",
                    value,
                    accepted
                        .iter()
                        .map(|accepted| format!("`{}`", accepted))
                        .collect::<Vec<String>>()
                        .join(", "),
                )
            }
            ErrorKind::Unexpected { msg } => {
                let location = location_json_description(location, " at");
                format!("Invalid value{location}: {msg}")
            }
        });

        Err(DeserrJsonError::new(message, C::default().error_code()))
    }
}

pub fn immutable_field_error(field: &str, accepted: &[&str], code: Code) -> DeserrJsonError {
    let msg = format!(
        "Immutable field `{field}`: expected one of {}",
        accepted
            .iter()
            .map(|accepted| format!("`{}`", accepted))
            .collect::<Vec<String>>()
            .join(", ")
    );

    DeserrJsonError::new(msg, code)
}

/// Return a description of the given location in query parameters, preceded by the
/// given article. e.g. `at key5[2]`. If the location is the origin, the given article
/// will not be included in the description.
pub fn location_query_param_description(location: ValuePointerRef, article: &str) -> String {
    fn rec(location: ValuePointerRef) -> String {
        match location {
            ValuePointerRef::Origin => String::new(),
            ValuePointerRef::Key { key, prev } => {
                if matches!(prev, ValuePointerRef::Origin) {
                    key.to_owned()
                } else {
                    rec(*prev) + "." + key
                }
            }
            ValuePointerRef::Index { index, prev } => format!("{}[{index}]", rec(*prev)),
        }
    }
    match location {
        ValuePointerRef::Origin => String::new(),
        _ => {
            format!("{article} `{}`", rec(location))
        }
    }
}

impl<C: Default + ErrorCode> deserr::DeserializeError for DeserrQueryParamError<C> {
    fn error<V: IntoValue>(
        _self_: Option<Self>,
        error: deserr::ErrorKind<V>,
        location: ValuePointerRef,
    ) -> Result<Self, Self> {
        let mut message = String::new();

        message.push_str(&match error {
            ErrorKind::IncorrectValueKind { actual, accepted } => {
                let expected = value_kinds_description_query_param(accepted);
                let received = value_description_with_kind_query_param(actual);

                let location = location_query_param_description(location, " for parameter");

                format!("Invalid value type{location}: expected {expected}, but found {received}")
            }
            ErrorKind::MissingField { field } => {
                let location = location_query_param_description(location, " inside");
                format!("Missing parameter `{field}`{location}")
            }
            ErrorKind::UnknownKey { key, accepted } => {
                let location = location_query_param_description(location, " inside");
                format!(
                    "Unknown parameter `{}`{location}: expected one of {}",
                    key,
                    accepted
                        .iter()
                        .map(|accepted| format!("`{}`", accepted))
                        .collect::<Vec<String>>()
                        .join(", ")
                )
            }
            ErrorKind::UnknownValue { value, accepted } => {
                let location = location_query_param_description(location, " for parameter");
                format!(
                    "Unknown value `{}`{location}: expected one of {}",
                    value,
                    accepted
                        .iter()
                        .map(|accepted| format!("`{}`", accepted))
                        .collect::<Vec<String>>()
                        .join(", "),
                )
            }
            ErrorKind::Unexpected { msg } => {
                let location = location_query_param_description(location, " in parameter");
                format!("Invalid value{location}: {msg}")
            }
        });

        Err(DeserrQueryParamError::new(message, C::default().error_code()))
    }
}

/// Return a description of the list of value kinds for query parameters
/// Since query parameters are always treated as strings, we always return
/// "a string" for now.
fn value_kinds_description_query_param(_accepted: &[ValueKind]) -> String {
    "a string".to_owned()
}

fn value_description_with_kind_query_param<V: IntoValue>(actual: deserr::Value<V>) -> String {
    match actual {
        deserr::Value::Null => "null".to_owned(),
        deserr::Value::Boolean(x) => format!("a boolean: `{x}`"),
        deserr::Value::Integer(x) => format!("an integer: `{x}`"),
        deserr::Value::NegativeInteger(x) => {
            format!("an integer: `{x}`")
        }
        deserr::Value::Float(x) => {
            format!("a number: `{x}`")
        }
        deserr::Value::String(x) => {
            format!("a string: `{x}`")
        }
        deserr::Value::Sequence(_) => "multiple values".to_owned(),
        deserr::Value::Map(_) => "multiple parameters".to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use deserr::ValueKind;

    use crate::deserr::error_messages::value_kinds_description_json;

    #[test]
    fn test_value_kinds_description_json() {
        insta::assert_display_snapshot!(value_kinds_description_json(&[]), @"a different value");

        insta::assert_display_snapshot!(value_kinds_description_json(&[ValueKind::Boolean]), @"a boolean");
        insta::assert_display_snapshot!(value_kinds_description_json(&[ValueKind::Integer]), @"a positive integer");
        insta::assert_display_snapshot!(value_kinds_description_json(&[ValueKind::NegativeInteger]), @"an integer");
        insta::assert_display_snapshot!(value_kinds_description_json(&[ValueKind::Integer]), @"a positive integer");
        insta::assert_display_snapshot!(value_kinds_description_json(&[ValueKind::String]), @"a string");
        insta::assert_display_snapshot!(value_kinds_description_json(&[ValueKind::Sequence]), @"an array");
        insta::assert_display_snapshot!(value_kinds_description_json(&[ValueKind::Map]), @"an object");

        insta::assert_display_snapshot!(value_kinds_description_json(&[ValueKind::Integer, ValueKind::Boolean]), @"a boolean or a positive integer");
        insta::assert_display_snapshot!(value_kinds_description_json(&[ValueKind::Null, ValueKind::Integer]), @"null or a positive integer");
        insta::assert_display_snapshot!(value_kinds_description_json(&[ValueKind::Sequence, ValueKind::NegativeInteger]), @"an integer or an array");
        insta::assert_display_snapshot!(value_kinds_description_json(&[ValueKind::Integer, ValueKind::Float]), @"a number");
        insta::assert_display_snapshot!(value_kinds_description_json(&[ValueKind::Integer, ValueKind::Float, ValueKind::NegativeInteger]), @"a number");
        insta::assert_display_snapshot!(value_kinds_description_json(&[ValueKind::Integer, ValueKind::Float, ValueKind::NegativeInteger, ValueKind::Null]), @"null or a number");
        insta::assert_display_snapshot!(value_kinds_description_json(&[ValueKind::Boolean, ValueKind::Integer, ValueKind::Float, ValueKind::NegativeInteger, ValueKind::Null]), @"null, a boolean, or a number");
        insta::assert_display_snapshot!(value_kinds_description_json(&[ValueKind::Null, ValueKind::Boolean, ValueKind::Integer, ValueKind::Float, ValueKind::NegativeInteger, ValueKind::Null]), @"null, a boolean, or a number");
    }
}
