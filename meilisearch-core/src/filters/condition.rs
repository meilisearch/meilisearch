use std::str::FromStr;
use std::cmp::Ordering;

use crate::error::Error;
use crate::{store::Index, DocumentId, MainT};
use heed::RoTxn;
use meilisearch_schema::{FieldId, Schema};
use pest::error::{Error as PestError, ErrorVariant};
use pest::iterators::Pair;
use serde_json::{Value, Number};
use super::parser::Rule;

#[derive(Debug)]
enum ConditionType {
    Greater,
    Less,
    Equal,
    LessEqual,
    GreaterEqual,
    NotEqual,
}

/// We need to infer type when the filter is constructed
/// and match every possible types it can be parsed into.
#[derive(Debug)]
struct ConditionValue<'a> {
    string: &'a str,
    boolean: Option<bool>,
    number: Option<Number>
}

impl<'a> ConditionValue<'a> {
    pub fn new(value: &Pair<'a, Rule>) -> Self {
        let value = match value.as_rule() {
            Rule::string | Rule::word => {
                let string =  value.as_str();
                let boolean = match value.as_str() {
                    "true" => Some(true),
                    "false" => Some(false),
                    _ => None,
                };
                let number = Number::from_str(value.as_str()).ok();
                ConditionValue { string, boolean, number }
            },
            _ => unreachable!(),
        };
        value
    }

    pub fn as_str(&self) -> &str {
        self.string.as_ref()
    }

    pub fn as_number(&self) -> Option<&Number> {
        self.number.as_ref()
    }

    pub fn as_bool(&self) -> Option<bool> {
        self.boolean
    }
}

#[derive(Debug)]
pub struct Condition<'a> {
    field: FieldId,
    condition: ConditionType,
    value: ConditionValue<'a>
}

fn get_field_value<'a>(schema: &Schema, pair: Pair<'a, Rule>) -> Result<(FieldId, ConditionValue<'a>), Error> {
    let mut items = pair.into_inner();
    // lexing ensures that we at least have a key
    let key = items.next().unwrap();
    let field = schema
        .id(key.as_str())
        .ok_or::<PestError<Rule>>(PestError::new_from_span(
                ErrorVariant::CustomError {
                    message: format!(
                                 "attribute `{}` not found, available attributes are: {}",
                                 key.as_str(),
                                 schema.names().collect::<Vec<_>>().join(", ")
                             ),
                },
                key.as_span()))?;
    let value = ConditionValue::new(&items.next().unwrap());
    Ok((field, value))
}

// undefined behavior with big numbers
fn compare_numbers(lhs: &Number, rhs: &Number) -> Option<Ordering> {
    match (lhs.as_i64(), lhs.as_u64(), lhs.as_f64(),
        rhs.as_i64(), rhs.as_u64(), rhs.as_f64()) {
    //    i64   u64  f64  i64  u64  f64
        (Some(lhs), _, _, Some(rhs), _, _) => lhs.partial_cmp(&rhs),
        (_, Some(lhs), _, _, Some(rhs), _) => lhs.partial_cmp(&rhs),
        (_, _, Some(lhs), _, _, Some(rhs)) => lhs.partial_cmp(&rhs),
        (_, _, _, _, _, _) => None,
    }
}

impl<'a> Condition<'a> {
    pub fn less(
        item: Pair<'a, Rule>,
        schema: &'a Schema,
    ) -> Result<Self, Error> {
        let (field, value) = get_field_value(schema, item)?;
        let condition = ConditionType::Less;
        Ok(Self { field, condition, value })
    }

    pub fn greater(
        item: Pair<'a, Rule>,
        schema: &'a Schema,
    ) -> Result<Self, Error> {
        let (field, value) = get_field_value(schema, item)?;
        let condition = ConditionType::Greater;
        Ok(Self { field, condition, value })
    }

    pub fn neq(
        item: Pair<'a, Rule>,
        schema: &'a Schema,
    ) -> Result<Self, Error> {
        let (field, value) = get_field_value(schema, item)?;
        let condition = ConditionType::NotEqual;
        Ok(Self { field, condition, value })
    }

    pub fn geq(
        item: Pair<'a, Rule>,
        schema: &'a Schema,
    ) -> Result<Self, Error> {
        let (field, value) = get_field_value(schema, item)?;
        let condition = ConditionType::GreaterEqual;
        Ok(Self { field, condition, value })
    }

    pub fn leq(
        item: Pair<'a, Rule>,
        schema: &'a Schema,
    ) -> Result<Self, Error> {
        let (field, value) = get_field_value(schema, item)?;
        let condition = ConditionType::LessEqual;
        Ok(Self { field, condition, value })
    }

    pub fn eq(
        item: Pair<'a, Rule>,
        schema: &'a Schema,
    ) -> Result<Self, Error> {
        let (field, value) = get_field_value(schema, item)?;
        let condition = ConditionType::Equal;
        Ok(Self { field, condition, value })
    }

    pub fn test(
        &self,
        reader: &RoTxn<MainT>,
        index: &Index,
        document_id: DocumentId,
    ) -> Result<bool, Error> {
        match index.document_attribute::<Value>(reader, document_id, self.field)? {
            Some(Value::String(s)) => {
                let value = self.value.as_str();
                match self.condition {
                    ConditionType::Equal => Ok(unicase::eq(value, &s)),
                    ConditionType::NotEqual => Ok(!unicase::eq(value, &s)),
                    _ => Ok(false)
                }
            },
            Some(Value::Number(n)) => { 
                if let Some(value) = self.value.as_number() {
                    if let Some(ord) = compare_numbers(&n, value) {
                        let res =  match self.condition {
                            ConditionType::Equal => ord == Ordering::Equal,
                            ConditionType::NotEqual => ord != Ordering::Equal,
                            ConditionType::GreaterEqual => ord != Ordering::Less,
                            ConditionType::LessEqual => ord != Ordering::Greater,
                            ConditionType::Greater => ord == Ordering::Greater,
                            ConditionType::Less => ord == Ordering::Less,
                        };
                        return Ok(res)
                    } 
                } 
                Ok(false)
            },
            Some(Value::Bool(b)) => {
                if let Some(value) = self.value.as_bool() {
                    return match self.condition {
                        ConditionType::Equal => Ok(b == value),
                        ConditionType::NotEqual => Ok(b != value),
                        _ => Ok(false)
                    }
                }
                Ok(false)
            },
            _ => Ok(false),
        }
    }
}
