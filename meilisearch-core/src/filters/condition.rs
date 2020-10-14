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

#[derive(Debug, PartialEq)]
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
        match value.as_rule() {
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
        }
    }

    pub fn as_str(&self) -> &str {
        self.string
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
        .ok_or_else(|| PestError::new_from_span(
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
            Some(Value::Array(values)) => Ok(values.iter().any(|v| self.match_value(Some(v)))),
            other => Ok(self.match_value(other.as_ref())),
        }
    }

    fn match_value(&self, value: Option<&Value>) -> bool {
        match value {
            Some(Value::String(s)) => {
                let value = self.value.as_str();
                match self.condition {
                    ConditionType::Equal => unicase::eq(value, &s),
                    ConditionType::NotEqual => !unicase::eq(value, &s),
                    _ => false
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
                        return res
                    } 
                } 
                false
            },
            Some(Value::Bool(b)) => {
                if let Some(value) = self.value.as_bool() {
                    let res = match self.condition {
                        ConditionType::Equal => *b == value,
                        ConditionType::NotEqual => *b != value,
                        _ => false
                    };
                    return res
                }
                false
            },
            // if field is not supported (or not found), all values are different from it,
            // so != should always return true in this case.
            _ => self.condition == ConditionType::NotEqual,
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use serde_json::Number;
    use std::cmp::Ordering;

    #[test]
    fn test_number_comp() {
        // test both u64
        let n1 = Number::from(1u64);
        let n2 = Number::from(2u64);
        assert_eq!(Some(Ordering::Less), compare_numbers(&n1, &n2));
        assert_eq!(Some(Ordering::Greater), compare_numbers(&n2, &n1));
        let n1 = Number::from(1u64);
        let n2 = Number::from(1u64);
        assert_eq!(Some(Ordering::Equal), compare_numbers(&n1, &n2));

        // test both i64
        let n1 = Number::from(1i64);
        let n2 = Number::from(2i64);
        assert_eq!(Some(Ordering::Less), compare_numbers(&n1, &n2));
        assert_eq!(Some(Ordering::Greater), compare_numbers(&n2, &n1));
        let n1 = Number::from(1i64);
        let n2 = Number::from(1i64);
        assert_eq!(Some(Ordering::Equal), compare_numbers(&n1, &n2));

        // test both f64
        let n1 = Number::from_f64(1f64).unwrap();
        let n2 = Number::from_f64(2f64).unwrap();
        assert_eq!(Some(Ordering::Less), compare_numbers(&n1, &n2));
        assert_eq!(Some(Ordering::Greater), compare_numbers(&n2, &n1));
        let n1 = Number::from_f64(1f64).unwrap();
        let n2 = Number::from_f64(1f64).unwrap();
        assert_eq!(Some(Ordering::Equal), compare_numbers(&n1, &n2));

        // test one u64 and one f64
        let n1 = Number::from_f64(1f64).unwrap();
        let n2 = Number::from(2u64);
        assert_eq!(Some(Ordering::Less), compare_numbers(&n1, &n2));
        assert_eq!(Some(Ordering::Greater), compare_numbers(&n2, &n1));

        // equality
        let n1 = Number::from_f64(1f64).unwrap();
        let n2 = Number::from(1u64);
        assert_eq!(Some(Ordering::Equal), compare_numbers(&n1, &n2));
        assert_eq!(Some(Ordering::Equal), compare_numbers(&n2, &n1));

        // float is neg
        let n1 = Number::from_f64(-1f64).unwrap();
        let n2 = Number::from(1u64);
        assert_eq!(Some(Ordering::Less), compare_numbers(&n1, &n2));
        assert_eq!(Some(Ordering::Greater), compare_numbers(&n2, &n1));

        // float is too big
        let n1 = Number::from_f64(std::f64::MAX).unwrap();
        let n2 = Number::from(1u64);
        assert_eq!(Some(Ordering::Greater), compare_numbers(&n1, &n2));
        assert_eq!(Some(Ordering::Less), compare_numbers(&n2, &n1));

        // misc
        let n1 = Number::from_f64(std::f64::MAX).unwrap();
        let n2 = Number::from(std::u64::MAX);
        assert_eq!(Some(Ordering::Greater), compare_numbers(&n1, &n2));
        assert_eq!(Some( Ordering::Less ), compare_numbers(&n2, &n1));
    }
}
