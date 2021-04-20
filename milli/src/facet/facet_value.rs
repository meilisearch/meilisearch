use ordered_float::OrderedFloat;
use serde::{Serialize, Serializer};

#[derive(Debug, Clone, PartialOrd, Ord, PartialEq, Eq, Hash)]
pub enum FacetValue {
    String(String),
    Number(OrderedFloat<f64>),
}

impl From<String> for FacetValue {
    fn from(string: String) -> FacetValue {
        FacetValue::String(string)
    }
}

impl From<&str> for FacetValue {
    fn from(string: &str) -> FacetValue {
        FacetValue::String(string.to_owned())
    }
}

impl From<f64> for FacetValue {
    fn from(float: f64) -> FacetValue {
        FacetValue::Number(OrderedFloat(float))
    }
}

impl From<OrderedFloat<f64>> for FacetValue {
    fn from(float: OrderedFloat<f64>) -> FacetValue {
        FacetValue::Number(float)
    }
}

impl From<i64> for FacetValue {
    fn from(integer: i64) -> FacetValue {
        FacetValue::Number(OrderedFloat(integer as f64))
    }
}

/// We implement Serialize ourselves because we need to always serialize it as a string,
/// JSON object keys must be strings not numbers.
// TODO remove this impl and convert them into string, by hand, when required.
impl Serialize for FacetValue {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            FacetValue::String(string) => serializer.serialize_str(string),
            FacetValue::Number(number) => {
                let string = number.to_string();
                serializer.serialize_str(&string)
            },
        }
    }
}
