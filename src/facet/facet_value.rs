use ordered_float::OrderedFloat;
use serde::{Serialize, Deserialize};

#[derive(Debug, Clone, PartialOrd, Ord, PartialEq, Eq, Hash)]
#[derive(Serialize, Deserialize)]
pub enum FacetValue {
    String(String),
    Float(OrderedFloat<f64>),
    Integer(i64),
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
        FacetValue::Float(OrderedFloat(float))
    }
}

impl From<OrderedFloat<f64>> for FacetValue {
    fn from(float: OrderedFloat<f64>) -> FacetValue {
        FacetValue::Float(float)
    }
}

impl From<i64> for FacetValue {
    fn from(integer: i64) -> FacetValue {
        FacetValue::Integer(integer)
    }
}
