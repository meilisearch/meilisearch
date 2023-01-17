use std::fmt;
use std::marker::PhantomData;
use std::str::FromStr;

use deserr::{DeserializeError, DeserializeFromValue, MergeWithError, ValueKind};
use serde::de::Visitor;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::deserr::query_params::FromQueryParameter;
use crate::error::unwrap_any;

/// A type that tries to match either a star (*) or
/// any other thing that implements `FromStr`.
#[derive(Debug, Clone)]
pub enum StarOr<T> {
    Star,
    Other(T),
}

impl<T: FromStr> FromStr for StarOr<T> {
    type Err = T::Err;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.trim() == "*" {
            Ok(StarOr::Star)
        } else {
            T::from_str(s).map(StarOr::Other)
        }
    }
}
impl<T: fmt::Display> fmt::Display for StarOr<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StarOr::Star => write!(f, "*"),
            StarOr::Other(x) => fmt::Display::fmt(x, f),
        }
    }
}

impl<T: PartialEq> PartialEq for StarOr<T> {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Star, Self::Star) => true,
            (Self::Other(left), Self::Other(right)) if left.eq(right) => true,
            _ => false,
        }
    }
}

impl<T: PartialEq + Eq> Eq for StarOr<T> {}

impl<'de, T, E> Deserialize<'de> for StarOr<T>
where
    T: FromStr<Err = E>,
    E: fmt::Display,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        /// Serde can't differentiate between `StarOr::Star` and `StarOr::Other` without a tag.
        /// Simply using `#[serde(untagged)]` + `#[serde(rename="*")]` will lead to attempting to
        /// deserialize everything as a `StarOr::Other`, including "*".
        /// [`#[serde(other)]`](https://serde.rs/variant-attrs.html#other) might have helped but is
        /// not supported on untagged enums.
        struct StarOrVisitor<T>(PhantomData<T>);

        impl<'de, T, FE> Visitor<'de> for StarOrVisitor<T>
        where
            T: FromStr<Err = FE>,
            FE: fmt::Display,
        {
            type Value = StarOr<T>;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("a string")
            }

            fn visit_str<SE>(self, v: &str) -> Result<Self::Value, SE>
            where
                SE: serde::de::Error,
            {
                match v {
                    "*" => Ok(StarOr::Star),
                    v => {
                        let other = FromStr::from_str(v).map_err(|e: T::Err| {
                            SE::custom(format!("Invalid `other` value: {}", e))
                        })?;
                        Ok(StarOr::Other(other))
                    }
                }
            }
        }

        deserializer.deserialize_str(StarOrVisitor(PhantomData))
    }
}

impl<T> Serialize for StarOr<T>
where
    T: ToString,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            StarOr::Star => serializer.serialize_str("*"),
            StarOr::Other(other) => serializer.serialize_str(&other.to_string()),
        }
    }
}

impl<T, E> DeserializeFromValue<E> for StarOr<T>
where
    T: FromStr,
    E: DeserializeError + MergeWithError<T::Err>,
{
    fn deserialize_from_value<V: deserr::IntoValue>(
        value: deserr::Value<V>,
        location: deserr::ValuePointerRef,
    ) -> Result<Self, E> {
        match value {
            deserr::Value::String(v) => {
                if v == "*" {
                    Ok(StarOr::Star)
                } else {
                    match T::from_str(&v) {
                        Ok(parsed) => Ok(StarOr::Other(parsed)),
                        Err(e) => Err(unwrap_any(E::merge(None, e, location))),
                    }
                }
            }
            _ => Err(unwrap_any(E::error::<V>(
                None,
                deserr::ErrorKind::IncorrectValueKind {
                    actual: value,
                    accepted: &[ValueKind::String],
                },
                location,
            ))),
        }
    }
}

/// A type representing the content of a query parameter that can either not exist,
/// be equal to a star (*), or another value
///
/// It is a convenient alternative to `Option<StarOr<T>>`.
#[derive(Debug, Default, Clone, Copy)]
pub enum OptionStarOr<T> {
    #[default]
    None,
    Star,
    Other(T),
}

impl<T> OptionStarOr<T> {
    pub fn is_some(&self) -> bool {
        match self {
            Self::None => false,
            Self::Star => false,
            Self::Other(_) => true,
        }
    }
    pub fn merge_star_and_none(self) -> Option<T> {
        match self {
            Self::None | Self::Star => None,
            Self::Other(x) => Some(x),
        }
    }
    pub fn try_map<U, E, F: Fn(T) -> Result<U, E>>(self, map_f: F) -> Result<OptionStarOr<U>, E> {
        match self {
            OptionStarOr::None => Ok(OptionStarOr::None),
            OptionStarOr::Star => Ok(OptionStarOr::Star),
            OptionStarOr::Other(x) => map_f(x).map(OptionStarOr::Other),
        }
    }
}

impl<T> FromQueryParameter for OptionStarOr<T>
where
    T: FromQueryParameter,
{
    type Err = T::Err;
    fn from_query_param(p: &str) -> Result<Self, Self::Err> {
        match p {
            "*" => Ok(OptionStarOr::Star),
            s => T::from_query_param(s).map(OptionStarOr::Other),
        }
    }
}

impl<T, E> DeserializeFromValue<E> for OptionStarOr<T>
where
    E: DeserializeError + MergeWithError<T::Err>,
    T: FromQueryParameter,
{
    fn deserialize_from_value<V: deserr::IntoValue>(
        value: deserr::Value<V>,
        location: deserr::ValuePointerRef,
    ) -> Result<Self, E> {
        match value {
            deserr::Value::String(s) => match s.as_str() {
                "*" => Ok(OptionStarOr::Star),
                s => match T::from_query_param(s) {
                    Ok(x) => Ok(OptionStarOr::Other(x)),
                    Err(e) => Err(unwrap_any(E::merge(None, e, location))),
                },
            },
            _ => Err(unwrap_any(E::error::<V>(
                None,
                deserr::ErrorKind::IncorrectValueKind {
                    actual: value,
                    accepted: &[ValueKind::String],
                },
                location,
            ))),
        }
    }
}

/// A type representing the content of a query parameter that can either not exist, be equal to a star (*), or represent a list of other values
#[derive(Debug, Default, Clone)]
pub enum OptionStarOrList<T> {
    #[default]
    None,
    Star,
    List(Vec<T>),
}

impl<T> OptionStarOrList<T> {
    pub fn is_some(&self) -> bool {
        match self {
            Self::None => false,
            Self::Star => false,
            Self::List(_) => true,
        }
    }
    pub fn map<U, F: Fn(T) -> U>(self, map_f: F) -> OptionStarOrList<U> {
        match self {
            Self::None => OptionStarOrList::None,
            Self::Star => OptionStarOrList::Star,
            Self::List(xs) => OptionStarOrList::List(xs.into_iter().map(map_f).collect()),
        }
    }
    pub fn try_map<U, E, F: Fn(T) -> Result<U, E>>(
        self,
        map_f: F,
    ) -> Result<OptionStarOrList<U>, E> {
        match self {
            Self::None => Ok(OptionStarOrList::None),
            Self::Star => Ok(OptionStarOrList::Star),
            Self::List(xs) => {
                xs.into_iter().map(map_f).collect::<Result<Vec<_>, _>>().map(OptionStarOrList::List)
            }
        }
    }
    pub fn merge_star_and_none(self) -> Option<Vec<T>> {
        match self {
            Self::None | Self::Star => None,
            Self::List(xs) => Some(xs),
        }
    }
    pub fn push(&mut self, el: T) {
        match self {
            Self::None => *self = Self::List(vec![el]),
            Self::Star => (),
            Self::List(xs) => xs.push(el),
        }
    }
}

impl<T, E> DeserializeFromValue<E> for OptionStarOrList<T>
where
    E: DeserializeError + MergeWithError<T::Err>,
    T: FromQueryParameter,
{
    fn deserialize_from_value<V: deserr::IntoValue>(
        value: deserr::Value<V>,
        location: deserr::ValuePointerRef,
    ) -> Result<Self, E> {
        match value {
            deserr::Value::String(s) => {
                let mut error = None;
                let mut is_star = false;
                // CS::<String>::from_str is infaillible
                let cs = serde_cs::vec::CS::<String>::from_str(&s).unwrap();
                let len_cs = cs.0.len();
                let mut els = vec![];
                for (i, el_str) in cs.into_iter().enumerate() {
                    if el_str == "*" {
                        is_star = true;
                    } else {
                        match T::from_query_param(&el_str) {
                            Ok(el) => {
                                els.push(el);
                            }
                            Err(e) => {
                                let location =
                                    if len_cs > 1 { location.push_index(i) } else { location };
                                error = Some(E::merge(error, e, location)?);
                            }
                        }
                    }
                }
                if let Some(error) = error {
                    return Err(error);
                }

                if is_star {
                    Ok(OptionStarOrList::Star)
                } else {
                    Ok(OptionStarOrList::List(els))
                }
            }
            _ => Err(unwrap_any(E::error::<V>(
                None,
                deserr::ErrorKind::IncorrectValueKind {
                    actual: value,
                    accepted: &[ValueKind::String],
                },
                location,
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_json::{json, Value};

    use super::*;

    #[test]
    fn star_or_serde_roundtrip() {
        fn roundtrip(content: Value, expected: StarOr<String>) {
            let deserialized: StarOr<String> = serde_json::from_value(content.clone()).unwrap();
            assert_eq!(deserialized, expected);
            assert_eq!(content, serde_json::to_value(deserialized).unwrap());
        }

        roundtrip(json!("products"), StarOr::Other("products".to_string()));
        roundtrip(json!("*"), StarOr::Star);
    }
}
