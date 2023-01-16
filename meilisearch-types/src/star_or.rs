use std::fmt::{Display, Formatter};
use std::marker::PhantomData;
use std::ops::Deref;
use std::str::FromStr;

use deserr::{DeserializeError, DeserializeFromValue, MergeWithError, ValueKind};
use serde::de::Visitor;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::error::unwrap_any;

/// A type that tries to match either a star (*) or
/// any other thing that implements `FromStr`.
#[derive(Debug, Clone)]
pub enum StarOr<T> {
    Star,
    Other(T),
}

impl<E: DeserializeError, T> DeserializeFromValue<E> for StarOr<T>
where
    T: FromStr,
    E: MergeWithError<T::Err>,
{
    fn deserialize_from_value<V: deserr::IntoValue>(
        value: deserr::Value<V>,
        location: deserr::ValuePointerRef,
    ) -> Result<Self, E> {
        match value {
            deserr::Value::String(v) => match v.as_str() {
                "*" => Ok(StarOr::Star),
                v => match FromStr::from_str(v) {
                    Ok(x) => Ok(StarOr::Other(x)),
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

impl<T: Deref<Target = str>> Deref for StarOr<T> {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        match self {
            Self::Star => "*",
            Self::Other(t) => t.deref(),
        }
    }
}

impl<T: Into<String>> From<StarOr<T>> for String {
    fn from(s: StarOr<T>) -> Self {
        match s {
            StarOr::Star => "*".to_string(),
            StarOr::Other(t) => t.into(),
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
    E: Display,
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
            FE: Display,
        {
            type Value = StarOr<T>;

            fn expecting(&self, formatter: &mut Formatter) -> std::fmt::Result {
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
    T: Deref<Target = str>,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            StarOr::Star => serializer.serialize_str("*"),
            StarOr::Other(other) => serializer.serialize_str(other.deref()),
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
