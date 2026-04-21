use permissive_json_pointer::contained_in;
use serde::de::{self, DeserializeSeed, MapAccess, Visitor};
use serde::{Deserialize, Deserializer};
use serde_json::Value;

/// A visitor that collects object fields matching
/// the accepted paths.
///
/// It's very handy for collecting fields and skipping
/// potentially huge parts of the document avoiding
/// deserialization and allocation costs.
pub struct ValuePathsVisitor<I> {
    /// List of accepted paths.
    accepted_paths: I,
    /// This is a string representing the current
    /// path with dots (.) separating levels, e.g., foo.bar.
    current_path: String,
}

impl<I> ValuePathsVisitor<I> {
    pub fn new_from_path(
        accepted_paths: impl IntoIterator<IntoIter = I>,
        current_path: impl Into<String>,
    ) -> Self {
        ValuePathsVisitor {
            accepted_paths: accepted_paths.into_iter(),
            current_path: current_path.into(),
        }
    }
}

impl<'de, S, I> Visitor<'de> for ValuePathsVisitor<I>
where
    S: AsRef<str>,
    I: Clone + Iterator<Item = S>,
{
    type Value = Value;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(formatter, "a JSON value")
    }

    fn visit_map<M>(mut self, mut map: M) -> Result<Self::Value, M::Error>
    where
        M: MapAccess<'de>,
    {
        let mut result = serde_json::Map::new();
        while let Some(key) = map.next_key::<String>()? {
            let key_length = key.len();
            // extend the current path with the key
            if !self.current_path.is_empty() {
                self.current_path.push('.');
            }
            self.current_path.push_str(&key);

            if self.accepted_paths.clone().any(|ap| {
                let ap = ap.as_ref();
                // We must accept both directions to handle partial paths
                contained_in(ap, &self.current_path) || contained_in(&self.current_path, ap)
            }) {
                let value = map.next_value_seed(ValuePathsVisitor {
                    accepted_paths: self.accepted_paths.clone(),
                    current_path: self.current_path.clone(),
                })?;
                result.insert(key, value);
            } else {
                // Skip the value
                let _ = map.next_value::<serde::de::IgnoredAny>()?;
            }

            // Remove the just added key from the current path along with the dot separator
            let new_len = (self.current_path.len() - key_length).saturating_sub(1);
            self.current_path.truncate(new_len);
        }

        Ok(Value::Object(result))
    }

    fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
    where
        A: de::SeqAccess<'de>,
    {
        let mut result = Vec::new();
        while let Some(value) = seq.next_element_seed(ValuePathsVisitor {
            accepted_paths: self.accepted_paths.clone(),
            current_path: self.current_path.clone(),
        })? {
            result.push(value);
        }
        Ok(Value::Array(result))
    }

    // Handle leaf values (e.g., strings, numbers, booleans, null)
    fn visit_bool<E>(self, v: bool) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(Value::Bool(v))
    }

    fn visit_i64<E>(self, v: i64) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(Value::from(v))
    }

    fn visit_u64<E>(self, v: u64) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(Value::from(v))
    }

    fn visit_f64<E>(self, v: f64) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(Value::from(v))
    }

    fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(Value::String(v.to_string()))
    }

    fn visit_none<E>(self) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(Value::Null)
    }

    fn visit_some<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        Deserialize::deserialize(deserializer)
    }

    fn visit_unit<E>(self) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(Value::Null)
    }
}

impl<'de, S, I> DeserializeSeed<'de> for ValuePathsVisitor<I>
where
    S: AsRef<str>,
    I: Clone + Iterator<Item = S>,
{
    type Value = Value;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_any(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nested_one() {
        let json_str = r#"
            {
                "dog": {
                    "name": {
                        "first": "Rex",
                        "nickname": "Buddy",
                        "last": "Smith"
                    },
                    "age": 3
                },
                "cat": {
                    "name": "Whiskers"
                },
                "bird": null
            }
        "#;

        let accepted_paths = &["dog.name.nickname", "cat", "bird"];
        let visitor = ValuePathsVisitor::new_from_path(accepted_paths, "");
        let mut deserializer = serde_json::de::Deserializer::from_str(json_str);
        let map: Value = visitor.deserialize(&mut deserializer).unwrap();

        let expected = serde_json::json!({
            "dog": { "name": { "nickname": "Buddy" } },
            "cat": { "name": "Whiskers" },
            "bird": null
        });
        assert_eq!(map, expected);
    }
}
