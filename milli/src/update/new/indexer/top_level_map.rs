use std::borrow::{Borrow, Cow};
use std::collections::BTreeMap;
use std::{fmt, ops};

use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use serde_json::{Map, Value};

#[derive(Deserialize, Serialize)]
pub struct TopLevelMap<'p>(#[serde(borrow)] pub BTreeMap<CowStr<'p>, &'p RawValue>);

impl TryFrom<&'_ TopLevelMap<'_>> for Map<String, Value> {
    type Error = serde_json::Error;

    fn try_from(tlmap: &TopLevelMap<'_>) -> Result<Self, Self::Error> {
        let mut object = Map::new();
        for (k, v) in &tlmap.0 {
            let value = serde_json::from_str(v.get())?;
            object.insert(k.to_string(), value);
        }
        Ok(object)
    }
}

impl TryFrom<TopLevelMap<'_>> for Map<String, Value> {
    type Error = serde_json::Error;

    fn try_from(tlmap: TopLevelMap<'_>) -> Result<Self, Self::Error> {
        TryFrom::try_from(&tlmap)
    }
}

impl<'p> ops::Deref for TopLevelMap<'p> {
    type Target = BTreeMap<CowStr<'p>, &'p RawValue>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl ops::DerefMut for TopLevelMap<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

#[derive(Deserialize, Serialize, PartialEq, Eq, PartialOrd, Ord, Hash, Clone)]
pub struct CowStr<'p>(#[serde(borrow)] pub Cow<'p, str>);

impl fmt::Display for CowStr<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}

impl AsRef<str> for CowStr<'_> {
    fn as_ref(&self) -> &str {
        self.0.as_ref()
    }
}

impl<'doc> Borrow<str> for CowStr<'doc> {
    fn borrow(&self) -> &str {
        self.0.borrow()
    }
}
