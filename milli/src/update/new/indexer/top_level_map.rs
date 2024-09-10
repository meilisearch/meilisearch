use std::borrow::{Borrow, Cow};
use std::collections::BTreeMap;
use std::fmt;

use serde::Deserialize;
use serde_json::value::RawValue;

#[derive(Deserialize)]
pub struct TopLevelMap<'p>(#[serde(borrow)] pub BTreeMap<CowStr<'p>, &'p RawValue>);

#[derive(Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash, Clone)]
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
