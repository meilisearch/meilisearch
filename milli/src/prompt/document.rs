use std::cell::OnceCell;
use std::collections::BTreeMap;

use liquid::model::{
    DisplayCow, KString, KStringCow, ObjectRender, ObjectSource, State, Value as LiquidValue,
};
use liquid::{ObjectView, ValueView};

use crate::update::del_add::{DelAdd, KvReaderDelAdd};
use crate::FieldsIdsMap;

#[derive(Debug, Clone)]
pub struct Document<'a>(BTreeMap<&'a str, (&'a [u8], ParsedValue)>);

#[derive(Debug, Clone)]
struct ParsedValue(std::cell::OnceCell<LiquidValue>);

impl ParsedValue {
    fn empty() -> ParsedValue {
        ParsedValue(OnceCell::new())
    }

    fn get(&self, raw: &[u8]) -> &LiquidValue {
        self.0.get_or_init(|| {
            let value: serde_json::Value = serde_json::from_slice(raw).unwrap();
            liquid::model::to_value(&value).unwrap()
        })
    }
}

impl<'a> Document<'a> {
    pub fn new(
        data: obkv::KvReaderU16<'a>,
        side: DelAdd,
        inverted_field_map: &'a FieldsIdsMap,
    ) -> Self {
        let mut out_data = BTreeMap::new();
        for (fid, raw) in data {
            let obkv = KvReaderDelAdd::new(raw);
            let Some(raw) = obkv.get(side) else {
                continue;
            };
            let Some(name) = inverted_field_map.name(fid) else {
                continue;
            };
            out_data.insert(name, (raw, ParsedValue::empty()));
        }
        Self(out_data)
    }

    fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    fn len(&self) -> usize {
        self.0.len()
    }

    fn iter(&self) -> impl Iterator<Item = (KString, LiquidValue)> + '_ {
        self.0.iter().map(|(&k, (raw, data))| (k.to_owned().into(), data.get(raw).to_owned()))
    }
}

impl<'a> ObjectView for Document<'a> {
    fn as_value(&self) -> &dyn ValueView {
        self
    }

    fn size(&self) -> i64 {
        self.len() as i64
    }

    fn keys<'k>(&'k self) -> Box<dyn Iterator<Item = KStringCow<'k>> + 'k> {
        let keys = BTreeMap::keys(&self.0).map(|&s| s.into());
        Box::new(keys)
    }

    fn values<'k>(&'k self) -> Box<dyn Iterator<Item = &'k dyn ValueView> + 'k> {
        Box::new(self.0.values().map(|(raw, v)| v.get(raw) as &dyn ValueView))
    }

    fn iter<'k>(&'k self) -> Box<dyn Iterator<Item = (KStringCow<'k>, &'k dyn ValueView)> + 'k> {
        Box::new(self.0.iter().map(|(&k, (raw, data))| (k.into(), data.get(raw) as &dyn ValueView)))
    }

    fn contains_key(&self, index: &str) -> bool {
        self.0.contains_key(index)
    }

    fn get<'s>(&'s self, index: &str) -> Option<&'s dyn ValueView> {
        self.0.get(index).map(|(raw, v)| v.get(raw) as &dyn ValueView)
    }
}

impl<'a> ValueView for Document<'a> {
    fn as_debug(&self) -> &dyn std::fmt::Debug {
        self
    }

    fn render(&self) -> liquid::model::DisplayCow<'_> {
        DisplayCow::Owned(Box::new(ObjectRender::new(self)))
    }

    fn source(&self) -> liquid::model::DisplayCow<'_> {
        DisplayCow::Owned(Box::new(ObjectSource::new(self)))
    }

    fn type_name(&self) -> &'static str {
        "object"
    }

    fn query_state(&self, state: liquid::model::State) -> bool {
        match state {
            State::Truthy => true,
            State::DefaultValue | State::Empty | State::Blank => self.is_empty(),
        }
    }

    fn to_kstr(&self) -> liquid::model::KStringCow<'_> {
        let s = ObjectRender::new(self).to_string();
        KStringCow::from_string(s)
    }

    fn to_value(&self) -> LiquidValue {
        LiquidValue::Object(self.iter().collect())
    }

    fn as_object(&self) -> Option<&dyn ObjectView> {
        Some(self)
    }
}
