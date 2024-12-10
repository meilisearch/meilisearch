use std::cell::OnceCell;
use std::collections::BTreeMap;
use std::fmt::{self, Debug};

use bumpalo::Bump;
use bumparaw_collections::{RawMap, RawVec, Value};
use liquid::model::{
    ArrayView, DisplayCow, KString, KStringCow, ObjectRender, ObjectSource, ScalarCow, State,
    Value as LiquidValue,
};
use liquid::{ObjectView, ValueView};
use rustc_hash::FxBuildHasher;
use serde_json::value::RawValue;

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
        data: &'a obkv::KvReaderU16,
        side: DelAdd,
        inverted_field_map: &'a FieldsIdsMap,
    ) -> Self {
        let mut out_data = BTreeMap::new();
        for (fid, raw) in data {
            let obkv = KvReaderDelAdd::from_slice(raw);
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
    fn as_debug(&self) -> &dyn Debug {
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

    fn is_object(&self) -> bool {
        true
    }
}

/// Implementation for any type that implements the Document trait
use crate::update::new::document::Document as DocumentTrait;

#[derive(Debug)]
pub struct ParseableDocument<'doc, D> {
    document: D,
    doc_alloc: &'doc Bump,
}

impl<'doc, D> ParseableDocument<'doc, D> {
    pub fn new(document: D, doc_alloc: &'doc Bump) -> Self {
        Self { document, doc_alloc }
    }
}

impl<'doc, D: DocumentTrait<'doc> + Debug> ObjectView for ParseableDocument<'doc, D> {
    fn as_value(&self) -> &dyn ValueView {
        self
    }

    fn size(&self) -> i64 {
        self.document.top_level_fields_count() as i64
    }

    fn keys<'k>(&'k self) -> Box<dyn Iterator<Item = KStringCow<'k>> + 'k> {
        Box::new(self.document.iter_top_level_fields().map(|res| {
            let (field, _) = res.unwrap();
            KStringCow::from_ref(field)
        }))
    }

    fn values<'k>(&'k self) -> Box<dyn Iterator<Item = &'k dyn ValueView> + 'k> {
        Box::new(self.document.iter_top_level_fields().map(|res| {
            let (_, value) = res.unwrap();
            ParseableValue::new_bump(value, self.doc_alloc) as _
        }))
    }

    fn iter<'k>(&'k self) -> Box<dyn Iterator<Item = (KStringCow<'k>, &'k dyn ValueView)> + 'k> {
        Box::new(self.document.iter_top_level_fields().map(|res| {
            let (field, value) = res.unwrap();
            (KStringCow::from_ref(field), ParseableValue::new_bump(value, self.doc_alloc) as _)
        }))
    }

    fn contains_key(&self, index: &str) -> bool {
        self.document.top_level_field(index).unwrap().is_some()
    }

    fn get<'s>(&'s self, index: &str) -> Option<&'s dyn ValueView> {
        let s = self.document.top_level_field(index).unwrap()?;
        Some(ParseableValue::new_bump(s, self.doc_alloc))
    }
}

impl<'doc, D: DocumentTrait<'doc> + Debug> ValueView for ParseableDocument<'doc, D> {
    fn as_debug(&self) -> &dyn Debug {
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
            State::DefaultValue | State::Empty | State::Blank => false,
        }
    }

    fn to_kstr(&self) -> liquid::model::KStringCow<'_> {
        let s = ObjectRender::new(self).to_string();
        KStringCow::from_string(s)
    }

    fn to_value(&self) -> LiquidValue {
        LiquidValue::Object(
            self.document
                .iter_top_level_fields()
                .map(|res| {
                    let (k, v) = res.unwrap();
                    (k.to_string().into(), ParseableValue::new(v, self.doc_alloc).to_value())
                })
                .collect(),
        )
    }

    fn as_object(&self) -> Option<&dyn ObjectView> {
        Some(self)
    }

    fn is_object(&self) -> bool {
        true
    }
}

struct ParseableValue<'doc> {
    value: Value<'doc, FxBuildHasher>,
}

impl<'doc> ParseableValue<'doc> {
    pub fn new(value: &'doc RawValue, doc_alloc: &'doc Bump) -> Self {
        let value = Value::from_raw_value_and_hasher(value, FxBuildHasher, doc_alloc).unwrap();
        Self { value }
    }

    pub fn new_bump(value: &'doc RawValue, doc_alloc: &'doc Bump) -> &'doc Self {
        doc_alloc.alloc(Self::new(value, doc_alloc))
    }
}

// transparent newtype for implementing ValueView
#[derive(Debug)]
#[repr(transparent)]
struct ParseableMap<'doc>(RawMap<'doc, FxBuildHasher>);

// transparent newtype for implementing ValueView
#[derive(Debug)]
#[repr(transparent)]
struct ParseableArray<'doc>(RawVec<'doc>);

impl<'doc> ParseableMap<'doc> {
    pub fn as_parseable<'a>(map: &'a RawMap<'doc, FxBuildHasher>) -> &'a ParseableMap<'doc> {
        // SAFETY: repr(transparent)
        unsafe { &*(map as *const RawMap<FxBuildHasher> as *const Self) }
    }
}

impl<'doc> ParseableArray<'doc> {
    pub fn as_parseable<'a>(array: &'a RawVec<'doc>) -> &'a ParseableArray<'doc> {
        // SAFETY: repr(transparent)
        unsafe { &*(array as *const RawVec as *const Self) }
    }
}

impl<'doc> ArrayView for ParseableArray<'doc> {
    fn as_value(&self) -> &dyn ValueView {
        self
    }

    fn size(&self) -> i64 {
        self.0.len() as _
    }

    fn values<'k>(&'k self) -> Box<dyn Iterator<Item = &'k dyn ValueView> + 'k> {
        Box::new(self.0.iter().map(|v| ParseableValue::new_bump(v, self.0.bump()) as _))
    }

    fn contains_key(&self, index: i64) -> bool {
        let index = convert_index(index, self.size());
        index < self.size() && index >= 0
    }

    fn get(&self, index: i64) -> Option<&dyn ValueView> {
        let index = convert_index(index, self.size());
        if index <= 0 {
            return None;
        }
        let v = self.0.get(index as usize)?;
        Some(ParseableValue::new_bump(v, self.0.bump()))
    }
}

impl<'doc> ValueView for ParseableArray<'doc> {
    fn as_debug(&self) -> &dyn std::fmt::Debug {
        self
    }

    fn render(&self) -> DisplayCow<'_> {
        DisplayCow::Owned(Box::new(ArrayRender { s: &self.0 }))
    }

    fn source(&self) -> DisplayCow<'_> {
        DisplayCow::Owned(Box::new(ArraySource { s: &self.0 }))
    }

    fn type_name(&self) -> &'static str {
        "array"
    }

    fn query_state(&self, state: State) -> bool {
        match state {
            State::Truthy => true,
            State::DefaultValue | State::Empty | State::Blank => self.0.is_empty(),
        }
    }

    fn to_kstr(&self) -> KStringCow<'_> {
        let s = ArrayRender { s: &self.0 }.to_string();
        KStringCow::from_string(s)
    }

    fn to_value(&self) -> LiquidValue {
        LiquidValue::Array(self.values().map(|v| v.to_value()).collect())
    }

    fn is_array(&self) -> bool {
        true
    }

    fn as_array(&self) -> Option<&dyn ArrayView> {
        Some(self as _)
    }
}

impl<'doc> ObjectView for ParseableMap<'doc> {
    fn as_value(&self) -> &dyn ValueView {
        self
    }

    fn size(&self) -> i64 {
        self.0.len() as i64
    }

    fn keys<'k>(&'k self) -> Box<dyn Iterator<Item = KStringCow<'k>> + 'k> {
        Box::new(self.0.keys().map(Into::into))
    }

    fn values<'k>(&'k self) -> Box<dyn Iterator<Item = &'k dyn ValueView> + 'k> {
        Box::new(self.0.values().map(|value| {
            let doc_alloc = self.0.bump();
            ParseableValue::new_bump(value, doc_alloc) as _
        }))
    }

    fn iter<'k>(&'k self) -> Box<dyn Iterator<Item = (KStringCow<'k>, &'k dyn ValueView)> + 'k> {
        Box::new(self.0.iter().map(|(k, v)| {
            let doc_alloc = self.0.bump();
            (k.into(), ParseableValue::new_bump(v, doc_alloc) as _)
        }))
    }

    fn contains_key(&self, index: &str) -> bool {
        self.0.get(index).is_some()
    }

    fn get<'s>(&'s self, index: &str) -> Option<&'s dyn ValueView> {
        let v = self.0.get(index)?;
        let doc_alloc = self.0.bump();
        let value = ParseableValue::new(v, doc_alloc);
        Some(doc_alloc.alloc(value) as _)
    }
}

impl<'doc> ValueView for ParseableMap<'doc> {
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
            State::DefaultValue | State::Empty | State::Blank => self.0.is_empty(),
        }
    }

    fn to_kstr(&self) -> liquid::model::KStringCow<'_> {
        let s = ObjectRender::new(self).to_string();
        KStringCow::from_string(s)
    }

    fn to_value(&self) -> LiquidValue {
        LiquidValue::Object(
            self.0
                .iter()
                .map(|(k, v)| {
                    (k.to_string().into(), ParseableValue::new(v, self.0.bump()).to_value())
                })
                .collect(),
        )
    }

    fn as_object(&self) -> Option<&dyn ObjectView> {
        Some(self)
    }

    fn is_object(&self) -> bool {
        true
    }
}

impl<'doc> ValueView for ParseableValue<'doc> {
    fn as_debug(&self) -> &dyn Debug {
        self
    }

    fn render(&self) -> DisplayCow<'_> {
        use bumparaw_collections::value::Number;
        use bumparaw_collections::Value;

        match &self.value {
            Value::Null => LiquidValue::Nil.render(),
            Value::Bool(v) => v.render(),
            Value::Number(number) => match number {
                Number::PosInt(x) => DisplayCow::Borrowed(x),
                Number::NegInt(x) => x.render(),
                Number::Finite(x) => x.render(),
            },
            Value::String(s) => s.render(),
            Value::Array(raw_vec) => ParseableArray::as_parseable(raw_vec).render(),
            Value::Object(raw_map) => ParseableMap::as_parseable(raw_map).render(),
        }
    }

    fn source(&self) -> DisplayCow<'_> {
        use bumparaw_collections::value::Number;
        use bumparaw_collections::Value;

        match &self.value {
            Value::Null => LiquidValue::Nil.source(),
            Value::Bool(v) => ValueView::source(v),
            Value::Number(number) => match number {
                Number::PosInt(x) => DisplayCow::Borrowed(x),
                Number::NegInt(x) => x.source(),
                Number::Finite(x) => x.source(),
            },
            Value::String(s) => s.source(),
            Value::Array(raw_vec) => ParseableArray::as_parseable(raw_vec).source(),
            Value::Object(raw_map) => ParseableMap::as_parseable(raw_map).source(),
        }
    }

    fn type_name(&self) -> &'static str {
        use bumparaw_collections::value::Number;
        use bumparaw_collections::Value;

        match &self.value {
            Value::Null => LiquidValue::Nil.type_name(),
            Value::Bool(v) => v.type_name(),
            Value::Number(number) => match number {
                Number::PosInt(_x) => "whole positive number",
                Number::NegInt(x) => x.type_name(),
                Number::Finite(x) => x.type_name(),
            },
            Value::String(s) => s.type_name(),
            Value::Array(_raw_vec) => "array",
            Value::Object(_raw_map) => "object",
        }
    }

    fn query_state(&self, state: State) -> bool {
        use bumparaw_collections::Value;

        match &self.value {
            Value::Null => ValueView::query_state(&LiquidValue::Nil, state),
            Value::Bool(v) => ValueView::query_state(v, state),
            Value::Number(_number) => match state {
                State::Truthy => true,
                State::DefaultValue => false,
                State::Empty => false,
                State::Blank => false,
            },
            Value::String(s) => ValueView::query_state(s, state),
            Value::Array(raw_vec) => ParseableArray::as_parseable(raw_vec).query_state(state),
            Value::Object(raw_map) => ParseableMap::as_parseable(raw_map).query_state(state),
        }
    }

    fn to_kstr(&self) -> KStringCow<'_> {
        use bumparaw_collections::Value;

        match &self.value {
            Value::Null => ValueView::to_kstr(&LiquidValue::Nil),
            Value::Bool(v) => ValueView::to_kstr(v),
            Value::Number(_number) => self.render().to_string().into(),
            Value::String(s) => KStringCow::from_ref(s),
            Value::Array(raw_vec) => ParseableArray::as_parseable(raw_vec).to_kstr(),
            Value::Object(raw_map) => ParseableMap::as_parseable(raw_map).to_kstr(),
        }
    }

    fn to_value(&self) -> LiquidValue {
        use bumparaw_collections::value::Number;
        use bumparaw_collections::Value;

        match &self.value {
            Value::Null => LiquidValue::Nil,
            Value::Bool(v) => LiquidValue::Scalar(liquid::model::ScalarCow::new(*v)),
            Value::Number(number) => match number {
                Number::PosInt(number) => {
                    let number: i64 = match (*number).try_into() {
                        Ok(number) => number,
                        Err(_) => {
                            return LiquidValue::Scalar(ScalarCow::new(self.render().to_string()))
                        }
                    };
                    LiquidValue::Scalar(ScalarCow::new(number))
                }
                Number::NegInt(number) => LiquidValue::Scalar(ScalarCow::new(*number)),
                Number::Finite(number) => LiquidValue::Scalar(ScalarCow::new(*number)),
            },
            Value::String(s) => LiquidValue::Scalar(liquid::model::ScalarCow::new(s.to_string())),
            Value::Array(raw_vec) => ParseableArray::as_parseable(raw_vec).to_value(),
            Value::Object(raw_map) => ParseableMap::as_parseable(raw_map).to_value(),
        }
    }

    fn as_scalar(&self) -> Option<liquid::model::ScalarCow<'_>> {
        use bumparaw_collections::value::Number;
        use bumparaw_collections::Value;

        match &self.value {
            Value::Bool(v) => Some(liquid::model::ScalarCow::new(*v)),
            Value::Number(number) => match number {
                Number::PosInt(number) => {
                    let number: i64 = match (*number).try_into() {
                        Ok(number) => number,
                        Err(_) => return Some(ScalarCow::new(self.render().to_string())),
                    };
                    Some(ScalarCow::new(number))
                }
                Number::NegInt(number) => Some(ScalarCow::new(*number)),
                Number::Finite(number) => Some(ScalarCow::new(*number)),
            },
            Value::String(s) => Some(ScalarCow::new(*s)),
            _ => None,
        }
    }

    fn is_scalar(&self) -> bool {
        use bumparaw_collections::Value;

        matches!(&self.value, Value::Bool(_) | Value::Number(_) | Value::String(_))
    }

    fn as_array(&self) -> Option<&dyn liquid::model::ArrayView> {
        if let Value::Array(array) = &self.value {
            return Some(ParseableArray::as_parseable(array) as _);
        }
        None
    }

    fn is_array(&self) -> bool {
        matches!(&self.value, bumparaw_collections::Value::Array(_))
    }

    fn as_object(&self) -> Option<&dyn ObjectView> {
        if let Value::Object(object) = &self.value {
            return Some(ParseableMap::as_parseable(object) as _);
        }
        None
    }

    fn is_object(&self) -> bool {
        matches!(&self.value, bumparaw_collections::Value::Object(_))
    }

    fn is_nil(&self) -> bool {
        matches!(&self.value, bumparaw_collections::Value::Null)
    }
}

impl Debug for ParseableValue<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ParseableValue").field("value", &self.value).finish()
    }
}

struct ArraySource<'s, 'doc> {
    s: &'s RawVec<'doc>,
}

impl<'s, 'doc> fmt::Display for ArraySource<'s, 'doc> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[")?;
        for item in self.s {
            let v = ParseableValue::new(item, self.s.bump());
            write!(f, "{}, ", v.render())?;
        }
        write!(f, "]")?;
        Ok(())
    }
}

struct ArrayRender<'s, 'doc> {
    s: &'s RawVec<'doc>,
}

impl<'s, 'doc> fmt::Display for ArrayRender<'s, 'doc> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for item in self.s {
            let v = ParseableValue::new(item, self.s.bump());

            write!(f, "{}", v.render())?;
        }
        Ok(())
    }
}

fn convert_index(index: i64, max_size: i64) -> i64 {
    if 0 <= index {
        index
    } else {
        max_size + index
    }
}
