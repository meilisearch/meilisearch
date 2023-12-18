use liquid::model::{
    ArrayView, DisplayCow, KStringCow, ObjectRender, ObjectSource, State, Value as LiquidValue,
};
use liquid::{ObjectView, ValueView};

use super::document::Document;
use crate::FieldsIdsMap;
#[derive(Debug, Clone)]
pub struct Fields<'a>(Vec<FieldValue<'a>>);

impl<'a> Fields<'a> {
    pub fn new(document: &'a Document<'a>, field_id_map: &'a FieldsIdsMap) -> Self {
        Self(
            std::iter::repeat(document)
                .zip(field_id_map.iter())
                .map(|(document, (_fid, name))| FieldValue { document, name })
                .collect(),
        )
    }
}

#[derive(Debug, Clone, Copy)]
pub struct FieldValue<'a> {
    name: &'a str,
    document: &'a Document<'a>,
}

impl<'a> ValueView for FieldValue<'a> {
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
        LiquidValue::Object(
            self.iter().map(|(k, v)| (k.to_string().into(), v.to_value())).collect(),
        )
    }

    fn as_object(&self) -> Option<&dyn ObjectView> {
        Some(self)
    }
}

impl<'a> FieldValue<'a> {
    pub fn name(&self) -> &&'a str {
        &self.name
    }

    pub fn value(&self) -> &dyn ValueView {
        self.document.get(self.name).unwrap_or(&LiquidValue::Nil)
    }

    pub fn is_empty(&self) -> bool {
        self.size() == 0
    }
}

impl<'a> ObjectView for FieldValue<'a> {
    fn as_value(&self) -> &dyn ValueView {
        self
    }

    fn size(&self) -> i64 {
        2
    }

    fn keys<'k>(&'k self) -> Box<dyn Iterator<Item = KStringCow<'k>> + 'k> {
        Box::new(["name", "value"].iter().map(|&x| KStringCow::from_static(x)))
    }

    fn values<'k>(&'k self) -> Box<dyn Iterator<Item = &'k dyn ValueView> + 'k> {
        Box::new(
            std::iter::once(self.name() as &dyn ValueView).chain(std::iter::once(self.value())),
        )
    }

    fn iter<'k>(&'k self) -> Box<dyn Iterator<Item = (KStringCow<'k>, &'k dyn ValueView)> + 'k> {
        Box::new(self.keys().zip(self.values()))
    }

    fn contains_key(&self, index: &str) -> bool {
        index == "name" || index == "value"
    }

    fn get<'s>(&'s self, index: &str) -> Option<&'s dyn ValueView> {
        match index {
            "name" => Some(self.name()),
            "value" => Some(self.value()),
            _ => None,
        }
    }
}

impl<'a> ArrayView for Fields<'a> {
    fn as_value(&self) -> &dyn ValueView {
        self.0.as_value()
    }

    fn size(&self) -> i64 {
        self.0.len() as i64
    }

    fn values<'k>(&'k self) -> Box<dyn Iterator<Item = &'k dyn ValueView> + 'k> {
        self.0.values()
    }

    fn contains_key(&self, index: i64) -> bool {
        self.0.contains_key(index)
    }

    fn get(&self, index: i64) -> Option<&dyn ValueView> {
        ArrayView::get(&self.0, index)
    }
}

impl<'a> ValueView for Fields<'a> {
    fn as_debug(&self) -> &dyn std::fmt::Debug {
        self
    }

    fn render(&self) -> liquid::model::DisplayCow<'_> {
        self.0.render()
    }

    fn source(&self) -> liquid::model::DisplayCow<'_> {
        self.0.source()
    }

    fn type_name(&self) -> &'static str {
        self.0.type_name()
    }

    fn query_state(&self, state: liquid::model::State) -> bool {
        self.0.query_state(state)
    }

    fn to_kstr(&self) -> liquid::model::KStringCow<'_> {
        self.0.to_kstr()
    }

    fn to_value(&self) -> LiquidValue {
        self.0.to_value()
    }

    fn as_array(&self) -> Option<&dyn ArrayView> {
        Some(self)
    }
}
