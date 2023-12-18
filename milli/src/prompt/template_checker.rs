use liquid::model::{
    ArrayView, DisplayCow, KStringCow, ObjectRender, ObjectSource, State, Value as LiquidValue,
};
use liquid::{Object, ObjectView, ValueView};

#[derive(Debug)]
pub struct TemplateChecker;

#[derive(Debug)]
pub struct DummyDoc;

#[derive(Debug)]
pub struct DummyFields;

#[derive(Debug)]
pub struct DummyField;

const DUMMY_VALUE: &LiquidValue = &LiquidValue::Nil;

impl ObjectView for DummyField {
    fn as_value(&self) -> &dyn ValueView {
        self
    }

    fn size(&self) -> i64 {
        2
    }

    fn keys<'k>(&'k self) -> Box<dyn Iterator<Item = KStringCow<'k>> + 'k> {
        Box::new(["name", "value"].iter().map(|s| KStringCow::from_static(s)))
    }

    fn values<'k>(&'k self) -> Box<dyn Iterator<Item = &'k dyn ValueView> + 'k> {
        Box::new(vec![DUMMY_VALUE.as_view(), DUMMY_VALUE.as_view()].into_iter())
    }

    fn iter<'k>(&'k self) -> Box<dyn Iterator<Item = (KStringCow<'k>, &'k dyn ValueView)> + 'k> {
        Box::new(self.keys().zip(self.values()))
    }

    fn contains_key(&self, index: &str) -> bool {
        index == "name" || index == "value"
    }

    fn get<'s>(&'s self, index: &str) -> Option<&'s dyn ValueView> {
        if self.contains_key(index) {
            Some(DUMMY_VALUE.as_view())
        } else {
            None
        }
    }
}

impl ValueView for DummyField {
    fn as_debug(&self) -> &dyn std::fmt::Debug {
        self
    }

    fn render(&self) -> DisplayCow<'_> {
        DUMMY_VALUE.render()
    }

    fn source(&self) -> DisplayCow<'_> {
        DUMMY_VALUE.source()
    }

    fn type_name(&self) -> &'static str {
        "object"
    }

    fn query_state(&self, state: State) -> bool {
        match state {
            State::Truthy => true,
            State::DefaultValue => false,
            State::Empty => false,
            State::Blank => false,
        }
    }

    fn to_kstr(&self) -> KStringCow<'_> {
        DUMMY_VALUE.to_kstr()
    }

    fn to_value(&self) -> LiquidValue {
        let mut this = Object::new();
        this.insert("name".into(), LiquidValue::Nil);
        this.insert("value".into(), LiquidValue::Nil);
        LiquidValue::Object(this)
    }

    fn as_object(&self) -> Option<&dyn ObjectView> {
        Some(self)
    }
}

impl ValueView for DummyFields {
    fn as_debug(&self) -> &dyn std::fmt::Debug {
        self
    }

    fn render(&self) -> DisplayCow<'_> {
        DUMMY_VALUE.render()
    }

    fn source(&self) -> DisplayCow<'_> {
        DUMMY_VALUE.source()
    }

    fn type_name(&self) -> &'static str {
        "array"
    }

    fn query_state(&self, state: State) -> bool {
        match state {
            State::Truthy => true,
            State::DefaultValue => false,
            State::Empty => false,
            State::Blank => false,
        }
    }

    fn to_kstr(&self) -> KStringCow<'_> {
        DUMMY_VALUE.to_kstr()
    }

    fn to_value(&self) -> LiquidValue {
        LiquidValue::Array(vec![DummyField.to_value()])
    }

    fn as_array(&self) -> Option<&dyn ArrayView> {
        Some(self)
    }
}

impl ArrayView for DummyFields {
    fn as_value(&self) -> &dyn ValueView {
        self
    }

    fn size(&self) -> i64 {
        u16::MAX as i64
    }

    fn values<'k>(&'k self) -> Box<dyn Iterator<Item = &'k dyn ValueView> + 'k> {
        Box::new(std::iter::once(DummyField.as_value()))
    }

    fn contains_key(&self, index: i64) -> bool {
        index < self.size()
    }

    fn get(&self, _index: i64) -> Option<&dyn ValueView> {
        Some(DummyField.as_value())
    }
}

impl ObjectView for DummyDoc {
    fn as_value(&self) -> &dyn ValueView {
        self
    }

    fn size(&self) -> i64 {
        1000
    }

    fn keys<'k>(&'k self) -> Box<dyn Iterator<Item = KStringCow<'k>> + 'k> {
        Box::new(std::iter::empty())
    }

    fn values<'k>(&'k self) -> Box<dyn Iterator<Item = &'k dyn ValueView> + 'k> {
        Box::new(std::iter::empty())
    }

    fn iter<'k>(&'k self) -> Box<dyn Iterator<Item = (KStringCow<'k>, &'k dyn ValueView)> + 'k> {
        Box::new(std::iter::empty())
    }

    fn contains_key(&self, _index: &str) -> bool {
        true
    }

    fn get<'s>(&'s self, _index: &str) -> Option<&'s dyn ValueView> {
        // Recursively sends itself
        Some(self)
    }
}

impl ValueView for DummyDoc {
    fn as_debug(&self) -> &dyn std::fmt::Debug {
        self
    }

    fn render(&self) -> DisplayCow<'_> {
        DUMMY_VALUE.render()
    }

    fn source(&self) -> DisplayCow<'_> {
        DUMMY_VALUE.source()
    }

    fn type_name(&self) -> &'static str {
        "object"
    }

    fn query_state(&self, state: State) -> bool {
        match state {
            State::Truthy => true,
            State::DefaultValue => false,
            State::Empty => false,
            State::Blank => false,
        }
    }

    fn to_kstr(&self) -> KStringCow<'_> {
        DUMMY_VALUE.to_kstr()
    }

    fn to_value(&self) -> LiquidValue {
        LiquidValue::Nil
    }

    fn as_object(&self) -> Option<&dyn ObjectView> {
        Some(self)
    }
}

impl ObjectView for TemplateChecker {
    fn as_value(&self) -> &dyn ValueView {
        self
    }

    fn size(&self) -> i64 {
        2
    }

    fn keys<'k>(&'k self) -> Box<dyn Iterator<Item = KStringCow<'k>> + 'k> {
        Box::new(["doc", "fields"].iter().map(|s| KStringCow::from_static(s)))
    }

    fn values<'k>(&'k self) -> Box<dyn Iterator<Item = &'k dyn ValueView> + 'k> {
        Box::new(
            std::iter::once(DummyDoc.as_value()).chain(std::iter::once(DummyFields.as_value())),
        )
    }

    fn iter<'k>(&'k self) -> Box<dyn Iterator<Item = (KStringCow<'k>, &'k dyn ValueView)> + 'k> {
        Box::new(self.keys().zip(self.values()))
    }

    fn contains_key(&self, index: &str) -> bool {
        index == "doc" || index == "fields"
    }

    fn get<'s>(&'s self, index: &str) -> Option<&'s dyn ValueView> {
        match index {
            "doc" => Some(DummyDoc.as_value()),
            "fields" => Some(DummyFields.as_value()),
            _ => None,
        }
    }
}

impl ValueView for TemplateChecker {
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
            State::DefaultValue | State::Empty | State::Blank => false,
        }
    }

    fn to_kstr(&self) -> liquid::model::KStringCow<'_> {
        let s = ObjectRender::new(self).to_string();
        KStringCow::from_string(s)
    }

    fn to_value(&self) -> LiquidValue {
        LiquidValue::Object(
            self.iter().map(|(k, x)| (k.to_string().into(), x.to_value())).collect(),
        )
    }

    fn as_object(&self) -> Option<&dyn ObjectView> {
        Some(self)
    }
}
