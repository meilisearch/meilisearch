use liquid::model::{
    ArrayView, DisplayCow, KStringCow, ObjectRender, ObjectSource, State, Value as LiquidValue,
};
use liquid::{ObjectView, ValueView};

#[derive(Debug, Clone)]
pub struct Context<'a, D: ObjectView, F: ArrayView> {
    document: &'a D,
    fields: &'a F,
}

impl<'a, D: ObjectView, F: ArrayView> Context<'a, D, F> {
    pub fn new(document: &'a D, fields: &'a F) -> Self {
        Self { document, fields }
    }
}

impl<'a, D: ObjectView, F: ArrayView> ObjectView for Context<'a, D, F> {
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
            std::iter::once(self.document.as_value())
                .chain(std::iter::once(self.fields.as_value())),
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
            "doc" => Some(self.document.as_value()),
            "fields" => Some(self.fields.as_value()),
            _ => None,
        }
    }
}

impl<'a, D: ObjectView, F: ArrayView> ValueView for Context<'a, D, F> {
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
