use liquid::model::{
    DisplayCow, KStringCow, ObjectRender, ObjectSource, State, Value as LiquidValue,
};
use liquid::{ObjectView, ValueView};

use super::document::Document;

#[derive(Clone, Debug)]
pub struct Context<'a> {
    document: Option<&'a Document<'a>>,
    context: Option<liquid::Object>,
}

impl<'a> Context<'a> {
    pub fn new(document: Option<&'a Document<'a>>, context: Option<serde_json::Value>) -> Self {
        /// FIXME: unwrap
        let context = context.map(|context| liquid::to_object(&context).unwrap());
        Self { document, context }
    }
}

impl<'a> ObjectView for Context<'a> {
    fn as_value(&self) -> &dyn ValueView {
        self
    }

    fn size(&self) -> i64 {
        match (self.context.as_ref(), self.document.as_ref()) {
            (None, None) => 0,
            (None, Some(_)) => 1,
            (Some(_), None) => 1,
            (Some(_), Some(_)) => 2,
        }
    }

    fn keys<'k>(&'k self) -> Box<dyn Iterator<Item = KStringCow<'k>> + 'k> {
        let keys = match (self.context.as_ref(), self.document.as_ref()) {
            (None, None) => [].as_slice(),
            (None, Some(_)) => ["doc"].as_slice(),
            (Some(_), None) => ["context"].as_slice(),
            (Some(_), Some(_)) => ["context", "doc"].as_slice(),
        };

        Box::new(keys.iter().map(|s| KStringCow::from_static(s)))
    }

    fn values<'k>(&'k self) -> Box<dyn Iterator<Item = &'k dyn ValueView> + 'k> {
        Box::new(
            self.context
                .as_ref()
                .map(|context| context.as_value())
                .into_iter()
                .chain(self.document.map(|document| document.as_value()).into_iter()),
        )
    }

    fn iter<'k>(&'k self) -> Box<dyn Iterator<Item = (KStringCow<'k>, &'k dyn ValueView)> + 'k> {
        Box::new(self.keys().zip(self.values()))
    }

    fn contains_key(&self, index: &str) -> bool {
        index == "context" || index == "doc"
    }

    fn get<'s>(&'s self, index: &str) -> Option<&'s dyn ValueView> {
        match index {
            "context" => self.context.as_ref().map(|context| context.as_value()),
            "doc" => self.document.as_ref().map(|doc| doc.as_value()),
            _ => None,
        }
    }
}

impl<'a> ValueView for Context<'a> {
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
