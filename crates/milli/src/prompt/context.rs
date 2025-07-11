use liquid::model::{
    ArrayView, DisplayCow, KStringCow, ObjectRender, ObjectSource, State, Value as LiquidValue,
};
use liquid::{ObjectView, ValueView};

#[derive(Debug, Clone)]
pub struct Context<'a, D: ObjectView, F: ArrayView> {
    document: &'a D,
    fields: Option<&'a F>,
}

impl<'a, D: ObjectView, F: ArrayView> Context<'a, D, F> {
    pub fn new(document: &'a D, fields: &'a F) -> Self {
        Self { document, fields: Some(fields) }
    }
}

impl<'a, D: ObjectView> Context<'a, D, Vec<bool>> {
    pub fn without_fields(document: &'a D) -> Self {
        Self { document, fields: None }
    }
}

impl<D: ObjectView, F: ArrayView> ObjectView for Context<'_, D, F> {
    fn as_value(&self) -> &dyn ValueView {
        self
    }

    fn size(&self) -> i64 {
        if self.fields.is_some() {
            2
        } else {
            1
        }
    }

    fn keys<'k>(&'k self) -> Box<dyn Iterator<Item = KStringCow<'k>> + 'k> {
        let keys = if self.fields.is_some() {
            either::Either::Left(["doc", "fields"])
        } else {
            either::Either::Right(["doc"])
        };

        Box::new(keys.into_iter().map(KStringCow::from_static))
    }

    fn values<'k>(&'k self) -> Box<dyn Iterator<Item = &'k dyn ValueView> + 'k> {
        Box::new(
            std::iter::once(self.document.as_value())
                .chain(self.fields.iter().map(|fields| fields.as_value())),
        )
    }

    fn iter<'k>(&'k self) -> Box<dyn Iterator<Item = (KStringCow<'k>, &'k dyn ValueView)> + 'k> {
        Box::new(self.keys().zip(self.values()))
    }

    fn contains_key(&self, index: &str) -> bool {
        index == "doc" || (index == "fields" && self.fields.is_some())
    }

    fn get<'s>(&'s self, index: &str) -> Option<&'s dyn ValueView> {
        match (index, &self.fields) {
            ("doc", _) => Some(self.document.as_value()),
            ("fields", Some(fields)) => Some(fields.as_value()),
            _ => None,
        }
    }
}

impl<D: ObjectView, F: ArrayView> ValueView for Context<'_, D, F> {
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
