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

/// A document wrapper that adds virtual fields for URL fetching.
///
/// Virtual fields are accessible in templates via `{{doc.field_name}}`.
#[derive(Debug)]
pub struct DocumentWithVirtualFields<'a, D: ObjectView> {
    document: &'a D,
    virtual_fields: Option<&'a std::collections::BTreeMap<String, String>>,
}

impl<'a, D: ObjectView> DocumentWithVirtualFields<'a, D> {
    pub fn new(
        document: &'a D,
        virtual_fields: &'a std::collections::BTreeMap<String, String>,
    ) -> Self {
        Self { document, virtual_fields: Some(virtual_fields) }
    }

    pub fn without_virtual_fields(document: &'a D) -> Self {
        Self { document, virtual_fields: None }
    }
}

impl<D: ObjectView> ObjectView for DocumentWithVirtualFields<'_, D> {
    fn as_value(&self) -> &dyn ValueView {
        self
    }

    fn size(&self) -> i64 {
        let base_size = self.document.size();
        let virtual_size = self.virtual_fields.map(|f| f.len() as i64).unwrap_or(0);
        base_size + virtual_size
    }

    fn keys<'k>(&'k self) -> Box<dyn Iterator<Item = KStringCow<'k>> + 'k> {
        let doc_keys = self.document.keys();
        let virtual_keys = self
            .virtual_fields
            .into_iter()
            .flat_map(|f| f.keys().map(|k| KStringCow::from_ref(k.as_str())));
        Box::new(doc_keys.chain(virtual_keys))
    }

    fn values<'k>(&'k self) -> Box<dyn Iterator<Item = &'k dyn ValueView> + 'k> {
        // Note: This is a simplified implementation that only returns document values
        // Virtual fields are handled in `get`
        self.document.values()
    }

    fn iter<'k>(&'k self) -> Box<dyn Iterator<Item = (KStringCow<'k>, &'k dyn ValueView)> + 'k> {
        Box::new(self.keys().zip(self.values()))
    }

    fn contains_key(&self, index: &str) -> bool {
        if self.document.contains_key(index) {
            return true;
        }
        self.virtual_fields.map(|f| f.contains_key(index)).unwrap_or(false)
    }

    fn get<'s>(&'s self, index: &str) -> Option<&'s dyn ValueView> {
        // First check document
        if let Some(value) = self.document.get(index) {
            return Some(value);
        }
        // Then check virtual fields
        if let Some(virtual_fields) = self.virtual_fields {
            if let Some(value) = virtual_fields.get(index) {
                return Some(value as &dyn ValueView);
            }
        }
        None
    }
}

impl<D: ObjectView> ValueView for DocumentWithVirtualFields<'_, D> {
    fn as_debug(&self) -> &dyn std::fmt::Debug {
        self
    }

    fn render(&self) -> DisplayCow<'_> {
        DisplayCow::Owned(Box::new(ObjectRender::new(self)))
    }

    fn source(&self) -> DisplayCow<'_> {
        DisplayCow::Owned(Box::new(ObjectSource::new(self)))
    }

    fn type_name(&self) -> &'static str {
        "object"
    }

    fn query_state(&self, state: State) -> bool {
        match state {
            State::Truthy => true,
            State::DefaultValue | State::Empty | State::Blank => false,
        }
    }

    fn to_kstr(&self) -> KStringCow<'_> {
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

/// Context that wraps a document with virtual fields for URL fetching.
#[derive(Debug)]
pub struct ContextWithVirtualFields<'a, D: ObjectView> {
    document: DocumentWithVirtualFields<'a, D>,
}

impl<'a, D: ObjectView> ContextWithVirtualFields<'a, D> {
    pub fn new(
        document: &'a D,
        virtual_fields: &'a std::collections::BTreeMap<String, String>,
    ) -> Self {
        Self { document: DocumentWithVirtualFields::new(document, virtual_fields) }
    }

    pub fn without_virtual_fields(document: &'a D) -> Self {
        Self { document: DocumentWithVirtualFields::without_virtual_fields(document) }
    }
}

impl<D: ObjectView> ObjectView for ContextWithVirtualFields<'_, D> {
    fn as_value(&self) -> &dyn ValueView {
        self
    }

    fn size(&self) -> i64 {
        1
    }

    fn keys<'k>(&'k self) -> Box<dyn Iterator<Item = KStringCow<'k>> + 'k> {
        Box::new(std::iter::once(KStringCow::from_static("doc")))
    }

    fn values<'k>(&'k self) -> Box<dyn Iterator<Item = &'k dyn ValueView> + 'k> {
        Box::new(std::iter::once(self.document.as_value()))
    }

    fn iter<'k>(&'k self) -> Box<dyn Iterator<Item = (KStringCow<'k>, &'k dyn ValueView)> + 'k> {
        Box::new(self.keys().zip(self.values()))
    }

    fn contains_key(&self, index: &str) -> bool {
        index == "doc"
    }

    fn get<'s>(&'s self, index: &str) -> Option<&'s dyn ValueView> {
        if index == "doc" {
            Some(self.document.as_value())
        } else {
            None
        }
    }
}

impl<D: ObjectView> ValueView for ContextWithVirtualFields<'_, D> {
    fn as_debug(&self) -> &dyn std::fmt::Debug {
        self
    }

    fn render(&self) -> DisplayCow<'_> {
        DisplayCow::Owned(Box::new(ObjectRender::new(self)))
    }

    fn source(&self) -> DisplayCow<'_> {
        DisplayCow::Owned(Box::new(ObjectSource::new(self)))
    }

    fn type_name(&self) -> &'static str {
        "object"
    }

    fn query_state(&self, state: State) -> bool {
        match state {
            State::Truthy => true,
            State::DefaultValue | State::Empty | State::Blank => false,
        }
    }

    fn to_kstr(&self) -> KStringCow<'_> {
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
