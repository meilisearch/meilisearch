use std::cell::RefCell;
use std::fmt;

use bumpalo::Bump;
use liquid::model::{
    ArrayView, DisplayCow, KStringCow, ObjectRender, ObjectSource, State, Value as LiquidValue,
};
use liquid::{ObjectView, ValueView};

use super::{FieldMetadata, FieldsIdsMapWithMetadata};
use crate::GlobalFieldsIdsMap;

#[derive(Debug, Clone, Copy)]
pub struct FieldValue<'a, D: ObjectView> {
    name: &'a str,
    document: &'a D,
    metadata: FieldMetadata,
}

impl<'a, D: ObjectView> ValueView for FieldValue<'a, D> {
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

impl<'a, D: ObjectView> FieldValue<'a, D> {
    pub fn name(&self) -> &&'a str {
        &self.name
    }

    pub fn value(&self) -> &dyn ValueView {
        self.document.get(self.name).unwrap_or(&LiquidValue::Nil)
    }

    pub fn is_searchable(&self) -> &bool {
        &self.metadata.searchable
    }

    pub fn is_empty(&self) -> bool {
        self.size() == 0
    }
}

impl<'a, D: ObjectView> ObjectView for FieldValue<'a, D> {
    fn as_value(&self) -> &dyn ValueView {
        self
    }

    fn size(&self) -> i64 {
        2
    }

    fn keys<'k>(&'k self) -> Box<dyn Iterator<Item = KStringCow<'k>> + 'k> {
        Box::new(["name", "value", "is_searchable"].iter().map(|&x| KStringCow::from_static(x)))
    }

    fn values<'k>(&'k self) -> Box<dyn Iterator<Item = &'k dyn ValueView> + 'k> {
        Box::new(
            std::iter::once(self.name() as &dyn ValueView)
                .chain(std::iter::once(self.value()))
                .chain(std::iter::once(self.is_searchable() as &dyn ValueView)),
        )
    }

    fn iter<'k>(&'k self) -> Box<dyn Iterator<Item = (KStringCow<'k>, &'k dyn ValueView)> + 'k> {
        Box::new(self.keys().zip(self.values()))
    }

    fn contains_key(&self, index: &str) -> bool {
        index == "name" || index == "value" || index == "is_searchable"
    }

    fn get<'s>(&'s self, index: &str) -> Option<&'s dyn ValueView> {
        match index {
            "name" => Some(self.name()),
            "value" => Some(self.value()),
            "is_searchable" => Some(self.is_searchable()),
            _ => None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct OwnedFields<'a, D: ObjectView>(Vec<FieldValue<'a, D>>);

#[derive(Debug)]
pub struct BorrowedFields<'a, 'map, D: ObjectView> {
    document: &'a D,
    field_id_map: &'a RefCell<GlobalFieldsIdsMap<'map>>,
    doc_alloc: &'a Bump,
}

impl<'a, D: ObjectView> OwnedFields<'a, D> {
    pub fn new(document: &'a D, field_id_map: &'a FieldsIdsMapWithMetadata<'a>) -> Self {
        Self(
            std::iter::repeat(document)
                .zip(field_id_map.iter())
                .map(|(document, (fid, name))| FieldValue {
                    document,
                    name,
                    metadata: field_id_map.metadata(fid).unwrap_or_default(),
                })
                .collect(),
        )
    }
}

impl<'a, 'map, D: ObjectView> BorrowedFields<'a, 'map, D> {
    pub fn new(
        document: &'a D,
        field_id_map: &'a RefCell<GlobalFieldsIdsMap<'map>>,
        doc_alloc: &'a Bump,
    ) -> Self {
        Self { document, field_id_map, doc_alloc }
    }
}

impl<'a, D: ObjectView> ArrayView for OwnedFields<'a, D> {
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

impl<'a, 'map, D: ObjectView> ArrayView for BorrowedFields<'a, 'map, D> {
    fn as_value(&self) -> &dyn ValueView {
        self
    }

    fn size(&self) -> i64 {
        self.document.size()
    }

    fn values<'k>(&'k self) -> Box<dyn Iterator<Item = &'k dyn ValueView> + 'k> {
        Box::new(self.document.keys().map(|k| {
            let mut field_id_map = self.field_id_map.borrow_mut();
            let (_, metadata) = field_id_map.id_with_metadata_or_insert(&k).unwrap();
            let fv = self.doc_alloc.alloc(FieldValue {
                name: self.doc_alloc.alloc_str(&k),
                document: self.document,
                metadata: FieldMetadata { searchable: metadata.searchable },
            });
            fv as _
        }))
    }

    fn contains_key(&self, index: i64) -> bool {
        let index = if index >= 0 { index } else { self.size() + index };
        index >= 0 && index < self.size()
    }

    fn get(&self, index: i64) -> Option<&dyn ValueView> {
        let index = if index >= 0 { index } else { self.size() + index };
        let index: usize = index.try_into().ok()?;
        let key = self.document.keys().nth(index)?;
        let mut field_id_map = self.field_id_map.borrow_mut();
        let (_, metadata) = field_id_map.id_with_metadata_or_insert(&key)?;
        let fv = self.doc_alloc.alloc(FieldValue {
            name: self.doc_alloc.alloc_str(&key),
            document: self.document,
            metadata: FieldMetadata { searchable: metadata.searchable },
        });
        Some(fv as _)
    }
}

impl<'a, 'map, D: ObjectView> ValueView for BorrowedFields<'a, 'map, D> {
    fn as_debug(&self) -> &dyn std::fmt::Debug {
        self
    }

    fn render(&self) -> liquid::model::DisplayCow<'_> {
        DisplayCow::Owned(Box::new(ArrayRender { s: self }))
    }

    fn source(&self) -> liquid::model::DisplayCow<'_> {
        DisplayCow::Owned(Box::new(ArraySource { s: self }))
    }

    fn type_name(&self) -> &'static str {
        "array"
    }

    fn query_state(&self, state: liquid::model::State) -> bool {
        match state {
            State::Truthy => true,
            State::DefaultValue | State::Empty | State::Blank => self.document.size() == 0,
        }
    }

    fn to_kstr(&self) -> liquid::model::KStringCow<'_> {
        let s = ArrayRender { s: self }.to_string();
        KStringCow::from_string(s)
    }

    fn to_value(&self) -> LiquidValue {
        LiquidValue::Array(self.values().map(|v| v.to_value()).collect())
    }

    fn as_array(&self) -> Option<&dyn ArrayView> {
        Some(self)
    }

    fn is_array(&self) -> bool {
        true
    }
}

impl<'a, D: ObjectView> ValueView for OwnedFields<'a, D> {
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

struct ArraySource<'a, 'map, D: ObjectView> {
    s: &'a BorrowedFields<'a, 'map, D>,
}

impl<'a, 'map, D: ObjectView> fmt::Display for ArraySource<'a, 'map, D> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[")?;
        for item in self.s.values() {
            write!(f, "{}, ", item.render())?;
        }
        write!(f, "]")?;
        Ok(())
    }
}

struct ArrayRender<'a, 'map, D: ObjectView> {
    s: &'a BorrowedFields<'a, 'map, D>,
}

impl<'a, 'map, D: ObjectView> fmt::Display for ArrayRender<'a, 'map, D> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for item in self.s.values() {
            write!(f, "{}", item.render())?;
        }
        Ok(())
    }
}
