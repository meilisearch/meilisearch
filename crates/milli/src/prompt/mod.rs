mod context;
mod document;
pub(crate) mod error;
mod fields;
mod template_checker;

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::convert::TryFrom;
use std::fmt::Debug;
use std::num::NonZeroUsize;
use std::ops::Deref;

use bumpalo::Bump;
use document::ParseableDocument;
use error::{NewPromptError, RenderPromptError};
use fields::{BorrowedFields, OwnedFields};

use self::context::Context;
use self::document::Document;
use crate::update::del_add::DelAdd;
use crate::{FieldId, FieldsIdsMap, GlobalFieldsIdsMap};

pub struct Prompt {
    template: liquid::Template,
    template_text: String,
    max_bytes: Option<NonZeroUsize>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct PromptData {
    pub template: String,
    pub max_bytes: Option<NonZeroUsize>,
}

impl From<Prompt> for PromptData {
    fn from(value: Prompt) -> Self {
        Self { template: value.template_text, max_bytes: value.max_bytes }
    }
}

impl TryFrom<PromptData> for Prompt {
    type Error = NewPromptError;

    fn try_from(value: PromptData) -> Result<Self, Self::Error> {
        Prompt::new(value.template, value.max_bytes)
    }
}

impl Clone for Prompt {
    fn clone(&self) -> Self {
        let template_text = self.template_text.clone();
        Self {
            template: new_template(&template_text).unwrap(),
            template_text,
            max_bytes: self.max_bytes,
        }
    }
}

fn new_template(text: &str) -> Result<liquid::Template, liquid::Error> {
    liquid::ParserBuilder::with_stdlib().build().unwrap().parse(text)
}

fn default_template() -> liquid::Template {
    new_template(default_template_text()).unwrap()
}

fn default_template_text() -> &'static str {
    "{% for field in fields %}\
    {% if field.is_searchable and field.value != nil %}\
    {{ field.name }}: {{ field.value }}\n\
    {% endif %}\
    {% endfor %}"
}

pub fn default_max_bytes() -> NonZeroUsize {
    NonZeroUsize::new(400).unwrap()
}

impl Default for Prompt {
    fn default() -> Self {
        Self {
            template: default_template(),
            template_text: default_template_text().into(),
            max_bytes: Some(default_max_bytes()),
        }
    }
}

impl Default for PromptData {
    fn default() -> Self {
        Self { template: default_template_text().into(), max_bytes: Some(default_max_bytes()) }
    }
}

impl Prompt {
    pub fn new(template: String, max_bytes: Option<NonZeroUsize>) -> Result<Self, NewPromptError> {
        let this = Self {
            template: liquid::ParserBuilder::with_stdlib()
                .build()
                .unwrap()
                .parse(&template)
                .map_err(NewPromptError::cannot_parse_template)?,
            template_text: template,
            max_bytes,
        };

        // render template with special object that's OK with `doc.*` and `fields.*`
        this.template
            .render(&template_checker::TemplateChecker)
            .map_err(NewPromptError::invalid_fields_in_template)?;

        Ok(this)
    }

    pub fn render_document<
        'a,       // lifetime of the borrow of the document
        'doc: 'a, // lifetime of the allocator, will live for an entire chunk of documents
    >(
        &self,
        external_docid: &str,
        document: impl crate::update::new::document::Document<'a> + Debug,
        field_id_map: &RefCell<GlobalFieldsIdsMap>,
        doc_alloc: &'doc Bump,
    ) -> Result<&'doc str, RenderPromptError> {
        let document = ParseableDocument::new(document, doc_alloc);
        let fields = BorrowedFields::new(&document, field_id_map, doc_alloc);
        let context = Context::new(&document, &fields);
        let mut rendered = bumpalo::collections::Vec::with_capacity_in(
            self.max_bytes.unwrap_or_else(default_max_bytes).get(),
            doc_alloc,
        );
        self.template.render_to(&mut rendered, &context).map_err(|liquid_error| {
            RenderPromptError::missing_context_with_external_docid(
                external_docid.to_owned(),
                liquid_error,
            )
        })?;
        Ok(std::str::from_utf8(rendered.into_bump_slice())
            .expect("render can only write UTF-8 because all inputs and processing preserve utf-8"))
    }

    pub fn render_kvdeladd(
        &self,
        document: &obkv::KvReaderU16,
        side: DelAdd,
        field_id_map: &FieldsIdsMapWithMetadata,
    ) -> Result<String, RenderPromptError> {
        let document = Document::new(document, side, field_id_map);
        let fields = OwnedFields::new(&document, field_id_map);
        let context = Context::new(&document, &fields);

        let mut rendered =
            self.template.render(&context).map_err(RenderPromptError::missing_context)?;
        if let Some(max_bytes) = self.max_bytes {
            truncate(&mut rendered, max_bytes.get());
        }
        Ok(rendered)
    }
}

fn truncate(s: &mut String, max_bytes: usize) {
    if max_bytes >= s.len() {
        return;
    }
    for i in (0..=max_bytes).rev() {
        if s.is_char_boundary(i) {
            s.truncate(i);
            break;
        }
    }
}

pub struct FieldsIdsMapWithMetadata<'a> {
    fields_ids_map: &'a FieldsIdsMap,
    metadata: BTreeMap<FieldId, FieldMetadata>,
}

impl<'a> FieldsIdsMapWithMetadata<'a> {
    pub fn new(fields_ids_map: &'a FieldsIdsMap, searchable_fields_ids: &'_ [FieldId]) -> Self {
        let mut metadata: BTreeMap<FieldId, FieldMetadata> =
            fields_ids_map.ids().map(|id| (id, Default::default())).collect();
        for searchable_field_id in searchable_fields_ids {
            let Some(metadata) = metadata.get_mut(searchable_field_id) else { continue };
            metadata.searchable = true;
        }
        Self { fields_ids_map, metadata }
    }

    pub fn metadata(&self, field_id: FieldId) -> Option<FieldMetadata> {
        self.metadata.get(&field_id).copied()
    }
}

impl<'a> Deref for FieldsIdsMapWithMetadata<'a> {
    type Target = FieldsIdsMap;

    fn deref(&self) -> &Self::Target {
        self.fields_ids_map
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct FieldMetadata {
    pub searchable: bool,
}

#[cfg(test)]
mod test {
    use super::Prompt;
    use crate::error::FaultSource;
    use crate::prompt::error::{NewPromptError, NewPromptErrorKind};
    use crate::prompt::truncate;

    #[test]
    fn default_template() {
        // does not panic
        Prompt::default();
    }

    #[test]
    fn empty_template() {
        Prompt::new("".into(), None).unwrap();
    }

    #[test]
    fn template_ok() {
        Prompt::new("{{doc.title}}: {{doc.overview}}".into(), None).unwrap();
    }

    #[test]
    fn template_syntax() {
        assert!(matches!(
            Prompt::new("{{doc.title: {{doc.overview}}".into(), None),
            Err(NewPromptError {
                kind: NewPromptErrorKind::CannotParseTemplate(_),
                fault: FaultSource::User
            })
        ));
    }

    #[test]
    fn template_missing_doc() {
        assert!(matches!(
            Prompt::new("{{title}}: {{overview}}".into(), None),
            Err(NewPromptError {
                kind: NewPromptErrorKind::InvalidFieldsInTemplate(_),
                fault: FaultSource::User
            })
        ));
    }

    #[test]
    fn template_nested_doc() {
        Prompt::new("{{doc.actor.firstName}}: {{doc.actor.lastName}}".into(), None).unwrap();
    }

    #[test]
    fn template_fields() {
        Prompt::new("{% for field in fields %}{{field}}{% endfor %}".into(), None).unwrap();
    }

    #[test]
    fn template_fields_ok() {
        Prompt::new(
            "{% for field in fields %}{{field.name}}: {{field.value}}{% endfor %}".into(),
            None,
        )
        .unwrap();
    }

    #[test]
    fn template_fields_invalid() {
        assert!(matches!(
            // intentionally garbled field
            Prompt::new("{% for field in fields %}{{field.vaelu}} {% endfor %}".into(), None),
            Err(NewPromptError {
                kind: NewPromptErrorKind::InvalidFieldsInTemplate(_),
                fault: FaultSource::User
            })
        ));
    }

    // todo: test truncation
    #[test]
    fn template_truncation() {
        let mut s = "インテル ザー ビーグル".to_string();

        truncate(&mut s, 42);
        assert_eq!(s, "インテル ザー ビーグル");

        assert_eq!(s.len(), 32);
        truncate(&mut s, 32);
        assert_eq!(s, "インテル ザー ビーグル");

        truncate(&mut s, 31);
        assert_eq!(s, "インテル ザー ビーグ");
        truncate(&mut s, 30);
        assert_eq!(s, "インテル ザー ビーグ");
        truncate(&mut s, 28);
        assert_eq!(s, "インテル ザー ビー");
        truncate(&mut s, 26);
        assert_eq!(s, "インテル ザー ビー");
        truncate(&mut s, 25);
        assert_eq!(s, "インテル ザー ビ");

        assert_eq!("イ".len(), 3);
        truncate(&mut s, 3);
        assert_eq!(s, "イ");
        truncate(&mut s, 2);
        assert_eq!(s, "");
    }
}
