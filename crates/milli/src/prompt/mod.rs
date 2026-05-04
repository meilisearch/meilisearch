mod context;
mod document;
pub(crate) mod error;
mod fields;

use std::cell::RefCell;
use std::convert::TryFrom;
use std::fmt::Debug;
use std::num::NonZeroUsize;

use bumpalo::Bump;
pub(crate) use document::{Document, ParseableDocument};
use error::{NewPromptError, RenderPromptError};
pub use fields::{BorrowedFields, OwnedFields};

pub use self::context::Context;
use crate::fields_ids_map::metadata::FieldIdMapWithMetadata;
use crate::update::del_add::DelAdd;
use crate::GlobalFieldsIdsMap;

pub mod filters;
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
    liquid::ParserBuilder::with_stdlib()
        .filter(filters::fetch_url::FetchUrl)
        .build()
        .unwrap()
        .parse(text)
}

fn default_template() -> liquid::Template {
    new_template(default_template_text()).unwrap()
}

pub fn default_template_text() -> &'static str {
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
                .filter(filters::fetch_url::FetchUrl)
                .build()
                .unwrap()
                .parse(&template)
                .map_err(NewPromptError::cannot_parse_template)?,
            template_text: template,
            max_bytes,
        };

        Ok(this)
    }

    pub fn render_document<
        'a,   // lifetime of the borrow of the document
        'doc, // lifetime of the allocator, will live for an entire chunk of documents
    >(
        &self,
        external_docid: &str,
        document: impl crate::update::new::document::Document<'a> + Debug,
        field_id_map: &RefCell<GlobalFieldsIdsMap>,
        doc_alloc: &'doc Bump,
        client: &http_client::ureq::Agent,
    ) -> Result<&'doc str, RenderPromptError> {
        let document = ParseableDocument::new(document, doc_alloc);
        let fields = BorrowedFields::new(&document, field_id_map, doc_alloc);
        let context = Context::new(&document, &fields);
        let mut rendered = bumpalo::collections::Vec::with_capacity_in(
            self.max_bytes.unwrap_or_else(default_max_bytes).get(),
            doc_alloc,
        );

        let tickets_urls = self
            .template
            .render_to_with(
                &mut rendered,
                &context,
                |_| {},
                |runtime| {
                    std::mem::take(
                        &mut *runtime.registers().get_mut::<filters::fetch_url::FetchUrlTickets>(),
                    )
                },
            )
            .map_err(|liquid_error| {
                RenderPromptError::missing_context_with_external_docid(
                    external_docid.to_owned(),
                    liquid_error,
                )
            })?;

        let rendered = std::str::from_utf8(rendered.into_bump_slice())
            .expect("render can only write UTF-8 because all inputs and processing preserve utf-8");

        let rendered = if let Some(replaced) = tickets_urls.resolve_url(client, rendered)? {
            doc_alloc.alloc_str(&replaced)
        } else if let Some(max_bytes) = self.max_bytes {
            if let Some(char_boundary) = must_truncate(rendered, max_bytes.get()) {
                &rendered[0..char_boundary]
            } else {
                rendered
            }
        } else {
            rendered
        };

        Ok(rendered)
    }

    pub fn render_kvdeladd(
        &self,
        document: &obkv::KvReaderU16,
        side: DelAdd,
        field_id_map: &FieldIdMapWithMetadata,
        client: &http_client::ureq::Agent,
    ) -> Result<String, RenderPromptError> {
        let document = Document::new(document, side, field_id_map.as_fields_ids_map());
        let fields = OwnedFields::new(&document, field_id_map);
        let context = Context::new(&document, &fields);

        let (mut rendered, tickets_urls) = self
            .template
            .render_with(
                &context,
                |_| {},
                |runtime| {
                    std::mem::take(
                        &mut *runtime.registers().get_mut::<filters::fetch_url::FetchUrlTickets>(),
                    )
                },
            )
            .map_err(RenderPromptError::missing_context)?;

        if let Some(replaced) = tickets_urls.resolve_url(client, &rendered)? {
            rendered = replaced;
        } else {
            // unfortunately, cannot truncate when we contain a ticket, otherwise we risk truncating the ticket or the resulting base64
            if let Some(max_bytes) = self.max_bytes {
                truncate(&mut rendered, max_bytes.get());
            }
        }
        Ok(rendered)
    }
}

fn truncate(s: &mut String, max_bytes: usize) {
    if let Some(char_boundary) = must_truncate(s, max_bytes) {
        s.truncate(char_boundary);
    }
}

fn must_truncate(s: &str, max_bytes: usize) -> Option<usize> {
    if max_bytes >= s.len() {
        return None;
    }
    for i in (0..=max_bytes).rev() {
        if s.is_char_boundary(i) {
            return Some(i);
        }
    }
    Some(0)
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
    #[ignore] // See <https://github.com/meilisearch/meilisearch/pull/5593> for explanation
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
    #[ignore] // See <https://github.com/meilisearch/meilisearch/pull/5593> for explanation
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
