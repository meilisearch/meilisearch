use crate::error::FaultSource;

#[derive(Debug, thiserror::Error)]
#[error("{fault}: {kind}")]
pub struct NewPromptError {
    pub kind: NewPromptErrorKind,
    pub fault: FaultSource,
}

impl From<NewPromptError> for crate::Error {
    fn from(value: NewPromptError) -> Self {
        crate::Error::UserError(crate::UserError::InvalidPrompt(value))
    }
}

impl NewPromptError {
    pub(crate) fn cannot_parse_template(inner: liquid::Error) -> NewPromptError {
        Self { kind: NewPromptErrorKind::CannotParseTemplate(inner), fault: FaultSource::User }
    }

    pub(crate) fn invalid_fields_in_template(inner: liquid::Error) -> NewPromptError {
        Self { kind: NewPromptErrorKind::InvalidFieldsInTemplate(inner), fault: FaultSource::User }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum NewPromptErrorKind {
    #[error("cannot parse template: {0}")]
    CannotParseTemplate(liquid::Error),
    #[error("template contains invalid fields: {0}. Only `doc.*`, `fields[i].name`, `fields[i].value` are supported")]
    InvalidFieldsInTemplate(liquid::Error),
}

#[derive(Debug, thiserror::Error)]
#[error("{fault}: {kind}")]
pub struct RenderPromptError {
    pub kind: RenderPromptErrorKind,
    pub fault: FaultSource,
}
impl RenderPromptError {
    pub(crate) fn missing_context_with_external_docid(
        external_docid: String,
        inner: liquid::Error,
    ) -> RenderPromptError {
        Self {
            kind: RenderPromptErrorKind::MissingContextWithExternalDocid(external_docid, inner),
            fault: FaultSource::User,
        }
    }

    pub(crate) fn missing_context(inner: liquid::Error) -> RenderPromptError {
        Self { kind: RenderPromptErrorKind::MissingContext(inner), fault: FaultSource::User }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum RenderPromptErrorKind {
    #[error("missing field in document: {0}")]
    MissingContext(liquid::Error),
    #[error("missing field in document `{0}`: {1}")]
    MissingContextWithExternalDocid(String, liquid::Error),
}

impl From<RenderPromptError> for crate::Error {
    fn from(value: RenderPromptError) -> Self {
        crate::Error::UserError(crate::UserError::MissingDocumentField(value))
    }
}
