use filter_parser::Token;
use roaring::{MultiOps, RoaringBitmap};

use crate::error::{Error, UserError};
use crate::vector::db::IndexEmbeddingConfig;
use crate::vector::{ArroyStats, ArroyWrapper};
use crate::Index;

#[derive(Debug)]
enum VectorFilterInner<'a> {
    Fragment(Token<'a>),
    DocumentTemplate,
    UserProvided,
    Regenerate,
    None,
}

#[derive(Debug)]
pub(super) struct VectorFilter<'a> {
    embedder: Option<Token<'a>>,
    inner: VectorFilterInner<'a>,
}

#[derive(Debug, thiserror::Error)]
pub enum VectorFilterError<'a> {
    #[error("Vector filter cannot be empty.")]
    EmptyFilter,

    #[error("Vector filter must start with `_vectors` but found `{}`.", _0.value())]
    InvalidPrefix(Token<'a>),

    #[error("Vector filter is inconsistent: either specify a fragment name or remove the `fragments` part.")]
    MissingFragmentName(Token<'a>),

    #[error("Vector filter cannot have both {}.", {
        _0.iter().map(|t| format!("`{}`", t.value())).collect::<Vec<_>>().join(" and ")
    })]
    ExclusiveOptions(Vec<Token<'a>>),

    #[error("Vector filter has leftover token: `{}`.", _0.value())]
    LeftoverToken(Token<'a>),

    #[error("The embedder `{}` does not exist. {}", embedder.value(), {
        if available.is_empty() {
            String::from("This index does not have any configured embedders.")
        } else {
            let mut available = available.clone();
            available.sort_unstable();
            format!("Available embedders are: {}.", available.iter().map(|e| format!("`{e}`")).collect::<Vec<_>>().join(", "))
        }
    })]
    EmbedderDoesNotExist { embedder: &'a Token<'a>, available: Vec<String> },

    #[error("The fragment `{}` does not exist on embedder `{}`. {}", fragment.value(), embedder.value(), {
        if available.is_empty() {
            String::from("This embedder does not have any configured fragments.")
        } else {
            let mut available = available.clone();
            available.sort_unstable();
            format!("Available fragments on this embedder are: {}.", available.iter().map(|f| format!("`{f}`")).collect::<Vec<_>>().join(", "))
        }
    })]
    FragmentDoesNotExist {
        embedder: &'a Token<'a>,
        fragment: &'a Token<'a>,
        available: Vec<String>,
    },
}

use VectorFilterError::*;

impl<'a> From<VectorFilterError<'a>> for Error {
    fn from(err: VectorFilterError<'a>) -> Self {
        match &err {
            EmptyFilter => Error::UserError(UserError::InvalidFilter(err.to_string())),
            InvalidPrefix(token) | MissingFragmentName(token) | LeftoverToken(token) => {
                token.clone().as_external_error(err).into()
            }
            ExclusiveOptions(tokens) => tokens
                .first()
                .cloned()
                .unwrap_or_else(|| Token::from("")) // Should never happen: tokens is never created empty
                .as_external_error(err)
                .into(),
            EmbedderDoesNotExist { embedder: token, .. }
            | FragmentDoesNotExist { fragment: token, .. } => token.as_external_error(err).into(),
        }
    }
}

impl<'a> VectorFilter<'a> {
    pub(super) fn matches(value: &str) -> bool {
        value.starts_with("_vectors.") || value == "_vectors"
    }

    /// Parses a vector filter string.
    ///
    /// Valid formats:
    /// - `_vectors`
    /// - `_vectors.{embedder_name}`
    /// - `_vectors.{embedder_name}.regenerate`
    /// - `_vectors.{embedder_name}.userProvided`
    /// - `_vectors.{embedder_name}.documentTemplate`
    /// - `_vectors.{embedder_name}.fragments.{fragment_name}`
    pub(super) fn parse(s: &'a Token<'a>) -> Result<Self, VectorFilterError<'a>> {
        let mut split = s.split(".").peekable();

        match split.next() {
            Some(token) if token.value() == "_vectors" => (),
            Some(token) => return Err(InvalidPrefix(token)),
            None => return Err(EmptyFilter),
        }

        let embedder_name = split.next();

        let mut fragment_tokens = None;
        if split.peek().map(|t| t.value()) == Some("fragments") {
            let token = split.next().expect("it was peeked before");
            let name = split.next().ok_or_else(|| MissingFragmentName(token.clone()))?;

            fragment_tokens = Some((token, name));
        }

        let mut remaining_tokens = split.collect::<Vec<_>>();

        let mut user_provided_token = None;
        if let Some(position) = remaining_tokens.iter().position(|t| t.value() == "userProvided") {
            user_provided_token = Some(remaining_tokens.remove(position));
        }

        let mut document_template_token = None;
        if let Some(position) =
            remaining_tokens.iter().position(|t| t.value() == "documentTemplate")
        {
            document_template_token = Some(remaining_tokens.remove(position));
        }

        let mut regenerate_token = None;
        if let Some(position) = remaining_tokens.iter().position(|t| t.value() == "regenerate") {
            regenerate_token = Some(remaining_tokens.remove(position));
        }

        if !remaining_tokens.is_empty() {
            return Err(LeftoverToken(remaining_tokens.remove(0)));
        }

        let inner =
            match (fragment_tokens, user_provided_token, document_template_token, regenerate_token)
            {
                (Some((_token, name)), None, None, None) => VectorFilterInner::Fragment(name),
                (None, Some(_), None, None) => VectorFilterInner::UserProvided,
                (None, None, Some(_), None) => VectorFilterInner::DocumentTemplate,
                (None, None, None, Some(_)) => VectorFilterInner::Regenerate,
                (None, None, None, None) => VectorFilterInner::None,
                (a, b, c, d) => {
                    let a = a.map(|(token, _)| token);
                    let present = [a, b, c, d].into_iter().flatten().collect();
                    return Err(ExclusiveOptions(present));
                }
            };

        Ok(Self { inner, embedder: embedder_name })
    }

    pub(super) fn evaluate(
        self,
        rtxn: &heed::RoTxn<'_>,
        index: &Index,
        universe: Option<&RoaringBitmap>,
    ) -> crate::Result<RoaringBitmap> {
        let index_embedding_configs = index.embedding_configs();
        let embedding_configs = index_embedding_configs.embedding_configs(rtxn)?;

        let embedders = match self.embedder {
            Some(embedder) => vec![embedder],
            None => {
                embedding_configs.iter().map(|config| Token::from(config.name.as_str())).collect()
            }
        };

        let mut docids = embedders
            .iter()
            .map(|e| self.inner.evaluate(rtxn, index, e, &embedding_configs))
            .union()?;

        if let Some(universe) = universe {
            docids &= universe;
        }

        Ok(docids)
    }
}

impl VectorFilterInner<'_> {
    fn evaluate(
        &self,
        rtxn: &heed::RoTxn<'_>,
        index: &Index,
        embedder: &Token<'_>,
        embedding_configs: &[IndexEmbeddingConfig],
    ) -> crate::Result<RoaringBitmap> {
        let embedder_name = embedder.value();
        let available_embedders =
            || embedding_configs.iter().map(|c| c.name.clone()).collect::<Vec<_>>();

        let embedding_config = embedding_configs
            .iter()
            .find(|config| config.name == embedder_name)
            .ok_or_else(|| EmbedderDoesNotExist { embedder, available: available_embedders() })?;

        let embedder_info = index
            .embedding_configs()
            .embedder_info(rtxn, embedder_name)?
            .ok_or_else(|| EmbedderDoesNotExist { embedder, available: available_embedders() })?;

        let arroy_wrapper = ArroyWrapper::new(
            index.vector_arroy,
            embedder_info.embedder_id,
            embedding_config.config.quantized(),
        );

        let docids = match self {
            VectorFilterInner::Fragment(fragment) => {
                let fragment_name = fragment.value();
                let fragment_config = embedding_config
                    .fragments
                    .as_slice()
                    .iter()
                    .find(|fragment| fragment.name == fragment_name)
                    .ok_or_else(|| FragmentDoesNotExist {
                        embedder,
                        fragment,
                        available: embedding_config
                            .fragments
                            .as_slice()
                            .iter()
                            .map(|f| f.name.clone())
                            .collect(),
                    })?;

                arroy_wrapper.items_in_store(rtxn, fragment_config.id, |bitmap| bitmap.clone())?
            }
            VectorFilterInner::DocumentTemplate => {
                if !embedding_config.fragments.as_slice().is_empty() {
                    return Ok(RoaringBitmap::new());
                }

                let user_provided_docsids = embedder_info.embedding_status.user_provided_docids();
                let mut stats = ArroyStats::default();
                arroy_wrapper.aggregate_stats(rtxn, &mut stats)?;
                stats.documents - user_provided_docsids.clone()
            }
            VectorFilterInner::UserProvided => {
                let user_provided_docsids = embedder_info.embedding_status.user_provided_docids();
                user_provided_docsids.clone()
            }
            VectorFilterInner::Regenerate => {
                let mut stats = ArroyStats::default();
                arroy_wrapper.aggregate_stats(rtxn, &mut stats)?;
                let skip_regenerate = embedder_info.embedding_status.skip_regenerate_docids();
                stats.documents - skip_regenerate
            }
            VectorFilterInner::None => {
                let mut stats = ArroyStats::default();
                arroy_wrapper.aggregate_stats(rtxn, &mut stats)?;
                stats.documents
            }
        };

        Ok(docids)
    }
}
