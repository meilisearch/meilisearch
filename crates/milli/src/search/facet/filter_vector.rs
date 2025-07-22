use filter_parser::Token;
use roaring::{MultiOps, RoaringBitmap};

use crate::error::{Error, UserError};
use crate::vector::db::IndexEmbeddingConfig;
use crate::vector::{ArroyStats, ArroyWrapper};
use crate::Index;

#[derive(Debug)]
enum VectorFilterInner<'a> {
    Fragment { embedder_token: Token<'a>, fragment_token: Token<'a> },
    DocumentTemplate { embedder_token: Token<'a> },
    UserProvided { embedder_token: Token<'a> },
    FullEmbedder { embedder_token: Token<'a> },
}

#[derive(Debug)]
pub(super) struct VectorFilter<'a> {
    inner: Option<VectorFilterInner<'a>>,
    regenerate: bool,
}

#[derive(Debug, thiserror::Error)]
pub enum VectorFilterError<'a> {
    #[error("Vector filter cannot be empty.")]
    EmptyFilter,

    #[error("Vector filter must start with `_vectors` but found `{}`.", _0.value())]
    InvalidPrefix(Token<'a>),

    #[error("Vector filter is inconsistent: either specify a fragment name or remove the `fragments` part.")]
    MissingFragmentName(Token<'a>),

    #[error("Vector filter cannot have both `{}` and `{}`.", _0.0.value(), _0.1.value())]
    ExclusiveOptions(Box<(Token<'a>, Token<'a>)>),

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
            ExclusiveOptions(tokens) => tokens.1.clone().as_external_error(err).into(),
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
    /// - `_vectors.{embedder_name}.userProvided.regenerate`
    /// - `_vectors.{embedder_name}.documentTemplate`
    /// - `_vectors.{embedder_name}.documentTemplate.regenerate`
    /// - `_vectors.{embedder_name}.fragments.{fragment_name}`
    /// - `_vectors.{embedder_name}.fragments.{fragment_name}.regenerate`
    pub(super) fn parse(s: &'a Token<'a>) -> Result<Self, VectorFilterError<'a>> {
        let mut split = s.split(".").peekable();

        match split.next() {
            Some(token) if token.value() == "_vectors" => (),
            Some(token) => return Err(InvalidPrefix(token)),
            None => return Err(EmptyFilter),
        }

        let embedder_name = split.next();

        let mut fragment_name = None;
        if split.peek().map(|t| t.value()) == Some("fragments") {
            let token = split.next().expect("it was peeked before");

            fragment_name = Some(split.next().ok_or(MissingFragmentName(token))?);
        }

        let mut user_provided_token = None;
        if split.peek().map(|t| t.value()) == Some("userProvided") {
            user_provided_token = split.next();
        }

        let mut document_template_token = None;
        if split.peek().map(|t| t.value()) == Some("documentTemplate") {
            document_template_token = split.next();
        }

        let mut regenerate_token = None;
        if split.peek().map(|t| t.value()) == Some("regenerate") {
            regenerate_token = split.next();
        }

        let inner = match (fragment_name, user_provided_token, document_template_token) {
            (Some(fragment_name), None, None) => Some(VectorFilterInner::Fragment {
                embedder_token: embedder_name
                    .expect("embedder name comes before fragment so it's always Some"),
                fragment_token: fragment_name,
            }),
            (None, Some(_), None) => Some(VectorFilterInner::UserProvided {
                embedder_token: embedder_name
                    .expect("embedder name comes before userProvided so it's always Some"),
            }),
            (None, None, Some(_)) => Some(VectorFilterInner::DocumentTemplate {
                embedder_token: embedder_name
                    .expect("embedder name comes before documentTemplate so it's always Some"),
            }),
            (Some(a), Some(b), _) | (_, Some(a), Some(b)) | (Some(a), None, Some(b)) => {
                return Err(ExclusiveOptions(Box::new((a, b))));
            }
            (None, None, None) => embedder_name
                .map(|embedder_token| VectorFilterInner::FullEmbedder { embedder_token }),
        };

        if let Some(next) = split.next() {
            return Err(LeftoverToken(next))?;
        }

        Ok(Self { inner, regenerate: regenerate_token.is_some() })
    }

    pub(super) fn evaluate(
        self,
        rtxn: &heed::RoTxn<'_>,
        index: &Index,
        universe: Option<&RoaringBitmap>,
    ) -> crate::Result<RoaringBitmap> {
        let index_embedding_configs = index.embedding_configs();
        let embedding_configs = index_embedding_configs.embedding_configs(rtxn)?;

        let inners = match self.inner {
            Some(inner) => vec![inner],
            None => embedding_configs
                .iter()
                .map(|config| VectorFilterInner::FullEmbedder {
                    embedder_token: Token::from(config.name.as_str()),
                })
                .collect(),
        };

        let mut docids = inners
            .iter()
            .map(|i| i.evaluate_inner(rtxn, index, &embedding_configs, self.regenerate))
            .union()?;

        if let Some(universe) = universe {
            docids &= universe;
        }

        Ok(docids)
    }
}

impl VectorFilterInner<'_> {
    fn evaluate_inner(
        &self,
        rtxn: &heed::RoTxn<'_>,
        index: &Index,
        embedding_configs: &[IndexEmbeddingConfig],
        regenerate: bool,
    ) -> crate::Result<RoaringBitmap> {
        let embedder = match self {
            VectorFilterInner::Fragment { embedder_token, .. } => embedder_token,
            VectorFilterInner::DocumentTemplate { embedder_token } => embedder_token,
            VectorFilterInner::UserProvided { embedder_token } => embedder_token,
            VectorFilterInner::FullEmbedder { embedder_token } => embedder_token,
        };
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

        let mut docids = match self {
            VectorFilterInner::Fragment { embedder_token: embedder, fragment_token: fragment } => {
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
            VectorFilterInner::DocumentTemplate { .. } => {
                if !embedding_config.fragments.as_slice().is_empty() {
                    return Ok(RoaringBitmap::new());
                }

                let user_provided_docsids = embedder_info.embedding_status.user_provided_docids();
                let mut stats = ArroyStats::default();
                arroy_wrapper.aggregate_stats(rtxn, &mut stats)?;
                stats.documents - user_provided_docsids.clone()
            }
            VectorFilterInner::UserProvided { .. } => {
                let user_provided_docsids = embedder_info.embedding_status.user_provided_docids();
                user_provided_docsids.clone()
            }
            VectorFilterInner::FullEmbedder { .. } => {
                let mut stats = ArroyStats::default();
                arroy_wrapper.aggregate_stats(rtxn, &mut stats)?;
                stats.documents
            }
        };

        if regenerate {
            let skip_regenerate = embedder_info.embedding_status.skip_regenerate_docids();
            docids -= skip_regenerate;
        }

        Ok(docids)
    }
}
