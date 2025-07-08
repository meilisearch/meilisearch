use filter_parser::Token;
use roaring::RoaringBitmap;

use crate::error::{Error, UserError};
use crate::vector::{ArroyStats, ArroyWrapper};
use crate::Index;

pub(super) struct VectorFilter<'a> {
    embedder_token: Option<Token<'a>>,
    fragment_token: Option<Token<'a>>,
    user_provided: bool,
}

#[derive(Debug)]
pub enum VectorFilterError<'a> {
    EmptyFilter,
    InvalidPrefix(Token<'a>),
    MissingFragmentName(Token<'a>),
    UserProvidedWithFragment(Token<'a>),
    LeftoverToken(Token<'a>),
    EmbedderDoesNotExist {
        embedder: &'a Token<'a>,
        available: Vec<String>,
    },
    FragmentDoesNotExist {
        embedder: &'a Token<'a>,
        fragment: &'a Token<'a>,
        available: Vec<String>,
    },
}

use VectorFilterError::*;

impl std::error::Error for VectorFilterError<'_> {}

impl std::fmt::Display for VectorFilterError<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EmptyFilter => {
                write!(f, "Vector filter cannot be empty.")
            }
            InvalidPrefix(prefix) => {
                write!(
                    f,
                    "Vector filter must start with `_vectors` but found `{}`.",
                    prefix.value()
                )
            }
            MissingFragmentName(_token) => {
                write!(f, "Vector filter is inconsistent: either specify a fragment name or remove the `fragments` part.")
            }
            UserProvidedWithFragment(_token) => {
                write!(f, "Vector filter cannot specify both a fragment name and userProvided.")
            }
            LeftoverToken(token) => {
                write!(f, "Vector filter has leftover token: `{}`.", token.value())
            }
            EmbedderDoesNotExist { embedder, available } => {
                write!(f, "The embedder `{}` does not exist.", embedder.value())?;
                if available.is_empty() {
                    write!(f, " This index does not have configured embedders.")
                } else {
                    write!(f, " Available embedders are: ")?;
                    let mut available = available.clone();
                    available.sort_unstable();
                    for (idx, embedder) in available.iter().enumerate() {
                        write!(f, "`{embedder}`")?;
                        if idx != available.len() - 1 {
                            write!(f, ", ")?;
                        }
                    }
                    write!(f, ".")
                }
            }
            FragmentDoesNotExist { embedder, fragment, available } => {
                write!(
                    f,
                    "The fragment `{}` does not exist on embedder `{}`.",
                    fragment.value(),
                    embedder.value(),
                )?;
                if available.is_empty() {
                    write!(f, " This embedder does not have configured fragments.")
                } else {
                    write!(f, " Available fragments on this embedder are: ")?;
                    let mut available = available.clone();
                    available.sort_unstable();
                    for (idx, fragment) in available.iter().enumerate() {
                        write!(f, "`{fragment}`")?;
                        if idx != available.len() - 1 {
                            write!(f, ", ")?;
                        }
                    }
                    write!(f, ".")
                }
            }
        }
    }
}

impl<'a> From<VectorFilterError<'a>> for Error {
    fn from(err: VectorFilterError<'a>) -> Self {
        match &err {
            EmptyFilter => Error::UserError(UserError::InvalidFilter(err.to_string())),
            InvalidPrefix(token)
            | MissingFragmentName(token)
            | UserProvidedWithFragment(token)
            | LeftoverToken(token) => token.clone().as_external_error(err).into(),
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
    /// - `_vectors.{embedder_name}.userProvided`
    /// - `_vectors.{embedder_name}.fragments.{fragment_name}`
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
        if split.peek().map(|t| t.value()) == Some("userProvided")
            || split.peek().map(|t| t.value()) == Some("user_provided")
        {
            user_provided_token = split.next();
        }

        if let (Some(_), Some(user_provided_token)) = (&fragment_name, &user_provided_token) {
            return Err(UserProvidedWithFragment(user_provided_token.clone()))?;
        }

        if let Some(next) = split.next() {
            return Err(LeftoverToken(next))?;
        }

        Ok(Self {
            embedder_token: embedder_name,
            fragment_token: fragment_name,
            user_provided: user_provided_token.is_some(),
        })
    }

    pub(super) fn evaluate(
        self,
        rtxn: &heed::RoTxn<'_>,
        index: &Index,
        universe: Option<&RoaringBitmap>,
    ) -> crate::Result<RoaringBitmap> {
        let index_embedding_configs = index.embedding_configs();
        let embedding_configs = index_embedding_configs.embedding_configs(rtxn)?;

        let mut embedders = Vec::new();
        if let Some(embedder_token) = &self.embedder_token {
            let embedder_name = embedder_token.value();
            let Some(embedder_config) =
                embedding_configs.iter().find(|config| config.name == embedder_name)
            else {
                return Err(EmbedderDoesNotExist {
                    embedder: embedder_token,
                    available: embedding_configs.iter().map(|c| c.name.clone()).collect(),
                })?;
            };
            let Some(embedder_info) = index_embedding_configs.embedder_info(rtxn, embedder_name)?
            else {
                return Err(EmbedderDoesNotExist {
                    embedder: embedder_token,
                    available: embedding_configs.iter().map(|c| c.name.clone()).collect(),
                })?;
            };

            embedders.push((embedder_config, embedder_info));
        } else {
            for embedder_config in embedding_configs.iter() {
                let Some(embedder_info) =
                    index_embedding_configs.embedder_info(rtxn, &embedder_config.name)?
                else {
                    continue;
                };
                embedders.push((embedder_config, embedder_info));
            }
        };

        let mut docids = RoaringBitmap::new();
        for (embedder_config, embedder_info) in embedders {
            let arroy_wrapper = ArroyWrapper::new(
                index.vector_arroy,
                embedder_info.embedder_id,
                embedder_config.config.quantized(),
            );

            let mut new_docids = if let Some(fragment_token) = &self.fragment_token {
                let fragment_name = fragment_token.value();
                let Some(fragment_config) = embedder_config
                    .fragments
                    .as_slice()
                    .iter()
                    .find(|fragment| fragment.name == fragment_name)
                else {
                    return Err(FragmentDoesNotExist {
                        embedder: self
                            .embedder_token
                            .as_ref()
                            .expect("there can't be a fragment without an embedder"),
                        fragment: fragment_token,
                        available: embedder_config
                            .fragments
                            .as_slice()
                            .iter()
                            .map(|f| f.name.clone())
                            .collect(),
                    })?;
                };

                if let Some(universe) = universe {
                    arroy_wrapper
                        .items_in_store(rtxn, fragment_config.id, |bitmap| bitmap & universe)?
                } else {
                    arroy_wrapper
                        .items_in_store(rtxn, fragment_config.id, |bitmap| bitmap.clone())?
                }
            } else {
                let mut universe = universe.cloned();
                if self.user_provided {
                    let user_provided_docsids =
                        embedder_info.embedding_status.user_provided_docids();
                    match &mut universe {
                        Some(universe) => *universe &= user_provided_docsids,
                        None => universe = Some(user_provided_docsids.clone()),
                    }
                }

                let mut stats = ArroyStats::default();
                arroy_wrapper.aggregate_stats(rtxn, &mut stats)?;
                if let Some(universe) = &universe {
                    stats.documents & universe
                } else {
                    stats.documents
                }
            };

            docids |= new_docids;
        }

        if let Some(universe) = universe {
            docids &= universe;
        }

        Ok(docids)
    }
}
