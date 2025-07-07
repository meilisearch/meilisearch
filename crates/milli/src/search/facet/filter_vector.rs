use filter_parser::Condition;
use roaring::RoaringBitmap;

use crate::error::{Error, UserError};
use crate::vector::{ArroyStats, ArroyWrapper};
use crate::{Index, Result};

pub(super) struct VectorFilter<'a> {
    embedder_name: Option<&'a str>,
    fragment_name: Option<&'a str>,
    user_provided: bool,
    // TODO: not_user_provided: bool,
}

impl<'a> VectorFilter<'a> {
    pub(super) fn matches(value: &str, op: &Condition) -> bool {
        matches!(op, Condition::Exists) && (value.starts_with("_vectors.") || value == "_vectors")
    }

    /// Parses a vector filter string.
    ///
    /// Valid formats:
    /// - `_vectors`
    /// - `_vectors.userProvided`
    /// - `_vectors.{embedder_name}`
    /// - `_vectors.{embedder_name}.userProvided`
    /// - `_vectors.{embedder_name}.fragments.{fragment_name}`
    /// - `_vectors.{embedder_name}.fragments.{fragment_name}.userProvided`
    pub(super) fn parse(s: &'a str) -> Result<Self> {
        let mut split = s.split('.').peekable();

        if split.next() != Some("_vectors") {
            return Err(Error::UserError(UserError::InvalidFilter(String::from(
                "Vector filter must start with '_vectors'",
            ))));
        }

        let embedder_name = split.next();

        let mut fragment_name = None;
        if split.peek() == Some(&"fragments") {
            split.next();

            fragment_name = Some(split.next().ok_or_else(|| {
                Error::UserError(UserError::InvalidFilter(
                    String::from("Vector filter is inconsistent: either specify a fragment name or remove the 'fragments' part"),
                ))
            })?);
        }

        let mut user_provided = false;
        if split.peek() == Some(&"userProvided") || split.peek() == Some(&"user_provided") {
            split.next();
            user_provided = true;
        }

        if let Some(next) = split.next() {
            return Err(Error::UserError(UserError::InvalidFilter(format!(
                "Unexpected part in vector filter: '{next}'"
            ))));
        }

        Ok(Self { embedder_name, fragment_name, user_provided })
    }

    pub(super) fn evaluate(
        &self,
        rtxn: &heed::RoTxn<'_>,
        index: &Index,
        universe: Option<&RoaringBitmap>,
    ) -> Result<RoaringBitmap> {
        let index_embedding_configs = index.embedding_configs();
        let embedding_configs = index_embedding_configs.embedding_configs(rtxn)?;

        let mut embedders = Vec::new();
        if let Some(embedder_name) = self.embedder_name {
            let Some(embedder_config) =
                embedding_configs.iter().find(|config| config.name == embedder_name)
            else {
                return Ok(RoaringBitmap::new());
            };
            let Some(embedder_info) =
                index_embedding_configs.embedder_info(rtxn, embedder_name)?
            else {
                return Ok(RoaringBitmap::new());
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

            let mut new_docids = if let Some(fragment_name) = self.fragment_name {
                let Some(fragment_config) = embedder_config
                    .fragments
                    .as_slice()
                    .iter()
                    .find(|fragment| fragment.name == fragment_name)
                else {
                    return Ok(RoaringBitmap::new());
                };

                arroy_wrapper.items_in_store(rtxn, fragment_config.id, |bitmap| bitmap.clone())?
            } else {
                let mut stats = ArroyStats::default();
                arroy_wrapper.aggregate_stats(rtxn, &mut stats)?;
                stats.documents
            };

            // FIXME: performance
            if self.user_provided {
                let user_provided_docsids = embedder_info.embedding_status.user_provided_docids();
                new_docids &= user_provided_docsids;
            }

            docids |= new_docids;
        }

        if let Some(universe) = universe {
            docids &= universe;
        }

        Ok(docids)
    }
}
