use filter_parser::{Token, VectorFilter};
use roaring::{MultiOps, RoaringBitmap};

use crate::error::{DidYouMean, Error};
use crate::vector::db::IndexEmbeddingConfig;
use crate::vector::{VectorStore, VectorStoreStats};
use crate::Index;

#[derive(Debug, thiserror::Error)]
pub enum VectorFilterError<'a> {
    #[error("The embedder `{}` does not exist. {}", embedder.value(), {
        if available.is_empty() {
            String::from("This index does not have any configured embedders.")
        } else {
            let mut available = available.clone();
            available.sort_unstable();
            let did_you_mean = DidYouMean::new(embedder.value(), &available);
            format!("Available embedders are: {}.{did_you_mean}", available.iter().map(|e| format!("`{e}`")).collect::<Vec<_>>().join(", "))
        }
    })]
    EmbedderDoesNotExist { embedder: &'a Token<'a>, available: Vec<String> },

    #[error("The fragment `{}` does not exist on embedder `{}`. {}", fragment.value(), embedder.value(), {
        if available.is_empty() {
            String::from("This embedder does not have any configured fragments.")
        } else {
            let mut available = available.clone();
            available.sort_unstable();
            let did_you_mean = DidYouMean::new(fragment.value(), &available);
            format!("Available fragments on this embedder are: {}.{did_you_mean}", available.iter().map(|f| format!("`{f}`")).collect::<Vec<_>>().join(", "))
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
            EmbedderDoesNotExist { embedder: token, .. }
            | FragmentDoesNotExist { fragment: token, .. } => token.as_external_error(err).into(),
        }
    }
}

pub(super) fn evaluate(
    rtxn: &heed::RoTxn<'_>,
    index: &Index,
    universe: Option<&RoaringBitmap>,
    embedder: Option<Token<'_>>,
    filter: &VectorFilter<'_>,
) -> crate::Result<RoaringBitmap> {
    let index_embedding_configs = index.embedding_configs();
    let embedding_configs = index_embedding_configs.embedding_configs(rtxn)?;

    let embedders = match embedder {
        Some(embedder) => vec![embedder],
        None => embedding_configs.iter().map(|config| Token::from(config.name.as_str())).collect(),
    };

    let mut docids = embedders
        .iter()
        .map(|e| evaluate_inner(rtxn, index, e, &embedding_configs, filter))
        .union()?;

    if let Some(universe) = universe {
        docids &= universe;
    }

    Ok(docids)
}

fn evaluate_inner(
    rtxn: &heed::RoTxn<'_>,
    index: &Index,
    embedder: &Token<'_>,
    embedding_configs: &[IndexEmbeddingConfig],
    filter: &VectorFilter<'_>,
) -> crate::Result<RoaringBitmap> {
    let backend = index.get_vector_store(rtxn)?.unwrap_or_default();
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

    let vector_store = VectorStore::new(
        backend,
        index.vector_store,
        embedder_info.embedder_id,
        embedding_config.config.quantized(),
    );

    let docids = match filter {
        VectorFilter::Fragment(fragment) => {
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

            let user_provided_docids = embedder_info.embedding_status.user_provided_docids();
            vector_store.items_in_store(rtxn, fragment_config.id, |bitmap| {
                bitmap.clone() - user_provided_docids
            })?
        }
        VectorFilter::DocumentTemplate => {
            if !embedding_config.fragments.as_slice().is_empty() {
                return Ok(RoaringBitmap::new());
            }

            let user_provided_docids = embedder_info.embedding_status.user_provided_docids();
            let mut stats = VectorStoreStats::default();
            vector_store.aggregate_stats(rtxn, &mut stats)?;
            stats.documents - user_provided_docids.clone()
        }
        VectorFilter::UserProvided => {
            let user_provided_docids = embedder_info.embedding_status.user_provided_docids();
            user_provided_docids.clone()
        }
        VectorFilter::Regenerate => {
            let mut stats = VectorStoreStats::default();
            vector_store.aggregate_stats(rtxn, &mut stats)?;
            let skip_regenerate = embedder_info.embedding_status.skip_regenerate_docids();
            stats.documents - skip_regenerate
        }
        VectorFilter::None => {
            let mut stats = VectorStoreStats::default();
            vector_store.aggregate_stats(rtxn, &mut stats)?;
            stats.documents
        }
    };

    Ok(docids)
}
