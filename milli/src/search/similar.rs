use std::sync::Arc;

use ordered_float::OrderedFloat;
use roaring::RoaringBitmap;

use crate::score_details::{self, ScoreDetails};
use crate::vector::Embedder;
use crate::{filtered_universe, DocumentId, Filter, Index, Result, SearchResult};

pub struct Similar<'a> {
    id: DocumentId,
    // this should be linked to the String in the query
    filter: Option<Filter<'a>>,
    offset: usize,
    limit: usize,
    rtxn: &'a heed::RoTxn<'a>,
    index: &'a Index,
    embedder_name: String,
    embedder: Arc<Embedder>,
}

impl<'a> Similar<'a> {
    pub fn new(
        id: DocumentId,
        offset: usize,
        limit: usize,
        index: &'a Index,
        rtxn: &'a heed::RoTxn<'a>,
        embedder_name: String,
        embedder: Arc<Embedder>,
    ) -> Self {
        Self { id, filter: None, offset, limit, rtxn, index, embedder_name, embedder }
    }

    pub fn filter(&mut self, filter: Filter<'a>) -> &mut Self {
        self.filter = Some(filter);
        self
    }

    pub fn execute(&self) -> Result<SearchResult> {
        let universe = filtered_universe(self.index, self.rtxn, &self.filter)?;

        let embedder_index =
            self.index
                .embedder_category_id
                .get(self.rtxn, &self.embedder_name)?
                .ok_or_else(|| crate::UserError::InvalidEmbedder(self.embedder_name.to_owned()))?;

        let readers: std::result::Result<Vec<_>, _> =
            self.index.arroy_readers(self.rtxn, embedder_index).collect();

        let readers = readers?;

        let mut results = Vec::new();

        for reader in readers.iter() {
            let nns_by_item = reader.nns_by_item(
                self.rtxn,
                self.id,
                self.limit + self.offset + 1,
                None,
                Some(&universe),
            )?;
            if let Some(mut nns_by_item) = nns_by_item {
                results.append(&mut nns_by_item);
            } else {
                break;
            }
        }

        results.sort_unstable_by_key(|(_, distance)| OrderedFloat(*distance));

        let mut documents_ids = Vec::with_capacity(self.limit);
        let mut document_scores = Vec::with_capacity(self.limit);
        // list of documents we've already seen, so that we don't return the same document multiple times.
        // initialized to the target document, that we never want to return.
        let mut documents_seen = RoaringBitmap::new();
        documents_seen.insert(self.id);

        for (docid, distance) in results
            .into_iter()
            // skip documents we've already seen & mark that we saw the current document
            .filter(|(docid, _)| documents_seen.insert(*docid))
            .skip(self.offset)
            // take **after** filter and skip so that we get exactly limit elements if available
            .take(self.limit)
        {
            documents_ids.push(docid);

            let score = 1.0 - distance;
            let score = self
                .embedder
                .distribution()
                .map(|distribution| distribution.shift(score))
                .unwrap_or(score);

            let score = ScoreDetails::Vector(score_details::Vector { similarity: Some(score) });

            document_scores.push(vec![score]);
        }

        Ok(SearchResult {
            matching_words: Default::default(),
            candidates: universe,
            documents_ids,
            document_scores,
            degraded: false,
            used_negative_operator: false,
        })
    }
}
