use std::sync::Arc;

use ordered_float::OrderedFloat;

use crate::score_details::{self, ScoreDetails};
use crate::vector::Embedder;
use crate::{filtered_universe, DocumentId, Filter, Index, Result, SearchResult};

pub struct Recommend<'a> {
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

impl<'a> Recommend<'a> {
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

        let writer_index = (embedder_index as u16) << 8;
        let readers: std::result::Result<Vec<_>, _> = (0..=u8::MAX)
            .map_while(|k| {
                arroy::Reader::open(self.rtxn, writer_index | (k as u16), self.index.vector_arroy)
                    .map(Some)
                    .or_else(|e| match e {
                        arroy::Error::MissingMetadata => Ok(None),
                        e => Err(e),
                    })
                    .transpose()
            })
            .collect();

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
            }
        }

        results.sort_unstable_by_key(|(_, distance)| OrderedFloat(*distance));

        let mut documents_ids = Vec::with_capacity(self.limit);
        let mut document_scores = Vec::with_capacity(self.limit);

        // skip offset +1 to skip the target document that is normally returned
        for (docid, distance) in results.into_iter().skip(self.offset + 1) {
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
