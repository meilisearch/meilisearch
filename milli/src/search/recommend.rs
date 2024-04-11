use std::sync::Arc;

use ordered_float::OrderedFloat;
use roaring::RoaringBitmap;
use serde_json::Value;

use crate::score_details::{self, ScoreDetails};
use crate::vector::Embedder;
use crate::{filtered_universe, DocumentId, Filter, Index, Result, SearchResult};

enum RecommendKind<'a> {
    Id(DocumentId),
    Prompt { prompt: &'a str, context: Option<Value>, id: Option<DocumentId> },
}

pub struct Recommend<'a> {
    kind: RecommendKind<'a>,
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
    pub fn with_docid(
        id: DocumentId,
        offset: usize,
        limit: usize,
        index: &'a Index,
        rtxn: &'a heed::RoTxn<'a>,
        embedder_name: String,
        embedder: Arc<Embedder>,
    ) -> Self {
        Self {
            kind: RecommendKind::Id(id),
            filter: None,
            offset,
            limit,
            rtxn,
            index,
            embedder_name,
            embedder,
        }
    }

    pub fn with_prompt(
        prompt: &'a str,
        id: Option<DocumentId>,
        context: Option<Value>,
        offset: usize,
        limit: usize,
        index: &'a Index,
        rtxn: &'a heed::RoTxn<'a>,
        embedder_name: String,
        embedder: Arc<Embedder>,
    ) -> Self {
        Self {
            kind: RecommendKind::Prompt { prompt, context, id },
            filter: None,
            offset,
            limit,
            rtxn,
            index,
            embedder_name,
            embedder,
        }
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

        /// FIXME: make id optional...
        let id = match &self.kind {
            RecommendKind::Id(id) => *id,
            RecommendKind::Prompt { prompt, context, id } => id.unwrap(),
        };

        let personalization_vector = if let RecommendKind::Prompt { prompt, context, id } =
            &self.kind
        {
            let fields_ids_map = self.index.fields_ids_map(self.rtxn)?;

            let document = if let Some(id) = id {
                Some(self.index.iter_documents(self.rtxn, std::iter::once(*id))?.next().unwrap()?.1)
            } else {
                None
            };
            let document = document
                .map(|document| crate::prompt::Document::from_doc_obkv(document, &fields_ids_map));

            let context =
                crate::prompt::recommend::Context::new(document.as_ref(), context.clone());

            /// FIXME: handle error bad template
            let template =
                liquid::ParserBuilder::new().stdlib().build().unwrap().parse(prompt).unwrap();

            /// FIXME: handle error bad context
            let rendered = template.render(&context).unwrap();

            /// FIXME: handle embedding error
            Some(self.embedder.embed_one(rendered).unwrap())
        } else {
            None
        };

        for reader in readers.iter() {
            let nns_by_item = reader.nns_by_item(
                self.rtxn,
                id,
                self.limit + self.offset + 1,
                None,
                Some(&universe),
            )?;

            if let Some(nns_by_item) = nns_by_item {
                let mut nns = match &personalization_vector {
                    Some(vector) => {
                        let candidates: RoaringBitmap =
                            nns_by_item.iter().map(|(docid, _)| docid).collect();
                        reader.nns_by_vector(
                            self.rtxn,
                            vector,
                            self.limit + self.offset + 1,
                            None,
                            Some(&candidates),
                        )?
                    }
                    None => nns_by_item,
                };

                results.append(&mut nns);
            }
        }

        results.sort_unstable_by_key(|(_, distance)| OrderedFloat(*distance));

        let mut documents_ids = Vec::with_capacity(self.limit);
        let mut document_scores = Vec::with_capacity(self.limit);

        // skip offset +1 to skip the target document that is normally returned
        for (docid, distance) in results.into_iter().skip(self.offset) {
            if documents_ids.len() == self.limit {
                break;
            }
            if id == docid {
                continue;
            }

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
