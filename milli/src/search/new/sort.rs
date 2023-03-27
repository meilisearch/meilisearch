use roaring::RoaringBitmap;

use super::logger::SearchLogger;
use super::{RankingRule, RankingRuleOutput, RankingRuleQueryTrait, SearchContext};
use crate::heed_codec::facet::FacetGroupKeyCodec;
use crate::heed_codec::ByteSliceRefCodec;
use crate::search::facet::{ascending_facet_sort, descending_facet_sort};
use crate::{FieldId, Index, Result};

pub trait RankingRuleOutputIter<'ctx, Query> {
    fn next_bucket(&mut self) -> Result<Option<RankingRuleOutput<Query>>>;
}

pub struct RankingRuleOutputIterWrapper<'ctx, Query> {
    iter: Box<dyn Iterator<Item = Result<RankingRuleOutput<Query>>> + 'ctx>,
}
impl<'ctx, Query> RankingRuleOutputIterWrapper<'ctx, Query> {
    pub fn new(iter: Box<dyn Iterator<Item = Result<RankingRuleOutput<Query>>> + 'ctx>) -> Self {
        Self { iter }
    }
}
impl<'ctx, Query> RankingRuleOutputIter<'ctx, Query> for RankingRuleOutputIterWrapper<'ctx, Query> {
    fn next_bucket(&mut self) -> Result<Option<RankingRuleOutput<Query>>> {
        match self.iter.next() {
            Some(x) => x.map(Some),
            None => Ok(None),
        }
    }
}

// `Query` type parameter: the same as the type parameter to bucket_sort
// implements RankingRuleQuery trait, either querygraph or placeholdersearch
// The sort ranking rule doesn't need the query parameter, it is doing the same thing
// whether we're doing a querygraph or placeholder search.
//
// Query Stored anyway because every ranking rule must return a query from next_bucket
// ---
// "Mismatch" between new/old impl.:
// - old impl: roaring bitmap as input, ranking rule iterates other all the buckets
// - new impl: still works like that but it shouldn't, because the universe may change for every call to next_bucket, itself due to:
//    1. elements that were already returned by the ranking rule are subtracted from the universe, also done in the old impl (subtracted from the candidates)
//    2. NEW in the new impl.: distinct rule might have been applied btwn calls to next_bucket
// new impl ignores docs removed in (2), which is a missed perf opt issue, see `next_bucket`
// this perf problem is P2
// mostly happens when many documents map to the same distinct attribute value.
pub struct Sort<'ctx, Query> {
    field_name: String,
    field_id: Option<FieldId>,
    is_ascending: bool,
    original_query: Option<Query>,
    iter: Option<RankingRuleOutputIterWrapper<'ctx, Query>>,
}
impl<'ctx, Query> Sort<'ctx, Query> {
    pub fn _new(
        index: &Index,
        rtxn: &'ctx heed::RoTxn,
        field_name: String,
        is_ascending: bool,
    ) -> Result<Self> {
        let fields_ids_map = index.fields_ids_map(rtxn)?;
        let field_id = fields_ids_map.id(&field_name);

        Ok(Self { field_name, field_id, is_ascending, original_query: None, iter: None })
    }
}

impl<'ctx, Query: RankingRuleQueryTrait> RankingRule<'ctx, Query> for Sort<'ctx, Query> {
    fn id(&self) -> String {
        let Self { field_name, is_ascending, .. } = self;
        format!("{field_name}:{}", if *is_ascending { "asc" } else { "desc " })
    }
    fn start_iteration(
        &mut self,
        ctx: &mut SearchContext<'ctx>,
        _logger: &mut dyn SearchLogger<Query>,
        parent_candidates: &RoaringBitmap,
        parent_query: &Query,
    ) -> Result<()> {
        let iter: RankingRuleOutputIterWrapper<Query> = match self.field_id {
            Some(field_id) => {
                let number_db = ctx
                    .index
                    .facet_id_f64_docids
                    .remap_key_type::<FacetGroupKeyCodec<ByteSliceRefCodec>>();
                let string_db = ctx
                    .index
                    .facet_id_string_docids
                    .remap_key_type::<FacetGroupKeyCodec<ByteSliceRefCodec>>();

                let (number_iter, string_iter) = if self.is_ascending {
                    let number_iter = ascending_facet_sort(
                        ctx.txn,
                        number_db,
                        field_id,
                        parent_candidates.clone(),
                    )?;
                    let string_iter = ascending_facet_sort(
                        ctx.txn,
                        string_db,
                        field_id,
                        parent_candidates.clone(),
                    )?;

                    (itertools::Either::Left(number_iter), itertools::Either::Left(string_iter))
                } else {
                    let number_iter = descending_facet_sort(
                        ctx.txn,
                        number_db,
                        field_id,
                        parent_candidates.clone(),
                    )?;
                    let string_iter = descending_facet_sort(
                        ctx.txn,
                        string_db,
                        field_id,
                        parent_candidates.clone(),
                    )?;

                    (itertools::Either::Right(number_iter), itertools::Either::Right(string_iter))
                };

                let query_graph = parent_query.clone();
                RankingRuleOutputIterWrapper::new(Box::new(number_iter.chain(string_iter).map(
                    move |r| {
                        let (docids, _) = r?;
                        Ok(RankingRuleOutput { query: query_graph.clone(), candidates: docids })
                    },
                )))
            }
            None => RankingRuleOutputIterWrapper::new(Box::new(std::iter::empty())),
        };
        self.original_query = Some(parent_query.clone());
        self.iter = Some(iter);
        Ok(())
    }

    fn next_bucket(
        &mut self,
        _ctx: &mut SearchContext<'ctx>,
        _logger: &mut dyn SearchLogger<Query>,
        universe: &RoaringBitmap,
    ) -> Result<Option<RankingRuleOutput<Query>>> {
        let iter = self.iter.as_mut().unwrap();
        // TODO: we should make use of the universe in the function below
        // good for correctness, but ideally iter.next_bucket would take the current universe into account,
        // as right now it could return buckets that don't intersect with the universe, meaning we will make many
        // unneeded calls.
        if let Some(mut bucket) = iter.next_bucket()? {
            bucket.candidates &= universe;
            Ok(Some(bucket))
        } else {
            let query = self.original_query.as_ref().unwrap().clone();
            Ok(Some(RankingRuleOutput { query, candidates: universe.clone() }))
        }
    }

    fn end_iteration(
        &mut self,
        _ctx: &mut SearchContext<'ctx>,
        _logger: &mut dyn SearchLogger<Query>,
    ) {
        self.original_query = None;
        self.iter = None;
    }
}
