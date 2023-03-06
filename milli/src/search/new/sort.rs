use super::logger::SearchLogger;
use super::{
    RankingRule, RankingRuleOutput, RankingRuleOutputIter, RankingRuleOutputIterWrapper,
    RankingRuleQueryTrait, SearchContext,
};
use crate::{
    // facet::FacetType,
    heed_codec::{facet::FacetGroupKeyCodec, ByteSliceRefCodec},
    search::facet::{ascending_facet_sort, descending_facet_sort},
    FieldId,
    Index,
    Result,
};
use roaring::RoaringBitmap;

pub struct Sort<'search, Query> {
    field_name: String,
    field_id: Option<FieldId>,
    is_ascending: bool,
    original_query: Option<Query>,
    iter: Option<RankingRuleOutputIterWrapper<'search, Query>>,
}
impl<'search, Query> Sort<'search, Query> {
    pub fn _new(
        index: &Index,
        rtxn: &'search heed::RoTxn,
        field_name: String,
        is_ascending: bool,
    ) -> Result<Self> {
        let fields_ids_map = index.fields_ids_map(rtxn)?;
        let field_id = fields_ids_map.id(&field_name);

        Ok(Self { field_name, field_id, is_ascending, original_query: None, iter: None })
    }
}

impl<'search, Query: RankingRuleQueryTrait> RankingRule<'search, Query> for Sort<'search, Query> {
    fn id(&self) -> String {
        let Self { field_name, is_ascending, .. } = self;
        format!("{field_name}:{}", if *is_ascending { "asc" } else { "desc " })
    }
    fn start_iteration(
        &mut self,
        ctx: &mut SearchContext<'search>,
        _logger: &mut dyn SearchLogger<Query>,
        parent_candidates: &RoaringBitmap,
        parent_query_graph: &Query,
    ) -> Result<()> {
        let iter: RankingRuleOutputIterWrapper<Query> = match self.field_id {
            Some(field_id) => {
                let make_iter =
                    if self.is_ascending { ascending_facet_sort } else { descending_facet_sort };

                let number_iter = make_iter(
                    ctx.txn,
                    ctx.index
                        .facet_id_f64_docids
                        .remap_key_type::<FacetGroupKeyCodec<ByteSliceRefCodec>>(),
                    field_id,
                    parent_candidates.clone(),
                )?;

                let string_iter = make_iter(
                    ctx.txn,
                    ctx.index
                        .facet_id_string_docids
                        .remap_key_type::<FacetGroupKeyCodec<ByteSliceRefCodec>>(),
                    field_id,
                    parent_candidates.clone(),
                )?;
                let query_graph = parent_query_graph.clone();
                RankingRuleOutputIterWrapper::new(Box::new(number_iter.chain(string_iter).map(
                    move |docids| {
                        Ok(RankingRuleOutput { query: query_graph.clone(), candidates: docids? })
                    },
                )))
            }
            None => RankingRuleOutputIterWrapper::new(Box::new(std::iter::empty())),
        };
        self.original_query = Some(parent_query_graph.clone());
        self.iter = Some(iter);
        Ok(())
    }

    fn next_bucket(
        &mut self,
        _ctx: &mut SearchContext<'search>,
        _logger: &mut dyn SearchLogger<Query>,
        universe: &RoaringBitmap,
    ) -> Result<Option<RankingRuleOutput<Query>>> {
        let iter = self.iter.as_mut().unwrap();
        // TODO: we should make use of the universe in the function below
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
        _ctx: &mut SearchContext<'search>,
        _logger: &mut dyn SearchLogger<Query>,
    ) {
        self.original_query = None;
        self.iter = None;
    }
}
