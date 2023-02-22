use std::borrow::Cow;
use std::collections::HashMap;
use std::mem::take;
use std::ops::{BitOr, BitOrAssign};

use roaring::RoaringBitmap;

use self::asc_desc::AscDesc;
use self::attribute::Attribute;
use self::exactness::Exactness;
use self::initial::Initial;
use self::proximity::Proximity;
use self::r#final::Final;
use self::typo::Typo;
use self::words::Words;
use super::query_tree::{Operation, PrimitiveQueryPart, Query, QueryKind};
use super::CriterionImplementationStrategy;
use crate::search::criteria::geo::Geo;
use crate::search::{word_derivations, Distinct, WordDerivationsCache};
use crate::update::{MAX_LENGTH_FOR_PREFIX_PROXIMITY_DB, MAX_PROXIMITY_FOR_PREFIX_PROXIMITY_DB};
use crate::{AscDesc as AscDescName, DocumentId, FieldId, Index, Member, Result};

mod asc_desc;
pub use asc_desc::{facet_max_value, facet_min_value};
mod attribute;
mod exactness;
pub mod r#final;
mod geo;
mod initial;
mod proximity;
mod typo;
mod words;

pub trait Criterion {
    fn next(&mut self, params: &mut CriterionParameters) -> Result<Option<CriterionResult>>;
}

/// The result of a call to the parent criterion.
#[derive(Debug, Clone, PartialEq)]
pub struct CriterionResult {
    /// The query tree that must be used by the children criterion to fetch candidates.
    query_tree: Option<Operation>,
    /// The candidates that this criterion is allowed to return subsets of,
    /// if None, it is up to the child to compute the candidates itself.
    candidates: Option<RoaringBitmap>,
    /// The candidates, coming from facet filters, that this criterion is allowed to return subsets of.
    filtered_candidates: Option<RoaringBitmap>,
    /// Candidates that comes from the current bucket of the initial criterion.
    initial_candidates: Option<InitialCandidates>,
}

#[derive(Debug, PartialEq)]
pub struct CriterionParameters<'a> {
    wdcache: &'a mut WordDerivationsCache,
    excluded_candidates: &'a RoaringBitmap,
}

/// Either a set of candidates that defines the candidates
/// that are allowed to be returned,
/// or the candidates that must never be returned.
#[derive(Debug)]
enum Candidates {
    Allowed(RoaringBitmap),
    Forbidden(RoaringBitmap),
}

impl Default for Candidates {
    fn default() -> Self {
        Self::Forbidden(RoaringBitmap::new())
    }
}

/// Either a set of candidates that defines the estimated set of candidates
/// that could be returned,
/// or the Exhaustive set of candidates that will be returned if all possible results are fetched.
#[derive(Debug, Clone, PartialEq)]
pub enum InitialCandidates {
    Estimated(RoaringBitmap),
    Exhaustive(RoaringBitmap),
}

impl InitialCandidates {
    fn take(&mut self) -> Self {
        match self {
            Self::Estimated(c) => Self::Estimated(take(c)),
            Self::Exhaustive(c) => Self::Exhaustive(take(c)),
        }
    }

    /// modify the containing roaring bitmap inplace if the set isn't already Exhaustive.
    pub fn map_inplace<F>(&mut self, f: F)
    where
        F: FnOnce(RoaringBitmap) -> RoaringBitmap,
    {
        if let Self::Estimated(c) = self {
            *c = f(take(c))
        }
    }

    pub fn into_inner(self) -> RoaringBitmap {
        match self {
            Self::Estimated(c) => c,
            Self::Exhaustive(c) => c,
        }
    }
}

impl BitOrAssign for InitialCandidates {
    /// Make an union between the containing roaring bitmaps if the set isn't already Exhaustive.
    /// In the case of rhs is Exhaustive and not self, then rhs replaces self.
    fn bitor_assign(&mut self, rhs: Self) {
        if let Self::Estimated(c) = self {
            *self = match rhs {
                Self::Estimated(rhs) => Self::Estimated(rhs | &*c),
                Self::Exhaustive(rhs) => Self::Exhaustive(rhs),
            }
        }
    }
}

impl BitOr for InitialCandidates {
    type Output = Self;

    /// Make an union between the containing roaring bitmaps if the set isn't already Exhaustive.
    /// In the case of rhs is Exhaustive and not self, then rhs replaces self.
    fn bitor(self, rhs: Self) -> Self::Output {
        if let Self::Estimated(c) = self {
            match rhs {
                Self::Estimated(rhs) => Self::Estimated(rhs | c),
                Self::Exhaustive(rhs) => Self::Exhaustive(rhs),
            }
        } else {
            self.clone()
        }
    }
}

pub trait Context<'c> {
    fn documents_ids(&self) -> heed::Result<RoaringBitmap>;
    fn word_docids(&self, word: &str) -> heed::Result<Option<RoaringBitmap>>;
    fn exact_word_docids(&self, word: &str) -> heed::Result<Option<RoaringBitmap>>;
    fn word_prefix_docids(&self, word: &str) -> heed::Result<Option<RoaringBitmap>>;
    fn exact_word_prefix_docids(&self, word: &str) -> heed::Result<Option<RoaringBitmap>>;

    fn word_pair_proximity_docids(
        &self,
        left: &str,
        right: &str,
        proximity: u8,
    ) -> heed::Result<Option<RoaringBitmap>>;
    fn word_prefix_pair_proximity_docids(
        &self,
        left: &str,
        right: &str,
        proximity: u8,
    ) -> heed::Result<Option<RoaringBitmap>>;
    fn prefix_word_pair_proximity_docids(
        &self,
        prefix: &str,
        right: &str,
        proximity: u8,
    ) -> heed::Result<Option<RoaringBitmap>>;
    fn words_fst<'t>(&self) -> &'t fst::Set<Cow<[u8]>>;
    fn in_prefix_cache(&self, word: &str) -> bool;
    fn docid_words_positions(
        &self,
        docid: DocumentId,
    ) -> heed::Result<HashMap<String, RoaringBitmap>>;
    #[allow(clippy::type_complexity)]
    fn word_position_iterator(
        &self,
        word: &str,
        in_prefix_cache: bool,
    ) -> heed::Result<Box<dyn Iterator<Item = heed::Result<((&'c str, u32), RoaringBitmap)>> + 'c>>;
    fn synonyms(&self, word: &str) -> heed::Result<Option<Vec<Vec<String>>>>;
    fn searchable_fields_ids(&self) -> Result<Vec<FieldId>>;
    fn field_id_word_count_docids(
        &self,
        field_id: FieldId,
        word_count: u8,
    ) -> heed::Result<Option<RoaringBitmap>>;
    fn word_position_docids(&self, word: &str, pos: u32) -> heed::Result<Option<RoaringBitmap>>;
}

pub struct CriteriaBuilder<'t> {
    rtxn: &'t heed::RoTxn<'t>,
    index: &'t Index,
    words_fst: fst::Set<Cow<'t, [u8]>>,
    words_prefixes_fst: fst::Set<Cow<'t, [u8]>>,
}

/// Return the docids for the following word pairs and proximities using [`Context::word_pair_proximity_docids`].
/// * `left, right, prox`   (leftward proximity)
/// * `right, left, prox-1` (rightward proximity)
///
/// ## Example
/// For a document with the text `the good fox eats the apple`, we have:
/// * `rightward_proximity(the, eats) = 3`
/// * `leftward_proximity(eats, the) = 1`
///
/// So both the expressions `word_pair_overall_proximity_docids(ctx, the, eats, 3)`
/// and `word_pair_overall_proximity_docids(ctx, the, eats, 2)` would return a bitmap containing
/// the id of this document.
fn word_pair_overall_proximity_docids(
    ctx: &dyn Context,
    left: &str,
    right: &str,
    prox: u8,
) -> heed::Result<Option<RoaringBitmap>> {
    let rightward = ctx.word_pair_proximity_docids(left, right, prox)?;
    let leftward =
        if prox > 1 { ctx.word_pair_proximity_docids(right, left, prox - 1)? } else { None };
    if let Some(mut all) = rightward {
        if let Some(leftward) = leftward {
            all |= leftward;
        }
        Ok(Some(all))
    } else {
        Ok(leftward)
    }
}

/// This function works identically to [`word_pair_overall_proximity_docids`] except that the
/// right word is replaced by a prefix string.
///
/// It will return None if no documents were found or if the prefix does not exist in the
/// `word_prefix_pair_proximity_docids` database.
fn word_prefix_pair_overall_proximity_docids(
    ctx: &dyn Context,
    left: &str,
    prefix: &str,
    proximity: u8,
) -> heed::Result<Option<RoaringBitmap>> {
    // We retrieve the docids for the original and swapped word pairs:
    // A: word1 prefix2 proximity
    // B: prefix2 word1 proximity-1
    let rightward = ctx.word_prefix_pair_proximity_docids(left, prefix, proximity)?;

    let leftward = if proximity > 1 {
        ctx.prefix_word_pair_proximity_docids(prefix, left, proximity - 1)?
    } else {
        None
    };
    if let Some(mut all) = rightward {
        if let Some(leftward) = leftward {
            all |= leftward;
        }
        Ok(Some(all))
    } else {
        Ok(leftward)
    }
}

impl<'c> Context<'c> for CriteriaBuilder<'c> {
    fn documents_ids(&self) -> heed::Result<RoaringBitmap> {
        self.index.documents_ids(self.rtxn)
    }

    fn word_docids(&self, word: &str) -> heed::Result<Option<RoaringBitmap>> {
        self.index.word_docids.get(self.rtxn, word)
    }

    fn exact_word_docids(&self, word: &str) -> heed::Result<Option<RoaringBitmap>> {
        self.index.exact_word_docids.get(self.rtxn, word)
    }

    fn word_prefix_docids(&self, word: &str) -> heed::Result<Option<RoaringBitmap>> {
        self.index.word_prefix_docids.get(self.rtxn, word)
    }

    fn exact_word_prefix_docids(&self, word: &str) -> heed::Result<Option<RoaringBitmap>> {
        self.index.exact_word_prefix_docids.get(self.rtxn, word)
    }

    fn word_pair_proximity_docids(
        &self,
        left: &str,
        right: &str,
        proximity: u8,
    ) -> heed::Result<Option<RoaringBitmap>> {
        self.index.word_pair_proximity_docids.get(self.rtxn, &(proximity, left, right))
    }

    fn word_prefix_pair_proximity_docids(
        &self,
        left: &str,
        prefix: &str,
        proximity: u8,
    ) -> heed::Result<Option<RoaringBitmap>> {
        self.index.word_prefix_pair_proximity_docids.get(self.rtxn, &(proximity, left, prefix))
    }
    fn prefix_word_pair_proximity_docids(
        &self,
        prefix: &str,
        right: &str,
        proximity: u8,
    ) -> heed::Result<Option<RoaringBitmap>> {
        self.index.prefix_word_pair_proximity_docids.get(self.rtxn, &(proximity, prefix, right))
    }

    fn words_fst<'t>(&self) -> &'t fst::Set<Cow<[u8]>> {
        &self.words_fst
    }

    fn in_prefix_cache(&self, word: &str) -> bool {
        self.words_prefixes_fst.contains(word)
    }

    fn docid_words_positions(
        &self,
        docid: DocumentId,
    ) -> heed::Result<HashMap<String, RoaringBitmap>> {
        let mut words_positions = HashMap::new();
        for result in self.index.docid_word_positions.prefix_iter(self.rtxn, &(docid, ""))? {
            let ((_, word), positions) = result?;
            words_positions.insert(word.to_string(), positions);
        }
        Ok(words_positions)
    }

    fn word_position_iterator(
        &self,
        word: &str,
        in_prefix_cache: bool,
    ) -> heed::Result<Box<dyn Iterator<Item = heed::Result<((&'c str, u32), RoaringBitmap)>> + 'c>>
    {
        let range = {
            let left = u32::min_value();
            let right = u32::max_value();
            let left = (word, left);
            let right = (word, right);
            left..=right
        };
        let db = match in_prefix_cache {
            true => self.index.word_prefix_position_docids,
            false => self.index.word_position_docids,
        };

        Ok(Box::new(db.range(self.rtxn, &range)?))
    }

    fn synonyms(&self, word: &str) -> heed::Result<Option<Vec<Vec<String>>>> {
        self.index.words_synonyms(self.rtxn, &[word])
    }

    fn searchable_fields_ids(&self) -> Result<Vec<FieldId>> {
        match self.index.searchable_fields_ids(self.rtxn)? {
            Some(searchable_fields_ids) => Ok(searchable_fields_ids),
            None => Ok(self.index.fields_ids_map(self.rtxn)?.ids().collect()),
        }
    }

    fn field_id_word_count_docids(
        &self,
        field_id: FieldId,
        word_count: u8,
    ) -> heed::Result<Option<RoaringBitmap>> {
        let key = (field_id, word_count);
        self.index.field_id_word_count_docids.get(self.rtxn, &key)
    }

    fn word_position_docids(&self, word: &str, pos: u32) -> heed::Result<Option<RoaringBitmap>> {
        let key = (word, pos);
        self.index.word_position_docids.get(self.rtxn, &key)
    }
}

impl<'t> CriteriaBuilder<'t> {
    pub fn new(rtxn: &'t heed::RoTxn<'t>, index: &'t Index) -> Result<Self> {
        let words_fst = index.words_fst(rtxn)?;
        let words_prefixes_fst = index.words_prefixes_fst(rtxn)?;
        Ok(Self { rtxn, index, words_fst, words_prefixes_fst })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn build<D: 't + Distinct>(
        &'t self,
        query_tree: Option<Operation>,
        primitive_query: Option<Vec<PrimitiveQueryPart>>,
        filtered_candidates: Option<RoaringBitmap>,
        sort_criteria: Option<Vec<AscDescName>>,
        exhaustive_number_hits: bool,
        distinct: Option<D>,
        implementation_strategy: CriterionImplementationStrategy,
    ) -> Result<Final<'t>> {
        use crate::criterion::Criterion as Name;

        let primitive_query = primitive_query.unwrap_or_default();

        let mut criterion = Box::new(Initial::new(
            self,
            query_tree,
            filtered_candidates,
            exhaustive_number_hits,
            distinct,
        )) as Box<dyn Criterion>;
        for name in self.index.criteria(self.rtxn)? {
            criterion = match name {
                Name::Words => Box::new(Words::new(self, criterion)),
                Name::Typo => Box::new(Typo::new(self, criterion)),
                Name::Sort => match sort_criteria {
                    Some(ref sort_criteria) => {
                        for asc_desc in sort_criteria {
                            criterion = match asc_desc {
                                AscDescName::Asc(Member::Field(field)) => Box::new(AscDesc::asc(
                                    self.index,
                                    self.rtxn,
                                    criterion,
                                    field.to_string(),
                                    implementation_strategy,
                                )?),
                                AscDescName::Desc(Member::Field(field)) => Box::new(AscDesc::desc(
                                    self.index,
                                    self.rtxn,
                                    criterion,
                                    field.to_string(),
                                    implementation_strategy,
                                )?),
                                AscDescName::Asc(Member::Geo(point)) => {
                                    Box::new(Geo::asc(self.index, self.rtxn, criterion, *point)?)
                                }
                                AscDescName::Desc(Member::Geo(point)) => {
                                    Box::new(Geo::desc(self.index, self.rtxn, criterion, *point)?)
                                }
                            };
                        }
                        criterion
                    }
                    None => criterion,
                },
                Name::Proximity => {
                    Box::new(Proximity::new(self, criterion, implementation_strategy))
                }
                Name::Attribute => {
                    Box::new(Attribute::new(self, criterion, implementation_strategy))
                }
                Name::Exactness => Box::new(Exactness::new(self, criterion, &primitive_query)?),
                Name::Asc(field) => Box::new(AscDesc::asc(
                    self.index,
                    self.rtxn,
                    criterion,
                    field,
                    implementation_strategy,
                )?),
                Name::Desc(field) => Box::new(AscDesc::desc(
                    self.index,
                    self.rtxn,
                    criterion,
                    field,
                    implementation_strategy,
                )?),
            };
        }

        Ok(Final::new(self, criterion))
    }
}

pub fn resolve_query_tree(
    ctx: &dyn Context,
    query_tree: &Operation,
    wdcache: &mut WordDerivationsCache,
) -> Result<RoaringBitmap> {
    fn resolve_operation(
        ctx: &dyn Context,
        query_tree: &Operation,
        wdcache: &mut WordDerivationsCache,
    ) -> Result<RoaringBitmap> {
        use Operation::{And, Or, Phrase, Query};

        match query_tree {
            And(ops) => {
                let mut ops = ops
                    .iter()
                    .map(|op| resolve_operation(ctx, op, wdcache))
                    .collect::<Result<Vec<_>>>()?;

                ops.sort_unstable_by_key(|cds| cds.len());

                let mut candidates = RoaringBitmap::new();
                let mut first_loop = true;
                for docids in ops {
                    if first_loop {
                        candidates = docids;
                        first_loop = false;
                    } else {
                        candidates &= &docids;
                    }
                }
                Ok(candidates)
            }
            Phrase(words) => resolve_phrase(ctx, words),
            Or(_, ops) => {
                let mut candidates = RoaringBitmap::new();
                for op in ops {
                    let docids = resolve_operation(ctx, op, wdcache)?;
                    candidates |= docids;
                }
                Ok(candidates)
            }
            Query(q) => Ok(query_docids(ctx, q, wdcache)?),
        }
    }

    resolve_operation(ctx, query_tree, wdcache)
}

pub fn resolve_phrase(ctx: &dyn Context, phrase: &[Option<String>]) -> Result<RoaringBitmap> {
    let mut candidates = RoaringBitmap::new();
    let mut first_iter = true;
    let winsize = phrase.len().min(3);

    if phrase.is_empty() {
        return Ok(candidates);
    }

    for win in phrase.windows(winsize) {
        // Get all the documents with the matching distance for each word pairs.
        let mut bitmaps = Vec::with_capacity(winsize.pow(2));
        for (offset, s1) in win
            .iter()
            .enumerate()
            .filter_map(|(index, word)| word.as_ref().map(|word| (index, word)))
        {
            for (dist, s2) in win
                .iter()
                .skip(offset + 1)
                .enumerate()
                .filter_map(|(index, word)| word.as_ref().map(|word| (index, word)))
            {
                if dist == 0 {
                    match ctx.word_pair_proximity_docids(s1, s2, 1)? {
                        Some(m) => bitmaps.push(m),
                        // If there are no document for this pair, there will be no
                        // results for the phrase query.
                        None => return Ok(RoaringBitmap::new()),
                    }
                } else {
                    let mut bitmap = RoaringBitmap::new();
                    for dist in 0..=dist {
                        if let Some(m) = ctx.word_pair_proximity_docids(s1, s2, dist as u8 + 1)? {
                            bitmap |= m
                        }
                    }
                    if bitmap.is_empty() {
                        return Ok(bitmap);
                    } else {
                        bitmaps.push(bitmap);
                    }
                }
            }
        }

        // We sort the bitmaps so that we perform the small intersections first, which is faster.
        bitmaps.sort_unstable_by_key(|a| a.len());

        for bitmap in bitmaps {
            if first_iter {
                candidates = bitmap;
                first_iter = false;
            } else {
                candidates &= bitmap;
            }
            // There will be no match, return early
            if candidates.is_empty() {
                break;
            }
        }
    }
    Ok(candidates)
}

fn all_word_pair_overall_proximity_docids<T: AsRef<str>, U: AsRef<str>>(
    ctx: &dyn Context,
    left_words: &[(T, u8)],
    right_words: &[(U, u8)],
    proximity: u8,
) -> Result<RoaringBitmap> {
    let mut docids = RoaringBitmap::new();
    for (left, _l_typo) in left_words {
        for (right, _r_typo) in right_words {
            let current_docids =
                word_pair_overall_proximity_docids(ctx, left.as_ref(), right.as_ref(), proximity)?
                    .unwrap_or_default();
            docids |= current_docids;
        }
    }
    Ok(docids)
}

fn query_docids(
    ctx: &dyn Context,
    query: &Query,
    wdcache: &mut WordDerivationsCache,
) -> Result<RoaringBitmap> {
    match &query.kind {
        QueryKind::Exact { word, original_typo } => {
            if query.prefix && ctx.in_prefix_cache(word) {
                let mut docids = ctx.word_prefix_docids(word)?.unwrap_or_default();
                // only add the exact docids if the word hasn't been derived
                if *original_typo == 0 {
                    docids |= ctx.exact_word_prefix_docids(word)?.unwrap_or_default();
                }
                Ok(docids)
            } else if query.prefix {
                let words = word_derivations(word, true, 0, ctx.words_fst(), wdcache)?;
                let mut docids = RoaringBitmap::new();
                for (word, _typo) in words {
                    docids |= ctx.word_docids(word)?.unwrap_or_default();
                    // only add the exact docids if the word hasn't been derived
                    if *original_typo == 0 {
                        docids |= ctx.exact_word_docids(word)?.unwrap_or_default();
                    }
                }
                Ok(docids)
            } else {
                let mut docids = ctx.word_docids(word)?.unwrap_or_default();
                // only add the exact docids if the word hasn't been derived
                if *original_typo == 0 {
                    docids |= ctx.exact_word_docids(word)?.unwrap_or_default();
                }
                Ok(docids)
            }
        }
        QueryKind::Tolerant { typo, word } => {
            let words = word_derivations(word, query.prefix, *typo, ctx.words_fst(), wdcache)?;
            let mut docids = RoaringBitmap::new();
            for (word, typo) in words {
                let mut current_docids = ctx.word_docids(word)?.unwrap_or_default();
                if *typo == 0 {
                    current_docids |= ctx.exact_word_docids(word)?.unwrap_or_default()
                }
                docids |= current_docids;
            }
            Ok(docids)
        }
    }
}

fn query_pair_proximity_docids(
    ctx: &dyn Context,
    left: &Query,
    right: &Query,
    proximity: u8,
    wdcache: &mut WordDerivationsCache,
) -> Result<RoaringBitmap> {
    if proximity >= 8 {
        let mut candidates = query_docids(ctx, left, wdcache)?;
        let right_candidates = query_docids(ctx, right, wdcache)?;
        candidates &= right_candidates;
        return Ok(candidates);
    }

    let prefix = right.prefix;
    match (&left.kind, &right.kind) {
        (QueryKind::Exact { word: left, .. }, QueryKind::Exact { word: right, .. }) => {
            if prefix {
                // There are three distinct cases which we need to distinguish regarding the prefix `right`:
                //
                // 1. `right` is not in any prefix cache because it is not the prefix of many words
                //     (and thus, it doesn't have many word derivations)
                // 2. `right` is in the prefix cache but cannot be found in the "word prefix pair proximity" databases either
                //     because it is too long or because the given proximity is too high.
                // 3. `right` is in the prefix cache and can be found in the "word prefix pair proximity" databases
                //
                // The three cases are handled as follows:
                // 1. We manually retrieve all the word derivations of `right` and check the `word_pair_proximity`
                //    database for each of them.
                // 2. It would be too expensive to apply the same strategy as (1), therefore, we "disable" the
                //    proximity ranking rule for the prefixes of the right word. This is done as follows:
                //    1. Only find the documents where left is in proximity to the exact (ie non-prefix) right word
                //    2. Otherwise, assume that their proximity in all the documents in which they coexist is >= 8
                //
                // 3. Query the prefix proximity databases.
                match (
                    ctx.in_prefix_cache(right),
                    right.len() <= MAX_LENGTH_FOR_PREFIX_PROXIMITY_DB
                        && proximity <= MAX_PROXIMITY_FOR_PREFIX_PROXIMITY_DB,
                ) {
                    // Case 1: not in prefix cache
                    (false, _) => {
                        let r_words = word_derivations(right, true, 0, ctx.words_fst(), wdcache)?;
                        all_word_pair_overall_proximity_docids(
                            ctx,
                            &[(left, 0)],
                            r_words,
                            proximity,
                        )
                    }
                    // Case 2: in prefix cache but either the prefix length or the proximity makes it impossible to
                    // query the prefix proximity databases.
                    (true, false) => {
                        // To "save" the relevancy a little bit, we still find the documents where the
                        // exact (i.e. non-prefix) right word is in the given proximity to the left word.
                        Ok(word_pair_overall_proximity_docids(
                            ctx,
                            left.as_str(),
                            right.as_str(),
                            proximity,
                        )?
                        .unwrap_or_default())
                    }
                    // Case 3: in prefix cache, short enough, and proximity is low enough
                    (true, true) => Ok(word_prefix_pair_overall_proximity_docids(
                        ctx,
                        left.as_str(),
                        right.as_str(),
                        proximity,
                    )?
                    .unwrap_or_default()),
                }
            } else {
                Ok(word_pair_overall_proximity_docids(
                    ctx,
                    left.as_str(),
                    right.as_str(),
                    proximity,
                )?
                .unwrap_or_default())
            }
        }
        (QueryKind::Tolerant { typo, word: left }, QueryKind::Exact { word: right, .. }) => {
            let l_words =
                word_derivations(left, false, *typo, ctx.words_fst(), wdcache)?.to_owned();
            if prefix {
                // The logic here is almost identical to the one in the previous match branch.
                // The difference is that we fetch the docids for each derivation of the left word.
                match (
                    ctx.in_prefix_cache(right),
                    right.len() <= MAX_LENGTH_FOR_PREFIX_PROXIMITY_DB
                        && proximity <= MAX_PROXIMITY_FOR_PREFIX_PROXIMITY_DB,
                ) {
                    // Case 1: not in prefix cache
                    (false, _) => {
                        let mut docids = RoaringBitmap::new();
                        let r_words = word_derivations(right, true, 0, ctx.words_fst(), wdcache)?;
                        for (left, _) in l_words {
                            docids |= all_word_pair_overall_proximity_docids(
                                ctx,
                                &[(left, 0)],
                                r_words,
                                proximity,
                            )?;
                        }
                        Ok(docids)
                    }
                    // Case 2: in prefix cache but either the prefix length or the proximity makes it impossible to
                    // query the prefix proximity databases.
                    (true, false) => {
                        // To "save" the relevancy a little bit, we still find the documents where the
                        // exact (i.e. non-prefix) right word is in proximity to any derivation of the left word.
                        let mut candidates = RoaringBitmap::new();
                        for (left, _) in l_words {
                            candidates |= ctx
                                .word_pair_proximity_docids(&left, right, proximity)?
                                .unwrap_or_default();
                        }
                        Ok(candidates)
                    }
                    // Case 3: in prefix cache, short enough, and proximity is low enough
                    (true, true) => {
                        let mut docids = RoaringBitmap::new();
                        for (left, _) in l_words {
                            docids |= word_prefix_pair_overall_proximity_docids(
                                ctx,
                                left.as_str(),
                                right.as_str(),
                                proximity,
                            )?
                            .unwrap_or_default();
                        }
                        Ok(docids)
                    }
                }
            } else {
                all_word_pair_overall_proximity_docids(ctx, &l_words, &[(right, 0)], proximity)
            }
        }
        (QueryKind::Exact { word: left, .. }, QueryKind::Tolerant { typo, word: right }) => {
            let r_words = word_derivations(right, prefix, *typo, ctx.words_fst(), wdcache)?;
            all_word_pair_overall_proximity_docids(ctx, &[(left, 0)], r_words, proximity)
        }
        (
            QueryKind::Tolerant { typo: l_typo, word: left },
            QueryKind::Tolerant { typo: r_typo, word: right },
        ) => {
            let l_words =
                word_derivations(left, false, *l_typo, ctx.words_fst(), wdcache)?.to_owned();
            let r_words = word_derivations(right, prefix, *r_typo, ctx.words_fst(), wdcache)?;
            all_word_pair_overall_proximity_docids(ctx, &l_words, r_words, proximity)
        }
    }
}

#[cfg(test)]
pub mod test {
    use std::collections::HashMap;
    use std::iter;

    use maplit::hashmap;
    use rand::rngs::StdRng;
    use rand::{Rng, SeedableRng};

    use super::*;

    fn s(s: &str) -> String {
        s.to_string()
    }
    pub struct TestContext<'t> {
        words_fst: fst::Set<Cow<'t, [u8]>>,
        word_docids: HashMap<String, RoaringBitmap>,
        exact_word_docids: HashMap<String, RoaringBitmap>,
        word_prefix_docids: HashMap<String, RoaringBitmap>,
        exact_word_prefix_docids: HashMap<String, RoaringBitmap>,
        word_pair_proximity_docids: HashMap<(String, String, i32), RoaringBitmap>,
        word_prefix_pair_proximity_docids: HashMap<(String, String, i32), RoaringBitmap>,
        prefix_word_pair_proximity_docids: HashMap<(String, String, i32), RoaringBitmap>,
        docid_words: HashMap<u32, Vec<String>>,
    }

    impl<'c> Context<'c> for TestContext<'c> {
        fn documents_ids(&self) -> heed::Result<RoaringBitmap> {
            Ok(self.word_docids.iter().fold(RoaringBitmap::new(), |acc, (_, docids)| acc | docids))
        }

        fn word_docids(&self, word: &str) -> heed::Result<Option<RoaringBitmap>> {
            Ok(self.word_docids.get(&word.to_string()).cloned())
        }

        fn exact_word_docids(&self, word: &str) -> heed::Result<Option<RoaringBitmap>> {
            Ok(self.exact_word_docids.get(&word.to_string()).cloned())
        }

        fn word_prefix_docids(&self, word: &str) -> heed::Result<Option<RoaringBitmap>> {
            Ok(self.word_prefix_docids.get(&word.to_string()).cloned())
        }

        fn exact_word_prefix_docids(&self, word: &str) -> heed::Result<Option<RoaringBitmap>> {
            Ok(self.exact_word_prefix_docids.get(&word.to_string()).cloned())
        }

        fn word_pair_proximity_docids(
            &self,
            left: &str,
            right: &str,
            proximity: u8,
        ) -> heed::Result<Option<RoaringBitmap>> {
            let key = (left.to_string(), right.to_string(), proximity.into());
            Ok(self.word_pair_proximity_docids.get(&key).cloned())
        }

        fn word_prefix_pair_proximity_docids(
            &self,
            word: &str,
            prefix: &str,
            proximity: u8,
        ) -> heed::Result<Option<RoaringBitmap>> {
            let key = (word.to_string(), prefix.to_string(), proximity.into());
            Ok(self.word_prefix_pair_proximity_docids.get(&key).cloned())
        }
        fn prefix_word_pair_proximity_docids(
            &self,
            prefix: &str,
            word: &str,
            proximity: u8,
        ) -> heed::Result<Option<RoaringBitmap>> {
            let key = (prefix.to_string(), word.to_string(), proximity.into());
            Ok(self.prefix_word_pair_proximity_docids.get(&key).cloned())
        }

        fn words_fst<'t>(&self) -> &'t fst::Set<Cow<[u8]>> {
            &self.words_fst
        }

        fn in_prefix_cache(&self, word: &str) -> bool {
            self.word_prefix_docids.contains_key(&word.to_string())
        }

        fn docid_words_positions(
            &self,
            docid: DocumentId,
        ) -> heed::Result<HashMap<String, RoaringBitmap>> {
            if let Some(docid_words) = self.docid_words.get(&docid) {
                Ok(docid_words
                    .iter()
                    .enumerate()
                    .map(|(i, w)| {
                        let bitmap = RoaringBitmap::from_sorted_iter(iter::once(i as u32)).unwrap();
                        (w.clone(), bitmap)
                    })
                    .collect())
            } else {
                Ok(HashMap::new())
            }
        }

        fn word_position_iterator(
            &self,
            _word: &str,
            _in_prefix_cache: bool,
        ) -> heed::Result<
            Box<dyn Iterator<Item = heed::Result<((&'c str, u32), RoaringBitmap)>> + 'c>,
        > {
            todo!()
        }

        fn synonyms(&self, _word: &str) -> heed::Result<Option<Vec<Vec<String>>>> {
            todo!()
        }

        fn searchable_fields_ids(&self) -> Result<Vec<FieldId>> {
            todo!()
        }

        fn word_position_docids(
            &self,
            _word: &str,
            _pos: u32,
        ) -> heed::Result<Option<RoaringBitmap>> {
            todo!()
        }

        fn field_id_word_count_docids(
            &self,
            _field_id: FieldId,
            _word_count: u8,
        ) -> heed::Result<Option<RoaringBitmap>> {
            todo!()
        }
    }

    impl<'a> Default for TestContext<'a> {
        fn default() -> TestContext<'a> {
            let mut rng = StdRng::seed_from_u64(102);
            let rng = &mut rng;

            fn random_postings<R: Rng>(rng: &mut R, len: usize) -> RoaringBitmap {
                let mut values = Vec::<u32>::with_capacity(len);
                while values.len() != len {
                    values.push(rng.gen());
                }
                values.sort_unstable();

                RoaringBitmap::from_sorted_iter(values.into_iter()).unwrap()
            }

            let word_docids = hashmap! {
                s("hello")      => random_postings(rng,   1500),
                s("hi")         => random_postings(rng,   4000),
                s("word")       => random_postings(rng,   2500),
                s("split")      => random_postings(rng,    400),
                s("ngrams")     => random_postings(rng,   1400),
                s("world")      => random_postings(rng, 15_000),
                s("earth")      => random_postings(rng,   8000),
                s("2021")       => random_postings(rng,    100),
                s("2020")       => random_postings(rng,    500),
                s("is")         => random_postings(rng, 50_000),
                s("this")       => random_postings(rng, 50_000),
                s("good")       => random_postings(rng,   1250),
                s("morning")    => random_postings(rng,    125),
            };

            let exact_word_docids = HashMap::new();

            let mut docid_words = HashMap::new();
            for (word, docids) in word_docids.iter() {
                for docid in docids {
                    let words: &mut Vec<_> = docid_words.entry(docid).or_default();
                    words.push(word.clone());
                }
            }

            let word_prefix_docids = hashmap! {
                s("h")   => &word_docids[&s("hello")] | &word_docids[&s("hi")],
                s("wor") => &word_docids[&s("word")]  | &word_docids[&s("world")],
                s("20")  => &word_docids[&s("2020")]  | &word_docids[&s("2021")],
            };

            let exact_word_prefix_docids = HashMap::new();

            let mut word_pair_proximity_docids = HashMap::new();
            let mut word_prefix_pair_proximity_docids = HashMap::new();
            let mut prefix_word_pair_proximity_docids = HashMap::new();

            for (lword, lcandidates) in &word_docids {
                for (rword, rcandidates) in &word_docids {
                    if lword == rword {
                        continue;
                    }
                    let candidates = lcandidates & rcandidates;
                    for candidate in candidates {
                        if let Some(docid_words) = docid_words.get(&candidate) {
                            let lposition = docid_words.iter().position(|w| w == lword).unwrap();
                            let rposition = docid_words.iter().position(|w| w == rword).unwrap();
                            let key = if lposition < rposition {
                                (s(lword), s(rword), (rposition - lposition) as i32)
                            } else {
                                (s(lword), s(rword), (lposition - rposition + 1) as i32)
                            };
                            let docids: &mut RoaringBitmap =
                                word_pair_proximity_docids.entry(key).or_default();
                            docids.push(candidate);
                        }
                    }
                }
                for (pword, pcandidates) in &word_prefix_docids {
                    if lword.starts_with(pword) {
                        continue;
                    }
                    let candidates = lcandidates & pcandidates;
                    for candidate in candidates {
                        if let Some(docid_words) = docid_words.get(&candidate) {
                            let lposition = docid_words.iter().position(|w| w == lword).unwrap();
                            let rposition =
                                docid_words.iter().position(|w| w.starts_with(pword)).unwrap();
                            if lposition < rposition {
                                let key = (s(lword), s(pword), (rposition - lposition) as i32);
                                let docids: &mut RoaringBitmap =
                                    word_prefix_pair_proximity_docids.entry(key).or_default();
                                docids.push(candidate);
                            } else {
                                let key = (s(lword), s(pword), (lposition - rposition) as i32);
                                let docids: &mut RoaringBitmap =
                                    prefix_word_pair_proximity_docids.entry(key).or_default();
                                docids.push(candidate);
                            };
                        }
                    }
                }
            }

            let mut keys = word_docids.keys().collect::<Vec<_>>();
            keys.sort_unstable();
            let words_fst = fst::Set::from_iter(keys).unwrap().map_data(Cow::Owned).unwrap();

            TestContext {
                words_fst,
                word_docids,
                exact_word_docids,
                word_prefix_docids,
                exact_word_prefix_docids,
                word_pair_proximity_docids,
                word_prefix_pair_proximity_docids,
                prefix_word_pair_proximity_docids,
                docid_words,
            }
        }
    }
}
