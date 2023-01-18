use std::collections::btree_map::Entry;
use std::collections::BTreeMap;
use std::convert::TryFrom;
use std::mem::take;

use log::debug;
use roaring::{MultiOps, RoaringBitmap};

use crate::search::criteria::{
    resolve_phrase, resolve_query_tree, Context, Criterion, CriterionParameters, CriterionResult,
    InitialCandidates,
};
use crate::search::query_tree::{Operation, PrimitiveQueryPart};
use crate::{absolute_from_relative_position, FieldId, Result};

pub struct Exactness<'t> {
    ctx: &'t dyn Context<'t>,
    query_tree: Option<Operation>,
    state: Option<State>,
    initial_candidates: InitialCandidates,
    parent: Box<dyn Criterion + 't>,
    query: Vec<ExactQueryPart>,
    cache: Option<ExactWordsCombinationCache>,
}

impl<'t> Exactness<'t> {
    pub fn new(
        ctx: &'t dyn Context<'t>,
        parent: Box<dyn Criterion + 't>,
        primitive_query: &[PrimitiveQueryPart],
    ) -> heed::Result<Self> {
        let mut query: Vec<_> = Vec::with_capacity(primitive_query.len());
        for part in primitive_query {
            query.push(ExactQueryPart::from_primitive_query_part(ctx, part)?);
        }

        Ok(Exactness {
            ctx,
            query_tree: None,
            state: None,
            initial_candidates: InitialCandidates::Estimated(RoaringBitmap::new()),
            parent,
            query,
            cache: None,
        })
    }
}

impl<'t> Criterion for Exactness<'t> {
    #[logging_timer::time("Exactness::{}")]
    fn next(&mut self, params: &mut CriterionParameters) -> Result<Option<CriterionResult>> {
        // remove excluded candidates when next is called, instead of doing it in the loop.
        if let Some(state) = self.state.as_mut() {
            state.difference_with(params.excluded_candidates);
        }
        loop {
            debug!("Exactness at state {:?}", self.state);

            match self.state.as_mut() {
                Some(state) if state.is_empty() => {
                    // reset state
                    self.state = None;
                    self.query_tree = None;
                    // we don't need to reset the combinations cache since it only depends on
                    // the primitive query, which does not change
                }
                Some(state) => {
                    let (candidates, state) =
                        resolve_state(self.ctx, take(state), &self.query, &mut self.cache)?;
                    self.state = state;

                    return Ok(Some(CriterionResult {
                        query_tree: self.query_tree.clone(),
                        candidates: Some(candidates),
                        filtered_candidates: None,
                        initial_candidates: Some(self.initial_candidates.take()),
                    }));
                }
                None => match self.parent.next(params)? {
                    Some(CriterionResult {
                        query_tree: Some(query_tree),
                        candidates,
                        filtered_candidates,
                        initial_candidates,
                    }) => {
                        let mut candidates = match candidates {
                            Some(candidates) => candidates,
                            None => {
                                resolve_query_tree(self.ctx, &query_tree, params.wdcache)?
                                    - params.excluded_candidates
                            }
                        };

                        if let Some(filtered_candidates) = filtered_candidates {
                            candidates &= filtered_candidates;
                        }

                        match initial_candidates {
                            Some(initial_candidates) => {
                                self.initial_candidates |= initial_candidates
                            }
                            None => self.initial_candidates.map_inplace(|c| c | &candidates),
                        }

                        self.state = Some(State::new(candidates));
                        self.query_tree = Some(query_tree);
                    }
                    Some(CriterionResult {
                        query_tree: None,
                        candidates,
                        filtered_candidates,
                        initial_candidates,
                    }) => {
                        return Ok(Some(CriterionResult {
                            query_tree: None,
                            candidates,
                            filtered_candidates,
                            initial_candidates,
                        }));
                    }
                    None => return Ok(None),
                },
            }
        }
    }
}

#[derive(Debug)]
enum State {
    /// Extract the documents that have an attribute that contains exactly the query.
    ExactAttribute(RoaringBitmap),
    /// Extract the documents that have an attribute that starts with exactly the query.
    AttributeStartsWith(RoaringBitmap),
    /// Rank the remaining documents by the number of exact words contained.
    ExactWords(RoaringBitmap),
    Remainings(Vec<RoaringBitmap>),
}

impl State {
    fn new(candidates: RoaringBitmap) -> Self {
        Self::ExactAttribute(candidates)
    }

    fn difference_with(&mut self, lhs: &RoaringBitmap) {
        match self {
            Self::ExactAttribute(candidates)
            | Self::AttributeStartsWith(candidates)
            | Self::ExactWords(candidates) => *candidates -= lhs,
            Self::Remainings(candidates_array) => {
                candidates_array.iter_mut().for_each(|candidates| *candidates -= lhs);
                candidates_array.retain(|candidates| !candidates.is_empty());
            }
        }
    }

    fn is_empty(&self) -> bool {
        match self {
            Self::ExactAttribute(candidates)
            | Self::AttributeStartsWith(candidates)
            | Self::ExactWords(candidates) => candidates.is_empty(),
            Self::Remainings(candidates_array) => {
                candidates_array.iter().all(RoaringBitmap::is_empty)
            }
        }
    }
}

impl Default for State {
    fn default() -> Self {
        Self::Remainings(vec![])
    }
}
#[logging_timer::time("Exactness::{}")]
fn resolve_state(
    ctx: &dyn Context,
    state: State,
    query: &[ExactQueryPart],
    cache: &mut Option<ExactWordsCombinationCache>,
) -> Result<(RoaringBitmap, Option<State>)> {
    use State::*;
    match state {
        ExactAttribute(mut allowed_candidates) => {
            let mut candidates = RoaringBitmap::new();
            if let Ok(query_len) = u8::try_from(query.len()) {
                let attributes_ids = ctx.searchable_fields_ids()?;
                for id in attributes_ids {
                    if let Some(attribute_allowed_docids) =
                        ctx.field_id_word_count_docids(id, query_len)?
                    {
                        let mut attribute_candidates_array =
                            attribute_start_with_docids(ctx, id, query)?;
                        attribute_candidates_array.push(attribute_allowed_docids);

                        candidates |= MultiOps::intersection(attribute_candidates_array);
                    }
                }

                // only keep allowed candidates
                candidates &= &allowed_candidates;
                // remove current candidates from allowed candidates
                allowed_candidates -= &candidates;
            }

            Ok((candidates, Some(AttributeStartsWith(allowed_candidates))))
        }
        AttributeStartsWith(mut allowed_candidates) => {
            let mut candidates = RoaringBitmap::new();
            let attributes_ids = ctx.searchable_fields_ids()?;
            for id in attributes_ids {
                let attribute_candidates_array = attribute_start_with_docids(ctx, id, query)?;
                candidates |= MultiOps::intersection(attribute_candidates_array);
            }

            // only keep allowed candidates
            candidates &= &allowed_candidates;
            // remove current candidates from allowed candidates
            allowed_candidates -= &candidates;
            Ok((candidates, Some(ExactWords(allowed_candidates))))
        }
        ExactWords(allowed_candidates) => {
            // Retrieve the cache if it already exist, otherwise create it.
            let owned_cache = if let Some(cache) = cache.take() {
                cache
            } else {
                compute_combinations(ctx, query)?
            };
            // The cache contains the sets of documents which contain exactly 1,2,3,.. exact words
            // from the query. It cannot be empty. All the candidates in it are disjoint.

            let mut candidates_array = owned_cache.combinations.clone();
            for candidates in candidates_array.iter_mut() {
                *candidates &= &allowed_candidates;
            }
            *cache = Some(owned_cache);

            let best_candidates = candidates_array.pop().unwrap();

            candidates_array.insert(0, allowed_candidates);
            Ok((best_candidates, Some(Remainings(candidates_array))))
        }
        // pop remainings candidates until the emptiness
        Remainings(mut candidates_array) => {
            let candidates = candidates_array.pop().unwrap_or_default();
            if !candidates_array.is_empty() {
                Ok((candidates, Some(Remainings(candidates_array))))
            } else {
                Ok((candidates, None))
            }
        }
    }
}

fn attribute_start_with_docids(
    ctx: &dyn Context,
    attribute_id: FieldId,
    query: &[ExactQueryPart],
) -> heed::Result<Vec<RoaringBitmap>> {
    let mut attribute_candidates_array = Vec::new();
    // start from attribute first position
    let mut pos = absolute_from_relative_position(attribute_id, 0);
    for part in query {
        use ExactQueryPart::*;
        match part {
            Synonyms(synonyms) => {
                let mut synonyms_candidates = RoaringBitmap::new();
                for word in synonyms {
                    let wc = ctx.word_position_docids(word, pos)?;
                    if let Some(word_candidates) = wc {
                        synonyms_candidates |= word_candidates;
                    }
                }
                attribute_candidates_array.push(synonyms_candidates);
                pos += 1;
            }
            Phrase(phrase) => {
                for word in phrase {
                    if let Some(word) = word {
                        let wc = ctx.word_position_docids(word, pos)?;
                        if let Some(word_candidates) = wc {
                            attribute_candidates_array.push(word_candidates);
                        }
                    }
                    pos += 1;
                }
            }
        }
    }

    Ok(attribute_candidates_array)
}

#[derive(Debug, Clone)]
pub enum ExactQueryPart {
    Phrase(Vec<Option<String>>),
    Synonyms(Vec<String>),
}

impl ExactQueryPart {
    fn from_primitive_query_part(
        ctx: &dyn Context,
        part: &PrimitiveQueryPart,
    ) -> heed::Result<Self> {
        let part = match part {
            PrimitiveQueryPart::Word(word, _) => {
                match ctx.synonyms(word)? {
                    Some(synonyms) => {
                        let mut synonyms: Vec<_> = synonyms
                            .into_iter()
                            .filter_map(|mut array| {
                                // keep 1 word synonyms only.
                                match array.pop() {
                                    Some(word) if array.is_empty() => Some(word),
                                    _ => None,
                                }
                            })
                            .collect();
                        synonyms.push(word.clone());
                        ExactQueryPart::Synonyms(synonyms)
                    }
                    None => ExactQueryPart::Synonyms(vec![word.clone()]),
                }
            }
            PrimitiveQueryPart::Phrase(phrase) => ExactQueryPart::Phrase(phrase.clone()),
        };

        Ok(part)
    }
}

struct ExactWordsCombinationCache {
    // index 0 is only 1 word
    combinations: Vec<RoaringBitmap>,
}

fn compute_combinations(
    ctx: &dyn Context,
    query: &[ExactQueryPart],
) -> Result<ExactWordsCombinationCache> {
    let number_of_part = query.len();
    let mut parts_candidates_array = Vec::with_capacity(number_of_part);
    for part in query {
        let mut candidates = RoaringBitmap::new();
        use ExactQueryPart::*;
        match part {
            Synonyms(synonyms) => {
                for synonym in synonyms {
                    if let Some(synonym_candidates) = ctx.word_docids(synonym)? {
                        candidates |= synonym_candidates;
                    }
                }
            }
            // compute intersection on pair of words with a proximity of 0.
            Phrase(phrase) => {
                candidates |= resolve_phrase(ctx, phrase)?;
            }
        }
        parts_candidates_array.push(candidates);
    }
    let combinations = create_disjoint_combinations(parts_candidates_array);

    Ok(ExactWordsCombinationCache { combinations })
}

/// Given a list of bitmaps `b0,b1,...,bn` , compute the list of bitmaps `X0,X1,...,Xn`
/// such that `Xi` contains all the elements that are contained in **at least** `i+1` bitmaps among `b0,b1,...,bn`.
///
/// The returned vector is guaranteed to be of length `n`. It is equal to `vec![X0, X1, ..., Xn]`.
///
/// ## Implementation
///
/// We do so by iteratively building a map containing the union of all the different ways to intersect `J` bitmaps among `b0,b1,...,bn`.
/// - The key of the map is the index `i` of the last bitmap in the intersections
/// - The value is the union of all the possible intersections of J bitmaps such that the last bitmap in the intersection is `bi`
///
/// For example, with the bitmaps `b0,b1,b2,b3`, this map should look like this
/// ```text
/// Map 0: (first iteration, contains all the combinations of 1 bitmap)
///     // What follows are unions of intersection of bitmaps asscociated with the index of their last component
///     0: [b0]
///     1: [b1]
///     2: [b2]
///     3: [b3]
/// Map 1: (second iteration, combinations of 2 bitmaps)
///     1: [b0&b1]
///     2: [b0&b2 | b1&b2]
///     3: [b0&b3 | b1&b3 | b2&b3]
/// Map 2: (third iteration, combinations of 3 bitmaps)
///     2: [b0&b1&b2]
///     3: [b0&b2&b3 | b1&b2&b3]
/// Map 3: (fourth iteration, combinations of 4 bitmaps)
///     3: [b0&b1&b2&b3]
/// ```
///
/// These maps are built one by one from the content of the preceding map.
/// For example, to create Map 2, we look at each line of Map 1, for example:
/// ```text
/// 2: [b0&b2 | b1&b2]
/// ```
/// And then for each i > 2, we compute `(b0&b2 | b1&b2) & bi = b0&b2&bi | b1&b2&bi`
/// and then add it the new map (Map 3) under the key `i` (if it is not empty):
/// ```text
/// 3: [b0&b2&b3 | b1&b2&b3]
/// 4: [b0&b2&b4 | b1&b2&b4]
/// 5: [b0&b2&b5 | b1&b2&b5]
/// etc.
/// ```
/// We only keep two maps in memory at any one point. As soon as Map J is built, we flatten Map J-1 into
/// a single bitmap by taking the union of all of its values. This union gives us Xj-1.
///
/// ## Memory Usage
/// This function is expected to be called on a maximum of 10 bitmaps. The worst case thus happens when
/// 10 identical large bitmaps are given.
///
/// In the context of Meilisearch, let's imagine that we are given 10 bitmaps containing all
/// the document ids. If the dataset contains 16 million documents, then each bitmap will take
/// around 2MB of memory.
///
/// When creating Map 3, we will have, in memory:
/// 1. The 10 original bitmaps (20MB)
/// 2. X0 : 2MB
/// 3. Map 1, containing 9 bitmaps: 18MB
/// 4. Map 2, containing 8 bitmaps: 16MB
/// 5. X1: 2MB
/// for a total of around 60MB of memory. This roughly represents the maximum memory usage of this function.
///
/// ## Time complexity
/// Let N be the size of the given list of bitmaps and M the length of each individual bitmap.
///
/// We need to create N new bitmaps. The most expensive one to create is the second one, where we need to
/// iterate over the N keys of Map 1, and for each of those keys `k_i`, we perform `N-k_i` bitmap unions.
/// Unioning two bitmaps is O(M), and we need to do it O(N^2) times.
///
/// Therefore the time complexity is O(N^3 * M).
fn create_non_disjoint_combinations(bitmaps: Vec<RoaringBitmap>) -> Vec<RoaringBitmap> {
    let nbr_parts = bitmaps.len();
    if nbr_parts == 1 {
        return bitmaps;
    }
    let mut flattened_levels = vec![];
    let mut last_level: BTreeMap<usize, RoaringBitmap> =
        bitmaps.clone().into_iter().enumerate().collect();

    for _ in 2..=nbr_parts {
        let mut new_level = BTreeMap::new();
        for (last_part_index, base_combination) in last_level.iter() {
            #[allow(clippy::needless_range_loop)]
            for new_last_part_index in last_part_index + 1..nbr_parts {
                let new_combination = base_combination & &bitmaps[new_last_part_index];
                if !new_combination.is_empty() {
                    match new_level.entry(new_last_part_index) {
                        Entry::Occupied(mut b) => {
                            *b.get_mut() |= new_combination;
                        }
                        Entry::Vacant(entry) => {
                            entry.insert(new_combination);
                        }
                    }
                }
            }
        }
        // Now flatten the last level to save memory
        let flattened_last_level = MultiOps::union(last_level.into_values());
        flattened_levels.push(flattened_last_level);
        last_level = new_level;
    }
    // Flatten the last level
    let flattened_last_level = MultiOps::union(last_level.into_values());
    flattened_levels.push(flattened_last_level);
    flattened_levels
}

/// Given a list of bitmaps `b0,b1,...,bn` , compute the list of bitmaps `X0,X1,...,Xn`
/// such that `Xi` contains all the elements that are contained in **exactly** `i+1` bitmaps among `b0,b1,...,bn`.
///
/// The returned vector is guaranteed to be of length `n`. It is equal to `vec![X0, X1, ..., Xn]`.
fn create_disjoint_combinations(parts_candidates_array: Vec<RoaringBitmap>) -> Vec<RoaringBitmap> {
    let non_disjoint_combinations = create_non_disjoint_combinations(parts_candidates_array);
    let mut disjoint_combinations = vec![];
    let mut combinations = non_disjoint_combinations.into_iter().peekable();
    while let Some(mut combination) = combinations.next() {
        if let Some(forbidden) = combinations.peek() {
            combination -= forbidden;
        }
        disjoint_combinations.push(combination)
    }

    disjoint_combinations
}

#[cfg(test)]
mod tests {
    use big_s::S;
    use roaring::RoaringBitmap;

    use crate::index::tests::TempIndex;
    use crate::search::criteria::exactness::{
        create_disjoint_combinations, create_non_disjoint_combinations,
    };
    use crate::snapshot_tests::display_bitmap;
    use crate::{Criterion, SearchResult};

    #[test]
    fn test_exact_words_subcriterion() {
        let index = TempIndex::new();

        index
            .update_settings(|settings| {
                settings.set_primary_key(S("id"));
                settings.set_criteria(vec![Criterion::Exactness]);
            })
            .unwrap();

        index
            .add_documents(documents!([
                // not relevant
                { "id": "0", "text": "cat good dog bad" },
                // 1 exact word
                { "id": "1", "text": "they said: cats arebetter thandogs" },
                // 3 exact words
                { "id": "2", "text": "they said: cats arebetter than dogs" },
                // 5 exact words
                { "id": "3", "text": "they said: cats are better than dogs" },
                // attribute starts with the exact words
                { "id": "4", "text": "cats are better than dogs except on Saturday" },
                // attribute equal to the exact words
                { "id": "5", "text": "cats are better than dogs" },
            ]))
            .unwrap();

        let rtxn = index.read_txn().unwrap();

        let SearchResult { matching_words: _, candidates: _, documents_ids } =
            index.search(&rtxn).query("cats are better than dogs").execute().unwrap();

        insta::assert_snapshot!(format!("{documents_ids:?}"), @"[5, 4, 3, 2, 1]");
    }

    fn print_combinations(rbs: &[RoaringBitmap]) -> String {
        let mut s = String::new();
        for rb in rbs {
            s.push_str(&format!("{}\n", &display_bitmap(rb)));
        }
        s
    }

    // In these unit tests, the test bitmaps always contain all the multiple of a certain number.
    // This makes it easy to check the validity of the results of `create_disjoint_combinations` by
    // counting the number of dividers of elements in the returned bitmaps.
    fn assert_correct_combinations(combinations: &[RoaringBitmap], dividers: &[u32]) {
        for (i, set) in combinations.iter().enumerate() {
            let expected_nbr_dividers = i + 1;
            for el in set {
                let nbr_dividers = dividers.iter().map(|d| usize::from(el % d == 0)).sum::<usize>();
                assert_eq!(
                    nbr_dividers, expected_nbr_dividers,
                    "{el} is divisible by {nbr_dividers} elements, not {expected_nbr_dividers}."
                );
            }
        }
    }

    #[test]
    fn compute_combinations_1() {
        let b0: RoaringBitmap = (0..).into_iter().map(|x| 2 * x).take_while(|x| *x < 150).collect();

        let parts_candidates = vec![b0];

        let combinations = create_disjoint_combinations(parts_candidates);
        insta::assert_snapshot!(print_combinations(&combinations), @r###"
        [0, 2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 22, 24, 26, 28, 30, 32, 34, 36, 38, 40, 42, 44, 46, 48, 50, 52, 54, 56, 58, 60, 62, 64, 66, 68, 70, 72, 74, 76, 78, 80, 82, 84, 86, 88, 90, 92, 94, 96, 98, 100, 102, 104, 106, 108, 110, 112, 114, 116, 118, 120, 122, 124, 126, 128, 130, 132, 134, 136, 138, 140, 142, 144, 146, 148, ]
        "###);

        assert_correct_combinations(&combinations, &[2]);
    }

    #[test]
    fn compute_combinations_2() {
        let b0: RoaringBitmap = (0..).into_iter().map(|x| 2 * x).take_while(|x| *x < 150).collect();
        let b1: RoaringBitmap = (0..).into_iter().map(|x| 3 * x).take_while(|x| *x < 150).collect();

        let parts_candidates = vec![b0, b1];

        let combinations = create_disjoint_combinations(parts_candidates);
        insta::assert_snapshot!(print_combinations(&combinations), @r###"
        [2, 3, 4, 8, 9, 10, 14, 15, 16, 20, 21, 22, 26, 27, 28, 32, 33, 34, 38, 39, 40, 44, 45, 46, 50, 51, 52, 56, 57, 58, 62, 63, 64, 68, 69, 70, 74, 75, 76, 80, 81, 82, 86, 87, 88, 92, 93, 94, 98, 99, 100, 104, 105, 106, 110, 111, 112, 116, 117, 118, 122, 123, 124, 128, 129, 130, 134, 135, 136, 140, 141, 142, 146, 147, 148, ]
        [0, 6, 12, 18, 24, 30, 36, 42, 48, 54, 60, 66, 72, 78, 84, 90, 96, 102, 108, 114, 120, 126, 132, 138, 144, ]
        "###);
    }

    #[test]
    fn compute_combinations_4() {
        let b0: RoaringBitmap = (0..).into_iter().map(|x| 2 * x).take_while(|x| *x < 150).collect();
        let b1: RoaringBitmap = (0..).into_iter().map(|x| 3 * x).take_while(|x| *x < 150).collect();
        let b2: RoaringBitmap = (0..).into_iter().map(|x| 5 * x).take_while(|x| *x < 150).collect();
        let b3: RoaringBitmap = (0..).into_iter().map(|x| 7 * x).take_while(|x| *x < 150).collect();

        let parts_candidates = vec![b0, b1, b2, b3];

        let combinations = create_disjoint_combinations(parts_candidates);

        insta::assert_snapshot!(print_combinations(&combinations), @r###"
        [2, 3, 4, 5, 7, 8, 9, 16, 22, 25, 26, 27, 32, 33, 34, 38, 39, 44, 46, 49, 51, 52, 55, 57, 58, 62, 64, 65, 68, 69, 74, 76, 77, 81, 82, 85, 86, 87, 88, 91, 92, 93, 94, 95, 99, 104, 106, 111, 115, 116, 117, 118, 119, 122, 123, 124, 125, 128, 129, 133, 134, 136, 141, 142, 145, 146, 148, ]
        [6, 10, 12, 14, 15, 18, 20, 21, 24, 28, 35, 36, 40, 45, 48, 50, 54, 56, 63, 66, 72, 75, 78, 80, 96, 98, 100, 102, 108, 110, 112, 114, 130, 132, 135, 138, 144, 147, ]
        [30, 42, 60, 70, 84, 90, 105, 120, 126, 140, ]
        [0, ]
        "###);

        // But we also check it programmatically
        assert_correct_combinations(&combinations, &[2, 3, 5, 7]);
    }
    #[test]
    fn compute_combinations_4_with_empty_results_at_end() {
        let b0: RoaringBitmap = (1..).into_iter().map(|x| 2 * x).take_while(|x| *x < 150).collect();
        let b1: RoaringBitmap = (1..).into_iter().map(|x| 3 * x).take_while(|x| *x < 150).collect();
        let b2: RoaringBitmap = (1..).into_iter().map(|x| 5 * x).take_while(|x| *x < 150).collect();
        let b3: RoaringBitmap = (1..).into_iter().map(|x| 7 * x).take_while(|x| *x < 150).collect();

        let parts_candidates = vec![b0, b1, b2, b3];

        let combinations = create_disjoint_combinations(parts_candidates);

        insta::assert_snapshot!(print_combinations(&combinations), @r###"
        [2, 3, 4, 5, 7, 8, 9, 16, 22, 25, 26, 27, 32, 33, 34, 38, 39, 44, 46, 49, 51, 52, 55, 57, 58, 62, 64, 65, 68, 69, 74, 76, 77, 81, 82, 85, 86, 87, 88, 91, 92, 93, 94, 95, 99, 104, 106, 111, 115, 116, 117, 118, 119, 122, 123, 124, 125, 128, 129, 133, 134, 136, 141, 142, 145, 146, 148, ]
        [6, 10, 12, 14, 15, 18, 20, 21, 24, 28, 35, 36, 40, 45, 48, 50, 54, 56, 63, 66, 72, 75, 78, 80, 96, 98, 100, 102, 108, 110, 112, 114, 130, 132, 135, 138, 144, 147, ]
        [30, 42, 60, 70, 84, 90, 105, 120, 126, 140, ]
        []
        "###);

        // But we also check it programmatically
        assert_correct_combinations(&combinations, &[2, 3, 5, 7]);
    }

    #[test]
    fn compute_combinations_4_with_some_equal_bitmaps() {
        let b0: RoaringBitmap = (0..).into_iter().map(|x| 2 * x).take_while(|x| *x < 150).collect();
        let b1: RoaringBitmap = (0..).into_iter().map(|x| 3 * x).take_while(|x| *x < 150).collect();
        let b2: RoaringBitmap = (0..).into_iter().map(|x| 5 * x).take_while(|x| *x < 150).collect();
        // b3 == b1
        let b3: RoaringBitmap = (0..).into_iter().map(|x| 3 * x).take_while(|x| *x < 150).collect();

        let parts_candidates = vec![b0, b1, b2, b3];

        let combinations = create_disjoint_combinations(parts_candidates);

        insta::assert_snapshot!(print_combinations(&combinations), @r###"
        [2, 4, 5, 8, 14, 16, 22, 25, 26, 28, 32, 34, 35, 38, 44, 46, 52, 55, 56, 58, 62, 64, 65, 68, 74, 76, 82, 85, 86, 88, 92, 94, 95, 98, 104, 106, 112, 115, 116, 118, 122, 124, 125, 128, 134, 136, 142, 145, 146, 148, ]
        [3, 9, 10, 20, 21, 27, 33, 39, 40, 50, 51, 57, 63, 69, 70, 80, 81, 87, 93, 99, 100, 110, 111, 117, 123, 129, 130, 140, 141, 147, ]
        [6, 12, 15, 18, 24, 36, 42, 45, 48, 54, 66, 72, 75, 78, 84, 96, 102, 105, 108, 114, 126, 132, 135, 138, 144, ]
        [0, 30, 60, 90, 120, ]
        "###);

        // But we also check it programmatically
        assert_correct_combinations(&combinations, &[2, 3, 5, 3]);
    }

    #[test]
    fn compute_combinations_10() {
        let dividers = [2, 3, 5, 7, 11, 6, 15, 35, 18, 14];
        let parts_candidates: Vec<RoaringBitmap> = dividers
            .iter()
            .map(|&divider| {
                (0..).into_iter().map(|x| divider * x).take_while(|x| *x <= 210).collect()
            })
            .collect();

        let combinations = create_disjoint_combinations(parts_candidates);
        insta::assert_snapshot!(print_combinations(&combinations), @r###"
        [2, 3, 4, 5, 7, 8, 9, 11, 16, 25, 26, 27, 32, 34, 38, 39, 46, 49, 51, 52, 57, 58, 62, 64, 65, 68, 69, 74, 76, 81, 82, 85, 86, 87, 91, 92, 93, 94, 95, 104, 106, 111, 115, 116, 117, 118, 119, 121, 122, 123, 124, 125, 128, 129, 133, 134, 136, 141, 142, 143, 145, 146, 148, 152, 153, 155, 158, 159, 161, 164, 166, 171, 172, 177, 178, 183, 184, 185, 187, 188, 194, 201, 202, 203, 205, 206, 207, 208, 209, ]
        [10, 20, 21, 22, 33, 40, 44, 50, 55, 63, 77, 80, 88, 99, 100, 130, 147, 160, 170, 176, 189, 190, 200, ]
        [6, 12, 14, 15, 24, 28, 35, 45, 48, 56, 75, 78, 96, 98, 102, 110, 112, 114, 135, 138, 156, 174, 175, 182, 186, 192, 195, 196, 204, ]
        [18, 36, 54, 66, 72, 108, 132, 144, 154, 162, 165, ]
        [30, 42, 60, 70, 84, 105, 120, 140, 150, 168, 198, ]
        [90, 126, 180, ]
        []
        [210, ]
        []
        [0, ]
        "###);

        assert_correct_combinations(&combinations, &dividers);
    }

    #[test]
    fn compute_combinations_30() {
        let dividers: [u32; 30] = [
            1, 2, 3, 4, 5, 1, 2, 3, 4, 5, 1, 2, 3, 4, 5, 1, 2, 3, 4, 5, 1, 2, 3, 4, 5, 1, 2, 3, 4,
            5,
        ];
        let parts_candidates: Vec<RoaringBitmap> = dividers
            .iter()
            .map(|divider| {
                (0..).into_iter().map(|x| divider * x).take_while(|x| *x <= 100).collect()
            })
            .collect();

        let combinations = create_non_disjoint_combinations(parts_candidates.clone());
        insta::assert_snapshot!(print_combinations(&combinations), @r###"
        [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 80, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95, 96, 97, 98, 99, 100, ]
        [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 80, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95, 96, 97, 98, 99, 100, ]
        [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 80, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95, 96, 97, 98, 99, 100, ]
        [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 80, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95, 96, 97, 98, 99, 100, ]
        [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 80, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95, 96, 97, 98, 99, 100, ]
        [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 80, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95, 96, 97, 98, 99, 100, ]
        [0, 2, 3, 4, 5, 6, 8, 9, 10, 12, 14, 15, 16, 18, 20, 21, 22, 24, 25, 26, 27, 28, 30, 32, 33, 34, 35, 36, 38, 39, 40, 42, 44, 45, 46, 48, 50, 51, 52, 54, 55, 56, 57, 58, 60, 62, 63, 64, 65, 66, 68, 69, 70, 72, 74, 75, 76, 78, 80, 81, 82, 84, 85, 86, 87, 88, 90, 92, 93, 94, 95, 96, 98, 99, 100, ]
        [0, 2, 3, 4, 5, 6, 8, 9, 10, 12, 14, 15, 16, 18, 20, 21, 22, 24, 25, 26, 27, 28, 30, 32, 33, 34, 35, 36, 38, 39, 40, 42, 44, 45, 46, 48, 50, 51, 52, 54, 55, 56, 57, 58, 60, 62, 63, 64, 65, 66, 68, 69, 70, 72, 74, 75, 76, 78, 80, 81, 82, 84, 85, 86, 87, 88, 90, 92, 93, 94, 95, 96, 98, 99, 100, ]
        [0, 2, 3, 4, 5, 6, 8, 9, 10, 12, 14, 15, 16, 18, 20, 21, 22, 24, 25, 26, 27, 28, 30, 32, 33, 34, 35, 36, 38, 39, 40, 42, 44, 45, 46, 48, 50, 51, 52, 54, 55, 56, 57, 58, 60, 62, 63, 64, 65, 66, 68, 69, 70, 72, 74, 75, 76, 78, 80, 81, 82, 84, 85, 86, 87, 88, 90, 92, 93, 94, 95, 96, 98, 99, 100, ]
        [0, 2, 3, 4, 5, 6, 8, 9, 10, 12, 14, 15, 16, 18, 20, 21, 22, 24, 25, 26, 27, 28, 30, 32, 33, 34, 35, 36, 38, 39, 40, 42, 44, 45, 46, 48, 50, 51, 52, 54, 55, 56, 57, 58, 60, 62, 63, 64, 65, 66, 68, 69, 70, 72, 74, 75, 76, 78, 80, 81, 82, 84, 85, 86, 87, 88, 90, 92, 93, 94, 95, 96, 98, 99, 100, ]
        [0, 2, 3, 4, 5, 6, 8, 9, 10, 12, 14, 15, 16, 18, 20, 21, 22, 24, 25, 26, 27, 28, 30, 32, 33, 34, 35, 36, 38, 39, 40, 42, 44, 45, 46, 48, 50, 51, 52, 54, 55, 56, 57, 58, 60, 62, 63, 64, 65, 66, 68, 69, 70, 72, 74, 75, 76, 78, 80, 81, 82, 84, 85, 86, 87, 88, 90, 92, 93, 94, 95, 96, 98, 99, 100, ]
        [0, 2, 3, 4, 5, 6, 8, 9, 10, 12, 14, 15, 16, 18, 20, 21, 22, 24, 25, 26, 27, 28, 30, 32, 33, 34, 35, 36, 38, 39, 40, 42, 44, 45, 46, 48, 50, 51, 52, 54, 55, 56, 57, 58, 60, 62, 63, 64, 65, 66, 68, 69, 70, 72, 74, 75, 76, 78, 80, 81, 82, 84, 85, 86, 87, 88, 90, 92, 93, 94, 95, 96, 98, 99, 100, ]
        [0, 4, 6, 8, 10, 12, 15, 16, 18, 20, 24, 28, 30, 32, 36, 40, 42, 44, 45, 48, 50, 52, 54, 56, 60, 64, 66, 68, 70, 72, 75, 76, 78, 80, 84, 88, 90, 92, 96, 100, ]
        [0, 4, 6, 8, 10, 12, 15, 16, 18, 20, 24, 28, 30, 32, 36, 40, 42, 44, 45, 48, 50, 52, 54, 56, 60, 64, 66, 68, 70, 72, 75, 76, 78, 80, 84, 88, 90, 92, 96, 100, ]
        [0, 4, 6, 8, 10, 12, 15, 16, 18, 20, 24, 28, 30, 32, 36, 40, 42, 44, 45, 48, 50, 52, 54, 56, 60, 64, 66, 68, 70, 72, 75, 76, 78, 80, 84, 88, 90, 92, 96, 100, ]
        [0, 4, 6, 8, 10, 12, 15, 16, 18, 20, 24, 28, 30, 32, 36, 40, 42, 44, 45, 48, 50, 52, 54, 56, 60, 64, 66, 68, 70, 72, 75, 76, 78, 80, 84, 88, 90, 92, 96, 100, ]
        [0, 4, 6, 8, 10, 12, 15, 16, 18, 20, 24, 28, 30, 32, 36, 40, 42, 44, 45, 48, 50, 52, 54, 56, 60, 64, 66, 68, 70, 72, 75, 76, 78, 80, 84, 88, 90, 92, 96, 100, ]
        [0, 4, 6, 8, 10, 12, 15, 16, 18, 20, 24, 28, 30, 32, 36, 40, 42, 44, 45, 48, 50, 52, 54, 56, 60, 64, 66, 68, 70, 72, 75, 76, 78, 80, 84, 88, 90, 92, 96, 100, ]
        [0, 12, 20, 24, 30, 36, 40, 48, 60, 72, 80, 84, 90, 96, 100, ]
        [0, 12, 20, 24, 30, 36, 40, 48, 60, 72, 80, 84, 90, 96, 100, ]
        [0, 12, 20, 24, 30, 36, 40, 48, 60, 72, 80, 84, 90, 96, 100, ]
        [0, 12, 20, 24, 30, 36, 40, 48, 60, 72, 80, 84, 90, 96, 100, ]
        [0, 12, 20, 24, 30, 36, 40, 48, 60, 72, 80, 84, 90, 96, 100, ]
        [0, 12, 20, 24, 30, 36, 40, 48, 60, 72, 80, 84, 90, 96, 100, ]
        [0, 60, ]
        [0, 60, ]
        [0, 60, ]
        [0, 60, ]
        [0, 60, ]
        [0, 60, ]
        "###);

        let combinations = create_disjoint_combinations(parts_candidates);
        insta::assert_snapshot!(print_combinations(&combinations), @r###"
        []
        []
        []
        []
        []
        [1, 7, 11, 13, 17, 19, 23, 29, 31, 37, 41, 43, 47, 49, 53, 59, 61, 67, 71, 73, 77, 79, 83, 89, 91, 97, ]
        []
        []
        []
        []
        []
        [2, 3, 5, 9, 14, 21, 22, 25, 26, 27, 33, 34, 35, 38, 39, 46, 51, 55, 57, 58, 62, 63, 65, 69, 74, 81, 82, 85, 86, 87, 93, 94, 95, 98, 99, ]
        []
        []
        []
        []
        []
        [4, 6, 8, 10, 15, 16, 18, 28, 32, 42, 44, 45, 50, 52, 54, 56, 64, 66, 68, 70, 75, 76, 78, 88, 92, ]
        []
        []
        []
        []
        []
        [12, 20, 24, 30, 36, 40, 48, 72, 80, 84, 90, 96, 100, ]
        []
        []
        []
        []
        []
        [0, 60, ]
        "###);

        assert_correct_combinations(&combinations, &dividers);
    }
}
