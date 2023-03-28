pub use self::facet::{FacetDistribution, Filter, DEFAULT_VALUES_PER_FACET};
pub use self::matches::{
    FormatOptions, MatchBounds, Matcher, MatcherBuilder, MatchingWord, MatchingWords,
};
use crate::{
    execute_search, AscDesc, DefaultSearchLogger, DocumentId, Index, Result, SearchContext,
};
use levenshtein_automata::{LevenshteinAutomatonBuilder as LevBuilder, DFA};
use once_cell::sync::Lazy;
use roaring::bitmap::RoaringBitmap;
use std::fmt;

// Building these factories is not free.
static LEVDIST0: Lazy<LevBuilder> = Lazy::new(|| LevBuilder::new(0, true));
static LEVDIST1: Lazy<LevBuilder> = Lazy::new(|| LevBuilder::new(1, true));
static LEVDIST2: Lazy<LevBuilder> = Lazy::new(|| LevBuilder::new(2, true));

pub mod facet;
mod fst_utils;
mod matches;
pub mod new;

pub struct Search<'a> {
    query: Option<String>,
    // this should be linked to the String in the query
    filter: Option<Filter<'a>>,
    offset: usize,
    limit: usize,
    sort_criteria: Option<Vec<AscDesc>>,
    terms_matching_strategy: TermsMatchingStrategy,
    authorize_typos: bool,
    words_limit: usize,
    exhaustive_number_hits: bool,
    rtxn: &'a heed::RoTxn<'a>,
    index: &'a Index,
}

impl<'a> Search<'a> {
    pub fn new(rtxn: &'a heed::RoTxn, index: &'a Index) -> Search<'a> {
        Search {
            query: None,
            filter: None,
            offset: 0,
            limit: 20,
            sort_criteria: None,
            terms_matching_strategy: TermsMatchingStrategy::default(),
            authorize_typos: true,
            exhaustive_number_hits: false,
            words_limit: 10,
            rtxn,
            index,
        }
    }

    pub fn query(&mut self, query: impl Into<String>) -> &mut Search<'a> {
        self.query = Some(query.into());
        self
    }

    pub fn offset(&mut self, offset: usize) -> &mut Search<'a> {
        self.offset = offset;
        self
    }

    pub fn limit(&mut self, limit: usize) -> &mut Search<'a> {
        self.limit = limit;
        self
    }

    pub fn sort_criteria(&mut self, criteria: Vec<AscDesc>) -> &mut Search<'a> {
        self.sort_criteria = Some(criteria);
        self
    }

    pub fn terms_matching_strategy(&mut self, value: TermsMatchingStrategy) -> &mut Search<'a> {
        self.terms_matching_strategy = value;
        self
    }

    pub fn authorize_typos(&mut self, value: bool) -> &mut Search<'a> {
        self.authorize_typos = value;
        self
    }

    pub fn words_limit(&mut self, value: usize) -> &mut Search<'a> {
        self.words_limit = value;
        self
    }

    pub fn filter(&mut self, condition: Filter<'a>) -> &mut Search<'a> {
        self.filter = Some(condition);
        self
    }

    /// Force the search to exhastivelly compute the number of candidates,
    /// this will increase the search time but allows finite pagination.
    pub fn exhaustive_number_hits(&mut self, exhaustive_number_hits: bool) -> &mut Search<'a> {
        self.exhaustive_number_hits = exhaustive_number_hits;
        self
    }

    // TODO!
    fn _is_typo_authorized(&self) -> Result<bool> {
        let index_authorizes_typos = self.index.authorize_typos(self.rtxn)?;
        // only authorize typos if both the index and the query allow it.
        Ok(self.authorize_typos && index_authorizes_typos)
    }

    pub fn execute(&self) -> Result<SearchResult> {
        let mut ctx = SearchContext::new(self.index, self.rtxn);
        execute_search(
            &mut ctx,
            &self.query,
            self.terms_matching_strategy,
            &self.filter,
            &self.sort_criteria,
            self.offset,
            self.limit,
            Some(self.words_limit),
            &mut DefaultSearchLogger,
            &mut DefaultSearchLogger,
        )
    }
}

impl fmt::Debug for Search<'_> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let Search {
            query,
            filter,
            offset,
            limit,
            sort_criteria,
            terms_matching_strategy,
            authorize_typos,
            words_limit,
            exhaustive_number_hits,
            rtxn: _,
            index: _,
        } = self;
        f.debug_struct("Search")
            .field("query", query)
            .field("filter", filter)
            .field("offset", offset)
            .field("limit", limit)
            .field("sort_criteria", sort_criteria)
            .field("terms_matching_strategy", terms_matching_strategy)
            .field("authorize_typos", authorize_typos)
            .field("exhaustive_number_hits", exhaustive_number_hits)
            .field("words_limit", words_limit)
            .finish()
    }
}

#[derive(Default, Debug)]
pub struct SearchResult {
    pub matching_words: MatchingWords,
    pub candidates: RoaringBitmap,
    // TODO those documents ids should be associated with their criteria scores.
    pub documents_ids: Vec<DocumentId>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TermsMatchingStrategy {
    // remove last word first
    Last,
    // all words are mandatory
    All,
}

impl Default for TermsMatchingStrategy {
    fn default() -> Self {
        Self::Last
    }
}

fn get_first(s: &str) -> &str {
    match s.chars().next() {
        Some(c) => &s[..c.len_utf8()],
        None => panic!("unexpected empty query"),
    }
}

pub fn build_dfa(word: &str, typos: u8, is_prefix: bool) -> DFA {
    let lev = match typos {
        0 => &LEVDIST0,
        1 => &LEVDIST1,
        _ => &LEVDIST2,
    };

    if is_prefix {
        lev.build_prefix_dfa(word)
    } else {
        lev.build_dfa(word)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::index::tests::TempIndex;

    #[cfg(feature = "default")]
    #[test]
    fn test_kanji_language_detection() {
        let index = TempIndex::new();

        index
            .add_documents(documents!([
                { "id": 0, "title": "The quick (\"brown\") fox can't jump 32.3 feet, right? Brr, it's 29.3°F!" },
                { "id": 1, "title": "東京のお寿司。" },
                { "id": 2, "title": "הַשּׁוּעָל הַמָּהִיר (״הַחוּם״) לֹא יָכוֹל לִקְפֹּץ 9.94 מֶטְרִים, נָכוֹן? ברר, 1.5°C- בַּחוּץ!" }
            ]))
            .unwrap();

        let txn = index.write_txn().unwrap();
        let mut search = Search::new(&txn, &index);

        search.query("東京");
        let SearchResult { documents_ids, .. } = search.execute().unwrap();

        assert_eq!(documents_ids, vec![1]);
    }

    // #[test]
    // fn test_is_authorized_typos() {
    //     let index = TempIndex::new();
    //     let mut txn = index.write_txn().unwrap();

    //     let mut search = Search::new(&txn, &index);

    //     // default is authorized
    //     assert!(search.is_typo_authorized().unwrap());

    //     search.authorize_typos(false);
    //     assert!(!search.is_typo_authorized().unwrap());

    //     index.put_authorize_typos(&mut txn, false).unwrap();
    //     txn.commit().unwrap();

    //     let txn = index.read_txn().unwrap();
    //     let mut search = Search::new(&txn, &index);

    //     assert!(!search.is_typo_authorized().unwrap());

    //     search.authorize_typos(true);
    //     assert!(!search.is_typo_authorized().unwrap());
    // }

    // #[test]
    // fn test_one_typos_tolerance() {
    //     let fst = fst::Set::from_iter(["zealand"].iter()).unwrap().map_data(Cow::Owned).unwrap();
    //     let mut cache = HashMap::new();
    //     let found = word_derivations("zealend", false, 1, &fst, &mut cache).unwrap();

    //     assert_eq!(found, &[("zealand".to_string(), 1)]);
    // }

    // #[test]
    // fn test_one_typos_first_letter() {
    //     let fst = fst::Set::from_iter(["zealand"].iter()).unwrap().map_data(Cow::Owned).unwrap();
    //     let mut cache = HashMap::new();
    //     let found = word_derivations("sealand", false, 1, &fst, &mut cache).unwrap();

    //     assert_eq!(found, &[]);
    // }

    // #[test]
    // fn test_two_typos_tolerance() {
    //     let fst = fst::Set::from_iter(["zealand"].iter()).unwrap().map_data(Cow::Owned).unwrap();
    //     let mut cache = HashMap::new();
    //     let found = word_derivations("zealemd", false, 2, &fst, &mut cache).unwrap();

    //     assert_eq!(found, &[("zealand".to_string(), 2)]);
    // }

    // #[test]
    // fn test_two_typos_first_letter() {
    //     let fst = fst::Set::from_iter(["zealand"].iter()).unwrap().map_data(Cow::Owned).unwrap();
    //     let mut cache = HashMap::new();
    //     let found = word_derivations("sealand", false, 2, &fst, &mut cache).unwrap();

    //     assert_eq!(found, &[("zealand".to_string(), 2)]);
    // }

    // #[test]
    // fn test_prefix() {
    //     let fst = fst::Set::from_iter(["zealand"].iter()).unwrap().map_data(Cow::Owned).unwrap();
    //     let mut cache = HashMap::new();
    //     let found = word_derivations("ze", true, 0, &fst, &mut cache).unwrap();

    //     assert_eq!(found, &[("zealand".to_string(), 0)]);
    // }

    // #[test]
    // fn test_bad_prefix() {
    //     let fst = fst::Set::from_iter(["zealand"].iter()).unwrap().map_data(Cow::Owned).unwrap();
    //     let mut cache = HashMap::new();
    //     let found = word_derivations("se", true, 0, &fst, &mut cache).unwrap();

    //     assert_eq!(found, &[]);
    // }

    // #[test]
    // fn test_prefix_with_typo() {
    //     let fst = fst::Set::from_iter(["zealand"].iter()).unwrap().map_data(Cow::Owned).unwrap();
    //     let mut cache = HashMap::new();
    //     let found = word_derivations("zae", true, 1, &fst, &mut cache).unwrap();

    //     assert_eq!(found, &[("zealand".to_string(), 1)]);
    // }
}
