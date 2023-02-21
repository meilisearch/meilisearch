use std::borrow::Cow;
use std::cmp::max;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::hash::Hash;
use std::rc::Rc;
use std::{fmt, mem};

use charabia::normalizer::NormalizedTokenIter;
use charabia::{SeparatorKind, TokenKind};
use roaring::RoaringBitmap;
use slice_group_by::GroupBy;

use crate::search::matches::matching_words::{MatchingWord, PrimitiveWordId};
use crate::search::TermsMatchingStrategy;
use crate::{CboRoaringBitmapLenCodec, Index, MatchingWords, Result};

type IsOptionalWord = bool;
type IsPrefix = bool;

#[derive(Clone, PartialEq, Eq, Hash)]
pub enum Operation {
    And(Vec<Operation>),
    // series of consecutive non prefix and exact words
    // `None` means a stop word.
    Phrase(Vec<Option<String>>),
    Or(IsOptionalWord, Vec<Operation>),
    Query(Query),
}

impl fmt::Debug for Operation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fn pprint_tree(f: &mut fmt::Formatter<'_>, op: &Operation, depth: usize) -> fmt::Result {
            match op {
                Operation::And(children) => {
                    writeln!(f, "{:1$}AND", "", depth * 2)?;
                    children.iter().try_for_each(|c| pprint_tree(f, c, depth + 1))
                }
                Operation::Phrase(children) => {
                    writeln!(f, "{:2$}PHRASE {:?}", "", children, depth * 2)
                }
                Operation::Or(true, children) => {
                    writeln!(f, "{:1$}OR(WORD)", "", depth * 2)?;
                    children.iter().try_for_each(|c| pprint_tree(f, c, depth + 1))
                }
                Operation::Or(false, children) => {
                    writeln!(f, "{:1$}OR", "", depth * 2)?;
                    children.iter().try_for_each(|c| pprint_tree(f, c, depth + 1))
                }
                Operation::Query(query) => writeln!(f, "{:2$}{:?}", "", query, depth * 2),
            }
        }

        pprint_tree(f, self, 0)
    }
}

impl Operation {
    fn and(mut ops: Vec<Self>) -> Self {
        if ops.len() == 1 {
            ops.pop().unwrap()
        } else {
            Self::And(ops)
        }
    }

    pub fn or(word_branch: IsOptionalWord, mut ops: Vec<Self>) -> Self {
        if ops.len() == 1 {
            ops.pop().unwrap()
        } else {
            let ops = ops
                .into_iter()
                .flat_map(|o| match o {
                    Operation::Or(wb, children) if wb == word_branch => children,
                    op => vec![op],
                })
                .collect();
            Self::Or(word_branch, ops)
        }
    }

    fn phrase(mut words: Vec<Option<String>>) -> Self {
        if words.len() == 1 {
            if let Some(word) = words.pop().unwrap() {
                Self::Query(Query { prefix: false, kind: QueryKind::exact(word) })
            } else {
                Self::Phrase(words)
            }
        } else {
            Self::Phrase(words)
        }
    }

    pub fn query(&self) -> Option<&Query> {
        match self {
            Operation::Query(query) => Some(query),
            _ => None,
        }
    }
}

#[derive(Clone, Eq, PartialEq, Hash)]
pub struct Query {
    pub prefix: IsPrefix,
    pub kind: QueryKind,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum QueryKind {
    Tolerant { typo: u8, word: String },
    Exact { original_typo: u8, word: String },
}

impl QueryKind {
    pub fn exact(word: String) -> Self {
        QueryKind::Exact { original_typo: 0, word }
    }

    pub fn tolerant(typo: u8, word: String) -> Self {
        QueryKind::Tolerant { typo, word }
    }

    pub fn typo(&self) -> u8 {
        match self {
            QueryKind::Tolerant { typo, .. } => *typo,
            QueryKind::Exact { original_typo, .. } => *original_typo,
        }
    }

    pub fn word(&self) -> &str {
        match self {
            QueryKind::Tolerant { word, .. } => word,
            QueryKind::Exact { word, .. } => word,
        }
    }
}

impl fmt::Debug for Query {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Query { prefix, kind } = self;
        let prefix = if *prefix { String::from("Prefix") } else { String::default() };
        match kind {
            QueryKind::Exact { word, .. } => {
                f.debug_struct(&(prefix + "Exact")).field("word", &word).finish()
            }
            QueryKind::Tolerant { typo, word } => f
                .debug_struct(&(prefix + "Tolerant"))
                .field("word", &word)
                .field("max typo", &typo)
                .finish(),
        }
    }
}

trait Context {
    fn word_docids(&self, word: &str) -> heed::Result<Option<RoaringBitmap>>;
    fn synonyms<S: AsRef<str>>(&self, words: &[S]) -> heed::Result<Option<Vec<Vec<String>>>>;
    fn word_documents_count(&self, word: &str) -> heed::Result<Option<u64>> {
        match self.word_docids(word)? {
            Some(rb) => Ok(Some(rb.len())),
            None => Ok(None),
        }
    }
    /// Returns the minimum word len for 1 and 2 typos.
    fn min_word_len_for_typo(&self) -> heed::Result<(u8, u8)>;
    fn exact_words(&self) -> Option<&fst::Set<Cow<[u8]>>>;
    fn word_pair_frequency(
        &self,
        left_word: &str,
        right_word: &str,
        proximity: u8,
    ) -> heed::Result<Option<u64>>;
}

/// The query tree builder is the interface to build a query tree.
pub struct QueryTreeBuilder<'a> {
    rtxn: &'a heed::RoTxn<'a>,
    index: &'a Index,
    terms_matching_strategy: TermsMatchingStrategy,
    authorize_typos: bool,
    words_limit: Option<usize>,
    exact_words: Option<fst::Set<Cow<'a, [u8]>>>,
}

impl<'a> Context for QueryTreeBuilder<'a> {
    fn word_docids(&self, word: &str) -> heed::Result<Option<RoaringBitmap>> {
        self.index.word_docids.get(self.rtxn, word)
    }

    fn synonyms<S: AsRef<str>>(&self, words: &[S]) -> heed::Result<Option<Vec<Vec<String>>>> {
        self.index.words_synonyms(self.rtxn, words)
    }

    fn word_documents_count(&self, word: &str) -> heed::Result<Option<u64>> {
        self.index.word_documents_count(self.rtxn, word)
    }

    fn min_word_len_for_typo(&self) -> heed::Result<(u8, u8)> {
        let one = self.index.min_word_len_one_typo(self.rtxn)?;
        let two = self.index.min_word_len_two_typos(self.rtxn)?;
        Ok((one, two))
    }

    fn exact_words(&self) -> Option<&fst::Set<Cow<[u8]>>> {
        self.exact_words.as_ref()
    }

    fn word_pair_frequency(
        &self,
        left_word: &str,
        right_word: &str,
        proximity: u8,
    ) -> heed::Result<Option<u64>> {
        let key = (proximity, left_word, right_word);
        self.index
            .word_pair_proximity_docids
            .remap_data_type::<CboRoaringBitmapLenCodec>()
            .get(self.rtxn, &key)
    }
}

impl<'a> QueryTreeBuilder<'a> {
    /// Create a `QueryTreeBuilder` from a heed ReadOnly transaction `rtxn`
    /// and an Index `index`.
    pub fn new(rtxn: &'a heed::RoTxn<'a>, index: &'a Index) -> Result<Self> {
        Ok(Self {
            rtxn,
            index,
            terms_matching_strategy: TermsMatchingStrategy::default(),
            authorize_typos: true,
            words_limit: None,
            exact_words: index.exact_words(rtxn)?,
        })
    }

    /// if `terms_matching_strategy` is set to `All` the query tree will be
    /// generated forcing all query words to be present in each matching documents
    /// (the criterion `words` will be ignored).
    /// default value if not called: `Last`
    pub fn terms_matching_strategy(
        &mut self,
        terms_matching_strategy: TermsMatchingStrategy,
    ) -> &mut Self {
        self.terms_matching_strategy = terms_matching_strategy;
        self
    }

    /// if `authorize_typos` is set to `false` the query tree will be generated
    /// forcing all query words to match documents without any typo
    /// (the criterion `typo` will be ignored).
    /// default value if not called: `true`
    pub fn authorize_typos(&mut self, authorize_typos: bool) -> &mut Self {
        self.authorize_typos = authorize_typos;
        self
    }

    /// Limit words and phrases that will be taken for query building.
    /// Any beyond `words_limit` will be ignored.
    pub fn words_limit(&mut self, words_limit: usize) -> &mut Self {
        self.words_limit = Some(words_limit);
        self
    }

    /// Build the query tree:
    /// - if `terms_matching_strategy` is set to `All` the query tree will be
    ///   generated forcing all query words to be present in each matching documents
    ///   (the criterion `words` will be ignored)
    /// - if `authorize_typos` is set to `false` the query tree will be generated
    ///   forcing all query words to match documents without any typo
    ///   (the criterion `typo` will be ignored)
    pub fn build<A: AsRef<[u8]>>(
        &self,
        query: NormalizedTokenIter<A>,
    ) -> Result<Option<(Operation, PrimitiveQuery, MatchingWords)>> {
        let primitive_query = create_primitive_query(query, self.words_limit);
        if !primitive_query.is_empty() {
            let qt = create_query_tree(
                self,
                self.terms_matching_strategy,
                self.authorize_typos,
                &primitive_query,
            )?;
            let matching_words =
                create_matching_words(self, self.authorize_typos, &primitive_query)?;
            Ok(Some((qt, primitive_query, matching_words)))
        } else {
            Ok(None)
        }
    }
}

/// Split the word depending on the frequency of pairs near together in the database documents.
fn split_best_frequency<'a>(
    ctx: &impl Context,
    word: &'a str,
) -> heed::Result<Option<(&'a str, &'a str)>> {
    let chars = word.char_indices().skip(1);
    let mut best = None;

    for (i, _) in chars {
        let (left, right) = word.split_at(i);

        let pair_freq = ctx.word_pair_frequency(left, right, 1)?.unwrap_or(0);

        if pair_freq != 0 && best.map_or(true, |(old, _, _)| pair_freq > old) {
            best = Some((pair_freq, left, right));
        }
    }

    Ok(best.map(|(_, left, right)| (left, right)))
}

#[derive(Clone)]
pub struct TypoConfig<'a> {
    pub max_typos: u8,
    pub word_len_one_typo: u8,
    pub word_len_two_typo: u8,
    pub exact_words: Option<&'a fst::Set<Cow<'a, [u8]>>>,
}

/// Return the `QueryKind` of a word depending on `authorize_typos`
/// and the provided word length.
fn typos(word: String, authorize_typos: bool, config: TypoConfig) -> QueryKind {
    if authorize_typos && !config.exact_words.map_or(false, |s| s.contains(&word)) {
        let count = word.chars().count().min(u8::MAX as usize) as u8;
        if count < config.word_len_one_typo {
            QueryKind::exact(word)
        } else if count < config.word_len_two_typo {
            QueryKind::tolerant(1.min(config.max_typos), word)
        } else {
            QueryKind::tolerant(2.min(config.max_typos), word)
        }
    } else {
        QueryKind::exact(word)
    }
}

/// Fetch synonyms from the `Context` for the provided word
/// and create the list of operations for the query tree
fn synonyms(ctx: &impl Context, word: &[&str]) -> heed::Result<Option<Vec<Operation>>> {
    let synonyms = ctx.synonyms(word)?;
    Ok(synonyms.map(|synonyms| {
        synonyms
            .into_iter()
            .map(|synonym| {
                if synonym.len() == 1 {
                    Operation::Query(Query {
                        prefix: false,
                        kind: QueryKind::exact(synonym[0].clone()),
                    })
                } else {
                    Operation::Phrase(synonym.into_iter().map(Some).collect())
                }
            })
            .collect()
    }))
}

/// Main function that creates the final query tree from the primitive query.
fn create_query_tree(
    ctx: &impl Context,
    terms_matching_strategy: TermsMatchingStrategy,
    authorize_typos: bool,
    query: &[PrimitiveQueryPart],
) -> Result<Operation> {
    /// Matches on the `PrimitiveQueryPart` and create an operation from it.
    fn resolve_primitive_part(
        ctx: &impl Context,
        authorize_typos: bool,
        part: PrimitiveQueryPart,
    ) -> Result<Operation> {
        match part {
            // 1. try to split word in 2
            // 2. try to fetch synonyms
            // 3. create an operation containing the word
            // 4. wrap all in an OR operation
            PrimitiveQueryPart::Word(word, prefix) => {
                let mut children = synonyms(ctx, &[&word])?.unwrap_or_default();
                if let Some((left, right)) = split_best_frequency(ctx, &word)? {
                    children.push(Operation::Phrase(vec![
                        Some(left.to_string()),
                        Some(right.to_string()),
                    ]));
                }
                let (word_len_one_typo, word_len_two_typo) = ctx.min_word_len_for_typo()?;
                let exact_words = ctx.exact_words();
                let config =
                    TypoConfig { max_typos: 2, word_len_one_typo, word_len_two_typo, exact_words };
                children.push(Operation::Query(Query {
                    prefix,
                    kind: typos(word, authorize_typos, config),
                }));
                Ok(Operation::or(false, children))
            }
            // create a CONSECUTIVE operation wrapping all word in the phrase
            PrimitiveQueryPart::Phrase(words) => Ok(Operation::phrase(words)),
        }
    }

    /// Create all ngrams 1..=3 generating query tree branches.
    fn ngrams(
        ctx: &impl Context,
        authorize_typos: bool,
        query: &[PrimitiveQueryPart],
        any_words: bool,
    ) -> Result<Operation> {
        const MAX_NGRAM: usize = 3;
        let mut op_children = Vec::new();

        for sub_query in query.linear_group_by(|a, b| !(a.is_phrase() || b.is_phrase())) {
            let mut or_op_children = Vec::new();

            for ngram in 1..=MAX_NGRAM.min(sub_query.len()) {
                if let Some(group) = sub_query.get(..ngram) {
                    let mut and_op_children = Vec::new();
                    let tail = &sub_query[ngram..];
                    let is_last = tail.is_empty();

                    match group {
                        [part] => {
                            let operation =
                                resolve_primitive_part(ctx, authorize_typos, part.clone())?;
                            and_op_children.push(operation);
                        }
                        words => {
                            let is_prefix = words.last().map_or(false, |part| part.is_prefix());
                            let words: Vec<_> = words
                                .iter()
                                .filter_map(|part| {
                                    if let PrimitiveQueryPart::Word(word, _) = part {
                                        Some(word.as_str())
                                    } else {
                                        None
                                    }
                                })
                                .collect();
                            let mut operations = synonyms(ctx, &words)?.unwrap_or_default();
                            let concat = words.concat();
                            let (word_len_one_typo, word_len_two_typo) =
                                ctx.min_word_len_for_typo()?;
                            let exact_words = ctx.exact_words();
                            let config = TypoConfig {
                                max_typos: 1,
                                word_len_one_typo,
                                word_len_two_typo,
                                exact_words,
                            };
                            let query = Query {
                                prefix: is_prefix,
                                kind: typos(concat, authorize_typos, config),
                            };
                            operations.push(Operation::Query(query));
                            and_op_children.push(Operation::or(false, operations));
                        }
                    }

                    if !is_last {
                        let ngrams = ngrams(ctx, authorize_typos, tail, any_words)?;
                        and_op_children.push(ngrams);
                    }

                    if any_words {
                        or_op_children.push(Operation::or(false, and_op_children));
                    } else {
                        or_op_children.push(Operation::and(and_op_children));
                    }
                }
            }
            op_children.push(Operation::or(false, or_op_children));
        }

        if any_words {
            Ok(Operation::or(false, op_children))
        } else {
            Ok(Operation::and(op_children))
        }
    }

    let number_phrases = query.iter().filter(|p| p.is_phrase()).count();
    let remove_count = query.len() - max(number_phrases, 1);
    if remove_count == 0 {
        return ngrams(ctx, authorize_typos, query, false);
    }

    let mut operation_children = Vec::new();
    let mut query = query.to_vec();
    for _ in 0..=remove_count {
        let pos = match terms_matching_strategy {
            TermsMatchingStrategy::All => return ngrams(ctx, authorize_typos, &query, false),
            TermsMatchingStrategy::Any => {
                let operation = Operation::Or(
                    true,
                    vec![
                        // branch allowing matching documents to contains any query word.
                        ngrams(ctx, authorize_typos, &query, true)?,
                        // branch forcing matching documents to contains all the query words,
                        // keeping this documents of the top of the resulted list.
                        ngrams(ctx, authorize_typos, &query, false)?,
                    ],
                );

                return Ok(operation);
            }
            TermsMatchingStrategy::Last => query
                .iter()
                .enumerate()
                .filter(|(_, part)| !part.is_phrase())
                .last()
                .map(|(pos, _)| pos),
            TermsMatchingStrategy::First => {
                query.iter().enumerate().find(|(_, part)| !part.is_phrase()).map(|(pos, _)| pos)
            }
            TermsMatchingStrategy::Size => query
                .iter()
                .enumerate()
                .filter(|(_, part)| !part.is_phrase())
                .min_by_key(|(_, part)| match part {
                    PrimitiveQueryPart::Word(s, _) => s.len(),
                    _ => unreachable!(),
                })
                .map(|(pos, _)| pos),
            TermsMatchingStrategy::Frequency => query
                .iter()
                .enumerate()
                .filter(|(_, part)| !part.is_phrase())
                .max_by_key(|(_, part)| match part {
                    PrimitiveQueryPart::Word(s, _) => {
                        ctx.word_documents_count(s).unwrap_or_default().unwrap_or(u64::max_value())
                    }
                    _ => unreachable!(),
                })
                .map(|(pos, _)| pos),
        };

        // compute and push the current branch on the front
        operation_children.insert(0, ngrams(ctx, authorize_typos, &query, false)?);
        // remove word from query before creating an new branch
        match pos {
            Some(pos) => query.remove(pos),
            None => break,
        };
    }

    Ok(Operation::or(true, operation_children))
}

#[derive(Default, Debug)]
struct MatchingWordCache {
    all: Vec<Rc<MatchingWord>>,
    map: HashMap<(String, u8, bool), Rc<MatchingWord>>,
}
impl MatchingWordCache {
    fn insert(&mut self, word: String, typo: u8, prefix: bool) -> Option<Rc<MatchingWord>> {
        match self.map.entry((word.clone(), typo, prefix)) {
            Entry::Occupied(idx) => Some(idx.get().clone()),
            Entry::Vacant(vacant) => {
                let matching_word = Rc::new(MatchingWord::new(word, typo, prefix)?);
                self.all.push(matching_word.clone());
                vacant.insert(matching_word.clone());
                Some(matching_word)
            }
        }
        // To deactivate the cache, for testing purposes, use the following instead:
        // let matching_word = Rc::new(MatchingWord::new(word, typo, prefix)?);
        // self.all.push(matching_word.clone());
        // Some(matching_word)
    }
}

/// Main function that matchings words used for crop and highlight.
fn create_matching_words(
    ctx: &impl Context,
    authorize_typos: bool,
    query: &[PrimitiveQueryPart],
) -> Result<MatchingWords> {
    /// Matches on the `PrimitiveQueryPart` and create matchings words from it.
    fn resolve_primitive_part(
        ctx: &impl Context,
        authorize_typos: bool,
        part: PrimitiveQueryPart,
        matching_words: &mut Vec<(Vec<Rc<MatchingWord>>, Vec<PrimitiveWordId>)>,
        matching_word_cache: &mut MatchingWordCache,
        id: PrimitiveWordId,
    ) -> Result<()> {
        match part {
            // 1. try to split word in 2
            // 2. try to fetch synonyms
            PrimitiveQueryPart::Word(word, prefix) => {
                if let Some(synonyms) = ctx.synonyms(&[word.as_str()])? {
                    for synonym in synonyms {
                        // Require that all words of the synonym have a corresponding MatchingWord
                        // before adding any of its words to the matching_words result.
                        if let Some(synonym_matching_words) = synonym
                            .into_iter()
                            .map(|word| matching_word_cache.insert(word, 0, false))
                            .collect()
                        {
                            matching_words.push((synonym_matching_words, vec![id]));
                        }
                    }
                }

                if let Some((left, right)) = split_best_frequency(ctx, &word)? {
                    // Require that both left and right words have a corresponding MatchingWord
                    // before adding them to the matching_words result
                    if let Some(left) = matching_word_cache.insert(left.to_string(), 0, false) {
                        if let Some(right) = matching_word_cache.insert(right.to_string(), 0, false)
                        {
                            matching_words.push((vec![left, right], vec![id]));
                        }
                    }
                }

                let (word_len_one_typo, word_len_two_typo) = ctx.min_word_len_for_typo()?;
                let exact_words = ctx.exact_words();
                let config =
                    TypoConfig { max_typos: 2, word_len_one_typo, word_len_two_typo, exact_words };

                let matching_word = match typos(word, authorize_typos, config) {
                    QueryKind::Exact { word, .. } => matching_word_cache.insert(word, 0, prefix),
                    QueryKind::Tolerant { typo, word } => {
                        matching_word_cache.insert(word, typo, prefix)
                    }
                };
                if let Some(matching_word) = matching_word {
                    matching_words.push((vec![matching_word], vec![id]));
                }
            }
            // create a CONSECUTIVE matchings words wrapping all word in the phrase
            PrimitiveQueryPart::Phrase(words) => {
                let ids: Vec<_> =
                    (0..words.len()).into_iter().map(|i| id + i as PrimitiveWordId).collect();
                // Require that all words of the phrase have a corresponding MatchingWord
                // before adding any of them to the matching_words result
                if let Some(phrase_matching_words) = words
                    .into_iter()
                    .flatten()
                    .map(|w| matching_word_cache.insert(w, 0, false))
                    .collect()
                {
                    matching_words.push((phrase_matching_words, ids));
                }
            }
        }

        Ok(())
    }

    /// Create all ngrams 1..=3 generating query tree branches.
    fn ngrams(
        ctx: &impl Context,
        authorize_typos: bool,
        query: &[PrimitiveQueryPart],
        matching_words: &mut Vec<(Vec<Rc<MatchingWord>>, Vec<PrimitiveWordId>)>,
        matching_word_cache: &mut MatchingWordCache,
        mut id: PrimitiveWordId,
    ) -> Result<()> {
        const MAX_NGRAM: usize = 3;

        for sub_query in query.linear_group_by(|a, b| !(a.is_phrase() || b.is_phrase())) {
            for ngram in 1..=MAX_NGRAM.min(sub_query.len()) {
                if let Some(group) = sub_query.get(..ngram) {
                    let tail = &sub_query[ngram..];
                    let is_last = tail.is_empty();

                    match group {
                        [part] => {
                            resolve_primitive_part(
                                ctx,
                                authorize_typos,
                                part.clone(),
                                matching_words,
                                matching_word_cache,
                                id,
                            )?;
                        }
                        words => {
                            let is_prefix = words.last().map_or(false, |part| part.is_prefix());
                            let words: Vec<_> = words
                                .iter()
                                .filter_map(|part| {
                                    if let PrimitiveQueryPart::Word(word, _) = part {
                                        Some(word.as_str())
                                    } else {
                                        None
                                    }
                                })
                                .collect();
                            let ids: Vec<_> = (0..words.len())
                                .into_iter()
                                .map(|i| id + i as PrimitiveWordId)
                                .collect();

                            if let Some(synonyms) = ctx.synonyms(&words)? {
                                for synonym in synonyms {
                                    if let Some(synonym) = synonym
                                        .into_iter()
                                        .map(|syn| matching_word_cache.insert(syn, 0, false))
                                        .collect()
                                    {
                                        matching_words.push((synonym, ids.clone()));
                                    }
                                }
                            }
                            let word = words.concat();
                            let (word_len_one_typo, word_len_two_typo) =
                                ctx.min_word_len_for_typo()?;
                            let exact_words = ctx.exact_words();
                            let config = TypoConfig {
                                max_typos: 1,
                                word_len_one_typo,
                                word_len_two_typo,
                                exact_words,
                            };
                            let matching_word = match typos(word, authorize_typos, config) {
                                QueryKind::Exact { word, .. } => {
                                    matching_word_cache.insert(word, 0, is_prefix)
                                }
                                QueryKind::Tolerant { typo, word } => {
                                    matching_word_cache.insert(word, typo, is_prefix)
                                }
                            };
                            if let Some(matching_word) = matching_word {
                                matching_words.push((vec![matching_word], ids));
                            }
                        }
                    }

                    if !is_last {
                        ngrams(
                            ctx,
                            authorize_typos,
                            tail,
                            matching_words,
                            matching_word_cache,
                            id + 1,
                        )?;
                    }
                }
            }
            id += sub_query.iter().map(|x| x.len() as PrimitiveWordId).sum::<PrimitiveWordId>();
        }

        Ok(())
    }

    let mut matching_word_cache = MatchingWordCache::default();
    let mut matching_words = Vec::new();
    ngrams(ctx, authorize_typos, query, &mut matching_words, &mut matching_word_cache, 0)?;
    Ok(MatchingWords::new(matching_words))
}

pub type PrimitiveQuery = Vec<PrimitiveQueryPart>;

#[derive(Debug, Clone)]
pub enum PrimitiveQueryPart {
    Phrase(Vec<Option<String>>),
    Word(String, IsPrefix),
}

impl PrimitiveQueryPart {
    fn is_phrase(&self) -> bool {
        matches!(self, Self::Phrase(_))
    }

    fn is_prefix(&self) -> bool {
        matches!(self, Self::Word(_, is_prefix) if *is_prefix)
    }

    fn len(&self) -> usize {
        match self {
            Self::Phrase(words) => words.len(),
            Self::Word(_, _) => 1,
        }
    }
}

/// Create primitive query from tokenized query string,
/// the primitive query is an intermediate state to build the query tree.
fn create_primitive_query<A>(
    query: NormalizedTokenIter<A>,
    words_limit: Option<usize>,
) -> PrimitiveQuery
where
    A: AsRef<[u8]>,
{
    let mut primitive_query = Vec::new();
    let mut phrase = Vec::new();
    let mut quoted = false;

    let parts_limit = words_limit.unwrap_or(usize::MAX);

    let mut peekable = query.peekable();
    while let Some(token) = peekable.next() {
        // early return if word limit is exceeded
        if primitive_query.len() >= parts_limit {
            return primitive_query;
        }

        match token.kind {
            TokenKind::Word | TokenKind::StopWord => {
                // 1. if the word is quoted we push it in a phrase-buffer waiting for the ending quote,
                // 2. if the word is not the last token of the query and is not a stop_word we push it as a non-prefix word,
                // 3. if the word is the last token of the query we push it as a prefix word.
                if quoted {
                    if let TokenKind::StopWord = token.kind {
                        phrase.push(None)
                    } else {
                        phrase.push(Some(token.lemma().to_string()));
                    }
                } else if peekable.peek().is_some() {
                    if let TokenKind::StopWord = token.kind {
                    } else {
                        primitive_query
                            .push(PrimitiveQueryPart::Word(token.lemma().to_string(), false));
                    }
                } else {
                    primitive_query.push(PrimitiveQueryPart::Word(token.lemma().to_string(), true));
                }
            }
            TokenKind::Separator(separator_kind) => {
                let quote_count = token.lemma().chars().filter(|&s| s == '"').count();
                // swap quoted state if we encounter a double quote
                if quote_count % 2 != 0 {
                    quoted = !quoted;
                }
                // if there is a quote or a hard separator we close the phrase.
                if quote_count > 0 || separator_kind == SeparatorKind::Hard {
                    let phrase = mem::take(&mut phrase);

                    // if the phrase only contains stop words, we don't keep it in the query.
                    if phrase.iter().any(|w| w.is_some()) {
                        primitive_query.push(PrimitiveQueryPart::Phrase(phrase));
                    }
                }
            }
            _ => (),
        }
    }

    // If a quote is never closed, we consider all of the end of the query as a phrase.
    if phrase.iter().any(|w| w.is_some()) {
        primitive_query.push(PrimitiveQueryPart::Phrase(mem::take(&mut phrase)));
    }

    primitive_query
}

/// Returns the maximum number of typos that this Operation allows.
pub fn maximum_typo(operation: &Operation) -> usize {
    use Operation::{And, Or, Phrase, Query};
    match operation {
        Or(_, ops) => ops.iter().map(maximum_typo).max().unwrap_or(0),
        And(ops) => ops.iter().map(maximum_typo).sum::<usize>(),
        Query(q) => q.kind.typo() as usize,
        // no typo allowed in phrases
        Phrase(_) => 0,
    }
}

/// Returns the maximum proximity that this Operation allows.
pub fn maximum_proximity(operation: &Operation) -> usize {
    use Operation::{And, Or, Phrase, Query};
    match operation {
        Or(_, ops) => ops.iter().map(maximum_proximity).max().unwrap_or(0),
        And(ops) => {
            ops.iter().map(maximum_proximity).sum::<usize>() + ops.len().saturating_sub(1) * 7
        }
        Query(_) | Phrase(_) => 0,
    }
}

#[cfg(test)]
mod test {
    use std::collections::HashMap;

    use charabia::Tokenize;
    use maplit::hashmap;
    use rand::rngs::StdRng;
    use rand::{Rng, SeedableRng};

    use super::*;
    use crate::index::tests::TempIndex;
    use crate::index::{DEFAULT_MIN_WORD_LEN_ONE_TYPO, DEFAULT_MIN_WORD_LEN_TWO_TYPOS};

    #[derive(Debug)]
    struct TestContext {
        synonyms: HashMap<Vec<String>, Vec<Vec<String>>>,
        postings: HashMap<String, RoaringBitmap>,
        exact_words: Option<fst::Set<Cow<'static, [u8]>>>,
    }

    impl TestContext {
        fn build<A: AsRef<[u8]>>(
            &self,
            terms_matching_strategy: TermsMatchingStrategy,
            authorize_typos: bool,
            words_limit: Option<usize>,
            query: NormalizedTokenIter<A>,
        ) -> Result<Option<(Operation, PrimitiveQuery)>> {
            let primitive_query = create_primitive_query(query, words_limit);
            if !primitive_query.is_empty() {
                let qt = create_query_tree(
                    self,
                    terms_matching_strategy,
                    authorize_typos,
                    &primitive_query,
                )?;
                Ok(Some((qt, primitive_query)))
            } else {
                Ok(None)
            }
        }
    }

    impl Context for TestContext {
        fn word_docids(&self, word: &str) -> heed::Result<Option<RoaringBitmap>> {
            Ok(self.postings.get(word).cloned())
        }

        fn synonyms<S: AsRef<str>>(&self, words: &[S]) -> heed::Result<Option<Vec<Vec<String>>>> {
            let words: Vec<_> = words.iter().map(|s| s.as_ref().to_owned()).collect();
            Ok(self.synonyms.get(&words).cloned())
        }

        fn min_word_len_for_typo(&self) -> heed::Result<(u8, u8)> {
            Ok((DEFAULT_MIN_WORD_LEN_ONE_TYPO, DEFAULT_MIN_WORD_LEN_TWO_TYPOS))
        }

        fn exact_words(&self) -> Option<&fst::Set<Cow<[u8]>>> {
            self.exact_words.as_ref()
        }

        fn word_pair_frequency(
            &self,
            left_word: &str,
            right_word: &str,
            _proximity: u8,
        ) -> heed::Result<Option<u64>> {
            match self.word_docids(&format!("{} {}", left_word, right_word))? {
                Some(rb) => Ok(Some(rb.len())),
                None => Ok(None),
            }
        }
    }

    impl Default for TestContext {
        fn default() -> TestContext {
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

            let exact_words = fst::SetBuilder::new(Vec::new()).unwrap().into_inner().unwrap();
            let exact_words =
                Some(fst::Set::new(exact_words).unwrap().map_data(Cow::Owned).unwrap());

            TestContext {
                synonyms: hashmap! {
                    vec![String::from("hello")] => vec![
                        vec![String::from("hi")],
                        vec![String::from("good"), String::from("morning")],
                    ],
                    vec![String::from("world")] => vec![
                        vec![String::from("earth")],
                        vec![String::from("nature")],
                    ],
                    // new york city
                    vec![String::from("nyc")] => vec![
                        vec![String::from("new"), String::from("york")],
                        vec![String::from("new"), String::from("york"), String::from("city")],
                    ],
                    vec![String::from("new"), String::from("york")] => vec![
                        vec![String::from("nyc")],
                        vec![String::from("new"), String::from("york"), String::from("city")],
                    ],
                    vec![String::from("new"), String::from("york"), String::from("city")] => vec![
                        vec![String::from("nyc")],
                        vec![String::from("new"), String::from("york")],
                    ],
                },
                postings: hashmap! {
                    String::from("hello")           => random_postings(rng,   1500),
                    String::from("hi")              => random_postings(rng,   4000),
                    String::from("word")            => random_postings(rng,   2500),
                    String::from("split")           => random_postings(rng,    400),
                    String::from("ngrams")          => random_postings(rng,   1400),
                    String::from("world")           => random_postings(rng, 15_000),
                    String::from("earth")           => random_postings(rng,   8000),
                    String::from("2021")            => random_postings(rng,    100),
                    String::from("2020")            => random_postings(rng,    500),
                    String::from("is")              => random_postings(rng, 50_000),
                    String::from("this")            => random_postings(rng, 50_000),
                    String::from("good")            => random_postings(rng,   1250),
                    String::from("morning")         => random_postings(rng,    125),
                    String::from("word split")      => random_postings(rng,   5000),
                    String::from("quick brownfox")  => random_postings(rng,   7000),
                    String::from("quickbrown fox")  => random_postings(rng,   8000),
                },
                exact_words,
            }
        }
    }

    #[test]
    fn prefix() {
        let query = "hey friends";
        let tokens = query.tokenize();

        let (query_tree, _) = TestContext::default()
            .build(TermsMatchingStrategy::All, true, None, tokens)
            .unwrap()
            .unwrap();

        insta::assert_debug_snapshot!(query_tree, @r###"
        OR
          AND
            Exact { word: "hey" }
            PrefixTolerant { word: "friends", max typo: 1 }
          PrefixTolerant { word: "heyfriends", max typo: 1 }
        "###);
    }

    #[test]
    fn no_prefix() {
        let query = "hey friends ";
        let tokens = query.tokenize();

        let (query_tree, _) = TestContext::default()
            .build(TermsMatchingStrategy::All, true, None, tokens)
            .unwrap()
            .unwrap();

        insta::assert_debug_snapshot!(query_tree, @r###"
        OR
          AND
            Exact { word: "hey" }
            Tolerant { word: "friends", max typo: 1 }
          Tolerant { word: "heyfriends", max typo: 1 }
        "###);
    }

    #[test]
    fn synonyms() {
        let query = "hello world ";
        let tokens = query.tokenize();

        let (query_tree, _) = TestContext::default()
            .build(TermsMatchingStrategy::All, true, None, tokens)
            .unwrap()
            .unwrap();

        insta::assert_debug_snapshot!(query_tree, @r###"
        OR
          AND
            OR
              Exact { word: "hi" }
              PHRASE [Some("good"), Some("morning")]
              Tolerant { word: "hello", max typo: 1 }
            OR
              Exact { word: "earth" }
              Exact { word: "nature" }
              Tolerant { word: "world", max typo: 1 }
          Tolerant { word: "helloworld", max typo: 1 }
        "###);
    }

    #[test]
    fn simple_synonyms() {
        let query = "nyc";
        let tokens = query.tokenize();

        let (query_tree, _) = TestContext::default()
            .build(TermsMatchingStrategy::Last, true, None, tokens)
            .unwrap()
            .unwrap();

        insta::assert_debug_snapshot!(query_tree, @r###"
        OR
          PHRASE [Some("new"), Some("york")]
          PHRASE [Some("new"), Some("york"), Some("city")]
          PrefixExact { word: "nyc" }
        "###);
    }

    #[test]
    fn complex_synonyms() {
        let query = "new york city ";
        let tokens = query.tokenize();

        let (query_tree, _) = TestContext::default()
            .build(TermsMatchingStrategy::All, true, None, tokens)
            .unwrap()
            .unwrap();

        insta::assert_debug_snapshot!(query_tree, @r###"
        OR
          AND
            Exact { word: "new" }
            OR
              AND
                Exact { word: "york" }
                Exact { word: "city" }
              Tolerant { word: "yorkcity", max typo: 1 }
          AND
            OR
              Exact { word: "nyc" }
              PHRASE [Some("new"), Some("york"), Some("city")]
              Tolerant { word: "newyork", max typo: 1 }
            Exact { word: "city" }
          Exact { word: "nyc" }
          PHRASE [Some("new"), Some("york")]
          Tolerant { word: "newyorkcity", max typo: 1 }
        "###);
    }

    #[test]
    fn ngrams() {
        let query = "n grams ";
        let tokens = query.tokenize();

        let (query_tree, _) = TestContext::default()
            .build(TermsMatchingStrategy::All, true, None, tokens)
            .unwrap()
            .unwrap();

        insta::assert_debug_snapshot!(query_tree, @r###"
        OR
          AND
            Exact { word: "n" }
            Tolerant { word: "grams", max typo: 1 }
          Tolerant { word: "ngrams", max typo: 1 }
        "###);
    }

    #[test]
    fn word_split() {
        let query = "wordsplit fish ";
        let tokens = query.tokenize();

        let (query_tree, _) = TestContext::default()
            .build(TermsMatchingStrategy::All, true, None, tokens)
            .unwrap()
            .unwrap();

        insta::assert_debug_snapshot!(query_tree, @r###"
        OR
          AND
            OR
              PHRASE [Some("word"), Some("split")]
              Tolerant { word: "wordsplit", max typo: 2 }
            Exact { word: "fish" }
          Tolerant { word: "wordsplitfish", max typo: 1 }
        "###);
    }

    #[test]
    fn word_split_choose_pair_with_max_freq() {
        let query = "quickbrownfox";
        let tokens = query.tokenize();

        let (query_tree, _) = TestContext::default()
            .build(TermsMatchingStrategy::All, true, None, tokens)
            .unwrap()
            .unwrap();

        insta::assert_debug_snapshot!(query_tree, @r###"
        OR
          PHRASE [Some("quickbrown"), Some("fox")]
          PrefixTolerant { word: "quickbrownfox", max typo: 2 }
        "###);
    }

    #[test]
    fn phrase() {
        let query = "\"hey friends\" \" \" \"wooop";
        let tokens = query.tokenize();

        let (query_tree, _) = TestContext::default()
            .build(TermsMatchingStrategy::All, true, None, tokens)
            .unwrap()
            .unwrap();

        insta::assert_debug_snapshot!(query_tree, @r###"
        AND
          PHRASE [Some("hey"), Some("friends")]
          Exact { word: "wooop" }
        "###);
    }

    #[test]
    fn phrase_2() {
        // https://github.com/meilisearch/meilisearch/issues/2722
        let query = "coco \"harry\"";
        let tokens = query.tokenize();

        let (query_tree, _) = TestContext::default()
            .build(TermsMatchingStrategy::default(), true, None, tokens)
            .unwrap()
            .unwrap();

        insta::assert_debug_snapshot!(query_tree, @r###"
        OR(WORD)
          Exact { word: "harry" }
          AND
            Exact { word: "coco" }
            Exact { word: "harry" }
        "###);
    }

    #[test]
    fn phrase_with_hard_separator() {
        let query = "\"hey friends. wooop wooop\"";
        let tokens = query.tokenize();

        let (query_tree, _) = TestContext::default()
            .build(TermsMatchingStrategy::All, true, None, tokens)
            .unwrap()
            .unwrap();

        insta::assert_debug_snapshot!(query_tree, @r###"
        AND
          PHRASE [Some("hey"), Some("friends")]
          PHRASE [Some("wooop"), Some("wooop")]
        "###);
    }

    #[test]
    fn optional_word() {
        let query = "hey my friend ";
        let tokens = query.tokenize();

        let (query_tree, _) = TestContext::default()
            .build(TermsMatchingStrategy::default(), true, None, tokens)
            .unwrap()
            .unwrap();

        insta::assert_debug_snapshot!(query_tree, @r###"
        OR(WORD)
          Exact { word: "hey" }
          OR
            AND
              Exact { word: "hey" }
              Exact { word: "my" }
            Tolerant { word: "heymy", max typo: 1 }
          OR
            AND
              Exact { word: "hey" }
              OR
                AND
                  Exact { word: "my" }
                  Tolerant { word: "friend", max typo: 1 }
                Tolerant { word: "myfriend", max typo: 1 }
            AND
              Tolerant { word: "heymy", max typo: 1 }
              Tolerant { word: "friend", max typo: 1 }
            Tolerant { word: "heymyfriend", max typo: 1 }
        "###);
    }

    #[test]
    fn optional_word_phrase() {
        let query = "\"hey my\"";
        let tokens = query.tokenize();

        let (query_tree, _) = TestContext::default()
            .build(TermsMatchingStrategy::default(), true, None, tokens)
            .unwrap()
            .unwrap();

        insta::assert_debug_snapshot!(query_tree, @r###"
        PHRASE [Some("hey"), Some("my")]
        "###);
    }

    #[test]
    fn optional_word_multiple_phrases() {
        let query = r#""hey" my good "friend""#;
        let tokens = query.tokenize();

        let (query_tree, _) = TestContext::default()
            .build(TermsMatchingStrategy::default(), true, None, tokens)
            .unwrap()
            .unwrap();

        insta::assert_debug_snapshot!(query_tree, @r###"
        OR(WORD)
          AND
            Exact { word: "hey" }
            Exact { word: "friend" }
          AND
            Exact { word: "hey" }
            Exact { word: "my" }
            Exact { word: "friend" }
          AND
            Exact { word: "hey" }
            OR
              AND
                Exact { word: "my" }
                Exact { word: "good" }
              Tolerant { word: "mygood", max typo: 1 }
            Exact { word: "friend" }
        "###);
    }

    #[test]
    fn no_typo() {
        let query = "hey friends ";
        let tokens = query.tokenize();

        let (query_tree, _) = TestContext::default()
            .build(TermsMatchingStrategy::All, false, None, tokens)
            .unwrap()
            .unwrap();

        insta::assert_debug_snapshot!(query_tree, @r###"
        OR
          AND
            Exact { word: "hey" }
            Exact { word: "friends" }
          Exact { word: "heyfriends" }
        "###);
    }

    #[test]
    fn words_limit() {
        let query = "\"hey my\" good friend";
        let tokens = query.tokenize();

        let (query_tree, _) = TestContext::default()
            .build(TermsMatchingStrategy::All, false, Some(2), tokens)
            .unwrap()
            .unwrap();

        insta::assert_debug_snapshot!(query_tree, @r###"
        AND
          PHRASE [Some("hey"), Some("my")]
          Exact { word: "good" }
        "###);
    }

    #[test]
    fn test_min_word_len_typo() {
        let exact_words = fst::Set::from_iter([b""]).unwrap().map_data(Cow::Owned).unwrap();
        let config = TypoConfig {
            max_typos: 2,
            word_len_one_typo: 5,
            word_len_two_typo: 7,
            exact_words: Some(&exact_words),
        };

        assert_eq!(
            typos("hello".to_string(), true, config.clone()),
            QueryKind::Tolerant { typo: 1, word: "hello".to_string() }
        );

        assert_eq!(
            typos("hell".to_string(), true, config.clone()),
            QueryKind::exact("hell".to_string())
        );

        assert_eq!(
            typos("verylongword".to_string(), true, config.clone()),
            QueryKind::Tolerant { typo: 2, word: "verylongword".to_string() }
        );
    }

    #[test]
    fn test_dont_create_matching_word_for_long_words() {
        let index = TempIndex::new();
        let rtxn = index.read_txn().unwrap();
        let query = "what a supercalifragilisticexpialidocioussupercalifragilisticexpialidocioussupercalifragilisticexpialidocioussupercalifragilisticexpialidocioussupercalifragilisticexpialidocioussupercalifragilisticexpialidocioussupercalifragilisticexpialidocioussupercalifragilisticexpialidocious house";
        let mut builder = QueryTreeBuilder::new(&rtxn, &index).unwrap();
        builder.words_limit(10);
        let (_, _, matching_words) = builder.build(query.tokenize()).unwrap().unwrap();
        insta::assert_snapshot!(format!("{matching_words:?}"), @r###"
        [
        ([MatchingWord { word: "house", typo: 1, prefix: true }], [3])
        ([MatchingWord { word: "house", typo: 1, prefix: true }], [2])
        ([MatchingWord { word: "whata", typo: 1, prefix: false }], [0, 1])
        ([MatchingWord { word: "house", typo: 1, prefix: true }], [2])
        ([MatchingWord { word: "house", typo: 1, prefix: true }], [1])
        ([MatchingWord { word: "what", typo: 0, prefix: false }], [0])
        ([MatchingWord { word: "a", typo: 0, prefix: false }], [1])
        ]
        "###);
    }

    #[test]
    fn disable_typo_on_word() {
        let query = "goodbye";
        let tokens = query.tokenize();

        let exact_words = fst::Set::from_iter(Some("goodbye")).unwrap().into_fst().into_inner();
        let exact_words = Some(fst::Set::new(exact_words).unwrap().map_data(Cow::Owned).unwrap());
        let context = TestContext { exact_words, ..Default::default() };
        let (query_tree, _) =
            context.build(TermsMatchingStrategy::All, true, Some(2), tokens).unwrap().unwrap();

        assert!(matches!(
            query_tree,
            Operation::Query(Query { prefix: true, kind: QueryKind::Exact { .. } })
        ));
    }

    // The memory usage test below is disabled because `cargo test` runs multiple tests in parallel,
    // which invalidates the measurements of memory usage. Nevertheless, it is a useful test to run
    // manually from time to time, so I kept it here, commented-out.

    // use std::alloc::{GlobalAlloc, System};
    // use std::sync::atomic::{self, AtomicI64};
    //
    // #[global_allocator]
    // static ALLOC: CountingAlloc =
    //     CountingAlloc { resident: AtomicI64::new(0), allocated: AtomicI64::new(0) };
    //
    // pub struct CountingAlloc {
    //     pub resident: AtomicI64,
    //     pub allocated: AtomicI64,
    // }
    // unsafe impl GlobalAlloc for CountingAlloc {
    //     unsafe fn alloc(&self, layout: std::alloc::Layout) -> *mut u8 {
    //         self.allocated.fetch_add(layout.size() as i64, atomic::Ordering::Relaxed);
    //         self.resident.fetch_add(layout.size() as i64, atomic::Ordering::Relaxed);
    //
    //         System.alloc(layout)
    //     }
    //
    //     unsafe fn dealloc(&self, ptr: *mut u8, layout: std::alloc::Layout) {
    //         self.resident.fetch_sub(layout.size() as i64, atomic::Ordering::Relaxed);
    //         System.dealloc(ptr, layout)
    //     }
    // }
    //
    // #[test]
    // fn memory_usage_of_ten_word_query() {
    //     let resident_before = ALLOC.resident.load(atomic::Ordering::SeqCst);
    //     let allocated_before = ALLOC.allocated.load(atomic::Ordering::SeqCst);
    //
    //     let index = TempIndex::new();
    //     let rtxn = index.read_txn().unwrap();
    //     let query = "a beautiful summer house by the beach overlooking what seems";
    //     let mut builder = QueryTreeBuilder::new(&rtxn, &index).unwrap();
    //     builder.words_limit(10);
    //     let x = builder.build(query.tokenize()).unwrap().unwrap();
    //     let resident_after = ALLOC.resident.load(atomic::Ordering::SeqCst);
    //     let allocated_after = ALLOC.allocated.load(atomic::Ordering::SeqCst);
    //
    //     // Weak check on the memory usage
    //     // Don't keep more than 5MB. (Arguably 5MB is already too high)
    //     assert!(resident_after - resident_before < 5_000_000);
    //     // Don't allocate more than 10MB.
    //     assert!(allocated_after - allocated_before < 10_000_000);
    //
    //     // Use these snapshots to measure the exact memory usage.
    //     // The values below were correct at the time I wrote them.
    //     // insta::assert_snapshot!(format!("{}", resident_after - resident_before), @"4486950");
    //     // insta::assert_snapshot!(format!("{}", allocated_after - allocated_before), @"7107502");
    //
    //     // Note, with the matching word cache deactivated, the memory usage was:
    //     // insta::assert_snapshot!(format!("{}", resident_after - resident_before), @"91248697");
    //     // insta::assert_snapshot!(format!("{}", allocated_after - allocated_before), @"125697588");
    //     // or about 20x more resident memory (90MB vs 4.5MB)
    //
    //     // Use x
    //     let _x = x;
    // }
}
