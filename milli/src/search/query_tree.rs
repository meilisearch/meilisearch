use std::borrow::Cow;
use std::{cmp, fmt, mem};

use charabia::classifier::ClassifiedTokenIter;
use charabia::{SeparatorKind, TokenKind};
use fst::Set;
use roaring::RoaringBitmap;
use slice_group_by::GroupBy;

use crate::search::matches::matching_words::{MatchingWord, PrimitiveWordId};
use crate::{Index, MatchingWords, Result};

type IsOptionalWord = bool;
type IsPrefix = bool;

#[derive(Clone, PartialEq, Eq, Hash)]
pub enum Operation {
    And(Vec<Operation>),
    // serie of consecutive non prefix and exact words
    Phrase(Vec<String>),
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
            Self::Or(word_branch, ops)
        }
    }

    fn phrase(mut words: Vec<String>) -> Self {
        if words.len() == 1 {
            Self::Query(Query { prefix: false, kind: QueryKind::exact(words.pop().unwrap()) })
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

    #[cfg(test)]
    pub fn exact_with_typo(original_typo: u8, word: String) -> Self {
        QueryKind::Exact { original_typo, word }
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
}

/// The query tree builder is the interface to build a query tree.
pub struct QueryTreeBuilder<'a> {
    rtxn: &'a heed::RoTxn<'a>,
    index: &'a Index,
    optional_words: bool,
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
        let one = self.index.min_word_len_one_typo(&self.rtxn)?;
        let two = self.index.min_word_len_two_typos(&self.rtxn)?;
        Ok((one, two))
    }

    fn exact_words(&self) -> Option<&fst::Set<Cow<[u8]>>> {
        self.exact_words.as_ref()
    }
}

impl<'a> QueryTreeBuilder<'a> {
    /// Create a `QueryTreeBuilder` from a heed ReadOnly transaction `rtxn`
    /// and an Index `index`.
    pub fn new(rtxn: &'a heed::RoTxn<'a>, index: &'a Index) -> Result<Self> {
        Ok(Self {
            rtxn,
            index,
            optional_words: true,
            authorize_typos: true,
            words_limit: None,
            exact_words: index.exact_words(rtxn)?,
        })
    }

    /// if `optional_words` is set to `false` the query tree will be
    /// generated forcing all query words to be present in each matching documents
    /// (the criterion `words` will be ignored).
    /// default value if not called: `true`
    pub fn optional_words(&mut self, optional_words: bool) -> &mut Self {
        self.optional_words = optional_words;
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
    /// - if `optional_words` is set to `false` the query tree will be
    ///   generated forcing all query words to be present in each matching documents
    ///   (the criterion `words` will be ignored)
    /// - if `authorize_typos` is set to `false` the query tree will be generated
    ///   forcing all query words to match documents without any typo
    ///   (the criterion `typo` will be ignored)
    pub fn build<A: AsRef<[u8]>>(
        &self,
        query: ClassifiedTokenIter<A>,
    ) -> Result<Option<(Operation, PrimitiveQuery, MatchingWords)>> {
        let stop_words = self.index.stop_words(self.rtxn)?;
        let primitive_query = create_primitive_query(query, stop_words, self.words_limit);
        if !primitive_query.is_empty() {
            let qt = create_query_tree(
                self,
                self.optional_words,
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

/// Split the word depending on the frequency of subwords in the database documents.
fn split_best_frequency<'a>(
    ctx: &impl Context,
    word: &'a str,
) -> heed::Result<Option<(&'a str, &'a str)>> {
    let chars = word.char_indices().skip(1);
    let mut best = None;

    for (i, _) in chars {
        let (left, right) = word.split_at(i);

        let left_freq = ctx.word_documents_count(left)?.unwrap_or(0);
        let right_freq = ctx.word_documents_count(right)?.unwrap_or(0);

        let min_freq = cmp::min(left_freq, right_freq);
        if min_freq != 0 && best.map_or(true, |(old, _, _)| min_freq > old) {
            best = Some((min_freq, left, right));
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
fn typos<'a>(word: String, authorize_typos: bool, config: TypoConfig<'a>) -> QueryKind {
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
                let words = synonym
                    .into_iter()
                    .map(|word| {
                        Operation::Query(Query { prefix: false, kind: QueryKind::exact(word) })
                    })
                    .collect();
                Operation::and(words)
            })
            .collect()
    }))
}

/// Main function that creates the final query tree from the primitive query.
fn create_query_tree(
    ctx: &impl Context,
    optional_words: bool,
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
                    children.push(Operation::Phrase(vec![left.to_string(), right.to_string()]));
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
                        let ngrams = ngrams(ctx, authorize_typos, tail)?;
                        and_op_children.push(ngrams);
                    }
                    or_op_children.push(Operation::and(and_op_children));
                }
            }
            op_children.push(Operation::or(false, or_op_children));
        }

        Ok(Operation::and(op_children))
    }

    /// Create a new branch removing the last non-phrase query parts.
    fn optional_word(
        ctx: &impl Context,
        authorize_typos: bool,
        query: PrimitiveQuery,
    ) -> Result<Operation> {
        let number_phrases = query.iter().filter(|p| p.is_phrase()).count();
        let mut operation_children = Vec::new();

        let start = number_phrases + (number_phrases == 0) as usize;
        for len in start..=query.len() {
            let mut word_count = len - number_phrases;
            let query: Vec<_> = query
                .iter()
                .filter(|p| {
                    if p.is_phrase() {
                        true
                    } else if word_count != 0 {
                        word_count -= 1;
                        true
                    } else {
                        false
                    }
                })
                .cloned()
                .collect();

            let ngrams = ngrams(ctx, authorize_typos, &query)?;
            operation_children.push(ngrams);
        }

        Ok(Operation::or(true, operation_children))
    }

    if optional_words {
        optional_word(ctx, authorize_typos, query.to_vec())
    } else {
        ngrams(ctx, authorize_typos, query)
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
        matching_words: &mut Vec<(Vec<MatchingWord>, Vec<PrimitiveWordId>)>,
        id: PrimitiveWordId,
    ) -> Result<()> {
        match part {
            // 1. try to split word in 2
            // 2. try to fetch synonyms
            PrimitiveQueryPart::Word(word, prefix) => {
                if let Some(synonyms) = ctx.synonyms(&[word.as_str()])? {
                    for synonym in synonyms {
                        let synonym = synonym
                            .into_iter()
                            .map(|syn| MatchingWord::new(syn.to_string(), 0, false))
                            .collect();
                        matching_words.push((synonym, vec![id]));
                    }
                }

                if let Some((left, right)) = split_best_frequency(ctx, &word)? {
                    let left = MatchingWord::new(left.to_string(), 0, false);
                    let right = MatchingWord::new(right.to_string(), 0, false);
                    matching_words.push((vec![left, right], vec![id]));
                }

                let (word_len_one_typo, word_len_two_typo) = ctx.min_word_len_for_typo()?;
                let exact_words = ctx.exact_words();
                let config =
                    TypoConfig { max_typos: 2, word_len_one_typo, word_len_two_typo, exact_words };

                let matching_word = match typos(word, authorize_typos, config) {
                    QueryKind::Exact { word, .. } => MatchingWord::new(word, 0, prefix),
                    QueryKind::Tolerant { typo, word } => MatchingWord::new(word, typo, prefix),
                };
                matching_words.push((vec![matching_word], vec![id]));
            }
            // create a CONSECUTIVE matchings words wrapping all word in the phrase
            PrimitiveQueryPart::Phrase(words) => {
                let ids: Vec<_> =
                    (0..words.len()).into_iter().map(|i| id + i as PrimitiveWordId).collect();
                let words =
                    words.into_iter().map(|w| MatchingWord::new(w.to_string(), 0, false)).collect();
                matching_words.push((words, ids));
            }
        }

        Ok(())
    }

    /// Create all ngrams 1..=3 generating query tree branches.
    fn ngrams(
        ctx: &impl Context,
        authorize_typos: bool,
        query: &[PrimitiveQueryPart],
        matching_words: &mut Vec<(Vec<MatchingWord>, Vec<PrimitiveWordId>)>,
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
                                    let synonym = synonym
                                        .into_iter()
                                        .map(|syn| MatchingWord::new(syn.to_string(), 0, false))
                                        .collect();
                                    matching_words.push((synonym, ids.clone()));
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
                                    MatchingWord::new(word, 0, is_prefix)
                                }
                                QueryKind::Tolerant { typo, word } => {
                                    MatchingWord::new(word, typo, is_prefix)
                                }
                            };
                            matching_words.push((vec![matching_word], ids));
                        }
                    }

                    if !is_last {
                        ngrams(ctx, authorize_typos, tail, matching_words, id + 1)?;
                    }
                }
            }
            id += sub_query.iter().map(|x| x.len() as PrimitiveWordId).sum::<PrimitiveWordId>();
        }

        Ok(())
    }

    let mut matching_words = Vec::new();
    ngrams(ctx, authorize_typos, query, &mut matching_words, 0)?;
    Ok(MatchingWords::new(matching_words))
}

pub type PrimitiveQuery = Vec<PrimitiveQueryPart>;

#[derive(Debug, Clone)]
pub enum PrimitiveQueryPart {
    Phrase(Vec<String>),
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
    query: ClassifiedTokenIter<A>,
    stop_words: Option<Set<&[u8]>>,
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
                    phrase.push(token.lemma().to_string());
                } else if peekable.peek().is_some() {
                    if !stop_words.as_ref().map_or(false, |swords| swords.contains(token.lemma())) {
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
                if !phrase.is_empty() && (quote_count > 0 || separator_kind == SeparatorKind::Hard)
                {
                    primitive_query.push(PrimitiveQueryPart::Phrase(mem::take(&mut phrase)));
                }
            }
            _ => (),
        }
    }

    // If a quote is never closed, we consider all of the end of the query as a phrase.
    if !phrase.is_empty() {
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
            optional_words: bool,
            authorize_typos: bool,
            words_limit: Option<usize>,
            query: ClassifiedTokenIter<A>,
        ) -> Result<Option<(Operation, PrimitiveQuery)>> {
            let primitive_query = create_primitive_query(query, None, words_limit);
            if !primitive_query.is_empty() {
                let qt =
                    create_query_tree(self, optional_words, authorize_typos, &primitive_query)?;
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
                    String::from("hello")      => random_postings(rng,   1500),
                    String::from("hi")         => random_postings(rng,   4000),
                    String::from("word")       => random_postings(rng,   2500),
                    String::from("split")      => random_postings(rng,    400),
                    String::from("ngrams")     => random_postings(rng,   1400),
                    String::from("world")      => random_postings(rng, 15_000),
                    String::from("earth")      => random_postings(rng,   8000),
                    String::from("2021")       => random_postings(rng,    100),
                    String::from("2020")       => random_postings(rng,    500),
                    String::from("is")         => random_postings(rng, 50_000),
                    String::from("this")       => random_postings(rng, 50_000),
                    String::from("good")       => random_postings(rng,   1250),
                    String::from("morning")    => random_postings(rng,    125),
                },
                exact_words,
            }
        }
    }

    #[test]
    fn prefix() {
        let query = "hey friends";
        let tokens = query.tokenize();

        let expected = Operation::Or(
            false,
            vec![
                Operation::And(vec![
                    Operation::Query(Query {
                        prefix: false,
                        kind: QueryKind::exact("hey".to_string()),
                    }),
                    Operation::Query(Query {
                        prefix: true,
                        kind: QueryKind::tolerant(1, "friends".to_string()),
                    }),
                ]),
                Operation::Query(Query {
                    prefix: true,
                    kind: QueryKind::tolerant(1, "heyfriends".to_string()),
                }),
            ],
        );

        let (query_tree, _) =
            TestContext::default().build(false, true, None, tokens).unwrap().unwrap();

        assert_eq!(expected, query_tree);
    }

    #[test]
    fn no_prefix() {
        let query = "hey friends ";
        let tokens = query.tokenize();

        let expected = Operation::Or(
            false,
            vec![
                Operation::And(vec![
                    Operation::Query(Query {
                        prefix: false,
                        kind: QueryKind::exact("hey".to_string()),
                    }),
                    Operation::Query(Query {
                        prefix: false,
                        kind: QueryKind::tolerant(1, "friends".to_string()),
                    }),
                ]),
                Operation::Query(Query {
                    prefix: false,
                    kind: QueryKind::tolerant(1, "heyfriends".to_string()),
                }),
            ],
        );

        let (query_tree, _) =
            TestContext::default().build(false, true, None, tokens).unwrap().unwrap();

        assert_eq!(expected, query_tree);
    }

    #[test]
    fn synonyms() {
        let query = "hello world ";
        let tokens = query.tokenize();

        let expected = Operation::Or(
            false,
            vec![
                Operation::And(vec![
                    Operation::Or(
                        false,
                        vec![
                            Operation::Query(Query {
                                prefix: false,
                                kind: QueryKind::exact("hi".to_string()),
                            }),
                            Operation::And(vec![
                                Operation::Query(Query {
                                    prefix: false,
                                    kind: QueryKind::exact("good".to_string()),
                                }),
                                Operation::Query(Query {
                                    prefix: false,
                                    kind: QueryKind::exact("morning".to_string()),
                                }),
                            ]),
                            Operation::Query(Query {
                                prefix: false,
                                kind: QueryKind::tolerant(1, "hello".to_string()),
                            }),
                        ],
                    ),
                    Operation::Or(
                        false,
                        vec![
                            Operation::Query(Query {
                                prefix: false,
                                kind: QueryKind::exact("earth".to_string()),
                            }),
                            Operation::Query(Query {
                                prefix: false,
                                kind: QueryKind::exact("nature".to_string()),
                            }),
                            Operation::Query(Query {
                                prefix: false,
                                kind: QueryKind::tolerant(1, "world".to_string()),
                            }),
                        ],
                    ),
                ]),
                Operation::Query(Query {
                    prefix: false,
                    kind: QueryKind::tolerant(1, "helloworld".to_string()),
                }),
            ],
        );

        let (query_tree, _) =
            TestContext::default().build(false, true, None, tokens).unwrap().unwrap();

        assert_eq!(expected, query_tree);
    }

    #[test]
    fn complex_synonyms() {
        let query = "new york city ";
        let tokens = query.tokenize();

        let expected = Operation::Or(
            false,
            vec![
                Operation::And(vec![
                    Operation::Query(Query {
                        prefix: false,
                        kind: QueryKind::exact("new".to_string()),
                    }),
                    Operation::Or(
                        false,
                        vec![
                            Operation::And(vec![
                                Operation::Query(Query {
                                    prefix: false,
                                    kind: QueryKind::exact("york".to_string()),
                                }),
                                Operation::Query(Query {
                                    prefix: false,
                                    kind: QueryKind::exact("city".to_string()),
                                }),
                            ]),
                            Operation::Query(Query {
                                prefix: false,
                                kind: QueryKind::tolerant(1, "yorkcity".to_string()),
                            }),
                        ],
                    ),
                ]),
                Operation::And(vec![
                    Operation::Or(
                        false,
                        vec![
                            Operation::Query(Query {
                                prefix: false,
                                kind: QueryKind::exact("nyc".to_string()),
                            }),
                            Operation::And(vec![
                                Operation::Query(Query {
                                    prefix: false,
                                    kind: QueryKind::exact("new".to_string()),
                                }),
                                Operation::Query(Query {
                                    prefix: false,
                                    kind: QueryKind::exact("york".to_string()),
                                }),
                                Operation::Query(Query {
                                    prefix: false,
                                    kind: QueryKind::exact("city".to_string()),
                                }),
                            ]),
                            Operation::Query(Query {
                                prefix: false,
                                kind: QueryKind::tolerant(1, "newyork".to_string()),
                            }),
                        ],
                    ),
                    Operation::Query(Query {
                        prefix: false,
                        kind: QueryKind::exact("city".to_string()),
                    }),
                ]),
                Operation::Or(
                    false,
                    vec![
                        Operation::Query(Query {
                            prefix: false,
                            kind: QueryKind::exact("nyc".to_string()),
                        }),
                        Operation::And(vec![
                            Operation::Query(Query {
                                prefix: false,
                                kind: QueryKind::exact("new".to_string()),
                            }),
                            Operation::Query(Query {
                                prefix: false,
                                kind: QueryKind::exact("york".to_string()),
                            }),
                        ]),
                        Operation::Query(Query {
                            prefix: false,
                            kind: QueryKind::tolerant(1, "newyorkcity".to_string()),
                        }),
                    ],
                ),
            ],
        );

        let (query_tree, _) =
            TestContext::default().build(false, true, None, tokens).unwrap().unwrap();

        assert_eq!(expected, query_tree);
    }

    #[test]
    fn ngrams() {
        let query = "n grams ";
        let tokens = query.tokenize();

        let expected = Operation::Or(
            false,
            vec![
                Operation::And(vec![
                    Operation::Query(Query {
                        prefix: false,
                        kind: QueryKind::exact("n".to_string()),
                    }),
                    Operation::Query(Query {
                        prefix: false,
                        kind: QueryKind::tolerant(1, "grams".to_string()),
                    }),
                ]),
                Operation::Query(Query {
                    prefix: false,
                    kind: QueryKind::tolerant(1, "ngrams".to_string()),
                }),
            ],
        );

        let (query_tree, _) =
            TestContext::default().build(false, true, None, tokens).unwrap().unwrap();

        assert_eq!(expected, query_tree);
    }

    #[test]
    fn word_split() {
        let query = "wordsplit fish ";
        let tokens = query.tokenize();

        let expected = Operation::Or(
            false,
            vec![
                Operation::And(vec![
                    Operation::Or(
                        false,
                        vec![
                            Operation::Phrase(vec!["word".to_string(), "split".to_string()]),
                            Operation::Query(Query {
                                prefix: false,
                                kind: QueryKind::tolerant(2, "wordsplit".to_string()),
                            }),
                        ],
                    ),
                    Operation::Query(Query {
                        prefix: false,
                        kind: QueryKind::exact("fish".to_string()),
                    }),
                ]),
                Operation::Query(Query {
                    prefix: false,
                    kind: QueryKind::tolerant(1, "wordsplitfish".to_string()),
                }),
            ],
        );

        let (query_tree, _) =
            TestContext::default().build(false, true, None, tokens).unwrap().unwrap();

        assert_eq!(expected, query_tree);
    }

    #[test]
    fn phrase() {
        let query = "\"hey friends\" \" \" \"wooop";
        let tokens = query.tokenize();

        let expected = Operation::And(vec![
            Operation::Phrase(vec!["hey".to_string(), "friends".to_string()]),
            Operation::Query(Query { prefix: false, kind: QueryKind::exact("wooop".to_string()) }),
        ]);

        let (query_tree, _) =
            TestContext::default().build(false, true, None, tokens).unwrap().unwrap();

        assert_eq!(expected, query_tree);
    }

    #[test]
    fn phrase_with_hard_separator() {
        let query = "\"hey friends. wooop wooop\"";
        let tokens = query.tokenize();

        let expected = Operation::And(vec![
            Operation::Phrase(vec!["hey".to_string(), "friends".to_string()]),
            Operation::Phrase(vec!["wooop".to_string(), "wooop".to_string()]),
        ]);

        let (query_tree, _) =
            TestContext::default().build(false, true, None, tokens).unwrap().unwrap();

        assert_eq!(expected, query_tree);
    }

    #[test]
    fn optional_word() {
        let query = "hey my friend ";
        let tokens = query.tokenize();

        let expected = Operation::Or(
            true,
            vec![
                Operation::Query(Query {
                    prefix: false,
                    kind: QueryKind::exact("hey".to_string()),
                }),
                Operation::Or(
                    false,
                    vec![
                        Operation::And(vec![
                            Operation::Query(Query {
                                prefix: false,
                                kind: QueryKind::exact("hey".to_string()),
                            }),
                            Operation::Query(Query {
                                prefix: false,
                                kind: QueryKind::exact("my".to_string()),
                            }),
                        ]),
                        Operation::Query(Query {
                            prefix: false,
                            kind: QueryKind::tolerant(1, "heymy".to_string()),
                        }),
                    ],
                ),
                Operation::Or(
                    false,
                    vec![
                        Operation::And(vec![
                            Operation::Query(Query {
                                prefix: false,
                                kind: QueryKind::exact("hey".to_string()),
                            }),
                            Operation::Or(
                                false,
                                vec![
                                    Operation::And(vec![
                                        Operation::Query(Query {
                                            prefix: false,
                                            kind: QueryKind::exact("my".to_string()),
                                        }),
                                        Operation::Query(Query {
                                            prefix: false,
                                            kind: QueryKind::tolerant(1, "friend".to_string()),
                                        }),
                                    ]),
                                    Operation::Query(Query {
                                        prefix: false,
                                        kind: QueryKind::tolerant(1, "myfriend".to_string()),
                                    }),
                                ],
                            ),
                        ]),
                        Operation::And(vec![
                            Operation::Query(Query {
                                prefix: false,
                                kind: QueryKind::tolerant(1, "heymy".to_string()),
                            }),
                            Operation::Query(Query {
                                prefix: false,
                                kind: QueryKind::tolerant(1, "friend".to_string()),
                            }),
                        ]),
                        Operation::Query(Query {
                            prefix: false,
                            kind: QueryKind::tolerant(1, "heymyfriend".to_string()),
                        }),
                    ],
                ),
            ],
        );
        let (query_tree, _) =
            TestContext::default().build(true, true, None, tokens).unwrap().unwrap();

        assert_eq!(expected, query_tree);
    }

    #[test]
    fn optional_word_phrase() {
        let query = "\"hey my\"";
        let tokens = query.tokenize();

        let expected = Operation::Phrase(vec!["hey".to_string(), "my".to_string()]);
        let (query_tree, _) =
            TestContext::default().build(true, true, None, tokens).unwrap().unwrap();

        assert_eq!(expected, query_tree);
    }

    #[test]
    fn optional_word_multiple_phrases() {
        let query = r#""hey" my good "friend""#;
        let tokens = query.tokenize();

        let expected = Operation::Or(
            true,
            vec![
                Operation::And(vec![
                    Operation::Query(Query {
                        prefix: false,
                        kind: QueryKind::exact("hey".to_string()),
                    }),
                    Operation::Query(Query {
                        prefix: false,
                        kind: QueryKind::exact("friend".to_string()),
                    }),
                ]),
                Operation::And(vec![
                    Operation::Query(Query {
                        prefix: false,
                        kind: QueryKind::exact("hey".to_string()),
                    }),
                    Operation::Query(Query {
                        prefix: false,
                        kind: QueryKind::exact("my".to_string()),
                    }),
                    Operation::Query(Query {
                        prefix: false,
                        kind: QueryKind::exact("friend".to_string()),
                    }),
                ]),
                Operation::And(vec![
                    Operation::Query(Query {
                        prefix: false,
                        kind: QueryKind::exact("hey".to_string()),
                    }),
                    Operation::Or(
                        false,
                        vec![
                            Operation::And(vec![
                                Operation::Query(Query {
                                    prefix: false,
                                    kind: QueryKind::exact("my".to_string()),
                                }),
                                Operation::Query(Query {
                                    prefix: false,
                                    kind: QueryKind::exact("good".to_string()),
                                }),
                            ]),
                            Operation::Query(Query {
                                prefix: false,
                                kind: QueryKind::tolerant(1, "mygood".to_string()),
                            }),
                        ],
                    ),
                    Operation::Query(Query {
                        prefix: false,
                        kind: QueryKind::exact("friend".to_string()),
                    }),
                ]),
            ],
        );
        let (query_tree, _) =
            TestContext::default().build(true, true, None, tokens).unwrap().unwrap();

        assert_eq!(expected, query_tree);
    }

    #[test]
    fn no_typo() {
        let query = "hey friends ";
        let tokens = query.tokenize();

        let expected = Operation::Or(
            false,
            vec![
                Operation::And(vec![
                    Operation::Query(Query {
                        prefix: false,
                        kind: QueryKind::exact("hey".to_string()),
                    }),
                    Operation::Query(Query {
                        prefix: false,
                        kind: QueryKind::exact("friends".to_string()),
                    }),
                ]),
                Operation::Query(Query {
                    prefix: false,
                    kind: QueryKind::exact("heyfriends".to_string()),
                }),
            ],
        );
        let (query_tree, _) =
            TestContext::default().build(false, false, None, tokens).unwrap().unwrap();

        assert_eq!(expected, query_tree);
    }

    #[test]
    fn words_limit() {
        let query = "\"hey my\" good friend";
        let tokens = query.tokenize();

        let expected = Operation::And(vec![
            Operation::Phrase(vec!["hey".to_string(), "my".to_string()]),
            Operation::Query(Query { prefix: false, kind: QueryKind::exact("good".to_string()) }),
        ]);

        let (query_tree, _) =
            TestContext::default().build(false, false, Some(2), tokens).unwrap().unwrap();

        assert_eq!(expected, query_tree);
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
    fn disable_typo_on_word() {
        let query = "goodbye";
        let tokens = query.tokenize();

        let exact_words = fst::Set::from_iter(Some("goodbye")).unwrap().into_fst().into_inner();
        let exact_words = Some(fst::Set::new(exact_words).unwrap().map_data(Cow::Owned).unwrap());
        let context = TestContext { exact_words, ..Default::default() };
        let (query_tree, _) = context.build(false, true, Some(2), tokens).unwrap().unwrap();

        assert!(matches!(
            query_tree,
            Operation::Query(Query { prefix: true, kind: QueryKind::Exact { .. } })
        ));
    }
}
