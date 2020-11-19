use std::borrow::Cow;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::ops::Range;
use std::time::Instant;
use std::{cmp, fmt, iter::once};

use fst::{IntoStreamer, Streamer};
use itertools::{EitherOrBoth, merge_join_by};
use log::debug;
use meilisearch_tokenizer::Token;
use meilisearch_tokenizer::tokenizer::{Analyzer, AnalyzerConfig};
use sdset::{Set, SetBuf, SetOperation};

use crate::database::MainT;
use crate::{store, DocumentId, DocIndex, MResult, FstSetCow};
use crate::automaton::{build_dfa, build_prefix_dfa, build_exact_dfa};
use crate::QueryWordsMapper;

#[derive(Clone, PartialEq, Eq, Hash)]
pub enum Operation {
    And(Vec<Operation>),
    Or(Vec<Operation>),
    Query(Query),
}

impl fmt::Debug for Operation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fn pprint_tree(f: &mut fmt::Formatter<'_>, op: &Operation, depth: usize) -> fmt::Result {
            match op {
                Operation::And(children) => {
                    writeln!(f, "{:1$}AND", "", depth * 2)?;
                    children.iter().try_for_each(|c| pprint_tree(f, c, depth + 1))
                },
                Operation::Or(children) => {
                    writeln!(f, "{:1$}OR", "", depth * 2)?;
                    children.iter().try_for_each(|c| pprint_tree(f, c, depth + 1))
                },
                Operation::Query(query) => writeln!(f, "{:2$}{:?}", "", query, depth * 2),
            }
        }

        pprint_tree(f, self, 0)
    }
}

impl Operation {
    fn tolerant(id: QueryId, prefix: bool, s: &str) -> Operation {
        Operation::Query(Query { id, prefix, exact: true, kind: QueryKind::Tolerant(s.to_string()) })
    }

    fn non_tolerant(id: QueryId, prefix: bool, s: &str) -> Operation {
        Operation::Query(Query { id, prefix, exact: true, kind: QueryKind::NonTolerant(s.to_string()) })
    }

    fn phrase2(id: QueryId, prefix: bool, (left, right): (&str, &str)) -> Operation {
        let kind = QueryKind::Phrase(vec![left.to_owned(), right.to_owned()]);
        Operation::Query(Query { id, prefix, exact: true, kind })
    }
}

pub type QueryId = usize;

#[derive(Clone, Eq)]
pub struct Query {
    pub id: QueryId,
    pub prefix: bool,
    pub exact: bool,
    pub kind: QueryKind,
}

impl PartialEq for Query {
    fn eq(&self, other: &Self) -> bool {
        self.prefix == other.prefix && self.kind == other.kind
    }
}

impl Hash for Query {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.prefix.hash(state);
        self.kind.hash(state);
    }
}

#[derive(Clone, PartialEq, Eq, Hash)]
pub enum QueryKind {
    Tolerant(String),
    NonTolerant(String),
    Phrase(Vec<String>),
}

impl fmt::Debug for Query {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Query { id, prefix, kind, .. } = self;
        let prefix = if *prefix { String::from("Prefix") } else { String::default() };
        match kind {
            QueryKind::NonTolerant(word) => {
                f.debug_struct(&(prefix + "NonTolerant")).field("id", &id).field("word", &word).finish()
            },
            QueryKind::Tolerant(word) => {
                f.debug_struct(&(prefix + "Tolerant")).field("id", &id).field("word", &word).finish()
            },
            QueryKind::Phrase(words) => {
                f.debug_struct(&(prefix + "Phrase")).field("id", &id).field("words", &words).finish()
            },
        }
    }
}

#[derive(Debug, Default)]
pub struct PostingsList {
    docids: SetBuf<DocumentId>,
    matches: SetBuf<DocIndex>,
}

pub struct Context<'a> {
    pub words_set: FstSetCow<'a>,
    pub stop_words: FstSetCow<'a>,
    pub synonyms: store::Synonyms,
    pub postings_lists: store::PostingsLists,
    pub prefix_postings_lists: store::PrefixPostingsListsCache,
}

fn split_best_frequency<'a>(reader: &heed::RoTxn<MainT>, ctx: &Context, word: &'a str) -> MResult<Option<(&'a str, &'a str)>> {
    let chars = word.char_indices().skip(1);
    let mut best = None;

    for (i, _) in chars {
        let (left, right) = word.split_at(i);

        let left_freq = ctx.postings_lists
            .postings_list(reader, left.as_bytes())?
            .map(|p| p.docids.len())
            .unwrap_or(0);
        let right_freq = ctx.postings_lists
            .postings_list(reader, right.as_bytes())?
            .map(|p| p.docids.len())
            .unwrap_or(0);

        let min_freq = cmp::min(left_freq, right_freq);
        if min_freq != 0 && best.map_or(true, |(old, _, _)| min_freq > old) {
            best = Some((min_freq, left, right));
        }
    }

    Ok(best.map(|(_, l, r)| (l, r)))
}

fn fetch_synonyms(reader: &heed::RoTxn<MainT>, ctx: &Context, words: &[&str]) -> MResult<Vec<Vec<String>>> {
    let words = &words.join(" ");
    let set = ctx.synonyms.synonyms_fst(reader, words.as_bytes())?;

    let mut strings = Vec::new();
    let mut stream = set.stream();
    while let Some(input) = stream.next() {
        if let Ok(input) = std::str::from_utf8(input) {
            let alts = input.split_ascii_whitespace().map(ToOwned::to_owned).collect();
            strings.push(alts);
        }
    }

    Ok(strings)
}

fn create_operation<I, F>(iter: I, f: F) -> Operation
where I: IntoIterator<Item=Operation>,
      F: Fn(Vec<Operation>) -> Operation,
{
    let mut iter = iter.into_iter();
    match (iter.next(), iter.next()) {
        (Some(first), None) => first,
        (first, second) => f(first.into_iter().chain(second).chain(iter).collect()),
    }
}

const MAX_NGRAM: usize = 3;

fn split_query_string(s: &str) -> Vec<(usize, String)> {
    // TODO: Use global instance instead
    let analyzer = Analyzer::new(AnalyzerConfig::default());
    analyzer
        .analyze(s)
        .tokens()
        .filter(|t| !t.is_stopword())
        .enumerate()
        .map(|(i, Token { word, .. })| (i, word.to_string()))
        .collect()
}

pub fn create_query_tree(
    reader: &heed::RoTxn<MainT>,
    ctx: &Context,
    query: &str,
) -> MResult<(Operation, HashMap<QueryId, Range<usize>>)>
{
    let words = split_query_string(query);

    let mut mapper = QueryWordsMapper::new(words.iter().map(|(_, w)| w));

    fn create_inner(
        reader: &heed::RoTxn<MainT>,
        ctx: &Context,
        mapper: &mut QueryWordsMapper,
        words: &[(usize, String)],
    ) -> MResult<Vec<Operation>>
    {
        let mut alts = Vec::new();

        for ngram in 1..=MAX_NGRAM {
            if let Some(group) = words.get(..ngram) {
                let mut group_ops = Vec::new();

                let tail = &words[ngram..];
                let is_last = tail.is_empty();

                let mut group_alts = Vec::new();
                match group {
                    [(id, word)] => {
                        let mut idgen = ((id + 1) * 100)..;
                        let range = (*id)..id+1;

                        let phrase = split_best_frequency(reader, ctx, word)?
                            .map(|ws| {
                                let id = idgen.next().unwrap();
                                idgen.next().unwrap();
                                mapper.declare(range.clone(), id, &[ws.0, ws.1]);
                                Operation::phrase2(id, is_last, ws)
                            });

                        let synonyms = fetch_synonyms(reader, ctx, &[word])?
                            .into_iter()
                            .map(|alts| {
                                let exact = alts.len() == 1;
                                let id = idgen.next().unwrap();
                                mapper.declare(range.clone(), id, &alts);

                                let mut idgen = once(id).chain(&mut idgen);
                                let iter = alts.into_iter().map(|w| {
                                    let id = idgen.next().unwrap();
                                    let kind = QueryKind::NonTolerant(w);
                                    Operation::Query(Query { id, prefix: false, exact, kind })
                                });

                                create_operation(iter, Operation::And)
                            });

                        let original = Operation::tolerant(*id, is_last, word);

                        group_alts.push(original);
                        group_alts.extend(synonyms.chain(phrase));
                    },
                    words => {
                        let id = words[0].0;
                        let mut idgen = ((id + 1) * 100_usize.pow(ngram as u32))..;
                        let range = id..id+ngram;

                        let words: Vec<_> = words.iter().map(|(_, s)| s.as_str()).collect();

                        for synonym in fetch_synonyms(reader, ctx, &words)? {
                            let exact = synonym.len() == 1;
                            let id = idgen.next().unwrap();
                            mapper.declare(range.clone(), id, &synonym);

                            let mut idgen = once(id).chain(&mut idgen);
                            let synonym = synonym.into_iter().map(|s| {
                                let id = idgen.next().unwrap();
                                let kind = QueryKind::NonTolerant(s);
                                Operation::Query(Query { id, prefix: false, exact, kind })
                            });
                            group_alts.push(create_operation(synonym, Operation::And));
                        }

                        let id = idgen.next().unwrap();
                        let concat = words.concat();
                        mapper.declare(range.clone(), id, &[&concat]);
                        group_alts.push(Operation::non_tolerant(id, is_last, &concat));
                    }
                }

                group_ops.push(create_operation(group_alts, Operation::Or));

                if !tail.is_empty() {
                    let tail_ops = create_inner(reader, ctx, mapper, tail)?;
                    group_ops.push(create_operation(tail_ops, Operation::Or));
                }

                alts.push(create_operation(group_ops, Operation::And));
            }
        }

        Ok(alts)
    }

    let alternatives = create_inner(reader, ctx, &mut mapper, &words)?;
    let operation = Operation::Or(alternatives);
    let mapping = mapper.mapping();

    Ok((operation, mapping))
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PostingsKey<'o> {
    pub query: &'o Query,
    pub input: Vec<u8>,
    pub distance: u8,
    pub is_exact: bool,
}

pub type Postings<'o, 'txn> = HashMap<PostingsKey<'o>, Cow<'txn, Set<DocIndex>>>;
pub type Cache<'o, 'txn> = HashMap<&'o Operation, Cow<'txn, Set<DocumentId>>>;

pub struct QueryResult<'o, 'txn> {
    pub docids: Cow<'txn, Set<DocumentId>>,
    pub queries: Postings<'o, 'txn>,
}

pub fn traverse_query_tree<'o, 'txn>(
    reader: &'txn heed::RoTxn<MainT>,
    ctx: &Context,
    tree: &'o Operation,
) -> MResult<QueryResult<'o, 'txn>>
{
    fn execute_and<'o, 'txn>(
        reader: &'txn heed::RoTxn<MainT>,
        ctx: &Context,
        cache: &mut Cache<'o, 'txn>,
        postings: &mut Postings<'o, 'txn>,
        depth: usize,
        operations: &'o [Operation],
    ) -> MResult<Cow<'txn, Set<DocumentId>>>
    {
        debug!("{:1$}AND", "", depth * 2);

        let before = Instant::now();
        let mut results = Vec::new();

        for op in operations {
            if cache.get(op).is_none() {
                let docids = match op {
                    Operation::And(ops) => execute_and(reader, ctx, cache, postings, depth + 1, &ops)?,
                    Operation::Or(ops) => execute_or(reader, ctx, cache, postings, depth + 1, &ops)?,
                    Operation::Query(query) => execute_query(reader, ctx, postings, depth + 1, &query)?,
                };
                cache.insert(op, docids);
            }
        }

        for op in operations {
            if let Some(docids) = cache.get(op) {
                results.push(docids.as_ref());
            }
        }

        let op = sdset::multi::Intersection::new(results);
        let docids = op.into_set_buf();

        debug!("{:3$}--- AND fetched {} documents in {:.02?}", "", docids.len(), before.elapsed(), depth * 2);

        Ok(Cow::Owned(docids))
    }

    fn execute_or<'o, 'txn>(
        reader: &'txn heed::RoTxn<MainT>,
        ctx: &Context,
        cache: &mut Cache<'o, 'txn>,
        postings: &mut Postings<'o, 'txn>,
        depth: usize,
        operations: &'o [Operation],
    ) -> MResult<Cow<'txn, Set<DocumentId>>>
    {
        debug!("{:1$}OR", "", depth * 2);

        let before = Instant::now();
        let mut results = Vec::new();

        for op in operations {
            if cache.get(op).is_none() {
                let docids = match op {
                    Operation::And(ops) => execute_and(reader, ctx, cache, postings, depth + 1, &ops)?,
                    Operation::Or(ops) => execute_or(reader, ctx, cache, postings, depth + 1, &ops)?,
                    Operation::Query(query) => execute_query(reader, ctx, postings, depth + 1, &query)?,
                };
                cache.insert(op, docids);
            }
        }

        for op in operations {
            if let Some(docids) = cache.get(op) {
                results.push(docids.as_ref());
            }
        }

        let op = sdset::multi::Union::new(results);
        let docids = op.into_set_buf();

        debug!("{:3$}--- OR fetched {} documents in {:.02?}", "", docids.len(), before.elapsed(), depth * 2);

        Ok(Cow::Owned(docids))
    }

    fn execute_query<'o, 'txn>(
        reader: &'txn heed::RoTxn<MainT>,
        ctx: &Context,
        postings: &mut Postings<'o, 'txn>,
        depth: usize,
        query: &'o Query,
    ) -> MResult<Cow<'txn, Set<DocumentId>>>
    {
        let before = Instant::now();

        let Query { prefix, kind, exact, .. } = query;
        let docids: Cow<Set<_>> = match kind {
            QueryKind::Tolerant(word) => {
                if *prefix && word.len() <= 2 {
                    let prefix = {
                        let mut array = [0; 4];
                        let bytes = word.as_bytes();
                        array[..bytes.len()].copy_from_slice(bytes);
                        array
                    };

                    // We retrieve the cached postings lists for all
                    // the words that starts with this short prefix.
                    let result = ctx.prefix_postings_lists.prefix_postings_list(reader, prefix)?.unwrap_or_default();
                    let key = PostingsKey { query, input: word.clone().into_bytes(), distance: 0, is_exact: false };
                    postings.insert(key, result.matches);
                    let prefix_docids = &result.docids;

                    // We retrieve the exact postings list for the prefix,
                    // because we must consider these matches as exact.
                    let result = ctx.postings_lists.postings_list(reader, word.as_bytes())?.unwrap_or_default();
                    let key = PostingsKey { query, input: word.clone().into_bytes(), distance: 0, is_exact: true };
                    postings.insert(key, result.matches);
                    let exact_docids = &result.docids;

                    let before = Instant::now();
                    let docids = sdset::duo::Union::new(prefix_docids, exact_docids).into_set_buf();
                    debug!("{:4$}prefix docids ({} and {}) construction took {:.02?}",
                        "", prefix_docids.len(), exact_docids.len(), before.elapsed(), depth * 2);

                    Cow::Owned(docids)

                } else {
                    let dfa = if *prefix { build_prefix_dfa(word) } else { build_dfa(word) };

                    let byte = word.as_bytes()[0];
                    let mut stream = if byte == u8::max_value() {
                        ctx.words_set.search(&dfa).ge(&[byte]).into_stream()
                    } else {
                        ctx.words_set.search(&dfa).ge(&[byte]).lt(&[byte + 1]).into_stream()
                    };

                    let before = Instant::now();
                    let mut results = Vec::new();
                    while let Some(input) = stream.next() {
                        if let Some(result) = ctx.postings_lists.postings_list(reader, input)? {
                            let distance = dfa.eval(input).to_u8();
                            let is_exact = *exact && distance == 0 && input.len() == word.len();
                            results.push(result.docids);
                            let key = PostingsKey { query, input: input.to_owned(), distance, is_exact };
                            postings.insert(key, result.matches);
                        }
                    }
                    debug!("{:3$}docids retrieval ({:?}) took {:.02?}", "", results.len(), before.elapsed(), depth * 2);

                    let before = Instant::now();
                    let docids = if results.len() > 10 {
                        let cap = results.iter().map(|dis| dis.len()).sum();
                        let mut docids = Vec::with_capacity(cap);
                        for dis in results {
                            docids.extend_from_slice(&dis);
                        }
                        SetBuf::from_dirty(docids)
                    } else {
                        let sets = results.iter().map(AsRef::as_ref).collect();
                        sdset::multi::Union::new(sets).into_set_buf()
                    };
                    debug!("{:2$}docids construction took {:.02?}", "", before.elapsed(), depth * 2);

                    Cow::Owned(docids)
                }
            },
            QueryKind::NonTolerant(word) => {
                // TODO support prefix and non-prefix exact DFA
                let dfa = build_exact_dfa(word);

                let byte = word.as_bytes()[0];
                let mut stream = if byte == u8::max_value() {
                    ctx.words_set.search(&dfa).ge(&[byte]).into_stream()
                } else {
                    ctx.words_set.search(&dfa).ge(&[byte]).lt(&[byte + 1]).into_stream()
                };

                let before = Instant::now();
                let mut results = Vec::new();
                while let Some(input) = stream.next() {
                    if let Some(result) = ctx.postings_lists.postings_list(reader, input)? {
                        let distance = dfa.eval(input).to_u8();
                        results.push(result.docids);
                        let key = PostingsKey { query, input: input.to_owned(), distance, is_exact: *exact };
                        postings.insert(key, result.matches);
                    }
                }
                debug!("{:3$}docids retrieval ({:?}) took {:.02?}", "", results.len(), before.elapsed(), depth * 2);

                let before = Instant::now();
                let docids = if results.len() > 10 {
                    let cap = results.iter().map(|dis| dis.len()).sum();
                    let mut docids = Vec::with_capacity(cap);
                    for dis in results {
                        docids.extend_from_slice(&dis);
                    }
                    SetBuf::from_dirty(docids)
                } else {
                    let sets = results.iter().map(AsRef::as_ref).collect();
                    sdset::multi::Union::new(sets).into_set_buf()
                };
                debug!("{:2$}docids construction took {:.02?}", "", before.elapsed(), depth * 2);

                Cow::Owned(docids)
            },
            QueryKind::Phrase(words) => {
                // TODO support prefix and non-prefix exact DFA
                if let [first, second] = words.as_slice() {
                    let first = ctx.postings_lists.postings_list(reader, first.as_bytes())?.unwrap_or_default();
                    let second = ctx.postings_lists.postings_list(reader, second.as_bytes())?.unwrap_or_default();

                    let iter = merge_join_by(first.matches.as_slice(), second.matches.as_slice(), |a, b| {
                        let x = (a.document_id, a.attribute, (a.word_index as u32) + 1);
                        let y = (b.document_id, b.attribute, b.word_index as u32);
                        x.cmp(&y)
                    });

                    let matches: Vec<_> = iter
                        .filter_map(EitherOrBoth::both)
                        .flat_map(|(a, b)| once(*a).chain(Some(*b)))
                        .collect();

                    let before = Instant::now();
                    let mut docids: Vec<_> = matches.iter().map(|m| m.document_id).collect();
                    docids.dedup();
                    let docids = SetBuf::new(docids).unwrap();
                    debug!("{:2$}docids construction took {:.02?}", "", before.elapsed(), depth * 2);

                    let matches = Cow::Owned(SetBuf::from_dirty(matches));
                    let key = PostingsKey { query, input: vec![], distance: 0, is_exact: true };
                    postings.insert(key, matches);

                    Cow::Owned(docids)
                } else {
                    debug!("{:2$}{:?} skipped", "", words, depth * 2);
                    Cow::default()
                }
            },
        };

        debug!("{:4$}{:?} fetched {:?} documents in {:.02?}", "", query, docids.len(), before.elapsed(), depth * 2);
        Ok(docids)
    }

    let mut cache = Cache::new();
    let mut postings = Postings::new();

    let docids = match tree {
        Operation::And(ops) => execute_and(reader, ctx, &mut cache, &mut postings, 0, &ops)?,
        Operation::Or(ops) => execute_or(reader, ctx, &mut cache, &mut postings, 0, &ops)?,
        Operation::Query(query) => execute_query(reader, ctx, &mut postings, 0, &query)?,
    };

    Ok(QueryResult { docids, queries: postings })
}
