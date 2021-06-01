use std::collections::HashSet;
use std::cmp::{min, Reverse};
use std::collections::BTreeMap;
use std::ops::{Index, IndexMut};

use levenshtein_automata::{DFA, Distance};

use crate::search::query_tree::{Operation, Query};

use super::build_dfa;

type IsPrefix = bool;

/// Structure created from a query tree
/// referencing words that match the given query tree.
#[derive(Default)]
pub struct MatchingWords {
    dfas: Vec<(DFA, String, u8, IsPrefix)>,
}

impl MatchingWords {
    pub fn from_query_tree(tree: &Operation) -> Self {
        // fetch matchable words from the query tree
        let mut dfas: Vec<_> = fetch_queries(tree)
            .into_iter()
            // create DFAs for each word
            .map(|(w, t, p)| (build_dfa(w, t, p), w.to_string(), t, p))
            .collect();
        // Sort word by len in DESC order prioritizing the longuest word,
        // in order to highlight the longuest part of the matched word.
        dfas.sort_unstable_by_key(|(_dfa, query_word, _typo, _is_prefix)| Reverse(query_word.len()));
        Self { dfas }
    }

    /// Returns the number of matching bytes if the word matches one of the query words.
    pub fn matching_bytes(&self, word: &str) -> Option<usize> {
        self.dfas.iter().find_map(|(dfa, query_word, typo, is_prefix)| match dfa.eval(word) {
            Distance::Exact(t) if t <= *typo => {
                if *is_prefix {
                    let (_dist, len) = prefix_damerau_levenshtein(query_word.as_bytes(), word.as_bytes());
                    Some(len)
                } else {
                    Some(word.len())
                }
            },
            _otherwise => None,
        })
    }
}

/// Lists all words which can be considered as a match for the query tree.
fn fetch_queries(tree: &Operation) -> HashSet<(&str, u8, IsPrefix)> {
    fn resolve_ops<'a>(tree: &'a Operation, out: &mut HashSet<(&'a str, u8, IsPrefix)>) {
        match tree {
            Operation::Or(_, ops) | Operation::And(ops) | Operation::Consecutive(ops) => {
                ops.as_slice().iter().for_each(|op| resolve_ops(op, out));
            },
            Operation::Query(Query { prefix, kind }) => {
                let typo = if kind.is_exact() { 0 } else { kind.typo() };
                out.insert((kind.word(), typo, *prefix));
            },
        }
    }

    let mut queries = HashSet::new();
    resolve_ops(tree, &mut queries);
    queries
}

// A simple wrapper around vec so we can get contiguous but index it like it's 2D array.
struct N2Array<T> {
    y_size: usize,
    buf: Vec<T>,
}

impl<T: Clone> N2Array<T> {
    fn new(x: usize, y: usize, value: T) -> N2Array<T> {
        N2Array {
            y_size: y,
            buf: vec![value; x * y],
        }
    }
}

impl<T> Index<(usize, usize)> for N2Array<T> {
    type Output = T;

    #[inline]
    fn index(&self, (x, y): (usize, usize)) -> &T {
        &self.buf[(x * self.y_size) + y]
    }
}

impl<T> IndexMut<(usize, usize)> for N2Array<T> {
    #[inline]
    fn index_mut(&mut self, (x, y): (usize, usize)) -> &mut T {
        &mut self.buf[(x * self.y_size) + y]
    }
}

/// Returns the distance between the source word and the target word,
/// and the number of byte matching in the target word.
fn prefix_damerau_levenshtein(source: &[u8], target: &[u8]) -> (u32, usize) {
    let (n, m) = (source.len(), target.len());

    if n == 0 {
        return (m as u32, 0);
    }
    if m == 0 {
        return (n as u32, 0);
    }

    if n == m && source == target {
        return (0, m);
    }

    let inf = n + m;
    let mut matrix = N2Array::new(n + 2, m + 2, 0);

    matrix[(0, 0)] = inf;
    for i in 0..n + 1 {
        matrix[(i + 1, 0)] = inf;
        matrix[(i + 1, 1)] = i;
    }
    for j in 0..m + 1 {
        matrix[(0, j + 1)] = inf;
        matrix[(1, j + 1)] = j;
    }

    let mut last_row = BTreeMap::new();

    for (row, char_s) in source.iter().enumerate() {
        let mut last_match_col = 0;
        let row = row + 1;

        for (col, char_t) in target.iter().enumerate() {
            let col = col + 1;
            let last_match_row = *last_row.get(&char_t).unwrap_or(&0);
            let cost = if char_s == char_t { 0 } else { 1 };

            let dist_add = matrix[(row, col + 1)] + 1;
            let dist_del = matrix[(row + 1, col)] + 1;
            let dist_sub = matrix[(row, col)] + cost;
            let dist_trans = matrix[(last_match_row, last_match_col)]
                + (row - last_match_row - 1)
                + 1
                + (col - last_match_col - 1);

            let dist = min(min(dist_add, dist_del), min(dist_sub, dist_trans));

            matrix[(row + 1, col + 1)] = dist;

            if cost == 0 {
                last_match_col = col;
            }
        }

        last_row.insert(char_s, row);
    }

    let mut minimum = (u32::max_value(), 0);

    for x in 0..=m {
        let dist = matrix[(n + 1, x + 1)] as u32;
        if dist < minimum.0 {
            minimum = (dist, x)
        }
    }

    minimum
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::MatchingWords;
    use crate::search::query_tree::{Operation, Query, QueryKind};

    #[test]
    fn matched_length() {
        let query = "Levenste";
        let text = "Levenshtein";

        let (dist, length) = prefix_damerau_levenshtein(query.as_bytes(), text.as_bytes());
        assert_eq!(dist, 1);
        assert_eq!(&text[..length], "Levenshte");
    }

    #[test]
    fn matching_words() {
        let query_tree = Operation::Or(false, vec![
            Operation::And(vec![
                Operation::Query(Query { prefix: true, kind: QueryKind::exact("split".to_string()) }),
                Operation::Query(Query { prefix: false, kind: QueryKind::exact("this".to_string()) }),
                Operation::Query(Query { prefix: true, kind: QueryKind::tolerant(1, "world".to_string()) }),
            ]),
        ]);

        let matching_words = MatchingWords::from_query_tree(&query_tree);

        assert_eq!(matching_words.matching_bytes("word"), Some(4));
        assert_eq!(matching_words.matching_bytes("nyc"), None);
        assert_eq!(matching_words.matching_bytes("world"), Some(5));
        assert_eq!(matching_words.matching_bytes("splitted"), Some(5));
        assert_eq!(matching_words.matching_bytes("thisnew"), None);
        assert_eq!(matching_words.matching_bytes("borld"), Some(5));
        assert_eq!(matching_words.matching_bytes("wordsplit"), Some(4));
    }
}
