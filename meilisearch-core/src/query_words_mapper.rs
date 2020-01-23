use std::collections::HashMap;
use std::iter::FromIterator;
use std::ops::Range;
use intervaltree::{Element, IntervalTree};

pub type QueryId = usize;

pub struct QueryWordsMapper {
    originals: Vec<String>,
    mappings: HashMap<QueryId, (Range<usize>, Vec<String>)>,
}

impl QueryWordsMapper {
    pub fn new<I, A>(originals: I) -> QueryWordsMapper
    where I: IntoIterator<Item = A>,
          A: ToString,
    {
        let originals = originals.into_iter().map(|s| s.to_string()).collect();
        QueryWordsMapper { originals, mappings: HashMap::new() }
    }

    pub fn declare<I, A>(&mut self, range: Range<usize>, id: QueryId, replacement: I)
    where I: IntoIterator<Item = A>,
          A: ToString,
    {
        assert!(range.len() != 0);
        assert!(self.originals.get(range.clone()).is_some());
        assert!(id >= self.originals.len());

        let replacement: Vec<_> = replacement.into_iter().map(|s| s.to_string()).collect();

        assert!(!replacement.is_empty());

        // We detect words at the end and at the front of the
        // replacement that are common with the originals:
        //
        //     x a b c d e f g
        //       ^^^/   \^^^
        //     a b x c d k j e f
        //     ^^^           ^^^
        //

        let left = &self.originals[..range.start];
        let right = &self.originals[range.end..];

        let common_left = longest_common_prefix(left, &replacement);
        let common_right = longest_common_prefix(&replacement, right);

        for i in 0..common_left {
            let range = range.start - common_left + i..range.start - common_left + i + 1;
            let replacement = vec![replacement[i].clone()];
            self.mappings.insert(id + i, (range, replacement));
        }

        {
            let replacement = replacement[common_left..replacement.len() - common_right].iter().cloned().collect();
            self.mappings.insert(id + common_left, (range.clone(), replacement));
        }

        for i in 0..common_right {
            let id = id + replacement.len() - common_right + i;
            let range = range.end + i..range.end + i + 1;
            let replacement = vec![replacement[replacement.len() - common_right + i].clone()];
            self.mappings.insert(id, (range, replacement));
        }
    }

    pub fn mapping(self) -> HashMap<QueryId, Range<usize>> {
        let mappings = self.mappings.into_iter().map(|(i, (r, v))| (r, (i, v)));
        let intervals = IntervalTree::from_iter(mappings);

        let mut output = HashMap::new();
        let mut offset = 0;

        // We map each original word to the biggest number of
        // associated words.
        for i in 0..self.originals.len() {
            let max = intervals.query_point(i)
                .filter_map(|e| {
                    if e.range.end - 1 == i {
                        let len = e.value.1.iter().skip(i - e.range.start).count();
                        if len != 0 { Some(len) } else { None }
                    } else { None }
                })
                .max()
                .unwrap_or(1);

            let range = i + offset..i + offset + max;
            output.insert(i, range);
            offset += max - 1;
        }

        // We retrieve the range that each original word
        // is mapped to and apply it to each of the words.
        for i in 0..self.originals.len() {

            let iter = intervals.query_point(i).filter(|e| e.range.end - 1 == i);
            for Element { range, value: (id, words) } in iter {

                // We ask for the complete range mapped to the area we map.
                let start = output.get(&range.start).map(|r| r.start).unwrap_or(range.start);
                let end = output.get(&(range.end - 1)).map(|r| r.end).unwrap_or(range.end);
                let range = start..end;

                // We map each query id to one word until the last,
                // we map it to the remainings words.
                let add = range.len() - words.len();
                for (j, x) in range.take(words.len()).enumerate() {
                    let add = if j == words.len() - 1 { add } else { 0 }; // is last?
                    let range = x..x + 1 + add;
                    output.insert(id + j, range);
                }
            }
        }

        output
    }
}

fn longest_common_prefix<T: Eq + std::fmt::Debug>(a: &[T], b: &[T]) -> usize {
    let mut best = None;
    for i in (0..a.len()).rev() {
        let count = a[i..].iter().zip(b).take_while(|(a, b)| a == b).count();
        best = match best {
            Some(old) if count > old => Some(count),
            Some(_) => break,
            None => Some(count),
        };
    }
    best.unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn original_unmodified() {
        let query = ["new", "york", "city", "subway"];
        //             0       1       2        3
        let mut builder = QueryWordsMapper::new(&query);

        // new york = new york city
        builder.declare(0..2, 4, &["new", "york", "city"]);
        //                    ^      4       5       6

        // new = new york city
        builder.declare(0..1, 7, &["new", "york", "city"]);
        //                    ^      7       8       9

        let mapping = builder.mapping();

        assert_eq!(mapping[&0], 0..1); // new
        assert_eq!(mapping[&1], 1..2); // york
        assert_eq!(mapping[&2], 2..3); // city
        assert_eq!(mapping[&3], 3..4); // subway

        assert_eq!(mapping[&4], 0..1); // new
        assert_eq!(mapping[&5], 1..2); // york
        assert_eq!(mapping[&6], 2..3); // city

        assert_eq!(mapping[&7], 0..1); // new
        assert_eq!(mapping[&8], 1..2); // york
        assert_eq!(mapping[&9], 2..3); // city
    }

    #[test]
    fn original_unmodified2() {
        let query = ["new", "york", "city", "subway"];
        //             0       1       2        3
        let mut builder = QueryWordsMapper::new(&query);

        // city subway = new york city underground train
        builder.declare(2..4, 4, &["new", "york", "city", "underground", "train"]);
        //                    ^      4      5       6           7           8

        let mapping = builder.mapping();

        assert_eq!(mapping[&0], 0..1); // new
        assert_eq!(mapping[&1], 1..2); // york
        assert_eq!(mapping[&2], 2..3); // city
        assert_eq!(mapping[&3], 3..5); // subway

        assert_eq!(mapping[&4], 0..1); // new
        assert_eq!(mapping[&5], 1..2); // york
        assert_eq!(mapping[&6], 2..3); // city
        assert_eq!(mapping[&7], 3..4); // underground
        assert_eq!(mapping[&8], 4..5); // train
    }

    #[test]
    fn original_unmodified3() {
        let query = ["a", "b", "x", "x", "a", "b", "c", "d", "e", "f", "g"];
        //            0    1    2    3    4    5    6    7    8    9    10
        let mut builder = QueryWordsMapper::new(&query);

        // c d = a b x c d k j e f
        builder.declare(6..8, 11, &["a", "b", "x", "c", "d", "k", "j", "e", "f"]);
        //                    ^^    11   12   13   14   15   16   17   18   19

        let mapping = builder.mapping();

        assert_eq!(mapping[&0],  0..1); // a
        assert_eq!(mapping[&1],  1..2); // b
        assert_eq!(mapping[&2],  2..3); // x
        assert_eq!(mapping[&3],  3..4); // x
        assert_eq!(mapping[&4],  4..5); // a
        assert_eq!(mapping[&5],  5..6); // b
        assert_eq!(mapping[&6],  6..7); // c
        assert_eq!(mapping[&7],  7..11); // d
        assert_eq!(mapping[&8],  11..12); // e
        assert_eq!(mapping[&9],  12..13); // f
        assert_eq!(mapping[&10], 13..14); // g

        assert_eq!(mapping[&11], 4..5); // a
        assert_eq!(mapping[&12], 5..6); // b
        assert_eq!(mapping[&13], 6..7); // x
        assert_eq!(mapping[&14], 7..8); // c
        assert_eq!(mapping[&15], 8..9); // d
        assert_eq!(mapping[&16], 9..10); // k
        assert_eq!(mapping[&17], 10..11); // j
        assert_eq!(mapping[&18], 11..12); // e
        assert_eq!(mapping[&19], 12..13); // f
    }

    #[test]
    fn simple_growing() {
        let query = ["new", "york", "subway"];
        //             0       1        2
        let mut builder = QueryWordsMapper::new(&query);

        // new york = new york city
        builder.declare(0..2, 3, &["new", "york", "city"]);
        //                    ^      3       4       5

        let mapping = builder.mapping();

        assert_eq!(mapping[&0], 0..1); // new
        assert_eq!(mapping[&1], 1..3); // york
        assert_eq!(mapping[&2], 3..4); // subway
        assert_eq!(mapping[&3], 0..1); // new
        assert_eq!(mapping[&4], 1..2); // york
        assert_eq!(mapping[&5], 2..3); // city
    }

    #[test]
    fn same_place_growings() {
        let query = ["NY", "subway"];
        //             0       1
        let mut builder = QueryWordsMapper::new(&query);

        // NY = new york
        builder.declare(0..1, 2, &["new", "york"]);
        //                    ^      2       3

        // NY = new york city
        builder.declare(0..1, 4, &["new", "york", "city"]);
        //                    ^      4       5       6

        // NY = NYC
        builder.declare(0..1, 7, &["NYC"]);
        //                    ^      7

        // NY = new york city
        builder.declare(0..1, 8, &["new", "york", "city"]);
        //                    ^      8       9      10

        // subway = underground train
        builder.declare(1..2, 11, &["underground", "train"]);
        //                    ^          11          12

        let mapping = builder.mapping();

        assert_eq!(mapping[&0], 0..3); // NY
        assert_eq!(mapping[&1], 3..5); // subway
        assert_eq!(mapping[&2], 0..1); // new
        assert_eq!(mapping[&3], 1..3); // york
        assert_eq!(mapping[&4], 0..1); // new
        assert_eq!(mapping[&5], 1..2); // york
        assert_eq!(mapping[&6], 2..3); // city
        assert_eq!(mapping[&7], 0..3); // NYC
        assert_eq!(mapping[&8], 0..1); // new
        assert_eq!(mapping[&9], 1..2); // york
        assert_eq!(mapping[&10], 2..3); // city
        assert_eq!(mapping[&11], 3..4); // underground
        assert_eq!(mapping[&12], 4..5); // train
    }

    #[test]
    fn bigger_growing() {
        let query = ["NYC", "subway"];
        //             0        1
        let mut builder = QueryWordsMapper::new(&query);

        // NYC = new york city
        builder.declare(0..1, 2, &["new", "york", "city"]);
        //                    ^      2       3       4

        let mapping = builder.mapping();

        assert_eq!(mapping[&0], 0..3); // NYC
        assert_eq!(mapping[&1], 3..4); // subway
        assert_eq!(mapping[&2], 0..1); // new
        assert_eq!(mapping[&3], 1..2); // york
        assert_eq!(mapping[&4], 2..3); // city
    }

    #[test]
    fn middle_query_growing() {
        let query = ["great", "awesome", "NYC", "subway"];
        //              0         1        2        3
        let mut builder = QueryWordsMapper::new(&query);

        // NYC = new york city
        builder.declare(2..3, 4, &["new", "york", "city"]);
        //                    ^      4       5       6

        let mapping = builder.mapping();

        assert_eq!(mapping[&0], 0..1); // great
        assert_eq!(mapping[&1], 1..2); // awesome
        assert_eq!(mapping[&2], 2..5); // NYC
        assert_eq!(mapping[&3], 5..6); // subway
        assert_eq!(mapping[&4], 2..3); // new
        assert_eq!(mapping[&5], 3..4); // york
        assert_eq!(mapping[&6], 4..5); // city
    }

    #[test]
    fn end_query_growing() {
        let query = ["NYC", "subway"];
        //             0        1
        let mut builder = QueryWordsMapper::new(&query);

        // NYC = new york city
        builder.declare(1..2, 2, &["underground", "train"]);
        //                    ^         2            3

        let mapping = builder.mapping();

        assert_eq!(mapping[&0], 0..1); // NYC
        assert_eq!(mapping[&1], 1..3); // subway
        assert_eq!(mapping[&2], 1..2); // underground
        assert_eq!(mapping[&3], 2..3); // train
    }

    #[test]
    fn multiple_growings() {
        let query = ["great", "awesome", "NYC", "subway"];
        //              0         1        2        3
        let mut builder = QueryWordsMapper::new(&query);

        // NYC = new york city
        builder.declare(2..3, 4, &["new", "york", "city"]);
        //                    ^      4       5       6

        // subway = underground train
        builder.declare(3..4, 7, &["underground", "train"]);
        //                    ^          7           8

        let mapping = builder.mapping();

        assert_eq!(mapping[&0], 0..1); // great
        assert_eq!(mapping[&1], 1..2); // awesome
        assert_eq!(mapping[&2], 2..5); // NYC
        assert_eq!(mapping[&3], 5..7); // subway
        assert_eq!(mapping[&4], 2..3); // new
        assert_eq!(mapping[&5], 3..4); // york
        assert_eq!(mapping[&6], 4..5); // city
        assert_eq!(mapping[&7], 5..6); // underground
        assert_eq!(mapping[&8], 6..7); // train
    }

    #[test]
    fn multiple_probable_growings() {
        let query = ["great", "awesome", "NYC", "subway"];
        //              0         1        2        3
        let mut builder = QueryWordsMapper::new(&query);

        // NYC = new york city
        builder.declare(2..3, 4, &["new", "york", "city"]);
        //                    ^      4       5       6

        // subway = underground train
        builder.declare(3..4, 7, &["underground", "train"]);
        //                    ^          7           8

        // great awesome = good
        builder.declare(0..2, 9, &["good"]);
        //                    ^       9

        // awesome NYC = NY
        builder.declare(1..3, 10, &["NY"]);
        //                    ^^     10

        // NYC subway = metro
        builder.declare(2..4, 11, &["metro"]);
        //                    ^^      11

        let mapping = builder.mapping();

        assert_eq!(mapping[&0], 0..1); // great
        assert_eq!(mapping[&1], 1..2); // awesome
        assert_eq!(mapping[&2], 2..5); // NYC
        assert_eq!(mapping[&3], 5..7); // subway
        assert_eq!(mapping[&4], 2..3); // new
        assert_eq!(mapping[&5], 3..4); // york
        assert_eq!(mapping[&6], 4..5); // city
        assert_eq!(mapping[&7], 5..6); // underground
        assert_eq!(mapping[&8], 6..7); // train
        assert_eq!(mapping[&9], 0..2); // good
        assert_eq!(mapping[&10], 1..5); // NY
        assert_eq!(mapping[&11], 2..7); // metro
    }
}
