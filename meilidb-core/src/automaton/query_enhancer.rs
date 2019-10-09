use std::ops::Range;
use std::cmp::Ordering::{Less, Greater, Equal};

/// Return `true` if the specified range can accept the given replacements words.
/// Returns `false` if the replacements words are already present in the original query
/// or if there is fewer replacement words than the range to replace.
//
//
// ## Ignored because already present in original
//
//     new york city subway
//     -------- ^^^^
//   /          \
//  [new york city]
//
//
// ## Ignored because smaller than the original
//
//   new york city subway
//   -------------
//   \          /
//    [new york]
//
//
// ## Accepted because bigger than the original
//
//        NYC subway
//        ---
//       /   \
//      /     \
//     /       \
//    /         \
//   /           \
//  [new york city]
//
fn rewrite_range_with<S, T>(query: &[S], range: Range<usize>, words: &[T]) -> bool
where S: AsRef<str>,
      T: AsRef<str>,
{
    if words.len() <= range.len() {
        // there is fewer or equal replacement words
        // than there is already in the replaced range
        return false
    }

    // retrieve the part to rewrite but with the length
    // of the replacement part
    let original = query.iter().skip(range.start).take(words.len());

    // check if the original query doesn't already contain
    // the replacement words
    !original.map(AsRef::as_ref).eq(words.iter().map(AsRef::as_ref))
}

type Origin = usize;
type RealLength = usize;

struct FakeIntervalTree {
    intervals: Vec<(Range<usize>, (Origin, RealLength))>,
}

impl FakeIntervalTree {
    fn new(mut intervals: Vec<(Range<usize>, (Origin, RealLength))>) -> FakeIntervalTree {
        intervals.sort_unstable_by_key(|(r, _)| (r.start, r.end));
        FakeIntervalTree { intervals }
    }

    fn query(&self, point: usize) -> Option<(Range<usize>, (Origin, RealLength))> {
        let element = self.intervals.binary_search_by(|(r, _)| {
            if point >= r.start {
                if point < r.end { Equal } else { Less }
            } else { Greater }
        });

        let n = match element { Ok(n) => n, Err(n) => n };

        match self.intervals.get(n) {
            Some((range, value)) if range.contains(&point) => Some((range.clone(), *value)),
            _otherwise => None,
        }
    }
}

pub struct QueryEnhancerBuilder<'a, S> {
    query: &'a [S],
    origins: Vec<usize>,
    real_to_origin: Vec<(Range<usize>, (Origin, RealLength))>,
}

impl<S: AsRef<str>> QueryEnhancerBuilder<'_, S> {
    pub fn new(query: &[S]) -> QueryEnhancerBuilder<S> {
        // we initialize origins query indices based on their positions
        let origins: Vec<_> = (0..query.len() + 1).collect();
        let real_to_origin = origins.iter().map(|&o| (o..o+1, (o, 1))).collect();

        QueryEnhancerBuilder { query, origins, real_to_origin }
    }

    /// Update the final real to origin query indices mapping.
    ///
    /// `range` is the original words range that this `replacement` words replace
    /// and `real` is the first real query index of these replacement words.
    pub fn declare<T>(&mut self, range: Range<usize>, real: usize, replacement: &[T])
    where T: AsRef<str>,
    {
        // check if the range of original words
        // can be rewritten with the replacement words
        if rewrite_range_with(self.query, range.clone(), replacement) {

            // this range can be replaced so we need to
            // modify the origins accordingly
            let offset = replacement.len() - range.len();

            let previous_padding = self.origins[range.end - 1];
            let current_offset = (self.origins[range.end] - 1) - previous_padding;
            let diff = offset.saturating_sub(current_offset);
            self.origins[range.end] += diff;

            for r in &mut self.origins[range.end + 1..] {
                *r += diff;
            }
        }

        // we need to store the real number and origins relations
        // this way it will be possible to know by how many
        // we need to pad real query indices
        let real_range = real..real + replacement.len().max(range.len());
        let real_length = replacement.len();
        self.real_to_origin.push((real_range, (range.start, real_length)));
    }

    pub fn build(self) -> QueryEnhancer {
        QueryEnhancer {
            origins: self.origins,
            real_to_origin: FakeIntervalTree::new(self.real_to_origin),
        }
    }
}

pub struct QueryEnhancer {
    origins: Vec<usize>,
    real_to_origin: FakeIntervalTree,
}

impl QueryEnhancer {
    /// Returns the query indices to use to replace this real query index.
    pub fn replacement(&self, real: u32) -> Range<u32> {
        let real = real as usize;

        // query the fake interval tree with the real query index
        let (range, (origin, real_length)) =
            self.real_to_origin
                .query(real)
                .expect("real has never been declared");

        // if `real` is the end bound of the range
        if (range.start + real_length - 1) == real {
            let mut count = range.len();
            let mut new_origin = origin;
            for (i, slice) in self.origins[new_origin..].windows(2).enumerate() {
                let len = slice[1] - slice[0];
                count = count.saturating_sub(len);
                if count == 0 { new_origin = origin + i; break }
            }

            let n = real - range.start;
            let start = self.origins[origin];
            let end = self.origins[new_origin + 1];
            let remaining = (end - start) - n;

            Range { start: (start + n) as u32, end: (start + n + remaining) as u32 }

        } else {
            // just return the origin along with
            // the real position of the word
            let n = real as usize - range.start;
            let origin = self.origins[origin];

            Range { start: (origin + n) as u32, end: (origin + n + 1) as u32 }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn original_unmodified() {
        let query = ["new", "york", "city", "subway"];
        //             0       1       2        3
        let mut builder = QueryEnhancerBuilder::new(&query);

        // new york = new york city
        builder.declare(0..2, 4, &["new", "york", "city"]);
        //                    ^      4       5       6

        let enhancer = builder.build();

        assert_eq!(enhancer.replacement(0), 0..1); // new
        assert_eq!(enhancer.replacement(1), 1..2); // york
        assert_eq!(enhancer.replacement(2), 2..3); // city
        assert_eq!(enhancer.replacement(3), 3..4); // subway
        assert_eq!(enhancer.replacement(4), 0..1); // new
        assert_eq!(enhancer.replacement(5), 1..2); // york
        assert_eq!(enhancer.replacement(6), 2..3); // city
    }

    #[test]
    fn simple_growing() {
        let query = ["new", "york", "subway"];
        //             0       1        2
        let mut builder = QueryEnhancerBuilder::new(&query);

        // new york = new york city
        builder.declare(0..2, 3, &["new", "york", "city"]);
        //                    ^      3       4       5

        let enhancer = builder.build();

        assert_eq!(enhancer.replacement(0), 0..1); // new
        assert_eq!(enhancer.replacement(1), 1..3); // york
        assert_eq!(enhancer.replacement(2), 3..4); // subway
        assert_eq!(enhancer.replacement(3), 0..1); // new
        assert_eq!(enhancer.replacement(4), 1..2); // york
        assert_eq!(enhancer.replacement(5), 2..3); // city
    }

    #[test]
    fn same_place_growings() {
        let query = ["NY", "subway"];
        //             0       1
        let mut builder = QueryEnhancerBuilder::new(&query);

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

        let enhancer = builder.build();

        assert_eq!(enhancer.replacement(0), 0..3); // NY
        assert_eq!(enhancer.replacement(1), 3..5); // subway
        assert_eq!(enhancer.replacement(2), 0..1); // new
        assert_eq!(enhancer.replacement(3), 1..3); // york
        assert_eq!(enhancer.replacement(4), 0..1); // new
        assert_eq!(enhancer.replacement(5), 1..2); // york
        assert_eq!(enhancer.replacement(6), 2..3); // city
        assert_eq!(enhancer.replacement(7), 0..3); // NYC
        assert_eq!(enhancer.replacement(8), 0..1); // new
        assert_eq!(enhancer.replacement(9), 1..2); // york
        assert_eq!(enhancer.replacement(10), 2..3); // city
        assert_eq!(enhancer.replacement(11), 3..4); // underground
        assert_eq!(enhancer.replacement(12), 4..5); // train
    }

    #[test]
    fn bigger_growing() {
        let query = ["NYC", "subway"];
        //             0        1
        let mut builder = QueryEnhancerBuilder::new(&query);

        // NYC = new york city
        builder.declare(0..1, 2, &["new", "york", "city"]);
        //                    ^      2       3       4

        let enhancer = builder.build();

        assert_eq!(enhancer.replacement(0), 0..3); // NYC
        assert_eq!(enhancer.replacement(1), 3..4); // subway
        assert_eq!(enhancer.replacement(2), 0..1); // new
        assert_eq!(enhancer.replacement(3), 1..2); // york
        assert_eq!(enhancer.replacement(4), 2..3); // city
    }

    #[test]
    fn middle_query_growing() {
        let query = ["great", "awesome", "NYC", "subway"];
        //              0         1        2        3
        let mut builder = QueryEnhancerBuilder::new(&query);

        // NYC = new york city
        builder.declare(2..3, 4, &["new", "york", "city"]);
        //                    ^      4       5       6

        let enhancer = builder.build();

        assert_eq!(enhancer.replacement(0), 0..1); // great
        assert_eq!(enhancer.replacement(1), 1..2); // awesome
        assert_eq!(enhancer.replacement(2), 2..5); // NYC
        assert_eq!(enhancer.replacement(3), 5..6); // subway
        assert_eq!(enhancer.replacement(4), 2..3); // new
        assert_eq!(enhancer.replacement(5), 3..4); // york
        assert_eq!(enhancer.replacement(6), 4..5); // city
    }

    #[test]
    fn end_query_growing() {
        let query = ["NYC", "subway"];
        //             0        1
        let mut builder = QueryEnhancerBuilder::new(&query);

        // NYC = new york city
        builder.declare(1..2, 2, &["underground", "train"]);
        //                    ^         2            3

        let enhancer = builder.build();

        assert_eq!(enhancer.replacement(0), 0..1); // NYC
        assert_eq!(enhancer.replacement(1), 1..3); // subway
        assert_eq!(enhancer.replacement(2), 1..2); // underground
        assert_eq!(enhancer.replacement(3), 2..3); // train
    }

    #[test]
    fn multiple_growings() {
        let query = ["great", "awesome", "NYC", "subway"];
        //              0         1        2        3
        let mut builder = QueryEnhancerBuilder::new(&query);

        // NYC = new york city
        builder.declare(2..3, 4, &["new", "york", "city"]);
        //                    ^      4       5       6

        // subway = underground train
        builder.declare(3..4, 7, &["underground", "train"]);
        //                    ^          7           8

        let enhancer = builder.build();

        assert_eq!(enhancer.replacement(0), 0..1); // great
        assert_eq!(enhancer.replacement(1), 1..2); // awesome
        assert_eq!(enhancer.replacement(2), 2..5); // NYC
        assert_eq!(enhancer.replacement(3), 5..7); // subway
        assert_eq!(enhancer.replacement(4), 2..3); // new
        assert_eq!(enhancer.replacement(5), 3..4); // york
        assert_eq!(enhancer.replacement(6), 4..5); // city
        assert_eq!(enhancer.replacement(7), 5..6); // underground
        assert_eq!(enhancer.replacement(8), 6..7); // train
    }

    #[test]
    fn multiple_probable_growings() {
        let query = ["great", "awesome", "NYC", "subway"];
        //              0         1        2        3
        let mut builder = QueryEnhancerBuilder::new(&query);

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

        let enhancer = builder.build();

        assert_eq!(enhancer.replacement(0),  0..1); // great
        assert_eq!(enhancer.replacement(1),  1..2); // awesome
        assert_eq!(enhancer.replacement(2),  2..5); // NYC
        assert_eq!(enhancer.replacement(3),  5..7); // subway
        assert_eq!(enhancer.replacement(4),  2..3); // new
        assert_eq!(enhancer.replacement(5),  3..4); // york
        assert_eq!(enhancer.replacement(6),  4..5); // city
        assert_eq!(enhancer.replacement(7),  5..6); // underground
        assert_eq!(enhancer.replacement(8),  6..7); // train
        assert_eq!(enhancer.replacement(9),  0..2); // good
        assert_eq!(enhancer.replacement(10), 1..5); // NY
        assert_eq!(enhancer.replacement(11), 2..5); // metro
    }
}
