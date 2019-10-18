use crate::criterion::Criterion;
use crate::RawDocument;
use slice_group_by::GroupBy;
use std::cmp::{self, Ordering};

const MAX_DISTANCE: u16 = 8;

#[inline]
fn clone_tuple<T: Clone, U: Clone>((a, b): (&T, &U)) -> (T, U) {
    (a.clone(), b.clone())
}

fn index_proximity(lhs: u16, rhs: u16) -> u16 {
    if lhs < rhs {
        cmp::min(rhs - lhs, MAX_DISTANCE)
    } else {
        cmp::min(lhs - rhs, MAX_DISTANCE) + 1
    }
}

fn attribute_proximity((lattr, lwi): (u16, u16), (rattr, rwi): (u16, u16)) -> u16 {
    if lattr != rattr {
        return MAX_DISTANCE;
    }
    index_proximity(lwi, rwi)
}

fn min_proximity((lattr, lwi): (&[u16], &[u16]), (rattr, rwi): (&[u16], &[u16])) -> u16 {
    let mut min_prox = u16::max_value();

    for a in lattr.iter().zip(lwi) {
        for b in rattr.iter().zip(rwi) {
            let a = clone_tuple(a);
            let b = clone_tuple(b);
            min_prox = cmp::min(min_prox, attribute_proximity(a, b));
        }
    }

    min_prox
}

fn matches_proximity(
    query_index: &[u32],
    distance: &[u8],
    attribute: &[u16],
    word_index: &[u16],
) -> u16 {
    let mut query_index_groups = query_index.linear_group();
    let mut proximity = 0;
    let mut index = 0;

    let get_attr_wi = |index: usize, group_len: usize| {
        // retrieve the first distance group (with the lowest values)
        let len = distance[index..index + group_len]
            .linear_group()
            .next()
            .unwrap()
            .len();

        let rattr = &attribute[index..index + len];
        let rwi = &word_index[index..index + len];

        (rattr, rwi)
    };

    let mut last = query_index_groups.next().map(|group| {
        let attr_wi = get_attr_wi(index, group.len());
        index += group.len();
        attr_wi
    });

    // iter by windows of size 2
    while let (Some(lhs), Some(rhs)) = (last, query_index_groups.next()) {
        let attr_wi = get_attr_wi(index, rhs.len());
        proximity += min_proximity(lhs, attr_wi);
        last = Some(attr_wi);
        index += rhs.len();
    }

    proximity
}

#[derive(Debug, Clone, Copy)]
pub struct WordsProximity;

impl Criterion for WordsProximity {
    fn evaluate(&self, lhs: &RawDocument, rhs: &RawDocument) -> Ordering {
        let lhs = {
            let query_index = lhs.query_index();
            let distance = lhs.distance();
            let attribute = lhs.attribute();
            let word_index = lhs.word_index();
            matches_proximity(query_index, distance, attribute, word_index)
        };

        let rhs = {
            let query_index = rhs.query_index();
            let distance = rhs.distance();
            let attribute = rhs.attribute();
            let word_index = rhs.word_index();
            matches_proximity(query_index, distance, attribute, word_index)
        };

        lhs.cmp(&rhs)
    }

    fn name(&self) -> &str {
        "WordsProximity"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn three_different_attributes() {
        // "soup" "of the" "the day"
        //
        // { id: 0, attr: 0, attr_index: 0 }
        // { id: 1, attr: 1, attr_index: 0 }
        // { id: 2, attr: 1, attr_index: 1 }
        // { id: 2, attr: 2, attr_index: 0 }
        // { id: 3, attr: 3, attr_index: 1 }

        let query_index = &[0, 1, 2, 2, 3];
        let distance = &[0, 0, 0, 0, 0];
        let attribute = &[0, 1, 1, 2, 3];
        let word_index = &[0, 0, 1, 0, 1];

        //   soup -> of = 8
        // + of -> the  = 1
        // + the -> day = 8 (not 1)
        assert_eq!(
            matches_proximity(query_index, distance, attribute, word_index),
            17
        );
    }

    #[test]
    fn two_different_attributes() {
        // "soup day" "soup of the day"
        //
        // { id: 0, attr: 0, attr_index: 0 }
        // { id: 0, attr: 1, attr_index: 0 }
        // { id: 1, attr: 1, attr_index: 1 }
        // { id: 2, attr: 1, attr_index: 2 }
        // { id: 3, attr: 0, attr_index: 1 }
        // { id: 3, attr: 1, attr_index: 3 }

        let query_index = &[0, 0, 1, 2, 3, 3];
        let distance = &[0, 0, 0, 0, 0, 0];
        let attribute = &[0, 1, 1, 1, 0, 1];
        let word_index = &[0, 0, 1, 2, 1, 3];

        //   soup -> of = 1
        // + of -> the  = 1
        // + the -> day = 1
        assert_eq!(
            matches_proximity(query_index, distance, attribute, word_index),
            3
        );
    }
}
