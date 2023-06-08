use std::cmp::Ordering;

use serde::Serialize;

use crate::distance_between_two_points;

#[derive(Debug, Clone, PartialEq)]
pub enum ScoreDetails {
    Words(Words),
    Typo(Typo),
    Proximity(Rank),
    Fid(Rank),
    Position(Rank),
    ExactAttribute(ExactAttribute),
    Exactness(Rank),
    Sort(Sort),
    GeoSort(GeoSort),
}

impl PartialOrd for ScoreDetails {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        use ScoreDetails::*;
        match (self, other) {
            // matching left and right hands => defer to sub impl
            (Words(left), Words(right)) => left.partial_cmp(right),
            (Typo(left), Typo(right)) => left.partial_cmp(right),
            (Proximity(left), Proximity(right)) => left.partial_cmp(right),
            (Fid(left), Fid(right)) => left.partial_cmp(right),
            (Position(left), Position(right)) => left.partial_cmp(right),
            (ExactAttribute(left), ExactAttribute(right)) => left.partial_cmp(right),
            (Exactness(left), Exactness(right)) => left.partial_cmp(right),
            (Sort(left), Sort(right)) => left.partial_cmp(right),
            (GeoSort(left), GeoSort(right)) => left.partial_cmp(right),
            // non matching left and right hands => None
            // written this way rather than with a single `_` arm, so that adding a new variant
            // still results in a compile error
            (Words(_), _) => None,
            (Typo(_), _) => None,
            (Proximity(_), _) => None,
            (Fid(_), _) => None,
            (Position(_), _) => None,
            (ExactAttribute(_), _) => None,
            (Exactness(_), _) => None,
            (Sort(_), _) => None,
            (GeoSort(_), _) => None,
        }
    }
}

impl ScoreDetails {
    pub fn local_score(&self) -> Option<f64> {
        self.rank().map(Rank::local_score)
    }

    pub fn rank(&self) -> Option<Rank> {
        match self {
            ScoreDetails::Words(details) => Some(details.rank()),
            ScoreDetails::Typo(details) => Some(details.rank()),
            ScoreDetails::Proximity(details) => Some(*details),
            ScoreDetails::Fid(details) => Some(*details),
            ScoreDetails::Position(details) => Some(*details),
            ScoreDetails::ExactAttribute(details) => Some(details.rank()),
            ScoreDetails::Exactness(details) => Some(*details),
            ScoreDetails::Sort(_) => None,
            ScoreDetails::GeoSort(_) => None,
        }
    }

    pub fn global_score<'a>(details: impl Iterator<Item = &'a Self>) -> f64 {
        Rank::global_score(details.filter_map(Self::rank))
    }

    pub fn global_score_linear_scale<'a>(details: impl Iterator<Item = &'a Self>) -> u64 {
        (Self::global_score(details) * LINEAR_SCALE_FACTOR).round() as u64
    }

    /// Panics
    ///
    /// - If Position is not preceded by Fid
    /// - If Exactness is not preceded by ExactAttribute
    /// - If a sort fid is not contained in the passed `fields_ids_map`.
    pub fn to_json_map<'a>(
        details: impl Iterator<Item = &'a Self>,
    ) -> serde_json::Map<String, serde_json::Value> {
        let mut order = 0;
        let mut details_map = serde_json::Map::default();
        for details in details {
            match details {
                ScoreDetails::Words(words) => {
                    let words_details = serde_json::json!({
                            "order": order,
                            "matchingWords": words.matching_words,
                            "maxMatchingWords": words.max_matching_words,
                            "score": words.rank().local_score_linear_scale(),
                    });
                    details_map.insert("words".into(), words_details);
                    order += 1;
                }
                ScoreDetails::Typo(typo) => {
                    let typo_details = serde_json::json!({
                        "order": order,
                        "typoCount": typo.typo_count,
                        "maxTypoCount": typo.max_typo_count,
                        "score": typo.rank().local_score_linear_scale(),
                    });
                    details_map.insert("typo".into(), typo_details);
                    order += 1;
                }
                ScoreDetails::Proximity(proximity) => {
                    let proximity_details = serde_json::json!({
                        "order": order,
                        "score": proximity.local_score_linear_scale(),
                    });
                    details_map.insert("proximity".into(), proximity_details);
                    order += 1;
                }
                ScoreDetails::Fid(fid) => {
                    // For now, fid is a virtual rule always followed by the "position" rule
                    let fid_details = serde_json::json!({
                        "order": order,
                        "attributes_ranking_order": fid.local_score_linear_scale(),
                    });
                    details_map.insert("attribute".into(), fid_details);
                    order += 1;
                }
                ScoreDetails::Position(position) => {
                    // For now, position is a virtual rule always preceded by the "fid" rule
                    let attribute_details = details_map
                        .get_mut("attribute")
                        .expect("position not preceded by attribute");
                    let attribute_details = attribute_details
                        .as_object_mut()
                        .expect("attribute details was not an object");
                    attribute_details.insert(
                        "attributes_query_word_order".into(),
                        position.local_score_linear_scale().into(),
                    );
                    // do not update the order since this was already done by fid
                }
                ScoreDetails::ExactAttribute(exact_attribute) => {
                    let exactness_details = serde_json::json!({
                        "order": order,
                        "exactIn": exact_attribute,
                        "score": exact_attribute.rank().local_score_linear_scale(),
                    });
                    details_map.insert("exactness".into(), exactness_details);
                    order += 1;
                }
                ScoreDetails::Exactness(details) => {
                    // For now, exactness is a virtual rule always preceded by the "ExactAttribute" rule
                    let exactness_details = details_map
                        .get_mut("exactness")
                        .expect("Exactness not preceded by exactAttribute");
                    let exactness_details = exactness_details
                        .as_object_mut()
                        .expect("exactness details was not an object");
                    if exactness_details.get("exactIn").expect("missing 'exactIn'")
                        == &serde_json::json!(ExactAttribute::NoExactMatch)
                    {
                        let score = Rank::global_score_linear_scale(
                            [ExactAttribute::NoExactMatch.rank(), *details].iter().copied(),
                        );
                        *exactness_details.get_mut("score").expect("missing score") = score.into();
                    }
                    // do not update the order since this was already done by exactAttribute
                }
                ScoreDetails::Sort(details) => {
                    let sort = format!(
                        "{}:{}",
                        details.field_name,
                        if details.ascending { "asc" } else { "desc" }
                    );
                    let sort_details = serde_json::json!({
                        "order": order,
                        "value": details.value,
                    });
                    details_map.insert(sort, sort_details);
                    order += 1;
                }
                ScoreDetails::GeoSort(details) => {
                    let sort = format!(
                        "_geoPoint({}, {}):{}",
                        details.target_point[0],
                        details.target_point[1],
                        if details.ascending { "asc" } else { "desc" }
                    );
                    let point = if let Some(value) = details.value {
                        serde_json::json!({ "lat": value[0], "lng": value[1]})
                    } else {
                        serde_json::Value::Null
                    };
                    let sort_details = serde_json::json!({
                        "order": order,
                        "value": point,
                        "distance": details.distance(),
                    });
                    details_map.insert(sort, sort_details);
                    order += 1;
                }
            }
        }
        details_map
    }

    pub fn partial_cmp_iter<'a>(
        mut left: impl Iterator<Item = &'a Self>,
        mut right: impl Iterator<Item = &'a Self>,
    ) -> Result<Ordering, NotComparable> {
        let mut index = 0;
        let mut order = match (left.next(), right.next()) {
            (Some(left), Some(right)) => left.partial_cmp(right).incomparable(index)?,
            _ => return Ok(Ordering::Equal),
        };
        for (left, right) in left.zip(right) {
            index += 1;
            order = order.then(left.partial_cmp(right).incomparable(index)?);
        }
        Ok(order)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct NotComparable(pub usize);

trait OptionToNotComparable<T> {
    fn incomparable(self, index: usize) -> Result<T, NotComparable>;
}

impl<T> OptionToNotComparable<T> for Option<T> {
    fn incomparable(self, index: usize) -> Result<T, NotComparable> {
        match self {
            Some(t) => Ok(t),
            None => Err(NotComparable(index)),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Words {
    pub matching_words: u32,
    pub max_matching_words: u32,
}

impl PartialOrd for Words {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        (self.max_matching_words == other.max_matching_words)
            .then(|| self.matching_words.cmp(&other.matching_words))
    }
}

impl Words {
    pub fn rank(&self) -> Rank {
        Rank { rank: self.matching_words, max_rank: self.max_matching_words }
    }

    pub(crate) fn from_rank(rank: Rank) -> Words {
        Words { matching_words: rank.rank, max_matching_words: rank.max_rank }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Typo {
    pub typo_count: u32,
    pub max_typo_count: u32,
}

impl PartialOrd for Typo {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        (self.max_typo_count == other.max_typo_count).then(|| {
            // the order is reverted as having fewer typos gives a better score
            self.typo_count.cmp(&other.typo_count).reverse()
        })
    }
}

impl Typo {
    pub fn rank(&self) -> Rank {
        Rank {
            rank: self.max_typo_count - self.typo_count + 1,
            max_rank: (self.max_typo_count + 1),
        }
    }

    // max_rank = max_typo + 1
    // max_typo = max_rank - 1
    //
    // rank = max_typo - typo + 1
    // rank = max_rank - 1 - typo + 1
    // rank + typo = max_rank
    // typo = max_rank - rank
    pub fn from_rank(rank: Rank) -> Typo {
        Typo { typo_count: rank.max_rank - rank.rank, max_typo_count: rank.max_rank - 1 }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Rank {
    /// The ordinal rank, such that `max_rank` is the first rank, and 0 is the last rank.
    ///
    /// The higher the better. Documents with a rank of 0 have a score of 0 and are typically never returned
    /// (they don't match the query).
    pub rank: u32,
    /// The maximum possible rank. Documents with this rank have a score of 1.
    ///
    /// The max rank should not be 0.
    pub max_rank: u32,
}

impl PartialOrd for Rank {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        (self.max_rank == other.max_rank).then(|| self.rank.cmp(&other.rank))
    }
}

impl Rank {
    pub fn local_score(self) -> f64 {
        self.rank as f64 / self.max_rank as f64
    }

    pub fn local_score_linear_scale(self) -> u64 {
        (self.local_score() * LINEAR_SCALE_FACTOR).round() as u64
    }

    pub fn global_score(details: impl Iterator<Item = Self>) -> f64 {
        let mut rank = Rank { rank: 1, max_rank: 1 };
        for inner_rank in details {
            rank.rank -= 1;

            rank.rank *= inner_rank.max_rank;
            rank.max_rank *= inner_rank.max_rank;

            rank.rank += inner_rank.rank;
        }
        rank.local_score()
    }

    pub fn global_score_linear_scale(details: impl Iterator<Item = Self>) -> u64 {
        (Self::global_score(details) * LINEAR_SCALE_FACTOR).round() as u64
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum ExactAttribute {
    // Do not reorder as the order is significant, from least relevant to most relevant
    NoExactMatch,
    MatchesStart,
    MatchesFull,
}

impl ExactAttribute {
    pub fn rank(&self) -> Rank {
        let rank = match self {
            ExactAttribute::MatchesFull => 3,
            ExactAttribute::MatchesStart => 2,
            ExactAttribute::NoExactMatch => 1,
        };
        Rank { rank, max_rank: 3 }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Sort {
    pub field_name: String,
    pub ascending: bool,
    pub value: serde_json::Value,
}

impl PartialOrd for Sort {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        if self.field_name != other.field_name {
            return None;
        }
        if self.ascending != other.ascending {
            return None;
        }
        match (&self.value, &other.value) {
            (serde_json::Value::Null, serde_json::Value::Null) => Some(Ordering::Equal),
            (serde_json::Value::Null, _) => Some(Ordering::Less),
            (_, serde_json::Value::Null) => Some(Ordering::Greater),
            // numbers are always before strings
            (serde_json::Value::Number(_), serde_json::Value::String(_)) => Some(Ordering::Greater),
            (serde_json::Value::String(_), serde_json::Value::Number(_)) => Some(Ordering::Less),
            (serde_json::Value::Number(left), serde_json::Value::Number(right)) => {
                //FIXME: unwrap permitted here?
                let order = left.as_f64().unwrap().partial_cmp(&right.as_f64().unwrap())?;
                // always reverted, as bigger is better
                Some(if self.ascending { order.reverse() } else { order })
            }
            (serde_json::Value::String(left), serde_json::Value::String(right)) => {
                let order = left.cmp(right);
                Some(if self.ascending { order.reverse() } else { order })
            }
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct GeoSort {
    pub target_point: [f64; 2],
    pub ascending: bool,
    pub value: Option<[f64; 2]>,
}

impl PartialOrd for GeoSort {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        if self.target_point != other.target_point {
            return None;
        }
        if self.ascending != other.ascending {
            return None;
        }
        Some(match (self.distance(), other.distance()) {
            (None, None) => Ordering::Equal,
            (None, Some(_)) => Ordering::Less,
            (Some(_), None) => Ordering::Greater,
            (Some(left), Some(right)) => {
                let order = left.partial_cmp(&right)?;
                if self.ascending {
                    // when ascending, the one with the smallest distance has the best score
                    order.reverse()
                } else {
                    order
                }
            }
        })
    }
}

impl GeoSort {
    pub fn distance(&self) -> Option<f64> {
        self.value.map(|value| distance_between_two_points(&self.target_point, &value))
    }
}

const LINEAR_SCALE_FACTOR: f64 = 1000.0;

#[cfg(test)]
mod test {
    use super::*;
    #[test]
    fn compare() {
        let left = [
            ScoreDetails::Words(Words { matching_words: 3, max_matching_words: 4 }),
            ScoreDetails::Sort(Sort {
                field_name: "doggo".into(),
                ascending: true,
                value: "Intel the Beagle".into(),
            }),
        ];
        let right = [
            ScoreDetails::Words(Words { matching_words: 3, max_matching_words: 4 }),
            ScoreDetails::Sort(Sort {
                field_name: "doggo".into(),
                ascending: true,
                value: "Max the Labrador".into(),
            }),
        ];
        assert_eq!(
            Ok(Ordering::Greater),
            ScoreDetails::partial_cmp_iter(left.iter(), right.iter())
        );
        // equal when all the common components are equal
        assert_eq!(
            Ok(Ordering::Equal),
            ScoreDetails::partial_cmp_iter(left[0..1].iter(), right.iter())
        );

        let right = [
            ScoreDetails::Words(Words { matching_words: 4, max_matching_words: 4 }),
            ScoreDetails::Sort(Sort {
                field_name: "doggo".into(),
                ascending: true,
                value: "Max the Labrador".into(),
            }),
        ];

        assert_eq!(Ok(Ordering::Less), ScoreDetails::partial_cmp_iter(left.iter(), right.iter()));
    }

    #[test]
    fn sort_not_comparable() {
        let left = [
            ScoreDetails::Words(Words { matching_words: 3, max_matching_words: 4 }),
            ScoreDetails::Sort(Sort {
                // not the same field name
                field_name: "catto".into(),
                ascending: true,
                value: "Sylver the cat".into(),
            }),
        ];
        let right = [
            ScoreDetails::Words(Words { matching_words: 3, max_matching_words: 4 }),
            ScoreDetails::Sort(Sort {
                field_name: "doggo".into(),
                ascending: true,
                value: "Max the Labrador".into(),
            }),
        ];
        assert_eq!(
            Err(NotComparable(1)),
            ScoreDetails::partial_cmp_iter(left.iter(), right.iter())
        );
        let left = [
            ScoreDetails::Words(Words { matching_words: 3, max_matching_words: 4 }),
            ScoreDetails::Sort(Sort {
                field_name: "doggo".into(),
                // Not the same order
                ascending: false,
                value: "Intel the Beagle".into(),
            }),
        ];
        let right = [
            ScoreDetails::Words(Words { matching_words: 3, max_matching_words: 4 }),
            ScoreDetails::Sort(Sort {
                field_name: "doggo".into(),
                ascending: true,
                value: "Max the Labrador".into(),
            }),
        ];
        assert_eq!(
            Err(NotComparable(1)),
            ScoreDetails::partial_cmp_iter(left.iter(), right.iter())
        );
    }

    #[test]
    fn sort_behavior() {
        let left = Sort { field_name: "price".into(), ascending: true, value: "5400".into() };
        let right = Sort { field_name: "price".into(), ascending: true, value: 53.into() };
        // number always better match than strings
        assert_eq!(Some(Ordering::Less), left.partial_cmp(&right));

        let left = Sort { field_name: "price".into(), ascending: false, value: "5400".into() };
        let right = Sort { field_name: "price".into(), ascending: false, value: 53.into() };
        // true regardless of the sort direction
        assert_eq!(Some(Ordering::Less), left.partial_cmp(&right));
    }
}
