use serde::Serialize;

use crate::distance_between_two_points;

#[derive(Debug, Clone, PartialEq)]
pub enum ScoreDetails {
    Words(Words),
    Boost(Boost),
    Typo(Typo),
    Proximity(Rank),
    Fid(Rank),
    Position(Rank),
    ExactAttribute(ExactAttribute),
    ExactWords(ExactWords),
    Sort(Sort),
    GeoSort(GeoSort),
}

impl ScoreDetails {
    pub fn local_score(&self) -> Option<f64> {
        self.rank().map(Rank::local_score)
    }

    pub fn rank(&self) -> Option<Rank> {
        match self {
            ScoreDetails::Words(details) => Some(details.rank()),
            ScoreDetails::Boost(_) => None,
            ScoreDetails::Typo(details) => Some(details.rank()),
            ScoreDetails::Proximity(details) => Some(*details),
            ScoreDetails::Fid(details) => Some(*details),
            ScoreDetails::Position(details) => Some(*details),
            ScoreDetails::ExactAttribute(details) => Some(details.rank()),
            ScoreDetails::ExactWords(details) => Some(details.rank()),
            ScoreDetails::Sort(_) => None,
            ScoreDetails::GeoSort(_) => None,
        }
    }

    pub fn global_score<'a>(details: impl Iterator<Item = &'a Self>) -> f64 {
        Rank::global_score(details.filter_map(Self::rank))
    }

    /// Panics
    ///
    /// - If Position is not preceded by Fid
    /// - If Exactness is not preceded by ExactAttribute
    pub fn to_json_map<'a>(
        details: impl Iterator<Item = &'a Self>,
    ) -> serde_json::Map<String, serde_json::Value> {
        let mut order = 0;
        let mut fid_details = None;
        let mut details_map = serde_json::Map::default();
        for details in details {
            match details {
                ScoreDetails::Words(words) => {
                    let words_details = serde_json::json!({
                            "order": order,
                            "matchingWords": words.matching_words,
                            "maxMatchingWords": words.max_matching_words,
                            "score": words.rank().local_score(),
                    });
                    details_map.insert("words".into(), words_details);
                    order += 1;
                }
                ScoreDetails::Boost(Boost { filter, matching }) => {
                    let sort = format!("boost:{}", filter);
                    let sort_details = serde_json::json!({
                        "value": matching,
                    });
                    details_map.insert(sort, sort_details);
                    order += 1;
                }
                ScoreDetails::Typo(typo) => {
                    let typo_details = serde_json::json!({
                        "order": order,
                        "typoCount": typo.typo_count,
                        "maxTypoCount": typo.max_typo_count,
                        "score": typo.rank().local_score(),
                    });
                    details_map.insert("typo".into(), typo_details);
                    order += 1;
                }
                ScoreDetails::Proximity(proximity) => {
                    let proximity_details = serde_json::json!({
                        "order": order,
                        "score": proximity.local_score(),
                    });
                    details_map.insert("proximity".into(), proximity_details);
                    order += 1;
                }
                ScoreDetails::Fid(fid) => {
                    // copy the rank for future use in Position.
                    fid_details = Some(*fid);
                    // For now, fid is a virtual rule always followed by the "position" rule
                    let fid_details = serde_json::json!({
                        "order": order,
                        "attributeRankingOrderScore": fid.local_score(),
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
                    let Some(fid_details) = fid_details else {
                        unimplemented!("position not preceded by attribute");
                    };

                    attribute_details
                        .insert("queryWordDistanceScore".into(), position.local_score().into());
                    let score = Rank::global_score([fid_details, *position].iter().copied());
                    attribute_details.insert("score".into(), score.into());

                    // do not update the order since this was already done by fid
                }
                ScoreDetails::ExactAttribute(exact_attribute) => {
                    let exactness_details = serde_json::json!({
                        "order": order,
                        "matchType": exact_attribute,
                        "score": exact_attribute.rank().local_score(),
                    });
                    details_map.insert("exactness".into(), exactness_details);
                    order += 1;
                }
                ScoreDetails::ExactWords(details) => {
                    // For now, exactness is a virtual rule always preceded by the "ExactAttribute" rule
                    let exactness_details = details_map
                        .get_mut("exactness")
                        .expect("Exactness not preceded by exactAttribute");
                    let exactness_details = exactness_details
                        .as_object_mut()
                        .expect("exactness details was not an object");
                    if exactness_details.get("matchType").expect("missing 'matchType'")
                        == &serde_json::json!(ExactAttribute::NoExactMatch)
                    {
                        let score = Rank::global_score(
                            [ExactAttribute::NoExactMatch.rank(), details.rank()].iter().copied(),
                        );
                        // tiny detail, but we want the score to be the last displayed field,
                        // so we're removing it here, adding the other fields, then adding the new score
                        exactness_details.remove("score");
                        exactness_details
                            .insert("matchingWords".into(), details.matching_words.into());
                        exactness_details
                            .insert("maxMatchingWords".into(), details.max_matching_words.into());
                        exactness_details.insert("score".into(), score.into());
                    }
                    // do not update the order since this was already done by exactAttribute
                }
                ScoreDetails::Sort(details) => {
                    let sort = if details.redacted {
                        format!("<hidden-rule-{order}>")
                    } else {
                        format!(
                            "{}:{}",
                            details.field_name,
                            if details.ascending { "asc" } else { "desc" }
                        )
                    };
                    let value =
                        if details.redacted { "<hidden>".into() } else { details.value.clone() };
                    let sort_details = serde_json::json!({
                        "order": order,
                        "value": value,
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
}

/// The strategy to compute scores.
///
/// It makes sense to pass down this strategy to the internals of the search, because
/// some optimizations (today, mainly skipping ranking rules for universes of a single document)
/// are not correct to do when computing the scores.
///
/// This strategy could feasibly be extended to differentiate between the normalized score and the
/// detailed scores, but it is not useful today as the normalized score is *derived from* the
/// detailed scores.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ScoringStrategy {
    /// Don't compute scores
    #[default]
    Skip,
    /// Compute detailed scores
    Detailed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Words {
    pub matching_words: u32,
    pub max_matching_words: u32,
}

impl Words {
    pub fn rank(&self) -> Rank {
        Rank { rank: self.matching_words, max_rank: self.max_matching_words }
    }

    pub(crate) fn from_rank(rank: Rank) -> Self {
        Self { matching_words: rank.rank, max_matching_words: rank.max_rank }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Boost {
    pub filter: String,
    pub matching: bool,
}

/// Structure that is super similar to [`Words`], but whose semantics is a bit distinct.
///
/// In exactness, the number of matching words can actually be 0 with a non-zero score,
/// if no words from the query appear exactly in the document.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ExactWords {
    pub matching_words: u32,
    pub max_matching_words: u32,
}

impl ExactWords {
    pub fn rank(&self) -> Rank {
        // 0 matching words means last rank (1)
        Rank { rank: self.matching_words + 1, max_rank: self.max_matching_words + 1 }
    }

    pub(crate) fn from_rank(rank: Rank) -> Self {
        // last rank (1) means that 0 words from the query appear exactly in the document.
        // first rank (max_rank) means that (max_rank - 1) words from the query appear exactly in the document.
        Self {
            matching_words: rank.rank.saturating_sub(1),
            max_matching_words: rank.max_rank.saturating_sub(1),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Typo {
    pub typo_count: u32,
    pub max_typo_count: u32,
}

impl Typo {
    pub fn rank(&self) -> Rank {
        Rank {
            rank: (self.max_typo_count + 1).saturating_sub(self.typo_count),
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
        Typo {
            typo_count: rank.max_rank.saturating_sub(rank.rank),
            max_typo_count: rank.max_rank.saturating_sub(1),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
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

impl Rank {
    pub fn local_score(self) -> f64 {
        self.rank as f64 / self.max_rank as f64
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
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum ExactAttribute {
    ExactMatch,
    MatchesStart,
    NoExactMatch,
}

impl ExactAttribute {
    pub fn rank(&self) -> Rank {
        let rank = match self {
            ExactAttribute::ExactMatch => 3,
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
    pub redacted: bool,
    pub value: serde_json::Value,
}

#[derive(Debug, Clone, Copy, PartialEq, PartialOrd)]
pub struct GeoSort {
    pub target_point: [f64; 2],
    pub ascending: bool,
    pub value: Option<[f64; 2]>,
}

impl GeoSort {
    pub fn distance(&self) -> Option<f64> {
        self.value.map(|value| distance_between_two_points(&self.target_point, &value))
    }
}
