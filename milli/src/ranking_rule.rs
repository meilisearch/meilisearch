use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::boost::{Boost, BoostError};
use crate::{AscDesc, AscDescError, Member};

#[derive(Error, Debug)]
pub enum RankingRuleError {
    #[error("`{name}` ranking rule is invalid. Valid ranking rules are words, typo, sort, proximity, attribute, exactness and custom ranking rules.")]
    InvalidName { name: String },
    #[error("`{name}` is a reserved keyword and thus can't be used as a ranking rule")]
    ReservedName { name: String },
    #[error(
        "`{name}` is a reserved keyword and thus can't be used as a ranking rule. \
`{name}` can only be used for sorting at search time"
    )]
    ReservedNameForSort { name: String },
    #[error(
        "`{name}` is a reserved keyword and thus can't be used as a ranking rule. \
`{name}` can only be used for filtering at search time"
    )]
    ReservedNameForFilter { name: String },
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub enum RankingRule {
    /// Sorted by decreasing number of matched query terms.
    /// Query words at the front of an attribute is considered better than if it was at the back.
    Words,
    /// Sorted by documents matching the given filter and then documents not matching it.
    Boost(String),
    /// Sorted by increasing number of typos.
    Typo,
    /// Sorted by increasing distance between matched query terms.
    Proximity,
    /// Documents with quey words contained in more important
    /// attributes are considered better.
    Attribute,
    /// Dynamically sort at query time the documents. None, one or multiple Asc/Desc sortable
    /// attributes can be used in place of this criterion at query time.
    Sort,
    /// Sorted by the similarity of the matched words with the query words.
    Exactness,
    /// Sorted by the increasing value of the field specified.
    Asc(String),
    /// Sorted by the decreasing value of the field specified.
    Desc(String),
}

impl RankingRule {
    /// Returns the field name parameter of this criterion.
    pub fn field_name(&self) -> Option<&str> {
        match self {
            RankingRule::Asc(name) | RankingRule::Desc(name) => Some(name),
            _otherwise => None,
        }
    }
}

impl FromStr for RankingRule {
    type Err = RankingRuleError;

    fn from_str(text: &str) -> Result<RankingRule, Self::Err> {
        match text {
            "words" => Ok(RankingRule::Words),
            "typo" => Ok(RankingRule::Typo),
            "proximity" => Ok(RankingRule::Proximity),
            "attribute" => Ok(RankingRule::Attribute),
            "sort" => Ok(RankingRule::Sort),
            "exactness" => Ok(RankingRule::Exactness),
            text => match (AscDesc::from_str(text), Boost::from_str(text)) {
                (Ok(asc_desc), _) => match asc_desc {
                    AscDesc::Asc(Member::Field(field)) => Ok(RankingRule::Asc(field)),
                    AscDesc::Desc(Member::Field(field)) => Ok(RankingRule::Desc(field)),
                    AscDesc::Asc(Member::Geo(_)) | AscDesc::Desc(Member::Geo(_)) => {
                        Err(RankingRuleError::ReservedNameForSort {
                            name: "_geoPoint".to_string(),
                        })?
                    }
                },
                (_, Ok(Boost(filter))) => Ok(RankingRule::Boost(filter)),
                (
                    Err(AscDescError::InvalidSyntax { name: asc_desc_name }),
                    Err(BoostError::InvalidSyntax { name: boost_name }),
                ) => Err(RankingRuleError::InvalidName {
                    // TODO improve the error message quality
                    name: format!("{asc_desc_name} {boost_name}"),
                }),
                (Err(asc_desc_error), _) => Err(asc_desc_error.into()),
            },
        }
    }
}

pub fn default_criteria() -> Vec<RankingRule> {
    vec![
        RankingRule::Words,
        RankingRule::Typo,
        RankingRule::Proximity,
        RankingRule::Attribute,
        RankingRule::Sort,
        RankingRule::Exactness,
    ]
}

impl fmt::Display for RankingRule {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use RankingRule::*;

        match self {
            Words => f.write_str("words"),
            Boost(filter) => write!(f, "boost:{filter}"),
            Typo => f.write_str("typo"),
            Proximity => f.write_str("proximity"),
            Attribute => f.write_str("attribute"),
            Sort => f.write_str("sort"),
            Exactness => f.write_str("exactness"),
            Asc(attr) => write!(f, "{attr}:asc"),
            Desc(attr) => write!(f, "{attr}:desc"),
        }
    }
}

#[cfg(test)]
mod tests {
    use big_s::S;
    use RankingRuleError::*;

    use super::*;

    #[test]
    fn parse_criterion() {
        let valid_criteria = [
            ("words", RankingRule::Words),
            ("typo", RankingRule::Typo),
            ("proximity", RankingRule::Proximity),
            ("attribute", RankingRule::Attribute),
            ("sort", RankingRule::Sort),
            ("exactness", RankingRule::Exactness),
            ("price:asc", RankingRule::Asc(S("price"))),
            ("price:desc", RankingRule::Desc(S("price"))),
            ("price:asc:desc", RankingRule::Desc(S("price:asc"))),
            ("truc:machin:desc", RankingRule::Desc(S("truc:machin"))),
            ("hello-world!:desc", RankingRule::Desc(S("hello-world!"))),
            ("it's spacy over there:asc", RankingRule::Asc(S("it's spacy over there"))),
        ];

        for (input, expected) in valid_criteria {
            let res = input.parse::<RankingRule>();
            assert!(
                res.is_ok(),
                "Failed to parse `{}`, was expecting `{:?}` but instead got `{:?}`",
                input,
                expected,
                res
            );
            assert_eq!(res.unwrap(), expected);
        }

        let invalid_criteria = [
            ("words suffix", InvalidName { name: S("words suffix") }),
            ("prefix typo", InvalidName { name: S("prefix typo") }),
            ("proximity attribute", InvalidName { name: S("proximity attribute") }),
            ("price", InvalidName { name: S("price") }),
            ("asc:price", InvalidName { name: S("asc:price") }),
            ("price:deesc", InvalidName { name: S("price:deesc") }),
            ("price:aasc", InvalidName { name: S("price:aasc") }),
            ("price:asc and desc", InvalidName { name: S("price:asc and desc") }),
            ("price:asc:truc", InvalidName { name: S("price:asc:truc") }),
            ("_geo:asc", ReservedName { name: S("_geo") }),
            ("_geoDistance:asc", ReservedName { name: S("_geoDistance") }),
            ("_geoPoint:asc", ReservedNameForSort { name: S("_geoPoint") }),
            ("_geoPoint(42, 75):asc", ReservedNameForSort { name: S("_geoPoint") }),
            ("_geoRadius:asc", ReservedNameForFilter { name: S("_geoRadius") }),
            ("_geoRadius(42, 75, 59):asc", ReservedNameForFilter { name: S("_geoRadius") }),
            ("_geoBoundingBox:asc", ReservedNameForFilter { name: S("_geoBoundingBox") }),
            (
                "_geoBoundingBox([42, 75], [75, 59]):asc",
                ReservedNameForFilter { name: S("_geoBoundingBox") },
            ),
        ];

        for (input, expected) in invalid_criteria {
            let res = input.parse::<RankingRule>();
            assert!(
                res.is_err(),
                "Should no be able to parse `{}`, was expecting an error but instead got: `{:?}`",
                input,
                res
            );
            let res = res.unwrap_err();
            assert_eq!(
                res.to_string(),
                expected.to_string(),
                "Bad error for input {}: got `{:?}` instead of `{:?}`",
                input,
                res,
                expected
            );
        }
    }
}
