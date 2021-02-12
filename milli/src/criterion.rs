use std::collections::HashMap;

use anyhow::{Context, bail};
use regex::Regex;
use serde::{Serialize, Deserialize};

use crate::facet::FacetType;

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub enum Criterion {
    /// Sorted by increasing number of typos.
    Typo,
    /// Sorted by decreasing number of matched query terms.
    Words,
    /// Sorted by increasing distance between matched query terms.
    Proximity,
    /// Documents with quey words contained in more important
    /// attributes are considred better.
    Attribute,
    /// Documents with query words at the front of an attribute is
    /// considered better than if it was at the back.
    WordsPosition,
    /// Sorted by the similarity of the matched words with the query words.
    Exactness,
    /// Sorted by the increasing value of the field specified.
    Asc(String),
    /// Sorted by the decreasing value of the field specified.
    Desc(String),
}

impl Criterion {
    pub fn from_str(faceted_attributes: &HashMap<String, FacetType>, txt: &str) -> anyhow::Result<Criterion> {
        match txt {
            "typo" => Ok(Criterion::Typo),
            "words" => Ok(Criterion::Words),
            "proximity" => Ok(Criterion::Proximity),
            "attribute" => Ok(Criterion::Attribute),
            "wordsposition" => Ok(Criterion::WordsPosition),
            "exactness" => Ok(Criterion::Exactness),
            text => {
                let re = Regex::new(r#"(asc|desc)\(([\w_-]+)\)"#)?;
                let caps = re.captures(text).with_context(|| format!("unknown criterion name: {}", text))?;
                let order = caps.get(1).unwrap().as_str();
                let field_name = caps.get(2).unwrap().as_str();
                faceted_attributes.get(field_name).with_context(|| format!("Can't use {:?} as a criterion as it isn't a faceted field.", field_name))?;
                match order {
                    "asc" => Ok(Criterion::Asc(field_name.to_string())),
                    "desc" => Ok(Criterion::Desc(field_name.to_string())),
                    otherwise => bail!("unknown criterion name: {}", otherwise),
                }
            },
        }
    }
}

pub fn default_criteria() -> Vec<Criterion> {
    vec![
        Criterion::Typo,
        Criterion::Words,
        Criterion::Proximity,
        Criterion::Attribute,
        Criterion::WordsPosition,
        Criterion::Exactness,
    ]
}
