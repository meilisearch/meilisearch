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
    CustomAsc(String),
    /// Sorted by the decreasing value of the field specified.
    CustomDesc(String),
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
