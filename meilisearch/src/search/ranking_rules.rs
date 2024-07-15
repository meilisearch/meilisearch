use std::collections::HashMap;
use std::fmt::Write;

use itertools::Itertools as _;
use meilisearch_types::error::{Code, ResponseError};
use meilisearch_types::milli::{AscDesc, Criterion, Member, TermsMatchingStrategy};

pub struct RankingRules {
    canonical_criteria: Vec<Criterion>,
    canonical_sort: Option<Vec<AscDesc>>,
    canonicalization_actions: Vec<CanonicalizationAction>,
    source_criteria: Vec<Criterion>,
    source_sort: Option<Vec<AscDesc>>,
}

pub enum CanonicalizationAction {
    PrependedWords {
        prepended_index: RankingRuleSource,
    },
    RemovedDuplicate {
        earlier_occurrence: RankingRuleSource,
        removed_occurrence: RankingRuleSource,
    },
    RemovedWords {
        reason: RemoveWords,
        removed_occurrence: RankingRuleSource,
    },
    RemovedPlaceholder {
        removed_occurrence: RankingRuleSource,
    },
    TruncatedVector {
        vector_rule: RankingRuleSource,
        truncated_from: RankingRuleSource,
    },
    RemovedVector {
        vector_rule: RankingRuleSource,
        removed_occurrence: RankingRuleSource,
    },
    RemovedSort {
        removed_occurrence: RankingRuleSource,
    },
}

pub enum RemoveWords {
    WasPrepended,
    MatchingStrategyAll,
}

impl std::fmt::Display for RemoveWords {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let reason = match self {
            RemoveWords::WasPrepended => "it was previously prepended",
            RemoveWords::MatchingStrategyAll => "`query.matchingWords` is set to `all`",
        };
        f.write_str(reason)
    }
}

pub enum CanonicalizationKind {
    Placeholder,
    Keyword,
    Vector,
}

pub struct CompatibilityError {
    previous: RankingRule,
    current: RankingRule,
}
impl CompatibilityError {
    pub(crate) fn to_response_error(
        &self,
        ranking_rules: &RankingRules,
        previous_ranking_rules: &RankingRules,
        query_index: usize,
        previous_query_index: usize,
        index_uid: &str,
        previous_index_uid: &str,
    ) -> meilisearch_types::error::ResponseError {
        let rule = self.current.as_string(
            &ranking_rules.canonical_criteria,
            &ranking_rules.canonical_sort,
            query_index,
            index_uid,
        );
        let previous_rule = self.previous.as_string(
            &previous_ranking_rules.canonical_criteria,
            &previous_ranking_rules.canonical_sort,
            previous_query_index,
            previous_index_uid,
        );

        let canonicalization_actions = ranking_rules.canonicalization_notes();
        let previous_canonicalization_actions = previous_ranking_rules.canonicalization_notes();

        let mut msg = String::new();
        let reason = self.reason();
        let _ = writeln!(
            &mut msg,
            "The results of queries #{previous_query_index} and #{query_index} are incompatible: "
        );
        let _ = writeln!(&mut msg, "  1. {previous_rule}");
        let _ = writeln!(&mut msg, "  2. {rule}");
        let _ = writeln!(&mut msg, "  - {reason}");

        if !previous_canonicalization_actions.is_empty() {
            let _ = write!(&mut msg, "  - note: The ranking rules of query #{previous_query_index} were modified during canonicalization:\n{previous_canonicalization_actions}");
        }

        if !canonicalization_actions.is_empty() {
            let _ = write!(&mut msg, "  - note: The ranking rules of query #{query_index} were modified during canonicalization:\n{canonicalization_actions}");
        }

        ResponseError::from_msg(msg, Code::InvalidMultiSearchQueryRankingRules)
    }
    pub fn reason(&self) -> &'static str {
        match (self.previous.kind, self.current.kind) {
            (RankingRuleKind::Relevancy, RankingRuleKind::AscendingSort)
            | (RankingRuleKind::Relevancy, RankingRuleKind::DescendingSort)
            | (RankingRuleKind::AscendingSort, RankingRuleKind::Relevancy)
            | (RankingRuleKind::DescendingSort, RankingRuleKind::Relevancy) => {
                "cannot compare a relevancy rule with a sort rule"
            }

            (RankingRuleKind::Relevancy, RankingRuleKind::AscendingGeoSort)
            | (RankingRuleKind::Relevancy, RankingRuleKind::DescendingGeoSort)
            | (RankingRuleKind::AscendingGeoSort, RankingRuleKind::Relevancy)
            | (RankingRuleKind::DescendingGeoSort, RankingRuleKind::Relevancy) => {
                "cannot compare a relevancy rule with a geosort rule"
            }

            (RankingRuleKind::AscendingSort, RankingRuleKind::DescendingSort)
            | (RankingRuleKind::DescendingSort, RankingRuleKind::AscendingSort) => {
                "cannot compare two sort rules in opposite directions"
            }

            (RankingRuleKind::AscendingSort, RankingRuleKind::AscendingGeoSort)
            | (RankingRuleKind::AscendingSort, RankingRuleKind::DescendingGeoSort)
            | (RankingRuleKind::DescendingSort, RankingRuleKind::AscendingGeoSort)
            | (RankingRuleKind::DescendingSort, RankingRuleKind::DescendingGeoSort)
            | (RankingRuleKind::AscendingGeoSort, RankingRuleKind::AscendingSort)
            | (RankingRuleKind::AscendingGeoSort, RankingRuleKind::DescendingSort)
            | (RankingRuleKind::DescendingGeoSort, RankingRuleKind::AscendingSort)
            | (RankingRuleKind::DescendingGeoSort, RankingRuleKind::DescendingSort) => {
                "cannot compare a sort rule with a geosort rule"
            }

            (RankingRuleKind::AscendingGeoSort, RankingRuleKind::DescendingGeoSort)
            | (RankingRuleKind::DescendingGeoSort, RankingRuleKind::AscendingGeoSort) => {
                "cannot compare two geosort rules in opposite directions"
            }
            (RankingRuleKind::Relevancy, RankingRuleKind::Relevancy)
            | (RankingRuleKind::AscendingSort, RankingRuleKind::AscendingSort)
            | (RankingRuleKind::DescendingSort, RankingRuleKind::DescendingSort)
            | (RankingRuleKind::AscendingGeoSort, RankingRuleKind::AscendingGeoSort)
            | (RankingRuleKind::DescendingGeoSort, RankingRuleKind::DescendingGeoSort) => {
                "internal error, comparison should be possible"
            }
        }
    }
}

impl RankingRules {
    pub fn new(
        criteria: Vec<Criterion>,
        sort: Option<Vec<AscDesc>>,
        terms_matching_strategy: TermsMatchingStrategy,
        canonicalization_kind: CanonicalizationKind,
    ) -> Self {
        let (canonical_criteria, canonical_sort, canonicalization_actions) =
            Self::canonicalize(&criteria, &sort, terms_matching_strategy, canonicalization_kind);
        Self {
            canonical_criteria,
            canonical_sort,
            canonicalization_actions,
            source_criteria: criteria,
            source_sort: sort,
        }
    }

    fn canonicalize(
        criteria: &[Criterion],
        sort: &Option<Vec<AscDesc>>,
        terms_matching_strategy: TermsMatchingStrategy,
        canonicalization_kind: CanonicalizationKind,
    ) -> (Vec<Criterion>, Option<Vec<AscDesc>>, Vec<CanonicalizationAction>) {
        match canonicalization_kind {
            CanonicalizationKind::Placeholder => Self::canonicalize_placeholder(criteria, sort),
            CanonicalizationKind::Keyword => {
                Self::canonicalize_keyword(criteria, sort, terms_matching_strategy)
            }
            CanonicalizationKind::Vector => Self::canonicalize_vector(criteria, sort),
        }
    }

    fn canonicalize_placeholder(
        criteria: &[Criterion],
        sort_query: &Option<Vec<AscDesc>>,
    ) -> (Vec<Criterion>, Option<Vec<AscDesc>>, Vec<CanonicalizationAction>) {
        let mut sort = None;

        let mut sorted_fields = HashMap::new();
        let mut canonicalization_actions = Vec::new();
        let mut canonical_criteria = Vec::new();
        let mut canonical_sort = None;

        for (criterion_index, criterion) in criteria.iter().enumerate() {
            match criterion.clone() {
                Criterion::Words
                | Criterion::Typo
                | Criterion::Proximity
                | Criterion::Attribute
                | Criterion::Exactness => {
                    canonicalization_actions.push(CanonicalizationAction::RemovedPlaceholder {
                        removed_occurrence: RankingRuleSource::Criterion(criterion_index),
                    })
                }

                Criterion::Sort => {
                    if let Some(previous_index) = sort {
                        canonicalization_actions.push(CanonicalizationAction::RemovedDuplicate {
                            earlier_occurrence: RankingRuleSource::Criterion(previous_index),
                            removed_occurrence: RankingRuleSource::Criterion(criterion_index),
                        });
                    } else if let Some(sort_query) = sort_query {
                        sort = Some(criterion_index);
                        canonical_criteria.push(criterion.clone());
                        canonical_sort = Some(canonicalize_sort(
                            &mut sorted_fields,
                            sort_query.as_slice(),
                            criterion_index,
                            &mut canonicalization_actions,
                        ));
                    } else {
                        canonicalization_actions.push(CanonicalizationAction::RemovedSort {
                            removed_occurrence: RankingRuleSource::Criterion(criterion_index),
                        })
                    }
                }
                Criterion::Asc(s) | Criterion::Desc(s) => match sorted_fields.entry(s) {
                    std::collections::hash_map::Entry::Occupied(entry) => canonicalization_actions
                        .push(CanonicalizationAction::RemovedDuplicate {
                            earlier_occurrence: *entry.get(),
                            removed_occurrence: RankingRuleSource::Criterion(criterion_index),
                        }),
                    std::collections::hash_map::Entry::Vacant(entry) => {
                        entry.insert(RankingRuleSource::Criterion(criterion_index));
                        canonical_criteria.push(criterion.clone())
                    }
                },
            }
        }

        (canonical_criteria, canonical_sort, canonicalization_actions)
    }

    fn canonicalize_vector(
        criteria: &[Criterion],
        sort_query: &Option<Vec<AscDesc>>,
    ) -> (Vec<Criterion>, Option<Vec<AscDesc>>, Vec<CanonicalizationAction>) {
        let mut sort = None;

        let mut sorted_fields = HashMap::new();
        let mut canonicalization_actions = Vec::new();
        let mut canonical_criteria = Vec::new();
        let mut canonical_sort = None;

        let mut vector = None;

        'criteria: for (criterion_index, criterion) in criteria.iter().enumerate() {
            match criterion.clone() {
                Criterion::Words
                | Criterion::Typo
                | Criterion::Proximity
                | Criterion::Attribute
                | Criterion::Exactness => match vector {
                    Some(previous_occurrence) => {
                        if sorted_fields.is_empty() {
                            canonicalization_actions.push(CanonicalizationAction::RemovedVector {
                                vector_rule: RankingRuleSource::Criterion(previous_occurrence),
                                removed_occurrence: RankingRuleSource::Criterion(criterion_index),
                            });
                        } else {
                            canonicalization_actions.push(
                                CanonicalizationAction::TruncatedVector {
                                    vector_rule: RankingRuleSource::Criterion(previous_occurrence),
                                    truncated_from: RankingRuleSource::Criterion(criterion_index),
                                },
                            );
                            break 'criteria;
                        }
                    }
                    None => {
                        canonical_criteria.push(criterion.clone());
                        vector = Some(criterion_index);
                    }
                },

                Criterion::Sort => {
                    if let Some(previous_index) = sort {
                        canonicalization_actions.push(CanonicalizationAction::RemovedDuplicate {
                            earlier_occurrence: RankingRuleSource::Criterion(previous_index),
                            removed_occurrence: RankingRuleSource::Criterion(criterion_index),
                        });
                    } else if let Some(sort_query) = sort_query {
                        sort = Some(criterion_index);
                        canonical_criteria.push(criterion.clone());
                        canonical_sort = Some(canonicalize_sort(
                            &mut sorted_fields,
                            sort_query.as_slice(),
                            criterion_index,
                            &mut canonicalization_actions,
                        ));
                    } else {
                        canonicalization_actions.push(CanonicalizationAction::RemovedSort {
                            removed_occurrence: RankingRuleSource::Criterion(criterion_index),
                        })
                    }
                }
                Criterion::Asc(s) | Criterion::Desc(s) => match sorted_fields.entry(s) {
                    std::collections::hash_map::Entry::Occupied(entry) => canonicalization_actions
                        .push(CanonicalizationAction::RemovedDuplicate {
                            earlier_occurrence: *entry.get(),
                            removed_occurrence: RankingRuleSource::Criterion(criterion_index),
                        }),
                    std::collections::hash_map::Entry::Vacant(entry) => {
                        entry.insert(RankingRuleSource::Criterion(criterion_index));
                        canonical_criteria.push(criterion.clone())
                    }
                },
            }
        }

        (canonical_criteria, canonical_sort, canonicalization_actions)
    }

    fn canonicalize_keyword(
        criteria: &[Criterion],
        sort_query: &Option<Vec<AscDesc>>,
        terms_matching_strategy: TermsMatchingStrategy,
    ) -> (Vec<Criterion>, Option<Vec<AscDesc>>, Vec<CanonicalizationAction>) {
        let mut words = None;
        let mut typo = None;
        let mut proximity = None;
        let mut sort = None;
        let mut attribute = None;
        let mut exactness = None;
        let mut sorted_fields = HashMap::new();

        let mut canonical_criteria = Vec::new();
        let mut canonical_sort = None;

        let mut canonicalization_actions = Vec::new();

        for (criterion_index, criterion) in criteria.iter().enumerate() {
            let criterion = criterion.clone();
            match criterion.clone() {
                Criterion::Words => {
                    if let TermsMatchingStrategy::All = terms_matching_strategy {
                        canonicalization_actions.push(CanonicalizationAction::RemovedWords {
                            reason: RemoveWords::MatchingStrategyAll,
                            removed_occurrence: RankingRuleSource::Criterion(criterion_index),
                        });
                        continue;
                    }
                    if let Some(maybe_previous_index) = words {
                        if let Some(previous_index) = maybe_previous_index {
                            canonicalization_actions.push(
                                CanonicalizationAction::RemovedDuplicate {
                                    earlier_occurrence: RankingRuleSource::Criterion(
                                        previous_index,
                                    ),
                                    removed_occurrence: RankingRuleSource::Criterion(
                                        criterion_index,
                                    ),
                                },
                            );
                            continue;
                        }
                        canonicalization_actions.push(CanonicalizationAction::RemovedWords {
                            reason: RemoveWords::WasPrepended,
                            removed_occurrence: RankingRuleSource::Criterion(criterion_index),
                        })
                    }
                    words = Some(Some(criterion_index));
                    canonical_criteria.push(criterion);
                }
                Criterion::Typo => {
                    canonicalize_criterion(
                        criterion,
                        criterion_index,
                        terms_matching_strategy,
                        &mut words,
                        &mut canonicalization_actions,
                        &mut canonical_criteria,
                        &mut typo,
                    );
                }
                Criterion::Proximity => {
                    canonicalize_criterion(
                        criterion,
                        criterion_index,
                        terms_matching_strategy,
                        &mut words,
                        &mut canonicalization_actions,
                        &mut canonical_criteria,
                        &mut proximity,
                    );
                }
                Criterion::Attribute => {
                    canonicalize_criterion(
                        criterion,
                        criterion_index,
                        terms_matching_strategy,
                        &mut words,
                        &mut canonicalization_actions,
                        &mut canonical_criteria,
                        &mut attribute,
                    );
                }
                Criterion::Exactness => {
                    canonicalize_criterion(
                        criterion,
                        criterion_index,
                        terms_matching_strategy,
                        &mut words,
                        &mut canonicalization_actions,
                        &mut canonical_criteria,
                        &mut exactness,
                    );
                }

                Criterion::Sort => {
                    if let Some(previous_index) = sort {
                        canonicalization_actions.push(CanonicalizationAction::RemovedDuplicate {
                            earlier_occurrence: RankingRuleSource::Criterion(previous_index),
                            removed_occurrence: RankingRuleSource::Criterion(criterion_index),
                        });
                    } else if let Some(sort_query) = sort_query {
                        sort = Some(criterion_index);
                        canonical_criteria.push(criterion);
                        canonical_sort = Some(canonicalize_sort(
                            &mut sorted_fields,
                            sort_query.as_slice(),
                            criterion_index,
                            &mut canonicalization_actions,
                        ));
                    } else {
                        canonicalization_actions.push(CanonicalizationAction::RemovedSort {
                            removed_occurrence: RankingRuleSource::Criterion(criterion_index),
                        })
                    }
                }
                Criterion::Asc(s) | Criterion::Desc(s) => match sorted_fields.entry(s) {
                    std::collections::hash_map::Entry::Occupied(entry) => canonicalization_actions
                        .push(CanonicalizationAction::RemovedDuplicate {
                            earlier_occurrence: *entry.get(),
                            removed_occurrence: RankingRuleSource::Criterion(criterion_index),
                        }),
                    std::collections::hash_map::Entry::Vacant(entry) => {
                        entry.insert(RankingRuleSource::Criterion(criterion_index));
                        canonical_criteria.push(criterion)
                    }
                },
            }
        }

        (canonical_criteria, canonical_sort, canonicalization_actions)
    }

    pub fn is_compatible_with(&self, previous: &Self) -> Result<(), CompatibilityError> {
        for (current, previous) in self.coalesce_iterator().zip(previous.coalesce_iterator()) {
            if current.kind != previous.kind {
                return Err(CompatibilityError { current, previous });
            }
        }
        Ok(())
    }

    pub fn constraint_count(&self) -> usize {
        self.coalesce_iterator().count()
    }

    fn coalesce_iterator(&self) -> impl Iterator<Item = RankingRule> + '_ {
        self.canonical_criteria
            .iter()
            .enumerate()
            .flat_map(|(criterion_index, criterion)| {
                RankingRule::from_criterion(criterion_index, criterion, &self.canonical_sort)
            })
            .coalesce(
                |previous @ RankingRule { source: previous_source, kind: previous_kind },
                 current @ RankingRule { source, kind }| {
                    match (previous_kind, kind) {
                        (RankingRuleKind::Relevancy, RankingRuleKind::Relevancy) => {
                            let merged_source = match (previous_source, source) {
                                (
                                    RankingRuleSource::Criterion(previous),
                                    RankingRuleSource::Criterion(current),
                                ) => RankingRuleSource::CoalescedCriteria(previous, current),
                                (
                                    RankingRuleSource::CoalescedCriteria(begin, _end),
                                    RankingRuleSource::Criterion(current),
                                ) => RankingRuleSource::CoalescedCriteria(begin, current),
                                (_previous, current) => current,
                            };
                            Ok(RankingRule { source: merged_source, kind })
                        }
                        _ => Err((previous, current)),
                    }
                },
            )
    }

    fn canonicalization_notes(&self) -> String {
        use CanonicalizationAction::*;
        let mut notes = String::new();
        for (index, action) in self.canonicalization_actions.iter().enumerate() {
            let index = index + 1;
            let _ = match action {
                PrependedWords { prepended_index } => writeln!(
                    &mut notes,
                    "    {index}. Prepended rule `words` before first relevancy rule `{}` at position {}",
                    prepended_index.rule_name(&self.source_criteria, &self.source_sort),
                    prepended_index.rule_position()
                ),
                RemovedDuplicate { earlier_occurrence, removed_occurrence } => writeln!(
                    &mut notes,
                    "    {index}. Removed duplicate rule `{}` at position {} as it already appears at position {}",
                    earlier_occurrence.rule_name(&self.source_criteria, &self.source_sort),
                    removed_occurrence.rule_position(),
                    earlier_occurrence.rule_position(),
                ),
                RemovedWords { reason, removed_occurrence } => writeln!(
                    &mut notes,
                    "    {index}. Removed rule `words` at position {} because {reason}",
                    removed_occurrence.rule_position()
                ),
                RemovedPlaceholder { removed_occurrence } => writeln!(
                    &mut notes,
                    "    {index}. Removed relevancy rule `{}` at position {} because the query is a placeholder search (`q`: \"\")",
                    removed_occurrence.rule_name(&self.source_criteria, &self.source_sort),
                    removed_occurrence.rule_position()
                ),
                TruncatedVector { vector_rule, truncated_from } => writeln!(
                    &mut notes,
                    "    {index}. Truncated relevancy rule `{}` at position {} and later rules because the query is a vector search and `vector` was inserted at position {}",
                    truncated_from.rule_name(&self.source_criteria, &self.source_sort),
                    truncated_from.rule_position(),
                    vector_rule.rule_position(),
                ),
                RemovedVector { vector_rule, removed_occurrence } => writeln!(
                    &mut notes,
                    "    {index}. Removed relevancy rule `{}` at position {} because the query is a vector search and `vector` was already inserted at position {}",
                    removed_occurrence.rule_name(&self.source_criteria, &self.source_sort),
                    removed_occurrence.rule_position(),
                    vector_rule.rule_position(),
                ),
                RemovedSort { removed_occurrence } => writeln!(
                    &mut notes,
                    "   {index}. Removed rule `sort` at position {} because `query.sort` is empty",
removed_occurrence.rule_position()
                ),
            };
        }
        notes
    }
}

fn canonicalize_sort(
    sorted_fields: &mut HashMap<String, RankingRuleSource>,
    sort_query: &[AscDesc],
    criterion_index: usize,
    canonicalization_actions: &mut Vec<CanonicalizationAction>,
) -> Vec<AscDesc> {
    let mut geo_sorted = None;
    let mut canonical_sort = Vec::new();
    for (sort_index, asc_desc) in sort_query.iter().enumerate() {
        let source = RankingRuleSource::Sort { criterion_index, sort_index };
        let asc_desc = asc_desc.clone();
        match asc_desc.clone() {
            AscDesc::Asc(Member::Field(s)) | AscDesc::Desc(Member::Field(s)) => {
                match sorted_fields.entry(s) {
                    std::collections::hash_map::Entry::Occupied(entry) => canonicalization_actions
                        .push(CanonicalizationAction::RemovedDuplicate {
                            earlier_occurrence: *entry.get(),
                            removed_occurrence: source,
                        }),
                    std::collections::hash_map::Entry::Vacant(entry) => {
                        entry.insert(source);
                        canonical_sort.push(asc_desc);
                    }
                }
            }
            AscDesc::Asc(Member::Geo(_)) | AscDesc::Desc(Member::Geo(_)) => match geo_sorted {
                Some(earlier_sort_index) => {
                    canonicalization_actions.push(CanonicalizationAction::RemovedDuplicate {
                        earlier_occurrence: RankingRuleSource::Sort {
                            criterion_index,
                            sort_index: earlier_sort_index,
                        },
                        removed_occurrence: source,
                    })
                }
                None => {
                    geo_sorted = Some(sort_index);
                    canonical_sort.push(asc_desc);
                }
            },
        }
    }
    canonical_sort
}

fn canonicalize_criterion(
    criterion: Criterion,
    criterion_index: usize,
    terms_matching_strategy: TermsMatchingStrategy,
    words: &mut Option<Option<usize>>,
    canonicalization_actions: &mut Vec<CanonicalizationAction>,
    canonical_criteria: &mut Vec<Criterion>,
    rule: &mut Option<usize>,
) {
    *words = match (terms_matching_strategy, words.take()) {
        (TermsMatchingStrategy::All, words) => words,
        (_, None) => {
            // inject words
            canonicalization_actions.push(CanonicalizationAction::PrependedWords {
                prepended_index: RankingRuleSource::Criterion(criterion_index),
            });
            canonical_criteria.push(Criterion::Words);
            Some(None)
        }
        (_, words) => words,
    };
    if let Some(previous_index) = *rule {
        canonicalization_actions.push(CanonicalizationAction::RemovedDuplicate {
            earlier_occurrence: RankingRuleSource::Criterion(previous_index),
            removed_occurrence: RankingRuleSource::Criterion(criterion_index),
        });
    } else {
        *rule = Some(criterion_index);
        canonical_criteria.push(criterion)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RankingRuleKind {
    Relevancy,
    AscendingSort,
    DescendingSort,
    AscendingGeoSort,
    DescendingGeoSort,
}

#[derive(Debug, Clone, Copy)]
pub struct RankingRule {
    source: RankingRuleSource,
    kind: RankingRuleKind,
}

#[derive(Debug, Clone, Copy)]
pub enum RankingRuleSource {
    Criterion(usize),
    CoalescedCriteria(usize, usize),
    Sort { criterion_index: usize, sort_index: usize },
}

impl RankingRuleSource {
    fn rule_name(&self, criteria: &[Criterion], sort: &Option<Vec<AscDesc>>) -> String {
        match self {
            RankingRuleSource::Criterion(criterion_index) => criteria
                .get(*criterion_index)
                .map(|c| c.to_string())
                .unwrap_or_else(|| "unknown".into()),
            RankingRuleSource::CoalescedCriteria(begin, end) => {
                let rules: Vec<_> = criteria
                    .get(*begin..=*end)
                    .iter()
                    .flat_map(|c| c.iter())
                    .map(|c| c.to_string())
                    .collect();
                rules.join(", ")
            }
            RankingRuleSource::Sort { criterion_index: _, sort_index } => {
                match sort.as_deref().and_then(|sort| sort.get(*sort_index)) {
                    Some(sort) => match sort {
                        AscDesc::Asc(Member::Field(field_name)) => format!("{field_name}:asc"),
                        AscDesc::Desc(Member::Field(field_name)) => {
                            format!("{field_name}:desc")
                        }
                        AscDesc::Asc(Member::Geo(_)) => "_geo(..):asc".to_string(),
                        AscDesc::Desc(Member::Geo(_)) => "_geo(..):desc".to_string(),
                    },
                    None => "unknown".into(),
                }
            }
        }
    }

    fn rule_position(&self) -> String {
        match self {
            RankingRuleSource::Criterion(criterion_index) => {
                format!("#{criterion_index} in ranking rules")
            }
            RankingRuleSource::CoalescedCriteria(begin, end) => {
                format!("#{begin} to #{end} in ranking rules")
            }
            RankingRuleSource::Sort { criterion_index, sort_index } => format!(
                "#{sort_index} in `query.sort` (as `sort` is #{criterion_index} in ranking rules)"
            ),
        }
    }
}

impl RankingRule {
    fn from_criterion<'a>(
        criterion_index: usize,
        criterion: &'a Criterion,
        sort: &'a Option<Vec<AscDesc>>,
    ) -> impl Iterator<Item = Self> + 'a {
        let kind = match criterion {
            Criterion::Words
            | Criterion::Typo
            | Criterion::Proximity
            | Criterion::Attribute
            | Criterion::Exactness => RankingRuleKind::Relevancy,
            Criterion::Asc(s) if s == "_geo" => RankingRuleKind::AscendingGeoSort,

            Criterion::Asc(_) => RankingRuleKind::AscendingSort,
            Criterion::Desc(s) if s == "_geo" => RankingRuleKind::DescendingGeoSort,

            Criterion::Desc(_) => RankingRuleKind::DescendingSort,
            Criterion::Sort => {
                return either::Right(sort.iter().flatten().enumerate().map(
                    move |(rule_index, asc_desc)| {
                        Self::from_asc_desc(asc_desc, criterion_index, rule_index)
                    },
                ))
            }
        };

        either::Left(std::iter::once(Self {
            source: RankingRuleSource::Criterion(criterion_index),
            kind,
        }))
    }

    fn from_asc_desc(asc_desc: &AscDesc, sort_index: usize, rule_index_in_sort: usize) -> Self {
        let kind = match asc_desc {
            AscDesc::Asc(Member::Field(_)) => RankingRuleKind::AscendingSort,
            AscDesc::Desc(Member::Field(_)) => RankingRuleKind::DescendingSort,
            AscDesc::Asc(Member::Geo(_)) => RankingRuleKind::AscendingGeoSort,
            AscDesc::Desc(Member::Geo(_)) => RankingRuleKind::DescendingGeoSort,
        };
        Self {
            source: RankingRuleSource::Sort {
                criterion_index: sort_index,
                sort_index: rule_index_in_sort,
            },
            kind,
        }
    }

    fn as_string(
        &self,
        canonical_criteria: &[Criterion],
        canonical_sort: &Option<Vec<AscDesc>>,
        query_index: usize,
        index_uid: &str,
    ) -> String {
        let kind = match self.kind {
            RankingRuleKind::Relevancy => "relevancy",
            RankingRuleKind::AscendingSort => "ascending sort",
            RankingRuleKind::DescendingSort => "descending sort",
            RankingRuleKind::AscendingGeoSort => "ascending geo sort",
            RankingRuleKind::DescendingGeoSort => "descending geo sort",
        };
        let rules = self.fetch_from_source(canonical_criteria, canonical_sort);

        let source = match self.source {
            RankingRuleSource::Criterion(criterion_index) => format!("`queries[{query_index}]`, `{index_uid}.rankingRules[{criterion_index}]`"),
            RankingRuleSource::CoalescedCriteria(begin, end) => format!("`queries[{query_index}]`, `{index_uid}.rankingRules[{begin}..={end}]`"),
            RankingRuleSource::Sort { criterion_index, sort_index } => format!("`queries[{query_index}].sort[{sort_index}]`, `{index_uid}.rankingRules[{criterion_index}]`"),
        };

        format!("{source}: {kind} {rules}")
    }

    fn fetch_from_source(
        &self,
        canonical_criteria: &[Criterion],
        canonical_sort: &Option<Vec<AscDesc>>,
    ) -> String {
        let rule_name = match self.source {
            RankingRuleSource::Criterion(index) => {
                canonical_criteria.get(index).map(|criterion| criterion.to_string())
            }
            RankingRuleSource::CoalescedCriteria(begin, end) => {
                let rules: Vec<String> = canonical_criteria
                    .get(begin..=end)
                    .into_iter()
                    .flat_map(|criteria| criteria.iter())
                    .map(|criterion| criterion.to_string())
                    .collect();

                (!rules.is_empty()).then_some(rules.join(", "))
            }
            RankingRuleSource::Sort { criterion_index: _, sort_index } => canonical_sort
                .as_deref()
                .and_then(|canonical_sort| canonical_sort.get(sort_index))
                .and_then(|asc_desc: &AscDesc| match asc_desc {
                    AscDesc::Asc(Member::Field(s)) | AscDesc::Desc(Member::Field(s)) => {
                        Some(format!("on field `{s}`"))
                    }
                    _ => None,
                }),
        };

        let rule_name = rule_name.unwrap_or_else(|| "default".into());

        format!("rule(s) {rule_name}")
    }
}
