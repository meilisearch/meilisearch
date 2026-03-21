use itertools::Itertools;
use meilisearch_types::dynamic_search_rules::{
    Condition, DynamicSearchRule, DynamicSearchRuleAction, DynamicSearchRules, Selector,
};
use meilisearch_types::heed::{self, RoTxn};
use meilisearch_types::milli::DocumentId;
use std::cmp::{Ordering, Reverse};
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use time::OffsetDateTime;

use crate::milli::Index;

use super::SearchQuery;

#[derive(Debug, PartialEq, Eq, Copy, Clone)]
pub struct Priority(u64);

impl PartialOrd for Priority {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Priority {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.cmp(&other.0).reverse()
    }
}

impl From<u64> for Priority {
    fn from(value: u64) -> Self {
        Self(value)
    }
}

#[derive(Debug, PartialEq, Eq, Copy, Clone)]
pub struct Positioning<'a> {
    pub selector: &'a Selector,
    pub priority: Priority,
    pub doc_id: &'a str,
    pub position: u32,
}

#[derive(Default)]
pub struct DynamicSearchContext<'a> {
    pub query: Option<String>,
    pub index_uid: &'a str,
}

impl DynamicSearchContext<'_> {
    pub fn query_is_empty(&self) -> bool {
        self.query.as_ref().is_none_or(|s| s.trim().is_empty())
    }

    pub fn query_contains(&self, value: &str) -> bool {
        self.query.as_ref().is_some_and(|q| q.contains(&value.to_lowercase()))
    }
}

pub struct ActiveRules<'a> {
    positioning_rules: Vec<Positioning<'a>>,
}

pub fn collect_active_rules<'a>(
    rules: &'a DynamicSearchRules,
    ctx: &DynamicSearchContext<'_>,
) -> ActiveRules<'a> {
    let mut positioning_rules = Vec::new();
    let now = OffsetDateTime::now_utc();

    for rule in rules.values() {
        if !is_rule_active(rule, ctx, now) {
            continue;
        }

        let priority: Priority = rule.priority.unwrap_or(u64::MAX).into();
        for action in &rule.actions {
            match &action.action {
                DynamicSearchRuleAction::Pin { position } => {
                    if let Some(doc_id) = &action.selector.id {
                        positioning_rules.push(Positioning {
                            selector: &action.selector,
                            priority,
                            position: *position,
                            doc_id: doc_id.as_str(),
                        });
                    }
                }
            }
        }
    }

    ActiveRules { positioning_rules }
}

pub fn resolve_pins(
    rules: &DynamicSearchRules,
    query: &SearchQuery,
    index_uid: &str,
    index: &Index,
    rtxn: &RoTxn<'_>,
) -> heed::Result<Vec<(u32, DocumentId)>> {
    let ctx = DynamicSearchContext { query: query.q.as_ref().map(|q| q.to_lowercase()), index_uid };

    let external_ids = index.external_documents_ids();
    let mut resolved_pins = collect_active_rules(rules, &ctx)
        .positioning_rules_for_index_uid(index_uid)
        .into_iter()
        .map(|act| external_ids.get(rtxn, act.doc_id).map(|res| (act.position, res)))
        .filter_map_ok(|(pos, res)| res.map(|res| (pos, res)))
        .collect::<heed::Result<Vec<_>>>()?;

    resolved_pins.sort_by_key(|&(pos, _)| pos);

    Ok(resolved_pins)
}

impl<'a> ActiveRules<'a> {
    pub fn is_empty(&self) -> bool {
        self.positioning_rules.is_empty()
    }

    pub fn positioning_rules(&self) -> &[Positioning<'_>] {
        &self.positioning_rules
    }

    pub fn positioning_rules_for_index_uid(&self, index_uid: &str) -> Vec<Positioning<'_>> {
        let candidates = self
            .positioning_rules
            .iter()
            .filter(|rule| selector_matches_index_uid(rule.selector, index_uid))
            .copied();

        let mut positions = HashMap::<&str, Positioning<'_>>::new();

        for candidate in candidates {
            match positions.entry(candidate.doc_id) {
                Entry::Occupied(mut entry) => {
                    if candidate.priority > entry.get().priority {
                        entry.insert(candidate);
                    }
                }
                Entry::Vacant(entry) => {
                    entry.insert(candidate);
                }
            }
        }

        let mut result = positions.into_values().collect::<Vec<_>>();
        result.sort_by_key(|positioning| Reverse(positioning.priority));
        result
    }
}

fn is_rule_active(
    rule: &DynamicSearchRule,
    ctx: &DynamicSearchContext<'_>,
    now: OffsetDateTime,
) -> bool {
    if !rule.active {
        return false;
    }
    rule.conditions.iter().all(|c| evaluate_condition(c, ctx, now))
}

fn evaluate_condition(
    condition: &Condition,
    ctx: &DynamicSearchContext<'_>,
    now: OffsetDateTime,
) -> bool {
    match condition {
        Condition::Query { is_empty, contains } => {
            if let Some(is_empty) = is_empty {
                return *is_empty == ctx.query_is_empty();
            }

            if let Some(value) = contains {
                return ctx.query_contains(value);
            }

            true
        }
        Condition::Time { start, end } => {
            if let Some(start) = start {
                if now < *start {
                    return false;
                }
            }
            if let Some(end) = end {
                if now > *end {
                    return false;
                }
            }
            true
        }
    }
}

fn selector_matches_index_uid(selector: &Selector, index_uid: &str) -> bool {
    selector.index_uid.as_ref().is_none_or(|selector_index_uid| selector_index_uid == index_uid)
}
