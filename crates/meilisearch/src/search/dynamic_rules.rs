use crate::milli::Index;
use itertools::Itertools;
use meilisearch_types::dynamic_search_rules::{
    DynamicSearchRuleAction, DynamicSearchRules, Selector,
};
use meilisearch_types::heed::{self, RoTxn};
use meilisearch_types::milli::PinDoc;
use std::cmp::{Ordering, Reverse};
use std::collections::hash_map::Entry;
use std::collections::HashMap;

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

pub struct ActiveRules<'a> {
    positioning_rules: Vec<Positioning<'a>>,
}

pub fn collect_active_rules(rules: &DynamicSearchRules) -> ActiveRules<'_> {
    let mut positioning_rules = Vec::new();

    for rule in rules.values() {
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
    index_uid: &str,
    index: &Index,
    rtxn: &RoTxn<'_>,
) -> heed::Result<Vec<PinDoc>> {
    let external_ids = index.external_documents_ids();
    let mut resolved_pins = collect_active_rules(rules)
        .positioning_rules_for_index_uid(index_uid)
        .into_iter()
        .map(|act| {
            external_ids
                .get(rtxn, act.doc_id)
                .map(|res| res.map(|doc_id| PinDoc { pos: act.position, doc_id }))
        })
        .filter_map_ok(|pin| pin)
        .collect::<heed::Result<Vec<_>>>()?;

    resolved_pins.sort_by_key(|pin| pin.pos);

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

fn selector_matches_index_uid(selector: &Selector, index_uid: &str) -> bool {
    selector
        .index_uid
        .as_ref()
        .is_none_or(|selector_index_uid| selector_index_uid.as_str() == index_uid)
}
