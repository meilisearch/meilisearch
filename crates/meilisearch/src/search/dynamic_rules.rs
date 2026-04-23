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

pub fn collect_pinning_rules(rules: &DynamicSearchRules) -> Vec<Positioning<'_>> {
    let mut dedup_pins = HashMap::<&String, Positioning<'_>>::new();

    for rule in rules.values() {
        let priority: Priority = rule.priority.unwrap_or(u64::MAX).into();
        for action in &rule.actions {
            match &action.action {
                DynamicSearchRuleAction::Pin { position } => {
                    if let Some(doc_id) = &action.selector.id {
                        let pin = Positioning {
                            selector: &action.selector,
                            priority,
                            position: *position,
                            doc_id: doc_id.as_str(),
                        };

                        match dedup_pins.entry(doc_id) {
                            Entry::Occupied(mut entry) => {
                                if pin.priority > entry.get().priority {
                                    entry.insert(pin);
                                }
                            }

                            Entry::Vacant(entry) => {
                                entry.insert(pin);
                            }
                        }
                    }
                }
            }
        }
    }

    let mut positioning_rules = dedup_pins.into_values().collect_vec();
    positioning_rules.sort_by_key(|positioning| Reverse(positioning.priority));

    positioning_rules
}

pub fn resolve_pins(
    rules: &DynamicSearchRules,
    index: &Index,
    rtxn: &RoTxn<'_>,
) -> heed::Result<Vec<PinDoc>> {
    let external_ids = index.external_documents_ids();
    let mut resolved_pins = collect_pinning_rules(rules)
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
