use meilisearch_types::dynamic_search_rules::{
    Action, Condition, DynamicSearchRule, DynamicSearchRules, QueryCondition, Selector,
    TimeCondition,
};
use std::cmp::{Ordering, Reverse};
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use time::OffsetDateTime;

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
    pub query_is_empty: bool,
    pub index_uid: &'a str,
}

pub struct ActiveRules<'a> {
    positioning_rules: Vec<Positioning<'a>>,
}

pub fn collect_active_rules<'a>(
    rules: &'a DynamicSearchRules,
    ctx: &DynamicSearchContext<'_>,
) -> ActiveRules<'a> {
    let mut positioning_rules = Vec::new();

    for rule in rules.values() {
        if !is_rule_active(rule, ctx, OffsetDateTime::now_utc()) {
            continue;
        }

        let priority: Priority = rule.priority.unwrap_or(u64::MAX).into();
        for action in &rule.actions {
            match &action.action {
                Action::Pin(args) => {
                    if let Some(doc_id) = &action.selector.id {
                        positioning_rules.push(Positioning {
                            selector: &action.selector,
                            priority,
                            position: args.position,
                            doc_id: doc_id.as_str(),
                        });
                    }
                }
            }
        }
    }

    ActiveRules { positioning_rules }
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
        Condition::Query(QueryCondition { is_empty }) => *is_empty == ctx.query_is_empty,
        Condition::Time(TimeCondition { start, end }) => {
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

#[cfg(test)]
mod tests {
    use meilisearch_types::dynamic_search_rules::*;

    use super::*;

    fn ctx(query_is_empty: bool) -> DynamicSearchContext<'static> {
        DynamicSearchContext { query_is_empty, index_uid: "movies" }
    }

    fn make_rule(uid: &str, actions: Vec<RuleAction>) -> DynamicSearchRule {
        DynamicSearchRule {
            uid: uid.to_string(),
            description: None,
            priority: None,
            active: true,
            conditions: vec![],
            actions,
        }
    }

    fn active_rules<'a>(
        rules: &'a DynamicSearchRules,
        ctx: &DynamicSearchContext<'_>,
    ) -> ActiveRules<'a> {
        collect_active_rules(rules, ctx)
    }

    #[test]
    fn pins_extracts_pin_actions() {
        let mut rules = DynamicSearchRules::new();
        rules.insert(
            "pin-3".into(),
            make_rule(
                "pin-3",
                vec![RuleAction {
                    selector: Selector { index_uid: None, id: Some("3".into()) },
                    action: Action::Pin(PinArgs { position: 0 }),
                }],
            ),
        );

        let ctx = ctx(false);
        let active = active_rules(&rules, &ctx);
        let pins = active
            .positioning_rules()
            .iter()
            .filter(|rule| selector_matches_index_uid(rule.selector, "movies"))
            .map(|rule| (rule.position, rule.selector.id.as_ref().unwrap().as_str()))
            .collect::<Vec<_>>();

        assert_eq!(pins, vec![(0, "3")]);
    }

    #[test]
    fn pins_without_id_are_ignored() {
        let mut rules = DynamicSearchRules::new();
        rules.insert(
            "pin-missing-id".into(),
            make_rule(
                "pin-missing-id",
                vec![RuleAction {
                    selector: Selector { index_uid: None, id: None },
                    action: Action::Pin(PinArgs { position: 0 }),
                }],
            ),
        );

        let ctx = ctx(false);
        let active = active_rules(&rules, &ctx);
        assert!(active.is_empty());
    }

    #[test]
    fn inactive_rule_is_skipped() {
        let mut rules = DynamicSearchRules::new();
        let mut rule = make_rule(
            "inactive-pin",
            vec![RuleAction {
                selector: Selector { index_uid: None, id: Some("1".into()) },
                action: Action::Pin(PinArgs { position: 0 }),
            }],
        );
        rule.active = false;
        rules.insert("inactive-pin".into(), rule);

        let ctx = ctx(false);
        let active = active_rules(&rules, &ctx);
        assert!(active.is_empty());
    }

    #[test]
    fn query_is_empty_condition() {
        let mut rules = DynamicSearchRules::new();
        let mut rule = make_rule(
            "empty-q",
            vec![RuleAction {
                selector: Selector { index_uid: None, id: Some("1".into()) },
                action: Action::Pin(PinArgs { position: 0 }),
            }],
        );
        rule.conditions = vec![Condition::Query(QueryCondition { is_empty: true })];
        rules.insert("empty-q".into(), rule);

        let ctx_non_empty = ctx(false);
        assert!(active_rules(&rules, &ctx_non_empty).is_empty());

        let ctx_empty = ctx(true);
        let active = active_rules(&rules, &ctx_empty);
        let pins = active.positioning_rules();
        assert_eq!(pins.len(), 1);
        assert_eq!(pins[0].doc_id, "1");
    }

    #[test]
    fn index_uid_selector_filters_actions() {
        let mut rules = DynamicSearchRules::new();
        rules.insert(
            "wrong-index".into(),
            make_rule(
                "wrong-index",
                vec![RuleAction {
                    selector: Selector {
                        index_uid: Some("other-index".into()),
                        id: Some("1".into()),
                    },
                    action: Action::Pin(PinArgs { position: 0 }),
                }],
            ),
        );

        let ctx = ctx(false);
        let active = active_rules(&rules, &ctx);
        assert!(active.positioning_rules_for_index_uid("movies").is_empty());
        assert_eq!(active.positioning_rules_for_index_uid("other-index").len(), 1);
    }

    #[test]
    fn priority_1_wins_over_higher_numbers() {
        let mut rules = DynamicSearchRules::new();

        let mut low_priority = make_rule(
            "pin-low",
            vec![RuleAction {
                selector: Selector { index_uid: None, id: Some("3".into()) },
                action: Action::Pin(PinArgs { position: 2 }),
            }],
        );
        low_priority.priority = Some(100);
        rules.insert("pin-low".into(), low_priority);

        let mut high_priority = make_rule(
            "pin-high",
            vec![RuleAction {
                selector: Selector { index_uid: None, id: Some("3".into()) },
                action: Action::Pin(PinArgs { position: 0 }),
            }],
        );
        high_priority.priority = Some(1);
        rules.insert("pin-high".into(), high_priority);

        let ctx = ctx(false);
        let active = active_rules(&rules, &ctx);
        let pins = active
            .positioning_rules_for_index_uid("movies")
            .iter()
            .map(|rule| (rule.position, rule.selector.id.as_ref().unwrap().as_str()))
            .collect::<Vec<_>>();

        assert_eq!(pins, vec![(0, "3")]);
    }

    #[test]
    fn no_priority_is_lowest() {
        let mut rules = DynamicSearchRules::new();

        let no_priority = make_rule(
            "pin-none",
            vec![RuleAction {
                selector: Selector { index_uid: None, id: Some("3".into()) },
                action: Action::Pin(PinArgs { position: 2 }),
            }],
        );
        rules.insert("pin-none".into(), no_priority);

        let mut with_priority = make_rule(
            "pin-explicit",
            vec![RuleAction {
                selector: Selector { index_uid: None, id: Some("3".into()) },
                action: Action::Pin(PinArgs { position: 0 }),
            }],
        );
        with_priority.priority = Some(1);
        rules.insert("pin-explicit".into(), with_priority);

        let ctx = ctx(false);
        let active = active_rules(&rules, &ctx);
        let pins = active
            .positioning_rules_for_index_uid("movies")
            .iter()
            .map(|rule| (rule.position, rule.selector.id.as_ref().unwrap().as_str()))
            .collect::<Vec<_>>();

        assert_eq!(pins, vec![(0, "3")]);
    }

    #[test]
    fn pins_filter_by_index_uid() {
        let mut rules = DynamicSearchRules::new();
        rules.insert(
            "pin-other".into(),
            make_rule(
                "pin-other",
                vec![RuleAction {
                    selector: Selector {
                        index_uid: Some("other-index".into()),
                        id: Some("1".into()),
                    },
                    action: Action::Pin(PinArgs { position: 0 }),
                }],
            ),
        );
        rules.insert(
            "pin-movies".into(),
            make_rule(
                "pin-movies",
                vec![RuleAction {
                    selector: Selector { index_uid: Some("movies".into()), id: Some("2".into()) },
                    action: Action::Pin(PinArgs { position: 1 }),
                }],
            ),
        );

        let ctx = ctx(false);
        let active = active_rules(&rules, &ctx);
        let pins = active
            .positioning_rules()
            .iter()
            .filter(|rule| selector_matches_index_uid(rule.selector, "movies"))
            .map(|rule| (rule.position, rule.selector.id.as_ref().unwrap().as_str()))
            .collect::<Vec<_>>();

        assert_eq!(pins, vec![(1, "2")]);
    }
}
