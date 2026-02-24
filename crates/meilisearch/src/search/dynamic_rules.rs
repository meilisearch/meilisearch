use meilisearch_types::dynamic_search_rules::{
    Action, BoostArgs, BuryArgs, Condition, DynamicSearchRule, DynamicSearchRules, QueryCondition,
    RuleAction, Selector, TimeCondition,
};
use serde_json::Value;
use time::OffsetDateTime;

use super::SearchHit;

pub struct SearchContext<'a> {
    pub query_is_empty: bool,
    pub index_uid: &'a str,
    pub primary_key: Option<&'a str>,
}

pub struct ActiveRules<'a> {
    actions: Vec<&'a RuleAction>,
}

impl<'a> ActiveRules<'a> {
    pub fn new(rules: &'a DynamicSearchRules, ctx: &SearchContext<'_>) -> Self {
        let now = OffsetDateTime::now_utc();

        let mut active_rules: Vec<&DynamicSearchRule> =
            rules.values().filter(|rule| is_rule_active(rule, ctx, now)).collect();

        active_rules.sort_by_key(|r| r.priority.unwrap_or(u64::MAX));

        let actions = active_rules.iter().flat_map(|r| r.actions.iter()).collect();

        Self { actions }
    }

    pub fn is_empty(&self) -> bool {
        self.actions.is_empty()
    }

    pub fn apply(&self, ctx: &SearchContext<'_>, hits: &mut Vec<SearchHit>) {
        if self.actions.is_empty() || hits.is_empty() {
            return;
        }

        apply_hide(&self.actions, ctx, hits);

        if hits.is_empty() {
            return;
        }

        apply_boost_bury(&self.actions, ctx, hits);
        apply_pin(&self.actions, ctx, hits);
    }
}

fn is_rule_active(rule: &DynamicSearchRule, ctx: &SearchContext<'_>, now: OffsetDateTime) -> bool {
    if !rule.active {
        return false;
    }
    rule.conditions.iter().all(|c| evaluate_condition(c, ctx, now))
}

fn evaluate_condition(condition: &Condition, ctx: &SearchContext<'_>, now: OffsetDateTime) -> bool {
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

fn selector_matches(selector: &Selector, ctx: &SearchContext<'_>, hit: &SearchHit) -> bool {
    if let Some(ref sel_index) = selector.index_uid {
        let hit_index = if ctx.index_uid.is_empty() {
            hit.document
                .get("_federation")
                .and_then(|f| f.as_object())
                .and_then(|f| f.get("indexUid"))
                .and_then(|v| v.as_str())
        } else {
            Some(ctx.index_uid)
        };
        match hit_index {
            Some(idx) if idx == sel_index.as_str() => {}
            _ => return false,
        }
    }

    if let Some(ref sel_id) = selector.id {
        match ctx.primary_key {
            Some(pk) => match hit.document.get(pk) {
                Some(doc_id) => {
                    let doc_id_str = match doc_id {
                        Value::String(s) => s.as_str().to_string(),
                        Value::Number(n) => n.to_string(),
                        other => other.to_string(),
                    };
                    if doc_id_str != *sel_id {
                        return false;
                    }
                }
                None => return false,
            },
            None => return false,
        }
    }

    if let Some(ref filter) = selector.filter {
        if !evaluate_filter(filter, hit) {
            return false;
        }
    }

    true
}

fn evaluate_filter(filter: &Value, hit: &SearchHit) -> bool {
    let obj = match filter.as_object() {
        Some(o) => o,
        None => return false,
    };

    let attribute = match obj.get("attribute").and_then(|v| v.as_str()) {
        Some(a) => a,
        None => return false,
    };
    let op = match obj.get("op").and_then(|v| v.as_str()) {
        Some(o) => o,
        None => return false,
    };
    let expected = match obj.get("value") {
        Some(v) => v,
        None => return false,
    };

    let actual = match hit.document.get(attribute) {
        Some(v) => v,
        None => return false,
    };

    match op {
        "eq" => values_equal(actual, expected),
        _ => false,
    }
}

fn values_equal(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::String(a), Value::String(b)) => a == b,
        (Value::Number(a), Value::Number(b)) => a.as_f64() == b.as_f64(),
        (Value::Bool(a), Value::Bool(b)) => a == b,
        (Value::Null, Value::Null) => true,
        _ => a.to_string().trim_matches('"') == b.to_string().trim_matches('"'),
    }
}

fn apply_hide(actions: &[&RuleAction], ctx: &SearchContext<'_>, hits: &mut Vec<SearchHit>) {
    hits.retain(|hit| {
        !actions.iter().any(|ra| {
            matches!(ra.action, Action::Hide(_)) && selector_matches(&ra.selector, ctx, hit)
        })
    });
}

fn apply_boost_bury(actions: &[&RuleAction], ctx: &SearchContext<'_>, hits: &mut Vec<SearchHit>) {
    let boost_bury_actions: Vec<&RuleAction> = actions
        .iter()
        .filter(|ra| matches!(ra.action, Action::Boost(_) | Action::Bury(_)))
        .copied()
        .collect();

    if boost_bury_actions.is_empty() {
        return;
    }

    let n = hits.len() as f64;

    let mut scored: Vec<(usize, f64)> = hits
        .iter()
        .enumerate()
        .map(|(i, hit)| {
            let mut score = n - i as f64;
            for ra in &boost_bury_actions {
                if selector_matches(&ra.selector, ctx, hit) {
                    match &ra.action {
                        Action::Boost(BoostArgs { score: factor }) => score *= factor,
                        Action::Bury(BuryArgs { score: factor }) => score *= factor,
                        _ => {}
                    }
                }
            }
            (i, score)
        })
        .collect();

    scored.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));

    let reordered: Vec<SearchHit> = scored.into_iter().map(|(i, _)| hits[i].clone()).collect();
    *hits = reordered;
}

fn apply_pin(actions: &[&RuleAction], ctx: &SearchContext<'_>, hits: &mut Vec<SearchHit>) {
    let mut pins: Vec<(u32, usize)> = Vec::new();

    for ra in actions {
        if let Action::Pin(ref pin) = ra.action {
            if let Some(idx) = hits.iter().position(|hit| selector_matches(&ra.selector, ctx, hit))
            {
                pins.push((pin.position, idx));
            }
        }
    }

    if pins.is_empty() {
        return;
    }

    let mut seen_hits: Vec<usize> = Vec::new();
    pins.retain(|(_, idx)| {
        if seen_hits.contains(idx) {
            false
        } else {
            seen_hits.push(*idx);
            true
        }
    });

    pins.sort_by_key(|(pos, _)| *pos);

    let mut removed: Vec<(u32, SearchHit)> = Vec::new();
    let mut indices_to_remove: Vec<usize> = pins.iter().map(|(_, idx)| *idx).collect();
    indices_to_remove.sort_unstable();
    indices_to_remove.dedup();

    let pin_map: std::collections::HashMap<usize, u32> =
        pins.iter().map(|(pos, idx)| (*idx, *pos)).collect();

    for &idx in indices_to_remove.iter().rev() {
        let hit = hits.remove(idx);
        if let Some(&pos) = pin_map.get(&idx) {
            removed.push((pos, hit));
        }
    }

    removed.sort_by_key(|(pos, _)| *pos);
    for (pos, hit) in removed {
        let insert_at = (pos as usize).min(hits.len());
        hits.insert(insert_at, hit);
    }
}

#[cfg(test)]
mod tests {
    use meilisearch_types::dynamic_search_rules::*;
    use serde_json::json;

    use super::*;
    use crate::search::SearchHit;

    fn make_hit(id: &str, fields: &[(&str, Value)]) -> SearchHit {
        let mut doc = serde_json::Map::new();
        doc.insert("id".to_string(), Value::String(id.to_string()));
        for (k, v) in fields {
            doc.insert(k.to_string(), v.clone());
        }
        SearchHit {
            document: doc,
            formatted: Default::default(),
            matches_position: None,
            ranking_score: None,
            ranking_score_details: None,
        }
    }

    fn ctx(query_is_empty: bool, pk: Option<&str>) -> SearchContext<'_> {
        SearchContext { query_is_empty, index_uid: "movies", primary_key: pk }
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

    fn active_rules<'a>(rules: &'a DynamicSearchRules, ctx: &SearchContext<'_>) -> ActiveRules<'a> {
        ActiveRules::new(rules, ctx)
    }

    #[test]
    fn hide_removes_matching_hits() {
        let mut rules = DynamicSearchRules::new();
        rules.insert(
            "hide-42".into(),
            make_rule(
                "hide-42",
                vec![RuleAction {
                    selector: Selector { index_uid: None, id: Some("42".into()), filter: None },
                    action: Action::Hide(HideArgs {}),
                }],
            ),
        );

        let ctx = ctx(false, Some("id"));
        let active = active_rules(&rules, &ctx);
        let mut hits = vec![make_hit("1", &[]), make_hit("42", &[]), make_hit("3", &[])];

        active.apply(&ctx, &mut hits);

        assert_eq!(hits.len(), 2);
        assert_eq!(hits[0].document["id"], "1");
        assert_eq!(hits[1].document["id"], "3");
    }

    #[test]
    fn pin_moves_hit_to_position() {
        let mut rules = DynamicSearchRules::new();
        rules.insert(
            "pin-3".into(),
            make_rule(
                "pin-3",
                vec![RuleAction {
                    selector: Selector { index_uid: None, id: Some("3".into()), filter: None },
                    action: Action::Pin(PinArgs { position: 0 }),
                }],
            ),
        );

        let ctx = ctx(false, Some("id"));
        let active = active_rules(&rules, &ctx);
        let mut hits = vec![make_hit("1", &[]), make_hit("2", &[]), make_hit("3", &[])];

        active.apply(&ctx, &mut hits);

        assert_eq!(hits[0].document["id"], "3");
        assert_eq!(hits[1].document["id"], "1");
        assert_eq!(hits[2].document["id"], "2");
    }

    #[test]
    fn boost_reorders_hits() {
        let mut rules = DynamicSearchRules::new();
        rules.insert(
            "boost-last".into(),
            make_rule(
                "boost-last",
                vec![RuleAction {
                    selector: Selector { index_uid: None, id: Some("3".into()), filter: None },
                    action: Action::Boost(BoostArgs { score: 10.0 }),
                }],
            ),
        );

        let ctx = ctx(false, Some("id"));
        let active = active_rules(&rules, &ctx);
        let mut hits = vec![make_hit("1", &[]), make_hit("2", &[]), make_hit("3", &[])];

        active.apply(&ctx, &mut hits);

        assert_eq!(hits[0].document["id"], "3");
    }

    #[test]
    fn bury_reorders_hits() {
        let mut rules = DynamicSearchRules::new();
        rules.insert(
            "bury-first".into(),
            make_rule(
                "bury-first",
                vec![RuleAction {
                    selector: Selector { index_uid: None, id: Some("1".into()), filter: None },
                    action: Action::Bury(BuryArgs { score: 0.01 }),
                }],
            ),
        );

        let ctx = ctx(false, Some("id"));
        let active = active_rules(&rules, &ctx);
        let mut hits = vec![make_hit("1", &[]), make_hit("2", &[]), make_hit("3", &[])];

        active.apply(&ctx, &mut hits);

        assert_eq!(hits[2].document["id"], "1");
    }

    #[test]
    fn inactive_rule_is_skipped() {
        let mut rules = DynamicSearchRules::new();
        let mut rule = make_rule(
            "hidden",
            vec![RuleAction {
                selector: Selector { index_uid: None, id: Some("1".into()), filter: None },
                action: Action::Hide(HideArgs {}),
            }],
        );
        rule.active = false;
        rules.insert("hidden".into(), rule);

        let ctx = ctx(false, Some("id"));
        let active = active_rules(&rules, &ctx);
        let mut hits = vec![make_hit("1", &[])];

        active.apply(&ctx, &mut hits);

        assert_eq!(hits.len(), 1);
    }

    #[test]
    fn query_is_empty_condition() {
        let mut rules = DynamicSearchRules::new();
        let mut rule = make_rule(
            "empty-q",
            vec![RuleAction {
                selector: Selector { index_uid: None, id: Some("1".into()), filter: None },
                action: Action::Hide(HideArgs {}),
            }],
        );
        rule.conditions = vec![Condition::Query(QueryCondition { is_empty: true })];
        rules.insert("empty-q".into(), rule);

        let ctx_non_empty = ctx(false, Some("id"));
        let active = active_rules(&rules, &ctx_non_empty);
        let mut hits = vec![make_hit("1", &[])];
        active.apply(&ctx_non_empty, &mut hits);
        assert_eq!(hits.len(), 1);

        let ctx_empty = ctx(true, Some("id"));
        let active = active_rules(&rules, &ctx_empty);
        let mut hits = vec![make_hit("1", &[])];
        active.apply(&ctx_empty, &mut hits);
        assert_eq!(hits.len(), 0);
    }

    #[test]
    fn filter_selector_matches() {
        let mut rules = DynamicSearchRules::new();
        rules.insert(
            "hide-premium".into(),
            make_rule(
                "hide-premium",
                vec![RuleAction {
                    selector: Selector {
                        index_uid: None,
                        id: None,
                        filter: Some(
                            json!({ "attribute": "brand", "op": "eq", "value": "premium" }),
                        ),
                    },
                    action: Action::Hide(HideArgs {}),
                }],
            ),
        );

        let ctx = ctx(false, Some("id"));
        let active = active_rules(&rules, &ctx);
        let mut hits = vec![
            make_hit("1", &[("brand", json!("premium"))]),
            make_hit("2", &[("brand", json!("basic"))]),
            make_hit("3", &[("brand", json!("premium"))]),
        ];

        active.apply(&ctx, &mut hits);

        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].document["id"], "2");
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
                        filter: None,
                    },
                    action: Action::Hide(HideArgs {}),
                }],
            ),
        );

        let ctx = ctx(false, Some("id"));
        let active = active_rules(&rules, &ctx);
        let mut hits = vec![make_hit("1", &[])];

        active.apply(&ctx, &mut hits);

        assert_eq!(hits.len(), 1);
    }

    #[test]
    fn priority_1_wins_over_higher_numbers() {
        let mut rules = DynamicSearchRules::new();

        // priority 100 (low) wants to pin "3" to position 2 (the end)
        let mut low_priority = make_rule(
            "pin-low",
            vec![RuleAction {
                selector: Selector { index_uid: None, id: Some("3".into()), filter: None },
                action: Action::Pin(PinArgs { position: 2 }),
            }],
        );
        low_priority.priority = Some(100);
        rules.insert("pin-low".into(), low_priority);

        // priority 1 (high) wants to pin "3" to position 0 (the front)
        let mut high_priority = make_rule(
            "pin-high",
            vec![RuleAction {
                selector: Selector { index_uid: None, id: Some("3".into()), filter: None },
                action: Action::Pin(PinArgs { position: 0 }),
            }],
        );
        high_priority.priority = Some(1);
        rules.insert("pin-high".into(), high_priority);

        let ctx = ctx(false, Some("id"));
        let active = active_rules(&rules, &ctx);
        let mut hits = vec![make_hit("1", &[]), make_hit("2", &[]), make_hit("3", &[])];

        active.apply(&ctx, &mut hits);

        // priority 1 wins: "3" is pinned to position 0
        assert_eq!(hits[0].document["id"], "3");
    }

    #[test]
    fn no_priority_is_lowest() {
        let mut rules = DynamicSearchRules::new();

        // no priority (lowest) wants to pin "3" to position 2
        let no_priority = make_rule(
            "pin-none",
            vec![RuleAction {
                selector: Selector { index_uid: None, id: Some("3".into()), filter: None },
                action: Action::Pin(PinArgs { position: 2 }),
            }],
        );
        rules.insert("pin-none".into(), no_priority);

        // priority 1 (highest) wants to pin "3" to position 0
        let mut with_priority = make_rule(
            "pin-explicit",
            vec![RuleAction {
                selector: Selector { index_uid: None, id: Some("3".into()), filter: None },
                action: Action::Pin(PinArgs { position: 0 }),
            }],
        );
        with_priority.priority = Some(1);
        rules.insert("pin-explicit".into(), with_priority);

        let ctx = ctx(false, Some("id"));
        let active = active_rules(&rules, &ctx);
        let mut hits = vec![make_hit("1", &[]), make_hit("2", &[]), make_hit("3", &[])];

        active.apply(&ctx, &mut hits);

        // priority 1 wins: "3" is pinned to position 0
        assert_eq!(hits[0].document["id"], "3");
    }
}
