use super::federated::Weight;
use super::{SearchHit, SearchQueryWithIndex};
use meilisearch_types::dynamic_search_rules::{
    Action, Condition, DynamicSearchRule, DynamicSearchRules, QueryCondition, Selector,
    TimeCondition,
};
use ordered_float::OrderedFloat;
use serde_json::Value;
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

pub struct Exclusion<'a> {
    pub priority: Priority,
    pub selector: &'a Selector,
}

#[derive(Debug, PartialEq, Eq, Copy, Clone)]
pub enum RelevanceTuningKind {
    Boost,
    Bury,
}

#[derive(Debug, Copy, Clone)]
pub struct RelevanceTuning<'a> {
    pub priority: Priority,
    pub selector: &'a Selector,
    pub factor: f64,
    pub kind: RelevanceTuningKind,
}

#[derive(Default)]
pub struct DynamicSearchContext<'a> {
    pub query_is_empty: bool,
    pub index_uid: &'a str,
    pub primary_key: Option<&'a str>,
}

pub struct ActiveRules<'a> {
    positioning_rules: Vec<Positioning<'a>>,
    relevance_tuning_rules: Vec<RelevanceTuning<'a>>,
    exclusions: Vec<Exclusion<'a>>,
}

pub fn collect_active_rules<'a>(
    rules: &'a DynamicSearchRules,
    ctx: &DynamicSearchContext<'_>,
) -> ActiveRules<'a> {
    let mut positioning_rules = Vec::new();
    let mut relevance_tuning_rules = Vec::new();
    let mut exclusions = Vec::new();

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

                Action::Boost(args) => relevance_tuning_rules.push(RelevanceTuning {
                    priority,
                    selector: &action.selector,
                    factor: args.score,
                    kind: RelevanceTuningKind::Boost,
                }),

                Action::Bury(args) => relevance_tuning_rules.push(RelevanceTuning {
                    priority,
                    selector: &action.selector,
                    factor: args.score,
                    kind: RelevanceTuningKind::Bury,
                }),

                Action::Hide(_) => {
                    exclusions.push(Exclusion { priority, selector: &action.selector });
                }
            }
        }
    }

    ActiveRules { positioning_rules, relevance_tuning_rules, exclusions }
}

impl<'a> ActiveRules<'a> {
    pub fn is_empty(&self) -> bool {
        self.positioning_rules.is_empty()
            && self.relevance_tuning_rules.is_empty()
            && self.exclusions.is_empty()
    }

    pub fn positioning_rules(&self) -> &[Positioning<'_>] {
        &self.positioning_rules
    }

    pub fn positioning_rules_for_index_uid(&self, index_uid: &str) -> Vec<Positioning<'_>> {
        let candidates = self
            .positioning_rules
            .iter()
            .filter(|r| {
                !self.has_exclusions_for_index_uid(r.priority, index_uid)
                    && selector_matches_index_uid(r.selector, index_uid)
            })
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
        result.sort_by_key(|a| Reverse(a.priority));
        result
    }

    pub fn has_positioning_for_index_uid(&self, priority: Priority, index_uid: &str) -> bool {
        self.positioning_rules
            .iter()
            .any(|r| priority >= r.priority && selector_matches_index_uid(r.selector, index_uid))
    }

    pub fn relevance_tuning_rules(&self) -> &[RelevanceTuning<'_>] {
        &self.relevance_tuning_rules
    }

    pub fn relevance_tuning_rules_for_index_uid(
        &self,
        index_uid: &str,
    ) -> Vec<RelevanceTuning<'_>> {
        self.relevance_tuning_rules
            .iter()
            .filter(|r| {
                !self.has_exclusions_for_index_uid(r.priority, index_uid)
                    && selector_matches_index_uid(r.selector, index_uid)
            })
            .copied()
            .collect::<Vec<_>>()
    }

    pub fn exclusions(&self) -> &[Exclusion<'_>] {
        &self.exclusions
    }

    pub fn has_exclusions_for_index_uid(&self, priority: Priority, index_uid: &str) -> bool {
        self.exclusions.iter().any(|exclusion| {
            priority >= exclusion.priority
                && selector_matches_index_uid(exclusion.selector, index_uid)
        })
    }

    pub fn apply_hide(&self, ctx: &DynamicSearchContext<'_>, hits: &mut Vec<SearchHit>) {
        apply_hide(self, ctx, hits);
    }

    pub fn apply(&self, ctx: &DynamicSearchContext<'_>, hits: &mut Vec<SearchHit>) {
        if self.is_empty() || hits.is_empty() {
            return;
        }

        apply_relevance_tuning(self, ctx, hits);
        apply_hide(self, ctx, hits);
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
    if let Some(selector_index_uid) = &selector.index_uid {
        selector_index_uid == index_uid
    } else {
        true
    }
}

fn selector_matches(selector: &Selector, ctx: &DynamicSearchContext<'_>, hit: &SearchHit) -> bool {
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

    if let Some(selector_doc_id) = &selector.id {
        match ctx.primary_key {
            Some(pk) => match hit.document.get(pk) {
                Some(doc_id) => {
                    let doc_id_str = match doc_id {
                        Value::String(s) => s.as_str().to_string(),
                        Value::Number(n) => n.to_string(),
                        other => other.to_string(),
                    };
                    if doc_id_str.as_str() != selector_doc_id {
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

fn apply_hide(rules: &ActiveRules, ctx: &DynamicSearchContext<'_>, hits: &mut Vec<SearchHit>) {
    hits.retain(|hit| {
        !rules.exclusions.iter().any(|exclusion| {
            let is_excluded = selector_matches(exclusion.selector, ctx, hit);

            if is_excluded {
                let is_positioned = rules.positioning_rules.iter().any(|positioning| {
                    positioning.priority > exclusion.priority
                        && selector_matches(positioning.selector, ctx, hit)
                });

                let is_fine_tuned = rules.relevance_tuning_rules.iter().any(|tuning| {
                    tuning.priority > exclusion.priority
                        && selector_matches(tuning.selector, ctx, hit)
                });

                return !is_positioned && !is_fine_tuned;
            }

            false
        })
    });
}

fn apply_relevance_tuning(
    rules: &ActiveRules,
    ctx: &DynamicSearchContext<'_>,
    hits: &mut [SearchHit],
) {
    if rules.relevance_tuning_rules.is_empty() {
        return;
    }

    for hit in hits.iter_mut() {
        let mut score = hit.ranking_score.unwrap_or_default();
        for tuning_rule in &rules.relevance_tuning_rules {
            if selector_matches(tuning_rule.selector, ctx, hit) {
                match &tuning_rule.kind {
                    RelevanceTuningKind::Boost => score *= tuning_rule.factor,
                    RelevanceTuningKind::Bury => score *= tuning_rule.factor.powi(-1),
                }
            }
        }
        hit.ranking_score = Some(score);
    }

    hits.sort_by_key(|h| Reverse(OrderedFloat(h.ranking_score.unwrap_or_default())));
}

/// TODO - Will move to Filter expression when persisting a rule
/// Convert a rule selector into a filter expression.
///
/// For id-based selectors, the primary key field name is needed. For
/// filter-based selectors the JSON `{"attribute": ..., "op": "eq", "value": ...}`
/// format is converted to a filter string like `attribute = 'value'`.
///
/// Returns `None` when the selector has neither an `id` nor a `filter`.
pub fn selector_to_filter(selector: &Selector, primary_key: Option<&str>) -> Option<Value> {
    let mut conditions: Vec<Value> = Vec::new();

    if let Some(id) = &selector.id {
        if let Some(pk) = primary_key {
            conditions.push(Value::String(format!("{pk} = '{id}'")));
        }
    }

    if let Some(filter) = selector.filter.as_ref().and_then(filter_json_to_string) {
        conditions.push(Value::String(filter));
    }

    match conditions.len() {
        0 => None,
        1 => Some(conditions.pop().unwrap()),
        _ => Some(Value::Array(conditions)), // AND of all conditions
    }
}

pub fn negate_filter(filter: &Value) -> Value {
    match filter {
        Value::String(s) => Value::String(format!("NOT ({s})")),
        Value::Array(arr) => {
            let negated = arr
                .iter()
                .map(|v| {
                    if let Value::String(s) = v {
                        Value::String(format!("NOT ({s})"))
                    } else {
                        v.clone()
                    }
                })
                .collect();

            Value::Array(negated)
        }
        other => other.clone(),
    }
}

/// TODO - Will be deleted once I change the filter syntax in `DynamicSearchRule` to match the
/// filter syntax in the engine.
/// Convert the rule-internal JSON filter format
/// `{"attribute": "brand", "op": "eq", "value": "Nike"}` into filter string like `brand = 'Nike'`.
fn filter_json_to_string(filter: &Value) -> Option<String> {
    let obj = filter.as_object()?;
    let attribute = obj.get("attribute")?.as_str()?;
    let op = obj.get("op")?.as_str()?;
    let value = obj.get("value")?;

    match op {
        "eq" => {
            let value_str = match value {
                Value::String(s) => format!("'{s}'"),
                Value::Number(n) => n.to_string(),
                Value::Bool(b) => b.to_string(),
                _ => return None,
            };
            Some(format!("{attribute} = {value_str}"))
        }
        _ => None,
    }
}

/// Combine an additional filter with an existing query filter
fn combine_filters(filter: &mut Option<Value>, additional: Value) {
    *filter = match filter.take() {
        None => Some(additional),
        Some(existing) => {
            let mut combined = if let Value::Array(arr) = existing { arr } else { vec![existing] };

            if let Value::Array(arr) = additional {
                combined.extend(arr);
            } else {
                combined.push(additional);
            }

            Some(Value::Array(combined))
        }
    };
}

/// Expand federated search queries with boost/bury sub-queries.
///
/// For each original query whose index matches one or more boost/bury rules the
/// query is replaced by:
///
/// * one sub-query per matching rule that carries the rule's filter and weighted score.
/// * a "base" sub-query that *excludes* all boosted/buried documents (with the
///   original weight).
///
/// Queries that don't match any boost/bury rule are kept unchanged.
pub fn expand_query_with_relevance_tuning(
    query: &mut SearchQueryWithIndex,
    active_rules: &ActiveRules<'_>,
    primary_key: Option<&str>,
) -> Vec<SearchQueryWithIndex> {
    let mut result = Vec::new();
    let tuning_rules = active_rules.relevance_tuning_rules_for_index_uid(query.index_uid.as_str());

    if tuning_rules.is_empty() {
        return vec![];
    }

    let original_weight = query.federation_options.as_ref().map(|o| o.weight).unwrap_or_default();

    let mut negated_filters = Vec::new();

    for entry in tuning_rules {
        if let Some(filter) = selector_to_filter(entry.selector, primary_key) {
            negated_filters.push(negate_filter(&filter));

            // Sub-query for the boosted / buried documents.
            let mut sub_query = query.clone();
            combine_filters(&mut sub_query.filter, filter);
            let weighted = *original_weight * entry.factor;
            sub_query.federation_options.get_or_insert_default().weight =
                Weight::try_from(weighted).unwrap_or_default();
            result.push(sub_query);
        }
    }

    // Base sub-query: excludes all boosted/buried documents, keeps
    // original weight.
    query.federation_options.get_or_insert_default().weight = original_weight;
    for negated in negated_filters {
        combine_filters(&mut query.filter, negated);
    }

    result
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
            ranking_score: Some(1f64),
            ranking_score_details: None,
        }
    }

    fn ctx(query_is_empty: bool, pk: Option<&str>) -> DynamicSearchContext<'_> {
        DynamicSearchContext { query_is_empty, index_uid: "movies", primary_key: pk }
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
    fn pins_extracts_pin_actions() {
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
        let pins = active
            .positioning_rules()
            .iter()
            .filter(|rule| selector_matches_index_uid(rule.selector, "movies"))
            .map(|rule| (rule.position, rule.selector.id.as_ref().unwrap().as_str()))
            .collect::<Vec<_>>();

        assert_eq!(pins, vec![(0, "3")]);
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
                    action: Action::Bury(BuryArgs { score: 10f64 }),
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

        let mut low_priority = make_rule(
            "pin-low",
            vec![RuleAction {
                selector: Selector { index_uid: None, id: Some("3".into()), filter: None },
                action: Action::Pin(PinArgs { position: 2 }),
            }],
        );
        low_priority.priority = Some(100);
        rules.insert("pin-low".into(), low_priority);

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
                selector: Selector { index_uid: None, id: Some("3".into()), filter: None },
                action: Action::Pin(PinArgs { position: 2 }),
            }],
        );
        rules.insert("pin-none".into(), no_priority);

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
        let pins = active
            .positioning_rules_for_index_uid("movies")
            .iter()
            .map(|rule| (rule.position, rule.selector.id.as_ref().unwrap().as_str()))
            .collect::<Vec<_>>();

        assert_eq!(pins, vec![(0, "3")]);
    }

    #[test]
    fn pins_filters_by_index_uid() {
        let mut rules = DynamicSearchRules::new();
        rules.insert(
            "pin-other".into(),
            make_rule(
                "pin-other",
                vec![RuleAction {
                    selector: Selector {
                        index_uid: Some("other-index".into()),
                        id: Some("1".into()),
                        filter: None,
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
                    selector: Selector {
                        index_uid: Some("movies".into()),
                        id: Some("2".into()),
                        filter: None,
                    },
                    action: Action::Pin(PinArgs { position: 1 }),
                }],
            ),
        );

        let ctx = ctx(false, Some("id"));
        let active = active_rules(&rules, &ctx);
        let pins = active
            .positioning_rules()
            .iter()
            .filter(|rule| selector_matches_index_uid(rule.selector, "movies"))
            .map(|rule| (rule.position, rule.selector.id.as_ref().unwrap().as_str()))
            .collect::<Vec<_>>();

        assert_eq!(pins, vec![(1, "2")]);
    }

    #[test]
    fn collect_boosts_and_buries_returns_matching_entries() {
        let mut rules = DynamicSearchRules::new();
        rules.insert(
            "boost-nike".into(),
            make_rule(
                "boost-nike",
                vec![RuleAction {
                    selector: Selector {
                        index_uid: None,
                        id: None,
                        filter: Some(json!({"attribute": "brand", "op": "eq", "value": "Nike"})),
                    },
                    action: Action::Boost(BoostArgs { score: 2.0 }),
                }],
            ),
        );
        rules.insert(
            "bury-clearance".into(),
            make_rule(
                "bury-clearance",
                vec![RuleAction {
                    selector: Selector {
                        index_uid: None,
                        id: None,
                        filter: Some(
                            json!({"attribute": "category", "op": "eq", "value": "clearance"}),
                        ),
                    },
                    action: Action::Bury(BuryArgs { score: 0.3 }),
                }],
            ),
        );

        let ctx = ctx(false, Some("id"));
        let active = active_rules(&rules, &ctx);
        let entries = active
            .relevance_tuning_rules
            .iter()
            .filter(|rule| selector_matches_index_uid(rule.selector, "movies"))
            .collect::<Vec<_>>();

        assert_eq!(entries.len(), 2);
        assert!((entries[0].factor - 2.0).abs() < f64::EPSILON);
        assert!((entries[1].factor - 0.3).abs() < f64::EPSILON);
    }

    #[test]
    fn collect_boosts_and_buries_filters_by_index_uid() {
        let mut rules = DynamicSearchRules::new();
        rules.insert(
            "boost-books".into(),
            make_rule(
                "boost-books",
                vec![RuleAction {
                    selector: Selector {
                        index_uid: Some("books".into()),
                        id: Some("42".into()),
                        filter: None,
                    },
                    action: Action::Boost(BoostArgs { score: 3.0 }),
                }],
            ),
        );

        let ctx = ctx(false, Some("id"));
        let active = active_rules(&rules, &ctx);

        let movies_rules = active
            .relevance_tuning_rules
            .iter()
            .filter(|rule| selector_matches_index_uid(rule.selector, "movies"))
            .collect::<Vec<_>>();

        let books_rules = active
            .relevance_tuning_rules
            .iter()
            .filter(|rule| selector_matches_index_uid(rule.selector, "books"))
            .collect::<Vec<_>>();

        assert_eq!(movies_rules.len(), 0);
        assert_eq!(books_rules.len(), 1);
    }

    #[test]
    fn selector_to_filter_id_only() {
        let selector = Selector { index_uid: None, id: Some("42".into()), filter: None };
        let id = "id".to_owned();
        let result = selector_to_filter(&selector, Some(&id));
        assert_eq!(result, Some(Value::String("id = '42'".into())));
    }

    #[test]
    fn selector_to_filter_attribute_only() {
        let selector = Selector {
            index_uid: None,
            id: None,
            filter: Some(json!({"attribute": "brand", "op": "eq", "value": "Nike"})),
        };
        let id = "id".to_owned();
        let result = selector_to_filter(&selector, Some(&id));
        assert_eq!(result, Some(Value::String("brand = 'Nike'".into())));
    }

    #[test]
    fn selector_to_filter_id_and_attribute() {
        let selector = Selector {
            index_uid: None,
            id: Some("42".into()),
            filter: Some(json!({"attribute": "brand", "op": "eq", "value": "Nike"})),
        };
        let id = "movieId".to_owned();
        let result = selector_to_filter(&selector, Some(&id));
        assert_eq!(
            result,
            Some(Value::Array(vec![
                Value::String("movieId = '42'".into()),
                Value::String("brand = 'Nike'".into()),
            ]))
        );
    }

    #[test]
    fn selector_to_filter_no_match() {
        let selector = Selector { index_uid: None, id: None, filter: None };
        let id = "id".to_owned();
        assert_eq!(selector_to_filter(&selector, Some(&id)), None);
    }

    #[test]
    fn negate_filter_simple_string() {
        let filter = Value::String("brand = 'Nike'".into());
        assert_eq!(negate_filter(&filter), Value::String("NOT (brand = 'Nike')".into()));
    }

    #[test]
    fn negate_filter_array() {
        let filter = Value::Array(vec![
            Value::String("id = '42'".into()),
            Value::String("brand = 'Nike'".into()),
        ]);
        let negated = negate_filter(&filter);
        assert_eq!(
            negated,
            Value::Array(vec![
                Value::String("NOT (id = '42')".into()),
                Value::String("NOT (brand = 'Nike')".into()),
            ])
        );
    }

    fn make_query(index_uid: &str) -> SearchQueryWithIndex {
        use super::super::{
            SearchQueryWithIndex, DEFAULT_CROP_LENGTH, DEFAULT_CROP_MARKER,
            DEFAULT_HIGHLIGHT_POST_TAG, DEFAULT_HIGHLIGHT_PRE_TAG,
        };
        SearchQueryWithIndex {
            index_uid: index_uid.to_string().try_into().unwrap(),
            q: None,
            vector: None,
            media: None,
            hybrid: None,
            offset: None,
            limit: None,
            page: None,
            hits_per_page: None,
            attributes_to_retrieve: None,
            retrieve_vectors: false,
            attributes_to_crop: None,
            crop_length: DEFAULT_CROP_LENGTH(),
            attributes_to_highlight: None,
            show_ranking_score: false,
            show_ranking_score_details: false,
            show_performance_details: None,
            use_network: None,
            show_matches_position: false,
            filter: None,
            sort: None,
            distinct: None,
            facets: None,
            highlight_pre_tag: DEFAULT_HIGHLIGHT_PRE_TAG(),
            highlight_post_tag: DEFAULT_HIGHLIGHT_POST_TAG(),
            crop_marker: DEFAULT_CROP_MARKER(),
            matching_strategy: Default::default(),
            attributes_to_search_on: None,
            ranking_score_threshold: None,
            locales: None,
            personalize: None,
            federation_options: None,
        }
    }

    #[test]
    fn expand_queries_single_boost_rule() {
        let mut rules = DynamicSearchRules::new();
        rules.insert(
            "boost-nike".into(),
            make_rule(
                "boost-nike",
                vec![RuleAction {
                    selector: Selector {
                        index_uid: None,
                        id: None,
                        filter: Some(json!({"attribute": "brand", "op": "eq", "value": "Nike"})),
                    },
                    action: Action::Boost(BoostArgs { score: 2.0 }),
                }],
            ),
        );

        let ctx = ctx(false, Some("id"));
        let active = active_rules(&rules, &ctx);

        let mut query = make_query("movies");
        let expanded = expand_query_with_relevance_tuning(&mut query, &active, Some("id"));

        // should produce 1 boost sub-query
        assert_eq!(expanded.len(), 1);

        // boosted sub-query with weight 2.0
        let boost_q = &expanded[0];
        assert!(boost_q.filter.is_some());
        let w = *boost_q.federation_options.as_ref().unwrap().weight;
        assert!((w - 2.0).abs() < f64::EPSILON);

        // base query with weight 1.0 and negated filter
        assert!(query.filter.is_some());
        let w = *query.federation_options.as_ref().unwrap().weight;
        assert!((w - 1.0).abs() < f64::EPSILON);
    }

    #[test]
    fn expand_queries_no_rules_passthrough() {
        let rules = DynamicSearchRules::new();
        let ctx = ctx(false, Some("id"));
        let active = active_rules(&rules, &ctx);
        let mut query = make_query("movies");
        let expanded = expand_query_with_relevance_tuning(&mut query, &active, None);

        // no rules so no expanded query
        assert!(expanded.is_empty());
    }
}
