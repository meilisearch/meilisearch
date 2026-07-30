/// name of the field holding the primary key in a DSR object.
///
/// - also present for the special "metadata" document.
pub const UID: &str = "uid";
/// name of the field indicating if the rule is active or not.
pub const ACTIVE: &str = "active";
/// name of the field holding the precedence of the rule.
pub const PRECEDENCE: &str = "precedence";
/// name of the field holding the human-readable description of the rule.
pub const DESCRIPTION: &str = "description";
/// name of the field holding the actions of the rule.
pub const ACTIONS: &str = "actions";
/// name of the field holding the last update time of the rule.
pub const LAST_UPDATED_AT: &str = "lastUpdatedAt";
/// name of the field holding the conditions of the rule.
pub const CONDITIONS: &str = "conditions";

/// last segment of conditions.filter field.
pub const FILTER: &str = "filter";
/// last segment of conditions.filter.nbConstraints field.
pub const NB_CONSTRAINTS: &str = "nbConstraints";

/// full path of the subfield holding the start time condition
pub const CONDITIONS_TIME_START: &str = "conditions.time.start";
/// full path of the subfield holding the end time condition
pub const CONDITIONS_TIME_END: &str = "conditions.time.end";
/// full path of the subfield holding whether a query should be empty.
pub const CONDITIONS_QUERY_IS_EMPTY: &str = "conditions.query.isEmpty";
/// full path of the subfield holding the condition on which words a query should contain.
pub const CONDITIONS_QUERY_WORDS: &str = "conditions.query.words";
/// full path of the subfield holding the number of filter constraints for this rule.
pub const CONDITIONS_FILTER_NB_CONSTRAINTS: &str = "conditions.filter.nbConstraints";
/// full path of the subfield holding the value for the filter constraints for this rule.
pub const CONDITIONS_FILTER_VALUES: &str = "conditions.filter.values";
