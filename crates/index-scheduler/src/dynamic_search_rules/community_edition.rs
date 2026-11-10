use meilisearch_types::dynamic_search_rules::{DynamicSearchRuleTarget, RuleUid, Targets};

use crate::{Error, Result};

use super::DynamicSearchRules;

impl<'a> DynamicSearchRules<'a> {
    pub fn rule_target(
        &self,
        _target_rule_uid: &RuleUid,
    ) -> Result<Option<DynamicSearchRuleTarget>> {
        Err(Error::RequiresEnterpriseEdition { action: "fetching the target of a rule" })
    }

    pub fn targets(&self) -> Result<Targets> {
        Err(Error::RequiresEnterpriseEdition {
            action: "fetching the target state of the dynamic search rules",
        })
    }
}
