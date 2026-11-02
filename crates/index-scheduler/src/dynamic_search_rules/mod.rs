use meilisearch_types::dynamic_search_rules::{DynamicSearchRule, RuleUid};
use meilisearch_types::index_uid::DsrIndex;
use meilisearch_types::milli::FaultSource;

use crate::{Error, IndexScheduler, Result};

pub struct DynamicSearchRules<'a> {
    index_scheduler: &'a IndexScheduler,
}

impl<'a> DynamicSearchRules<'a> {
    // not fetching features in index_scheduler so that the caller can pass features instantiated once per request
    pub fn new(index_scheduler: &'a IndexScheduler) -> Self {
        Self { index_scheduler }
    }

    pub fn get(&self, rule_uid: &RuleUid) -> Result<Option<DynamicSearchRule>> {
        let from_milli = |err| Error::from_milli(err, None);
        let Some(dsrs) = self.milli_dsrs()? else { return Ok(None) };

        let Some(doc) = dsrs.get(rule_uid.as_str()).map_err(from_milli)? else { return Ok(None) };

        Ok(Some(
            DynamicSearchRule::try_from_meili_doc(doc, FaultSource::Runtime).map_err(from_milli)?,
        ))
    }

    pub fn milli_dsrs(
        &self,
    ) -> Result<Option<meilisearch_types::milli::dynamic_search_rules::DynamicSearchRules>> {
        let from_milli = |err| Error::from_milli(err, None);
        let rtxn = self.index_scheduler.read_txn()?;
        let index = match self.index_scheduler.index_mapper.index(&rtxn, DsrIndex) {
            Ok(index) => index,
            Err(crate::Error::IndexNotFound(_)) => return Ok(None),
            Err(err) => return Err(err),
        };
        drop(rtxn);

        Ok(Some(
            meilisearch_types::milli::dynamic_search_rules::DynamicSearchRules::new(index)
                .map_err(from_milli)?,
        ))
    }
}
