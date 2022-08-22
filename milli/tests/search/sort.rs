use big_s::S;
use milli::Criterion::{Attribute, Exactness, Proximity, Typo, Words};
use milli::{AscDesc, Error, Member, Search, TermsMatchingStrategy, UserError};

use crate::search::{self, EXTERNAL_DOCUMENTS_IDS};

#[test]
fn sort_ranking_rule_missing() {
    let criteria = vec![Words, Typo, Proximity, Attribute, Exactness];
    // sortables: `tag` and `asc_desc_rank`
    let index = search::setup_search_index_with_criteria(&criteria);
    let rtxn = index.read_txn().unwrap();

    let mut search = Search::new(&rtxn, &index);
    search.query(search::TEST_QUERY);
    search.limit(EXTERNAL_DOCUMENTS_IDS.len());
    search.authorize_typos(true);
    search.terms_matching_strategy(TermsMatchingStrategy::default());
    search.sort_criteria(vec![AscDesc::Asc(Member::Field(S("tag")))]);

    let result = search.execute();
    assert!(matches!(result, Err(Error::UserError(UserError::SortRankingRuleMissing))));
}
