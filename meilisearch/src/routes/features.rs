#[derive(Debug, Clone, Copy)]
pub struct RouteFeatures {
    pub score_details: bool,
}

#[derive(Debug, thiserror::Error)]
#[error("{disabled_action} requires passing the `{flag}` command-line option. See {issue_link}")]
pub struct FeatureNotEnabledError {
    disabled_action: &'static str,
    flag: &'static str,
    issue_link: &'static str,
}

impl RouteFeatures {
    pub fn check_score_details(&self) -> Result<(), FeatureNotEnabledError> {
        if self.score_details {
            Ok(())
        } else {
            Err(FeatureNotEnabledError {
                disabled_action: "Computing score details",
                flag: "--experimental-score-details",
                issue_link: "https://github.com/meilisearch/product/discussions/674",
            })
        }
    }
}
