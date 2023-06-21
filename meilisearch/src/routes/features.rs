use crate::Opt;

#[derive(Debug, Clone, Copy)]
pub struct RouteFeatures {
    score_details: bool,
    metrics: bool,
}

#[derive(Debug, thiserror::Error)]
#[error("{disabled_action} requires passing the `{flag}` command-line option. See {issue_link}")]
pub struct FeatureNotEnabledError {
    disabled_action: &'static str,
    flag: &'static str,
    issue_link: &'static str,
}

impl RouteFeatures {
    pub fn from_options(options: &Opt) -> Self {
        Self {
            score_details: options.experimental_score_details,
            metrics: options.experimental_enable_metrics,
        }
    }

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

    pub fn check_metrics(&self) -> Result<(), FeatureNotEnabledError> {
        if self.metrics {
            Ok(())
        } else {
            Err(FeatureNotEnabledError {
                disabled_action: "Getting metrics",
                flag: "--experimental-enable-metrics",
                issue_link: "https://github.com/meilisearch/meilisearch/discussions/3518",
            })
        }
    }
}
