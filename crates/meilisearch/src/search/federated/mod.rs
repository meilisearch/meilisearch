mod perform;
mod proxy;
mod types;
mod weighted_scores;

pub use perform::perform_federated_search;
pub use proxy::{PROXY_SEARCH_HEADER, PROXY_SEARCH_HEADER_VALUE};
pub use types::{
    FederatedSearch, FederatedSearchResult, Federation, FederationOptions, MergeFacets,
};
