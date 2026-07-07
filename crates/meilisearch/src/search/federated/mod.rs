mod network;
mod perform;
pub mod proxy;
pub mod types;
pub mod weighted_scores;

pub use network::Partition;
pub use network::ProxyQuery;
pub use perform::perform_federated_search;
pub use types::{
    FederatedSearch, FederatedSearchResult, Federation, FederationOptions, MergeFacets,
    ShowFederationInfo,
};
