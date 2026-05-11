mod network;
mod perform;
pub mod proxy;
mod types;
mod weighted_scores;

pub use network::Partition;
pub use perform::perform_federated_search;
pub use types::{
    FederatedSearch, FederatedSearchResult, Federation, FederationOptions, MergeFacets,
};
