pub mod data;
pub mod error;
pub mod metrics;
pub mod network;
pub mod storage;

pub mod raftproto {
    tonic::include_proto!("raft");
}

use actix::*;
use actix_raft::Raft;
use actix_raft::config::Config;
use data::{Data, DataResponse};
use error::Error;
use metrics::AppMetrics;
use network::AppNetwork;
use storage::AppStorage;
use raftproto::*;

/// A type alias used to define an application's concrete Raft type.
pub type AppRaft = Raft<Data, DataResponse, Error, AppNetwork, AppStorage>;

struct Peer {

    // app: AppRaft,
}

impl Peer {
    fn new() -> Peer {
        // Build the actix system.
        let sys = actix::System::new("meilisearch-raft");

        // Build the needed runtime config for Raft specifying where
        // snapshots will be stored. See the storage chapter for more details.
        let config = Config::build(String::from("/tmp/snapshots")).validate().unwrap();

        // Start off with just a single node in the cluster. Applications
        // should implement their own discovery system. See the cluster
        // formation chapter for more details.
        let members = vec![1];

        // Start the various actor types and hold on to their addrs.
        let network = AppNetwork::start_default();
        let storage = AppStorage::start_default();
        let metrics = AppMetrics::start_default();
        let app_raft = AppRaft::new(1, config, network, storage, metrics.recipient()).start();

        // Run the actix system. Unix signals for termination &
        // graceful shutdown are automatically handled.
        let _ = sys.run();

        return Peer {
            // app: app_raft
        }
    }
}
