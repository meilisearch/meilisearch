use std::net::ToSocketAddrs;

use ductile::{connect_channel, ChannelReceiver, ChannelSender};
use meilisearch_types::tasks::KindWithContent;
use serde::{Deserialize, Serialize};

mod leader;

pub use leader::Leader;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Network issue occured")]
    NetworkIssue,
    #[error("Internal error: {0}")]
    SerdeJson(#[from] serde_json::Error),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum LeaderMsg {
    // Starts a new batch
    StartBatch { id: u32, batch: Vec<u32> },
    // Tell the follower to commit the update asap
    Commit(u32),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FollowerMsg {
    // Let the leader knows you're ready to commit
    ReadyToCommit(u32),
    RegisterNewTask(KindWithContent),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Consistency {
    Zero,
    One,
    Two,
    Quorum,
    All,
}

pub struct Follower {
    sender: ChannelSender<FollowerMsg>,
    receiver: ChannelReceiver<LeaderMsg>,
    batch_id: u32,
}

impl Follower {
    pub fn join(leader: impl ToSocketAddrs) -> Follower {
        let (sender, receiver) = connect_channel(leader).unwrap();
        Follower { sender, receiver, batch_id: 0 }
    }

    pub fn get_new_batch(&mut self) -> Vec<u32> {
        loop {
            match self.receiver.recv() {
                Ok(LeaderMsg::StartBatch { id, batch }) if id == self.batch_id => {
                    self.batch_id = id;
                    break batch;
                }
                Err(_) => log::error!("lost connection to the leader"),
                _ => (),
            }
        }
    }

    pub fn ready_to_commit(&mut self) {
        self.sender.send(FollowerMsg::ReadyToCommit(self.batch_id)).unwrap();

        loop {
            match self.receiver.recv() {
                Ok(LeaderMsg::Commit(id)) if id == self.batch_id => break,
                Err(_) => panic!("lost connection to the leader"),
                _ => (),
            }
        }
    }
}
