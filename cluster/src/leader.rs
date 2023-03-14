use std::net::ToSocketAddrs;
use std::time::Duration;

use ductile::{ChannelReceiver, ChannelSender, ChannelServer};
use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::{Consistency, Error, FollowerMsg, LeaderMsg};

pub struct Leader {
    listener: ChannelServer<LeaderMsg, FollowerMsg>,
    active_followers: Vec<Follower>,
    new_followers: Vec<Follower>,
    dead_followers: Vec<Follower>,

    batch_id: u32,
    tick: Duration,
}

struct Follower {
    sender: ChannelSender<LeaderMsg>,
    receiver: ChannelReceiver<FollowerMsg>,
}

impl Leader {
    pub fn new(listen_on: impl ToSocketAddrs) -> Leader {
        let listener = ChannelServer::bind(listen_on).unwrap();

        Leader {
            listener,
            active_followers: Vec::new(),
            new_followers: Vec::new(),
            dead_followers: Vec::new(),
            batch_id: 0,
            tick: Duration::new(1, 0),
        }
    }

    pub fn starts_batch(&mut self, batch: Vec<u32>) -> Result<(), Error> {
        let mut dead_nodes = Vec::new();

        for (idx, follower) in self.active_followers.iter_mut().enumerate() {
            match follower
                .sender
                .send(LeaderMsg::StartBatch { id: self.batch_id, batch: batch.clone() })
            {
                Ok(_) => (),
                // if a node can't be joined we consider it as dead
                Err(_) => dead_nodes.push(idx),
            }
        }

        // we do it from the end so the indices stays correct while removing elements
        for dead_node in dead_nodes.into_iter().rev() {
            let dead = self.active_followers.swap_remove(dead_node);
            self.dead_followers.push(dead);
        }

        Ok(())
    }

    pub fn commit(&mut self, consistency_level: Consistency) -> Result<(), Error> {
        let mut dead_nodes = Vec::new();
        let mut ready_to_commit = 0;
        // get the size of the cluster to compute what a quorum means
        // it's mutable because if followers die we must remove them
        // from the quorum
        let mut cluster_size = self.active_followers.len();

        // wait till enough nodes are ready to commit
        for (idx, follower) in self.active_followers.iter_mut().enumerate() {
            match consistency_level {
                Consistency::Zero => break,
                Consistency::One if ready_to_commit >= 1 => break,
                Consistency::Two if ready_to_commit >= 2 => break,
                Consistency::Quorum if ready_to_commit >= (cluster_size / 2) => break,
                _ => (),
            }
            match follower.receiver.recv() {
                Ok(FollowerMsg::ReadyToCommit(id)) if id == self.batch_id => ready_to_commit += 1,
                Ok(FollowerMsg::RegisterNewTask(_)) => log::warn!("Missed a task"),
                Ok(_) => (),
                // if a node can't be joined we consider it as dead
                Err(_) => {
                    dead_nodes.push(idx);
                    cluster_size -= 1
                }
            }
        }

        let dn = dead_nodes.clone();
        for (idx, follower) in
            self.active_followers.iter_mut().enumerate().filter(|(i, _)| !dn.contains(i))
        {
            match follower.sender.send(LeaderMsg::Commit(self.batch_id)) {
                Ok(_) => (),
                Err(_) => dead_nodes.push(idx),
            }
        }

        // we do it from the end so the indices stays correct while removing elements
        for dead_node in dead_nodes.into_iter().rev() {
            let dead = self.active_followers.swap_remove(dead_node);
            self.dead_followers.push(dead);
        }

        self.batch_id += 1;

        Ok(())
    }
}
