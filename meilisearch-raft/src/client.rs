use std::collections::HashMap;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use grpcio::{ChannelBuilder, EnvBuilder};
use log::*;
use raft::eraftpb::{ConfChange, ConfChangeType};

use crate::proto::indexpb_grpc::IndexClient;
use crate::proto::indexrpcpb::*;

pub fn create_client(addr: &str) -> IndexClient {
    let env = Arc::new(EnvBuilder::new().build());
    let ch = ChannelBuilder::new(env).connect(&addr);
    debug!("create channel for {}", addr);
    let index_client = IndexClient::new(ch);
    debug!("create index client for {}", addr);
    index_client
}

pub struct Clerk {
    servers: Vec<IndexClient>,
    leader_id: usize,
    max_retry_count: usize,
}

impl Clerk {
    pub fn new(servers: Vec<IndexClient>) -> Clerk {
        Clerk {
            servers,
            leader_id: 0,
            max_retry_count: 5,
        }
    }

    pub fn join(&mut self, id: u64, ip: &str, port: u16) {
        self.join_with_retry(id, ip, port, 1, Duration::from_millis(100))
    }

    pub fn join_with_retry(
        &mut self,
        id: u64,
        ip: &str,
        port: u16,
        max_retry: usize,
        duration: Duration,
    ) {
        let mut cc = ConfChange::new();
        cc.set_id(id);
        cc.set_node_id(id);
        cc.set_change_type(ConfChangeType::AddNode);

        let mut cc_req = ConfChangeReq::new();
        cc_req.set_cc(cc.clone());
        cc_req.set_ip(ip.to_string());
        cc_req.set_port(port as u32);

        let mut request_count: usize = 0;
        loop {
            if request_count >= max_retry {
                let msg = "exceeded max retry count";
                debug!("{}", msg);

                let mut ret = HashMap::new();
                ret.insert("error", msg.to_string());

                return;
            }

            let reply = self.servers[self.leader_id]
                .raft_conf_change(&cc_req)
                .unwrap_or_else(|e| {
                    let msg = format!("{:?}", e);
                    error!("{:?}", msg);

                    let mut resp = RaftDone::new();
                    resp.set_err(RespErr::ErrWrongLeader);
                    resp
                });
            match reply.err {
                RespErr::OK => return,
                err => error!("failed to add node - {:?} err: {:?}", cc, err),
            }

            self.leader_id = (self.leader_id + 1) % self.servers.len();
            request_count += 1;
            debug!("{}", "retry to add node");

            thread::sleep(duration);
        }
    }

    pub fn leave(&mut self, id: u64) {
        let mut cc = ConfChange::new();
        cc.set_id(id);
        cc.set_node_id(id);
        cc.set_change_type(ConfChangeType::RemoveNode);
        let mut cc_req = ConfChangeReq::new();
        cc_req.set_cc(cc);

        let mut request_count: usize = 0;
        loop {
            if request_count > self.max_retry_count {
                let msg = "exceeded max retry count";
                debug!("{}", msg);

                return;
            }

            let reply = self.servers[self.leader_id]
                .raft_conf_change(&cc_req)
                .unwrap_or_else(|e| {
                    let msg = format!("{:?}", e);
                    error!("{:?}", msg);

                    let mut resp = RaftDone::new();
                    resp.set_err(RespErr::ErrWrongLeader);
                    resp
                });
            match reply.err {
                RespErr::OK => return,
                _ => error!("failed to delete from the cluster"),
            }

            self.leader_id = (self.leader_id + 1) % self.servers.len();
            request_count += 1;
            debug!("{}", "retry to add node");

            thread::sleep(Duration::from_millis(100));
        }
    }
}
