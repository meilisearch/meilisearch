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
    client_id: u64,
    request_seq: u64,
    leader_id: usize,
    max_retry_count: usize,
}

impl Clerk {
    pub fn new(servers: &Vec<IndexClient>, client_id: u64) -> Clerk {
        Clerk {
            servers: servers.clone(),
            client_id,
            request_seq: 0,
            leader_id: 0,
            max_retry_count: 5,
        }
    }

    pub fn join(&mut self, id: u64, ip: &str, port: u16) {
        self.join_with_retry(id, ip, port, 5, Duration::from_millis(100))
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
        cc_req.set_cc(cc);
        cc_req.set_ip(ip.to_string());
        cc_req.set_port(port as u32);

        let mut request_count: usize = 0;
        loop {
            if request_count > max_retry {
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
                _ => error!("failed to add node"),
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

    pub fn probe(&mut self) -> String {
        let mut req = ProbeReq::new();
        req.set_client_id(self.client_id);
        req.set_seq(self.request_seq);
        self.request_seq += 1;

        let mut request_count: usize = 0;
        loop {
            if request_count > self.max_retry_count {
                let msg = "exceeded max retry count";
                debug!("{}", msg);

                let mut ret = HashMap::new();
                ret.insert("error", msg.to_string());

                return serde_json::to_string(&ret).unwrap();
            }

            let reply = self.servers[self.leader_id]
                .probe(&req)
                .unwrap_or_else(|e| {
                    let msg = format!("{:?}", e);
                    error!("{:?}", msg);

                    let mut ret = HashMap::new();
                    ret.insert("error", msg.to_string());
                    ret.insert("health", "NG".to_string());

                    let mut resp = ProbeResp::new();
                    resp.set_err(RespErr::ErrProbeFailed);
                    resp.set_value(serde_json::to_string(&ret).unwrap());
                    resp
                });
            match reply.err {
                RespErr::OK => return reply.value,
                _ => error!("failed to probe node"),
            }

            self.leader_id = (self.leader_id + 1) % self.servers.len();
            request_count += 1;
            debug!("{}", "retry to probe node");

            thread::sleep(Duration::from_millis(100));
        }
    }

    pub fn peers(&mut self) -> String {
        let mut req = PeersReq::new();
        req.set_client_id(self.client_id);
        req.set_seq(self.request_seq);
        self.request_seq += 1;

        let mut request_count: usize = 0;
        loop {
            if request_count > self.max_retry_count {
                let msg = "exceeded max retry count";
                debug!("{}", msg);

                let mut ret = HashMap::new();
                ret.insert("error", msg.to_string());

                return serde_json::to_string(&ret).unwrap();
            }

            let reply = self.servers[self.leader_id]
                .peers(&req)
                .unwrap_or_else(|e| {
                    let msg = format!("{:?}", e);
                    error!("{:?}", msg);

                    let mut ret = HashMap::new();
                    ret.insert("error", msg.to_string());

                    let mut resp = PeersResp::new();
                    resp.set_err(RespErr::ErrPeerFailed);
                    resp.set_value(serde_json::to_string(&ret).unwrap());
                    resp
                });
            match reply.err {
                RespErr::OK => return reply.value,
                _ => error!("failed to get peers"),
            }

            self.leader_id = (self.leader_id + 1) % self.servers.len();
            request_count += 1;
            debug!("{}", "retry to get peers");

            thread::sleep(Duration::from_millis(100));
        }
    }

    pub fn get(&mut self, doc_id: &str) -> String {
        let mut req = GetReq::new();
        req.set_client_id(self.client_id);
        req.set_seq(self.request_seq);
        req.set_doc_id(doc_id.to_owned());
        self.request_seq += 1;

        let mut request_count: usize = 0;
        loop {
            if request_count > self.max_retry_count {
                let msg = "exceeded max retry count";
                debug!("{}", msg);

                let mut ret = HashMap::new();
                ret.insert("error", msg.to_string());

                return serde_json::to_string(&ret).unwrap();
            }

            let reply = self.servers[self.leader_id].get(&req).unwrap_or_else(|e| {
                let msg = format!("{:?}", e);
                error!("{:?}", msg);

                let mut ret = HashMap::new();
                ret.insert("error", msg.to_string());

                let mut resp = GetResp::new();
                resp.set_err(RespErr::ErrGetFailed);
                resp.set_value(serde_json::to_string(&ret).unwrap());
                resp
            });
            match reply.err {
                RespErr::OK => return reply.value,
                _ => error!("failed to get document"),
            }

            self.leader_id = (self.leader_id + 1) % self.servers.len();
            request_count += 1;
            debug!("{}", "retry to get document");

            thread::sleep(Duration::from_millis(100));
        }
    }

    pub fn put(&mut self, doc_id: &str, fields: &str) -> String {
        let mut put_req = PutReq::new();
        put_req.set_client_id(self.client_id);
        put_req.set_seq(self.request_seq);
        put_req.set_doc_id(doc_id.to_owned());
        put_req.set_fields(fields.to_owned());

        let mut req = ApplyReq::new();
        req.set_client_id(self.client_id);
        req.set_req_type(ReqType::Put);
        req.set_put_req(put_req);

        self.request_seq += 1;

        let mut request_count: usize = 0;
        loop {
            if request_count > self.max_retry_count {
                let msg = "exceeded max retry count";
                debug!("{}", msg);

                let mut ret = HashMap::new();
                ret.insert("error", msg.to_string());

                return serde_json::to_string(&ret).unwrap();
            }

            let reply = self.servers[self.leader_id].put(&req).unwrap_or_else(|e| {
                let msg = format!("{:?}", e);
                error!("{:?}", msg);

                let mut ret = HashMap::new();
                ret.insert("error", msg.to_string());

                let mut resp = PutResp::new();
                resp.set_err(RespErr::ErrPutFailed);
                resp.set_value(serde_json::to_string(&ret).unwrap());
                resp
            });
            match reply.err {
                RespErr::OK => return reply.value,
                _ => error!("failed to put document"),
            }

            self.leader_id = (self.leader_id + 1) % self.servers.len();
            request_count += 1;
            debug!("{}", "retry to put document");

            thread::sleep(Duration::from_millis(100));
        }
    }
}
