use std::thread;
use std::time::Duration;

use log::*;
use tonic::transport::*;

use indexpb::index_client::IndexClient;
use indexpb::*;

pub mod indexpb {
    tonic::include_proto!("indexpb");
}

pub async fn create_client(addr: &str) -> IndexClient<Channel> {
    IndexClient::connect(addr.to_owned()).await.unwrap()
}

pub struct Clerk {
    servers: Vec<IndexClient<Channel>>,
    leader_id: usize,
    max_retry_count: usize,
    retry_duration: Duration,
}

impl Clerk {
    pub fn new(servers: Vec<IndexClient<Channel>>) -> Clerk {
        Clerk {
            servers,
            leader_id: 0,
            max_retry_count: 5,
            retry_duration: Duration::from_millis(100),
        }
    }

    pub async fn join(&mut self, id: u64, ip: &str, port: u16) {
        let conf_change = ConfChange {
            change_type: ConfChangeType::AddNode as i32,
            node_id: id,
            context: Vec::new(), //Unused
            id: id,
        };

        let conf_change_request = ConfChangeReq {
            cc: Some(conf_change.clone()),
            ip: ip.to_owned(),
            port: port as u32,
        };

        let mut request_count: usize = 0;
        loop {
            if request_count >= self.max_retry_count {
                debug!("exceeded max retry count");
                return;
            }

            let request = tonic::Request::new(conf_change_request.clone());

            self.servers[self.leader_id]
                .raft_conf_change(request).await
                .map(|r| {
                    match r.into_inner().err {
                        0 => return, // Success
                        err => error!("failed to add node - {:?} err: {:?}", conf_change, err),
                    }
                })
                .unwrap_or_else(|e| {
                    error!("{:?}", e);
                });

            self.leader_id = (self.leader_id + 1) % self.servers.len();
            request_count += 1;
            debug!("{}", "retry to add node");

            thread::sleep(self.retry_duration);
        }
    }

    pub async fn leave(&mut self, id: u64) {
        let conf_change = ConfChange {
            change_type: ConfChangeType::RemoveNode as i32,
            node_id: id,
            context: Vec::new(), //Unused
            id: id,
        };

        let conf_change_request = ConfChangeReq {
            cc: Some(conf_change),
            ip: String::from(""), //Unused
            port: 0, //Unused
        };

        let mut request_count: usize = 0;
        loop {
            if request_count > self.max_retry_count {
                debug!("exceeded max retry count");
                return;
            }

            let request = tonic::Request::new(conf_change_request.clone());

            self.servers[self.leader_id]
                .raft_conf_change(request).await
                .map(|r| {
                    match r.into_inner().err {
                        0 => return, // Success
                        _ => error!("failed to delete from the cluster"),
                    }
                })
                .unwrap_or_else(|e| {
                    error!("{:?}", e);
                });

            self.leader_id = (self.leader_id + 1) % self.servers.len();
            request_count += 1;
            debug!("{}", "retry to add node");

            thread::sleep(self.retry_duration);
        }
    }
}
