use std::collections::HashMap;
use std::sync::mpsc::{self, Receiver, SyncSender};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use futures::Future;
use grpcio::{ChannelBuilder, EnvBuilder, Environment, RpcContext, ServerBuilder, UnarySink};
use log::*;
use protobuf::Message as ProtoMessage;
use raft::eraftpb::{ConfChange, ConfChangeType, Entry, EntryType, Message};

use crate::client::{create_client, Clerk};
use crate::peer::{Peer, PeerMessage};
use crate::proto::indexpb_grpc::{self, Index as IndexService, IndexClient};
use crate::proto::indexrpcpb::*;
use crate::util;

#[derive(Debug)]
struct NotifyArgs(u64, String, RespErr);

#[derive(Clone)]
pub struct IndexServer {
    id: u64,
    leader: bool,
    peers: Arc<Mutex<HashMap<u64, IndexClient>>>,
    peers_addr: Arc<Mutex<HashMap<u64, String>>>,
    rf_message_ch: SyncSender<PeerMessage>,
    notify_ch_map: Arc<Mutex<HashMap<u64, SyncSender<NotifyArgs>>>>,
    index: Arc<Mutex<HashMap<String, String>>>,
}

impl IndexServer {
    pub fn start_server(
        id: u64,
        host: &str,
        port: u16,
        peers_addr: HashMap<u64, String>,
    ) -> IndexServer {
        let mut peers = HashMap::new();
        peers.insert(id, create_client(&format!("{}:{}", host, port)));
        for (peer_id, peer_addr) in peers_addr.iter() {
            peers.insert(*peer_id, create_client(peer_addr));
        }

        // Send/Receive PeerMessage channel
        let (rf_sender, rf_receiver) = mpsc::sync_channel(100);
        // Send/Receive Message channel
        let (rpc_sender, rpc_receiver) = mpsc::sync_channel(100);
        // Send/Receive Entry channel
        let (apply_sender, apply_receiver) = mpsc::sync_channel(100);

        let mut index_server = IndexServer {
            id,
            leader: false,
            peers: Arc::new(Mutex::new(peers)),
            peers_addr: Arc::new(Mutex::new(peers_addr)),
            rf_message_ch: rf_sender,
            notify_ch_map: Arc::new(Mutex::new(HashMap::new())),
            index: Arc::new(Mutex::new(HashMap::new())),
        };

        index_server.async_rpc_sender(rpc_receiver);
        index_server.async_applier(apply_receiver);

        let env = Arc::new(Environment::new(10));
        let service = indexpb_grpc::create_index(index_server.clone());
        let mut server = ServerBuilder::new(env)
            .register_service(service)
            .bind(host, port)
            .build()
            .unwrap_or_else(|e| {
                panic!("build server error: {}", e);
            });

        thread::spawn(move || {
            server.start();
            for &(ref host, port) in server.bind_addrs() {
                info!("listening on {}:{}", host, port);
            }
            loop {}
        });

        let peer = Peer::new(id, apply_sender);
        Peer::activate(peer, rpc_sender, rf_receiver);

        let mut servers: Vec<IndexClient> = Vec::new();
        for (_, value) in index_server.peers.clone().lock().unwrap().iter() {
            servers.push(value.clone());
        }

        let mut client = Clerk::new(servers);

        client.join_with_retry(id, host, port, 10, Duration::from_secs(3));

        index_server
    }

    pub fn clerk(&self) -> Clerk {
        let mut clients = Vec::new();
        for (_, client) in self.peers.lock().unwrap().clone() {
            clients.push(client);
        }
        Clerk::new(clients)
    }

    fn async_rpc_sender(&mut self, receiver: Receiver<Message>) {
        let l = self.peers.clone();
        thread::spawn(move || loop {
            if let Ok(message) = receiver.recv() {
                let peers = l.lock().unwrap();
                let op = peers.get(&message.to);
                if let Some(c) = op {
                    let client = c.clone();
                    thread::spawn(move || {
                        client.raft(&message).unwrap_or_else(|e| {
                            error!("send raft msg to {} failed: {:?}", message.to, e);
                            RaftDone::new()
                        });
                    });
                }
            }
        });
    }

    // Send message to Peer
    fn start_op(&mut self, req: &ApplyReq) -> (RespErr, String) {
        let (sh, rh) = mpsc::sync_channel(0);
        {
            let mut map = self.notify_ch_map.lock().unwrap();
            map.insert(req.get_client_id(), sh);
        }
        self.rf_message_ch
            .send(PeerMessage::Propose(req.write_to_bytes().unwrap_or_else(
                |e| {
                    panic!("request write to bytes error: {:?}", e);
                },
            )))
            .unwrap_or_else(|e| {
                error!("send propose to raft error: {:?}", e);
            });
        match rh.recv_timeout(Duration::from_millis(1000)) {
            Ok(args) => {
                (args.2, args.1)
            }
            Err(_err) => {
                {
                    let mut map = self.notify_ch_map.lock().unwrap();
                    map.remove(&req.get_client_id());
                }
                (RespErr::ErrWrongLeader, String::from(""))
            }
        }
    }

    // TODO: check duplicate request.
    fn async_applier(&mut self, apply_receiver: Receiver<Entry>) {
        let notify_ch_map = self.notify_ch_map.clone();
        let peers = self.peers.clone();
        let peers_addr = self.peers_addr.clone();
        let index = self.index.clone();

        thread::spawn(move || loop {
            if let Ok(entry) = apply_receiver.recv() {
                match entry.get_entry_type() {
                    EntryType::EntryNormal => {
                        let result: NotifyArgs;
                        let req: ApplyReq = util::parse_data(entry.get_data());
                        let client_id = req.get_client_id();
                        if entry.data.is_empty() {
                            result = Self::apply_entry(
                                entry.term,
                                &req,
                                peers.clone(),
                                peers_addr.clone(),
                                index.clone(),
                            );
                            debug!("{:?}: {:?}", result.2, req);
                        } else {
                            result = NotifyArgs(0, String::from(""), RespErr::ErrWrongLeader);
                            debug!("{:?}", req);
                        }
                        let mut map = notify_ch_map.lock().unwrap();
                        if let Some(s) = map.get(&client_id) {
                            s.send(result).unwrap_or_else(|e| {
                                error!("notify apply result error: {:?}", e);
                            });
                        }
                        map.remove(&client_id);
                    }
                    EntryType::EntryConfChange => {
                        let result = NotifyArgs(0, String::from(""), RespErr::OK);
                        let cc: ConfChange = util::parse_data(entry.get_data());
                        let mut map = notify_ch_map.lock().unwrap();
                        if let Some(s) = map.get(&cc.get_node_id()) {
                            s.send(result).unwrap_or_else(|e| {
                                error!("notify apply result error: {:?}", e);
                            });
                        }
                        map.remove(&cc.get_node_id());
                    }
                    EntryType::EntryConfChangeV2 => {
                        unimplemented!();
                    }
                }
            }
        });
    }

    // This function is the final one where all specifi usages finished.
    fn apply_entry(
        term: u64,
        req: &ApplyReq,
        peers: Arc<Mutex<HashMap<u64, IndexClient>>>,
        peers_addr: Arc<Mutex<HashMap<u64, String>>>,
        index: Arc<Mutex<HashMap<String, String>>>,
    ) -> NotifyArgs {
        match req.req_type {
            ReqType::Join => {
                let mut prs = peers.lock().unwrap();
                let env = Arc::new(EnvBuilder::new().build());
                let ch = ChannelBuilder::new(env).connect(&req.get_join_req().peer_addr);
                prs.insert(req.get_join_req().peer_id, IndexClient::new(ch));

                let mut prs_addr = peers_addr.lock().unwrap();
                prs_addr.insert(
                    req.get_join_req().peer_id,
                    req.get_join_req().peer_addr.clone(),
                );

                NotifyArgs(term, String::from(""), RespErr::OK)
            }
            ReqType::Leave => {
                let mut prs = peers.lock().unwrap();
                prs.remove(&req.get_leave_req().peer_id);

                let mut prs_addr = peers_addr.lock().unwrap();
                prs_addr.remove(&req.get_leave_req().peer_id);

                NotifyArgs(term, String::from(""), RespErr::OK)
            }
            ReqType::Put => {
                let doc_id = req.get_put_req().get_doc_id().to_string();
                let fields = req.get_put_req().get_fields().to_string();
                index.lock().unwrap().insert(doc_id, fields);

                let mut ret = HashMap::new();
                ret.insert("opstamp", 0);

                NotifyArgs(term, serde_json::to_string(&ret).unwrap(), RespErr::OK)
            }
        }
    }

    pub fn get_peers(&mut self) -> String {
        serde_json::to_string(&self.peers_addr.lock().unwrap().clone()).unwrap()
    }

    pub fn put_data(&mut self, doc_id: &str, fields: &str) {
        let mut put_req = PutReq::new();
        put_req.set_client_id(self.id);
        put_req.set_seq(0);
        put_req.set_doc_id(doc_id.to_owned());
        put_req.set_fields(fields.to_owned());

        let mut req = ApplyReq::new();
        req.set_client_id(self.id);
        req.set_req_type(ReqType::Put);
        req.set_put_req(put_req);
        let (_err, _ret) = Self::start_op(self, &req);
    }
}

impl IndexService for IndexServer {
    fn raft(&mut self, ctx: RpcContext, req: Message, sink: UnarySink<RaftDone>) {
        self.rf_message_ch
            .send(PeerMessage::Message(req.clone()))
            .unwrap_or_else(|e| {
                error!("send message to raft error: {:?}", e);
            });

        let resp = RaftDone::new();

        ctx.spawn(
            sink.success(resp)
                .map_err(move |e| error!("failed to reply {:?}: {:?}", req, e)),
        )
    }

    fn raft_conf_change(&mut self, ctx: RpcContext, req: ConfChangeReq, sink: UnarySink<RaftDone>) {
        debug!("request: {:?}", req);

        let cc = req.cc.clone().unwrap();
        let mut resp = RaftDone::new();
        let mut apply_req = ApplyReq::new();

        match cc.change_type {
            ConfChangeType::AddNode | ConfChangeType::AddLearnerNode => {
                apply_req.set_req_type(ReqType::Join);
                let mut join_req = JoinReq::new();
                join_req.set_client_id(cc.get_node_id());
                join_req.set_peer_id(cc.get_node_id());
                join_req.set_peer_addr(format!("{}:{}", req.ip, req.port));
                apply_req.set_join_req(join_req);
            }
            ConfChangeType::RemoveNode => {
                apply_req.set_req_type(ReqType::Leave);
                let mut leave_req = LeaveReq::new();
                leave_req.set_client_id(cc.get_node_id());
                leave_req.set_peer_id(cc.get_node_id());
                leave_req.set_peer_addr(format!("{}:{}", req.ip, req.port));
                apply_req.set_leave_req(leave_req);
            }
        }

        let (err, _) = self.start_op(&apply_req);
        match err {
            RespErr::OK => {
                let (sh, rh) = mpsc::sync_channel(0);
                {
                    let mut map = self.notify_ch_map.lock().unwrap();
                    map.insert(cc.get_node_id(), sh);
                }
                self.rf_message_ch
                    .send(PeerMessage::ConfChange(cc))
                    .unwrap();
                match rh.recv_timeout(Duration::from_millis(1000)) {
                    Ok(_) => resp.set_err(RespErr::OK),
                    Err(_err) => resp.set_err(RespErr::ErrWrongLeader),
                }
            }
            _ => resp.set_err(RespErr::ErrWrongLeader),
        }

        debug!("response: {:?}", resp);

        ctx.spawn(
            sink.success(resp)
                .map_err(move |e| error!("failed to reply {:?}: {:?}", req, e)),
        )
    }
}
