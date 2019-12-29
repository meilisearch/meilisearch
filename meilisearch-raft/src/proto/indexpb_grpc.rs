// This file is generated. Do not edit
// @generated

// https://github.com/Manishearth/rust-clippy/issues/702
#![allow(unknown_lints)]
#![allow(clippy::all)]

#![cfg_attr(rustfmt, rustfmt_skip)]

#![allow(box_pointers)]
#![allow(dead_code)]
#![allow(missing_docs)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(trivial_casts)]
#![allow(unsafe_code)]
#![allow(unused_imports)]
#![allow(unused_results)]

const METHOD_INDEX_RAFT: ::grpcio::Method<super::eraftpb::Message, super::indexrpcpb::RaftDone> = ::grpcio::Method {
    ty: ::grpcio::MethodType::Unary,
    name: "/indexpb.Index/Raft",
    req_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
    resp_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
};

const METHOD_INDEX_RAFT_CONF_CHANGE: ::grpcio::Method<super::indexrpcpb::ConfChangeReq, super::indexrpcpb::RaftDone> = ::grpcio::Method {
    ty: ::grpcio::MethodType::Unary,
    name: "/indexpb.Index/RaftConfChange",
    req_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
    resp_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
};

const METHOD_INDEX_PROBE: ::grpcio::Method<super::indexrpcpb::ProbeReq, super::indexrpcpb::ProbeResp> = ::grpcio::Method {
    ty: ::grpcio::MethodType::Unary,
    name: "/indexpb.Index/Probe",
    req_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
    resp_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
};

const METHOD_INDEX_PEERS: ::grpcio::Method<super::indexrpcpb::PeersReq, super::indexrpcpb::PeersResp> = ::grpcio::Method {
    ty: ::grpcio::MethodType::Unary,
    name: "/indexpb.Index/Peers",
    req_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
    resp_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
};

const METHOD_INDEX_GET: ::grpcio::Method<super::indexrpcpb::GetReq, super::indexrpcpb::GetResp> = ::grpcio::Method {
    ty: ::grpcio::MethodType::Unary,
    name: "/indexpb.Index/Get",
    req_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
    resp_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
};

const METHOD_INDEX_PUT: ::grpcio::Method<super::indexrpcpb::ApplyReq, super::indexrpcpb::PutResp> = ::grpcio::Method {
    ty: ::grpcio::MethodType::Unary,
    name: "/indexpb.Index/Put",
    req_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
    resp_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
};

#[derive(Clone)]
pub struct IndexClient {
    client: ::grpcio::Client,
}

impl IndexClient {
    pub fn new(channel: ::grpcio::Channel) -> Self {
        IndexClient {
            client: ::grpcio::Client::new(channel),
        }
    }

    pub fn raft_opt(&self, req: &super::eraftpb::Message, opt: ::grpcio::CallOption) -> ::grpcio::Result<super::indexrpcpb::RaftDone> {
        self.client.unary_call(&METHOD_INDEX_RAFT, req, opt)
    }

    pub fn raft(&self, req: &super::eraftpb::Message) -> ::grpcio::Result<super::indexrpcpb::RaftDone> {
        self.raft_opt(req, ::grpcio::CallOption::default())
    }

    pub fn raft_async_opt(&self, req: &super::eraftpb::Message, opt: ::grpcio::CallOption) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<super::indexrpcpb::RaftDone>> {
        self.client.unary_call_async(&METHOD_INDEX_RAFT, req, opt)
    }

    pub fn raft_async(&self, req: &super::eraftpb::Message) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<super::indexrpcpb::RaftDone>> {
        self.raft_async_opt(req, ::grpcio::CallOption::default())
    }

    pub fn raft_conf_change_opt(&self, req: &super::indexrpcpb::ConfChangeReq, opt: ::grpcio::CallOption) -> ::grpcio::Result<super::indexrpcpb::RaftDone> {
        self.client.unary_call(&METHOD_INDEX_RAFT_CONF_CHANGE, req, opt)
    }

    pub fn raft_conf_change(&self, req: &super::indexrpcpb::ConfChangeReq) -> ::grpcio::Result<super::indexrpcpb::RaftDone> {
        self.raft_conf_change_opt(req, ::grpcio::CallOption::default())
    }

    pub fn raft_conf_change_async_opt(&self, req: &super::indexrpcpb::ConfChangeReq, opt: ::grpcio::CallOption) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<super::indexrpcpb::RaftDone>> {
        self.client.unary_call_async(&METHOD_INDEX_RAFT_CONF_CHANGE, req, opt)
    }

    pub fn raft_conf_change_async(&self, req: &super::indexrpcpb::ConfChangeReq) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<super::indexrpcpb::RaftDone>> {
        self.raft_conf_change_async_opt(req, ::grpcio::CallOption::default())
    }

    pub fn probe_opt(&self, req: &super::indexrpcpb::ProbeReq, opt: ::grpcio::CallOption) -> ::grpcio::Result<super::indexrpcpb::ProbeResp> {
        self.client.unary_call(&METHOD_INDEX_PROBE, req, opt)
    }

    pub fn probe(&self, req: &super::indexrpcpb::ProbeReq) -> ::grpcio::Result<super::indexrpcpb::ProbeResp> {
        self.probe_opt(req, ::grpcio::CallOption::default())
    }

    pub fn probe_async_opt(&self, req: &super::indexrpcpb::ProbeReq, opt: ::grpcio::CallOption) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<super::indexrpcpb::ProbeResp>> {
        self.client.unary_call_async(&METHOD_INDEX_PROBE, req, opt)
    }

    pub fn probe_async(&self, req: &super::indexrpcpb::ProbeReq) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<super::indexrpcpb::ProbeResp>> {
        self.probe_async_opt(req, ::grpcio::CallOption::default())
    }

    pub fn peers_opt(&self, req: &super::indexrpcpb::PeersReq, opt: ::grpcio::CallOption) -> ::grpcio::Result<super::indexrpcpb::PeersResp> {
        self.client.unary_call(&METHOD_INDEX_PEERS, req, opt)
    }

    pub fn peers(&self, req: &super::indexrpcpb::PeersReq) -> ::grpcio::Result<super::indexrpcpb::PeersResp> {
        self.peers_opt(req, ::grpcio::CallOption::default())
    }

    pub fn peers_async_opt(&self, req: &super::indexrpcpb::PeersReq, opt: ::grpcio::CallOption) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<super::indexrpcpb::PeersResp>> {
        self.client.unary_call_async(&METHOD_INDEX_PEERS, req, opt)
    }

    pub fn peers_async(&self, req: &super::indexrpcpb::PeersReq) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<super::indexrpcpb::PeersResp>> {
        self.peers_async_opt(req, ::grpcio::CallOption::default())
    }

    pub fn get_opt(&self, req: &super::indexrpcpb::GetReq, opt: ::grpcio::CallOption) -> ::grpcio::Result<super::indexrpcpb::GetResp> {
        self.client.unary_call(&METHOD_INDEX_GET, req, opt)
    }

    pub fn get(&self, req: &super::indexrpcpb::GetReq) -> ::grpcio::Result<super::indexrpcpb::GetResp> {
        self.get_opt(req, ::grpcio::CallOption::default())
    }

    pub fn get_async_opt(&self, req: &super::indexrpcpb::GetReq, opt: ::grpcio::CallOption) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<super::indexrpcpb::GetResp>> {
        self.client.unary_call_async(&METHOD_INDEX_GET, req, opt)
    }

    pub fn get_async(&self, req: &super::indexrpcpb::GetReq) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<super::indexrpcpb::GetResp>> {
        self.get_async_opt(req, ::grpcio::CallOption::default())
    }

    pub fn put_opt(&self, req: &super::indexrpcpb::ApplyReq, opt: ::grpcio::CallOption) -> ::grpcio::Result<super::indexrpcpb::PutResp> {
        self.client.unary_call(&METHOD_INDEX_PUT, req, opt)
    }

    pub fn put(&self, req: &super::indexrpcpb::ApplyReq) -> ::grpcio::Result<super::indexrpcpb::PutResp> {
        self.put_opt(req, ::grpcio::CallOption::default())
    }

    pub fn put_async_opt(&self, req: &super::indexrpcpb::ApplyReq, opt: ::grpcio::CallOption) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<super::indexrpcpb::PutResp>> {
        self.client.unary_call_async(&METHOD_INDEX_PUT, req, opt)
    }

    pub fn put_async(&self, req: &super::indexrpcpb::ApplyReq) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<super::indexrpcpb::PutResp>> {
        self.put_async_opt(req, ::grpcio::CallOption::default())
    }
    pub fn spawn<F>(&self, f: F) where F: ::futures::Future<Item = (), Error = ()> + Send + 'static {
        self.client.spawn(f)
    }
}

pub trait Index {
    fn raft(&mut self, ctx: ::grpcio::RpcContext, req: super::eraftpb::Message, sink: ::grpcio::UnarySink<super::indexrpcpb::RaftDone>);
    fn raft_conf_change(&mut self, ctx: ::grpcio::RpcContext, req: super::indexrpcpb::ConfChangeReq, sink: ::grpcio::UnarySink<super::indexrpcpb::RaftDone>);
    fn probe(&mut self, ctx: ::grpcio::RpcContext, req: super::indexrpcpb::ProbeReq, sink: ::grpcio::UnarySink<super::indexrpcpb::ProbeResp>);
    fn peers(&mut self, ctx: ::grpcio::RpcContext, req: super::indexrpcpb::PeersReq, sink: ::grpcio::UnarySink<super::indexrpcpb::PeersResp>);
    fn get(&mut self, ctx: ::grpcio::RpcContext, req: super::indexrpcpb::GetReq, sink: ::grpcio::UnarySink<super::indexrpcpb::GetResp>);
    fn put(&mut self, ctx: ::grpcio::RpcContext, req: super::indexrpcpb::ApplyReq, sink: ::grpcio::UnarySink<super::indexrpcpb::PutResp>);
}

pub fn create_index<S: Index + Send + Clone + 'static>(s: S) -> ::grpcio::Service {
    let mut builder = ::grpcio::ServiceBuilder::new();
    let mut instance = s.clone();
    builder = builder.add_unary_handler(&METHOD_INDEX_RAFT, move |ctx, req, resp| {
        instance.raft(ctx, req, resp)
    });
    let mut instance = s.clone();
    builder = builder.add_unary_handler(&METHOD_INDEX_RAFT_CONF_CHANGE, move |ctx, req, resp| {
        instance.raft_conf_change(ctx, req, resp)
    });
    let mut instance = s.clone();
    builder = builder.add_unary_handler(&METHOD_INDEX_PROBE, move |ctx, req, resp| {
        instance.probe(ctx, req, resp)
    });
    let mut instance = s.clone();
    builder = builder.add_unary_handler(&METHOD_INDEX_PEERS, move |ctx, req, resp| {
        instance.peers(ctx, req, resp)
    });
    let mut instance = s.clone();
    builder = builder.add_unary_handler(&METHOD_INDEX_GET, move |ctx, req, resp| {
        instance.get(ctx, req, resp)
    });
    let mut instance = s.clone();
    builder = builder.add_unary_handler(&METHOD_INDEX_PUT, move |ctx, req, resp| {
        instance.put(ctx, req, resp)
    });
    builder.build()
}
