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
    pub fn spawn<F>(&self, f: F) where F: ::futures::Future<Item = (), Error = ()> + Send + 'static {
        self.client.spawn(f)
    }
}

pub trait Index {
    fn raft(&mut self, ctx: ::grpcio::RpcContext, req: super::eraftpb::Message, sink: ::grpcio::UnarySink<super::indexrpcpb::RaftDone>);
    fn raft_conf_change(&mut self, ctx: ::grpcio::RpcContext, req: super::indexrpcpb::ConfChangeReq, sink: ::grpcio::UnarySink<super::indexrpcpb::RaftDone>);
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
    builder.build()
}
