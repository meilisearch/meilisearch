use actix::{Actor, Context, Handler};
use actix_raft::RaftMetrics;

/// Your application's metrics interface actor.
#[derive(Default)]
pub struct AppMetrics {}

impl Actor for AppMetrics {
    type Context = Context<Self>;

    // ... snip ... other actix methods can be implemented here as needed.
}

impl Handler<RaftMetrics> for AppMetrics {
    type Result = ();

    fn handle(&mut self, _msg: RaftMetrics, _ctx: &mut Context<Self>) -> Self::Result {
        // ... snip ...
    }
}
