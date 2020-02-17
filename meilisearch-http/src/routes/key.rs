use tide::{Request, Response};
use serde_json::json;
use crate::error::SResult;
use crate::helpers::tide::RequestExt;
use crate::helpers::tide::ACL::*;
use crate::Data;

pub async fn list(ctx: Request<Data>) -> SResult<Response> {
    ctx.is_allowed(Admin)?;

    let keys = &ctx.state().api_keys;

    Ok(tide::Response::new(200)
        .body_json(&json!({
            "private": keys.private,
            "public": keys.public,
        }))?)
}
