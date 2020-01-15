use crate::error::{ResponseError, SResult};
use crate::helpers::tide::RequestExt;
use crate::models::token::ACL::*;
use crate::Data;

use heed::types::{Str, Unit};
use serde::Deserialize;
use tide::{Response, Request};

const UNHEALTHY_KEY: &str = "_is_unhealthy";

pub async fn get_health(ctx: Request<Data>) -> SResult<Response> {
    let db = &ctx.state().db;
    let reader = db.main_read_txn().map_err(ResponseError::internal)?;

    let common_store = ctx.state().db.common_store();

    if let Ok(Some(_)) = common_store.get::<_, Str, Unit>(&reader, UNHEALTHY_KEY) {
        return Err(ResponseError::Maintenance);
    }

    Ok(tide::Response::new(200))
}

pub async fn set_healthy(ctx: Request<Data>) -> SResult<Response> {
    ctx.is_allowed(Admin)?;

    let db = &ctx.state().db;
    let mut writer = db.main_write_txn().map_err(ResponseError::internal)?;

    let common_store = ctx.state().db.common_store();
    match common_store.delete::<_, Str>(&mut writer, UNHEALTHY_KEY) {
        Ok(_) => (),
        Err(e) => return Err(ResponseError::internal(e)),
    }

    if let Err(e) = writer.commit() {
        return Err(ResponseError::internal(e));
    }

    Ok(tide::Response::new(200))
}

pub async fn set_unhealthy(ctx: Request<Data>) -> SResult<Response> {
    ctx.is_allowed(Admin)?;

    let db = &ctx.state().db;
    let mut writer = db.main_write_txn().map_err(ResponseError::internal)?;

    let common_store = ctx.state().db.common_store();

    if let Err(e) = common_store.put::<_, Str, Unit>(&mut writer, UNHEALTHY_KEY, &()) {
        return Err(ResponseError::internal(e));
    }

    if let Err(e) = writer.commit() {
        return Err(ResponseError::internal(e));
    }

    Ok(tide::Response::new(200))
}

#[derive(Deserialize, Clone)]
struct HealtBody {
    health: bool,
}

pub async fn change_healthyness(mut ctx: Request<Data>) -> SResult<Response> {
    let body: HealtBody = ctx.body_json().await.map_err(ResponseError::bad_request)?;

    if body.health {
        set_healthy(ctx).await
    } else {
        set_unhealthy(ctx).await
    }
}
