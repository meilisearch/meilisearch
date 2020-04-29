use actix_web::{web, HttpResponse};
use actix_web_macros::{get, put};
use heed::types::{Str, Unit};
use serde::Deserialize;

use crate::error::ResponseError;
use crate::helpers::Authentication;
use crate::Data;

const UNHEALTHY_KEY: &str = "_is_unhealthy";

pub fn services(cfg: &mut web::ServiceConfig) {
    cfg.service(get_health).service(change_healthyness);
}

#[get("/health", wrap = "Authentication::Private")]
async fn get_health(data: web::Data<Data>) -> Result<HttpResponse, ResponseError> {
    let reader = data.db.main_read_txn()?;

    let common_store = data.db.common_store();

    if let Ok(Some(_)) = common_store.get::<_, Str, Unit>(&reader, UNHEALTHY_KEY) {
        return Err(ResponseError::Maintenance);
    }

    Ok(HttpResponse::Ok().finish())
}

async fn set_healthy(data: web::Data<Data>) -> Result<HttpResponse, ResponseError> {
    let mut writer = data.db.main_write_txn()?;
    let common_store = data.db.common_store();
    common_store.delete::<_, Str>(&mut writer, UNHEALTHY_KEY)?;
    writer.commit()?;

    Ok(HttpResponse::Ok().finish())
}

async fn set_unhealthy(data: web::Data<Data>) -> Result<HttpResponse, ResponseError> {
    let mut writer = data.db.main_write_txn()?;
    let common_store = data.db.common_store();
    common_store.put::<_, Str, Unit>(&mut writer, UNHEALTHY_KEY, &())?;
    writer.commit()?;

    Ok(HttpResponse::Ok().finish())
}

#[derive(Deserialize, Clone)]
struct HealtBody {
    health: bool,
}

#[put("/health", wrap = "Authentication::Private")]
async fn change_healthyness(
    data: web::Data<Data>,
    body: web::Json<HealtBody>,
) -> Result<HttpResponse, ResponseError> {
    if body.health {
        set_healthy(data).await
    } else {
        set_unhealthy(data).await
    }
}
