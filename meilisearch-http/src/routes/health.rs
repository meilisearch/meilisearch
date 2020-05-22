use actix_web::{web, HttpResponse};
use actix_web_macros::{get, put};
use serde::Deserialize;

use crate::error::{Error, ResponseError};
use crate::helpers::Authentication;
use crate::Data;

pub fn services(cfg: &mut web::ServiceConfig) {
    cfg.service(get_health).service(change_healthyness);
}

#[get("/health", wrap = "Authentication::Private")]
async fn get_health(data: web::Data<Data>) -> Result<HttpResponse, ResponseError> {
    let reader = data.db.main_read_txn()?;
    if let Ok(Some(_)) = data.db.get_health(&reader) {
        return Err(Error::Maintenance.into());
    }
    Ok(HttpResponse::Ok().finish())
}

async fn set_healthy(data: web::Data<Data>) -> Result<HttpResponse, ResponseError> {
    data.db.main_write::<_, _, ResponseError>(|writer| {
        data.db.set_healthy(writer)?;
        Ok(())
    })?;
    Ok(HttpResponse::Ok().finish())
}

async fn set_unhealthy(data: web::Data<Data>) -> Result<HttpResponse, ResponseError> {
    data.db.main_write::<_, _, ResponseError>(|writer| {
        data.db.set_unhealthy(writer)?;
        Ok(())
    })?;
    Ok(HttpResponse::Ok().finish())
}

#[derive(Deserialize, Clone)]
struct HealthBody {
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
