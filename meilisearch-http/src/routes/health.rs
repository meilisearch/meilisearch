use crate::error::ResponseError;
use crate::Data;
use actix_web as aweb;
use actix_web::{get, put, web, HttpResponse};
use heed::types::{Str, Unit};
use serde::Deserialize;

const UNHEALTHY_KEY: &str = "_is_unhealthy";

#[get("/health")]
pub async fn get_health(data: web::Data<Data>) -> aweb::Result<HttpResponse> {
    let reader = data
        .db
        .main_read_txn()
        .map_err(|err| ResponseError::Internal(err.to_string()))?;

    let common_store = data.db.common_store();

    if let Ok(Some(_)) = common_store.get::<_, Str, Unit>(&reader, UNHEALTHY_KEY) {
        return Err(ResponseError::Maintenance.into());
    }

    Ok(HttpResponse::Ok().finish())
}

pub async fn set_healthy(data: web::Data<Data>) -> aweb::Result<HttpResponse> {
    let mut writer = data
        .db
        .main_write_txn()
        .map_err(|err| ResponseError::Internal(err.to_string()))?;
    let common_store = data.db.common_store();
    common_store
        .delete::<_, Str>(&mut writer, UNHEALTHY_KEY)
        .map_err(|e| ResponseError::Internal(e.to_string()))?;
    writer
        .commit()
        .map_err(|err| ResponseError::Internal(err.to_string()))?;

    Ok(HttpResponse::Ok().finish())
}

pub async fn set_unhealthy(data: web::Data<Data>) -> aweb::Result<HttpResponse> {
    let mut writer = data
        .db
        .main_write_txn()
        .map_err(|err| ResponseError::Internal(err.to_string()))?;
    let common_store = data.db.common_store();
    common_store
        .put::<_, Str, Unit>(&mut writer, UNHEALTHY_KEY, &())
        .map_err(|e| ResponseError::Internal(e.to_string()))?;
    writer
        .commit()
        .map_err(|err| ResponseError::Internal(err.to_string()))?;

    Ok(HttpResponse::Ok().finish())
}

#[derive(Deserialize, Clone)]
pub struct HealtBody {
    health: bool,
}

#[put("/health")]
pub async fn change_healthyness(
    data: web::Data<Data>,
    body: web::Json<HealtBody>,
) -> aweb::Result<HttpResponse> {
    if body.health {
        set_healthy(data).await
    } else {
        set_unhealthy(data).await
    }
}
