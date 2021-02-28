use crate::make_update_delete_routes;
use actix_web::{web, HttpResponse, get};

use crate::error::{Error, ResponseError};
use crate::helpers::Authentication;
use crate::Data;

#[get(
    "/indexes/{index_uid}/settings/ranking-rules",
    wrap = "Authentication::Private"
)]
async fn get(
    data: web::Data<Data>,
    index_uid: web::Path<String>,
) -> Result<HttpResponse, ResponseError> {
    todo!()
}

make_update_delete_routes!(
    "/indexes/{index_uid}/settings/ranking-rules",
    Vec<String>,
    ranking_rules
);
