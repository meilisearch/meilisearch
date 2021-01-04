use std::collections::HashSet;

use actix_web::{web, HttpResponse, get};

use crate::error::{Error, ResponseError};
use crate::helpers::Authentication;
use crate::make_update_delete_routes;
use crate::Data;

#[get(
    "/indexes/{index_uid}/settings/displayed-attributes",
    wrap = "Authentication::Private"
)]
async fn get(
    data: web::Data<Data>,
    index_uid: web::Path<String>,
) -> Result<HttpResponse, ResponseError> {
    todo!()
}

make_update_delete_routes!(
    "/indexes/{index_uid}/settings/displayed-attributes",
    HashSet<String>,
    displayed_attributes
);
