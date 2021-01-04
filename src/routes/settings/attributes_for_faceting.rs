use actix_web::{web, HttpResponse, get};

use crate::error::{Error, ResponseError};
use crate::helpers::Authentication;
use crate::make_update_delete_routes;
use crate::Data;

#[get(
    "/indexes/{index_uid}/settings/attributes-for-faceting",
    wrap = "Authentication::Private"
)]
async fn get(
    data: web::Data<Data>,
    index_uid: web::Path<String>,
) -> Result<HttpResponse, ResponseError> {
    let index = data
        .db
        .load()
        .open_index(&index_uid.as_ref())
        .ok_or(Error::index_not_found(&index_uid.as_ref()))?;

    let attributes_for_faceting = data.db.load().main_read::<_, _, ResponseError>(|reader| {
        let schema = index.main.schema(reader)?;
        let attrs = index.main.attributes_for_faceting(reader)?;
        let attr_names = match (&schema, &attrs) {
            (Some(schema), Some(attrs)) => attrs
                .iter()
                .filter_map(|&id| schema.name(id))
                .map(str::to_string)
                .collect(),
            _ => vec![],
        };
        Ok(attr_names)
    })?;

    Ok(HttpResponse::Ok().json(attributes_for_faceting))
}

make_update_delete_routes!(
    "/indexes/{index_uid}/settings/attributes-for-faceting",
    Vec<String>,
    attributes_for_faceting
);
