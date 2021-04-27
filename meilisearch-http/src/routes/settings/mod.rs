use actix_web::{delete, get, post, web, HttpResponse};

use crate::error::ResponseError;
use crate::helpers::Authentication;
use crate::index::Settings;
use crate::Data;

#[macro_export]
macro_rules! make_setting_route {
    ($route:literal, $type:ty, $attr:ident) => {
        mod $attr {
            use actix_web::{web, HttpResponse};

            use crate::data;
            use crate::error::ResponseError;
            use crate::helpers::Authentication;
            use crate::index::Settings;

            #[actix_web::delete($route, wrap = "Authentication::Private")]
            pub async fn delete(
                data: web::Data<data::Data>,
                index_uid: web::Path<String>,
            ) -> Result<HttpResponse, ResponseError> {
                use crate::index::Settings;
                let settings = Settings {
                    $attr: Some(None),
                    ..Default::default()
                };
                match data.update_settings(index_uid.into_inner(), settings, false).await {
                    Ok(update_status) => {
                        Ok(HttpResponse::Accepted().json(serde_json::json!({ "updateId": update_status.id() })))
                    }
                    Err(e) => {
                        Ok(HttpResponse::BadRequest().json(serde_json::json!({ "error": e.to_string() })))
                    }
                }
            }

            #[actix_web::post($route, wrap = "Authentication::Private")]
            pub async fn update(
                data: actix_web::web::Data<data::Data>,
                index_uid: actix_web::web::Path<String>,
                body: actix_web::web::Json<Option<$type>>,
            ) -> std::result::Result<HttpResponse, ResponseError> {
                let settings = Settings {
                    $attr: Some(body.into_inner()),
                    ..Default::default()
                };

                match data.update_settings(index_uid.into_inner(), settings, true).await {
                    Ok(update_status) => {
                        Ok(HttpResponse::Accepted().json(serde_json::json!({ "updateId": update_status.id() })))
                    }
                    Err(e) => {
                        Ok(HttpResponse::BadRequest().json(serde_json::json!({ "error": e.to_string() })))
                    }
                }
            }

            #[actix_web::get($route, wrap = "Authentication::Private")]
            pub async fn get(
                data: actix_web::web::Data<data::Data>,
                index_uid: actix_web::web::Path<String>,
            ) -> std::result::Result<HttpResponse, ResponseError> {
                match data.settings(index_uid.into_inner()).await {
                    Ok(settings) => Ok(HttpResponse::Ok().json(settings.$attr)),
                    Err(e) => {
                        Ok(HttpResponse::BadRequest().json(serde_json::json!({ "error": e.to_string() })))
                    }
                }
            }
        }
    };
}

make_setting_route!(
    "/indexes/{index_uid}/settings/attributes-for-faceting",
    std::collections::HashMap<String, String>,
    attributes_for_faceting
);

make_setting_route!(
    "/indexes/{index_uid}/settings/displayed-attributes",
    Vec<String>,
    displayed_attributes
);

make_setting_route!(
    "/indexes/{index_uid}/settings/searchable-attributes",
    Vec<String>,
    searchable_attributes
);

make_setting_route!(
    "/indexes/{index_uid}/settings/stop-words",
    std::collections::BTreeSet<String>,
    stop_words
);

make_setting_route!(
    "/indexes/{index_uid}/settings/distinct-attribute",
    String,
    distinct_attribute
);

make_setting_route!(
    "/indexes/{index_uid}/settings/ranking-rules",
    Vec<String>,
    ranking_rules
);

macro_rules! create_services {
    ($($mod:ident),*) => {
        pub fn services(cfg: &mut web::ServiceConfig) {
            cfg
                .service(update_all)
                .service(get_all)
                .service(delete_all)
                $(
                    .service($mod::get)
                    .service($mod::update)
                    .service($mod::delete)
                )*;
        }
    };
}

create_services!(
    attributes_for_faceting,
    displayed_attributes,
    searchable_attributes,
    distinct_attribute,
    stop_words,
    ranking_rules
);

#[post("/indexes/{index_uid}/settings", wrap = "Authentication::Private")]
async fn update_all(
    data: web::Data<Data>,
    index_uid: web::Path<String>,
    body: web::Json<Settings>,
) -> Result<HttpResponse, ResponseError> {
    match data
        .update_settings(index_uid.into_inner(), body.into_inner(), true)
        .await
    {
        Ok(update_result) => Ok(
            HttpResponse::Accepted().json(serde_json::json!({ "updateId": update_result.id() }))
        ),
        Err(e) => {
            Ok(HttpResponse::BadRequest().json(serde_json::json!({ "error": e.to_string() })))
        }
    }
}

#[get("/indexes/{index_uid}/settings", wrap = "Authentication::Private")]
async fn get_all(
    data: web::Data<Data>,
    index_uid: web::Path<String>,
) -> Result<HttpResponse, ResponseError> {
    match data.settings(index_uid.into_inner()).await {
        Ok(settings) => Ok(HttpResponse::Ok().json(settings)),
        Err(e) => {
            Ok(HttpResponse::BadRequest().json(serde_json::json!({ "error": e.to_string() })))
        }
    }
}

#[delete("/indexes/{index_uid}/settings", wrap = "Authentication::Private")]
async fn delete_all(
    data: web::Data<Data>,
    index_uid: web::Path<String>,
) -> Result<HttpResponse, ResponseError> {
    let settings = Settings::cleared();
    match data
        .update_settings(index_uid.into_inner(), settings, false)
        .await
    {
        Ok(update_result) => Ok(
            HttpResponse::Accepted().json(serde_json::json!({ "updateId": update_result.id() }))
        ),
        Err(e) => {
            Ok(HttpResponse::BadRequest().json(serde_json::json!({ "error": e.to_string() })))
        }
    }
}
