use actix_web::{web, HttpResponse, delete, get, post};

use crate::Data;
use crate::error::ResponseError;
use crate::index_controller::Settings;
use crate::helpers::Authentication;

#[macro_export]
macro_rules! make_setting_route {
    ($route:literal, $type:ty, $attr:ident) => {
        mod $attr {
            use actix_web::{web, HttpResponse};

            use crate::data;
            use crate::error::ResponseError;
            use crate::helpers::Authentication;
            use crate::index_controller::Settings;

            #[actix_web::delete($route, wrap = "Authentication::Private")]
            pub async fn delete(
                data: web::Data<data::Data>,
                index_uid: web::Path<String>,
            ) -> Result<HttpResponse, ResponseError> {
                use crate::index_controller::Settings;
                let settings = Settings {
                    $attr: Some(None),
                    ..Default::default()
                };
                match data.update_settings(index_uid.into_inner(), settings).await {
                    Ok(update_status) => {
                        let json = serde_json::to_string(&update_status).unwrap();
                        Ok(HttpResponse::Ok().body(json))
                    }
                    Err(e) => {
                        Ok(HttpResponse::BadRequest().body(serde_json::json!({ "error": e.to_string() })))
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

                match data.update_settings(index_uid.into_inner(), settings).await {
                    Ok(update_status) => {
                        let json = serde_json::to_string(&update_status).unwrap();
                        Ok(HttpResponse::Ok().body(json))
                    }
                    Err(e) => {
                        Ok(HttpResponse::BadRequest().body(serde_json::json!({ "error": e.to_string() })))
                    }
                }
            }

            #[actix_web::get($route, wrap = "Authentication::Private")]
            pub async fn get(
                data: actix_web::web::Data<data::Data>,
                index_uid: actix_web::web::Path<String>,
            ) -> std::result::Result<HttpResponse, ResponseError> {
                match data.settings(index_uid.as_ref()) {
                    Ok(settings) => {
                        let setting = settings.$attr;
                        let json = serde_json::to_string(&setting).unwrap();
                        Ok(HttpResponse::Ok().body(json))
                    }
                    Err(e) => {
                        Ok(HttpResponse::BadRequest().body(serde_json::json!({ "error": e.to_string() })))
                    }
                }
            }
        }
    };
}

make_setting_route!(
    "/indexes/{index_uid}/settings/attributes-for-faceting",
    std::collections::HashMap<String, String>,
    faceted_attributes
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

//make_setting_route!(
    //"/indexes/{index_uid}/settings/distinct-attribute",
    //String,
    //distinct_attribute
//);

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
    faceted_attributes,
    displayed_attributes,
    searchable_attributes,
    ranking_rules
);

#[post("/indexes/{index_uid}/settings", wrap = "Authentication::Private")]
async fn update_all(
    data: web::Data<Data>,
    index_uid: web::Path<String>,
    body: web::Json<Settings>,
) -> Result<HttpResponse, ResponseError> {
    match data.update_settings(index_uid.into_inner(), body.into_inner()).await {
        Ok(update_result) => {
            let json = serde_json::to_string(&update_result).unwrap();
            Ok(HttpResponse::Ok().body(json))
        }
        Err(e) => {
            Ok(HttpResponse::BadRequest().body(serde_json::json!({ "error": e.to_string() })))
        }
    }
}

#[get("/indexes/{index_uid}/settings", wrap = "Authentication::Private")]
async fn get_all(
    data: web::Data<Data>,
    index_uid: web::Path<String>,
) -> Result<HttpResponse, ResponseError> {
    match data.settings(index_uid.as_ref()) {
        Ok(settings) => {
            let json = serde_json::to_string(&settings).unwrap();
            Ok(HttpResponse::Ok().body(json))
        }
        Err(e) => {
            Ok(HttpResponse::BadRequest().body(serde_json::json!({ "error": e.to_string() })))
        }
    }
}

#[delete("/indexes/{index_uid}/settings", wrap = "Authentication::Private")]
async fn delete_all(
    data: web::Data<Data>,
    index_uid: web::Path<String>,
) -> Result<HttpResponse, ResponseError> {
    let settings = Settings::cleared();
    match data.update_settings(index_uid.into_inner(), settings).await {
        Ok(update_result) => {
            let json = serde_json::to_string(&update_result).unwrap();
            Ok(HttpResponse::Ok().body(json))
        }
        Err(e) => {
            Ok(HttpResponse::BadRequest().body(serde_json::json!({ "error": e.to_string() })))
        }
    }
}

