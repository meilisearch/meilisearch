use actix_web::{web, HttpResponse};
use log::debug;

use crate::extractors::authentication::{policies::*, GuardedData};
use crate::index::Settings;
use crate::Data;
use crate::{error::ResponseError, index::Unchecked};

#[macro_export]
macro_rules! make_setting_route {
    ($route:literal, $type:ty, $attr:ident, $camelcase_attr:literal) => {
        mod $attr {
            use log::debug;
            use actix_web::{web, HttpResponse, Resource};

            use crate::data;
            use crate::error::ResponseError;
            use crate::index::Settings;
            use crate::extractors::authentication::{GuardedData, policies::*};

            async fn delete(
                data: GuardedData<Private, data::Data>,
                index_uid: web::Path<String>,
            ) -> Result<HttpResponse, ResponseError> {
                use crate::index::Settings;
                let settings = Settings {
                    $attr: Some(None),
                    ..Default::default()
                };
                let update_status = data.update_settings(index_uid.into_inner(), settings, false).await?;
                debug!("returns: {:?}", update_status);
                Ok(HttpResponse::Accepted().json(serde_json::json!({ "updateId": update_status.id() })))
            }

            async fn update(
                data: GuardedData<Private, data::Data>,
                index_uid: actix_web::web::Path<String>,
                body: actix_web::web::Json<Option<$type>>,
            ) -> std::result::Result<HttpResponse, ResponseError> {
                let settings = Settings {
                    $attr: Some(body.into_inner()),
                    ..Default::default()
                };

                let update_status = data.update_settings(index_uid.into_inner(), settings, true).await?;
                debug!("returns: {:?}", update_status);
                Ok(HttpResponse::Accepted().json(serde_json::json!({ "updateId": update_status.id() })))
            }

            async fn get(
                data: GuardedData<Private, data::Data>,
                index_uid: actix_web::web::Path<String>,
            ) -> std::result::Result<HttpResponse, ResponseError> {
                let settings = data.settings(index_uid.into_inner()).await?;
                debug!("returns: {:?}", settings);
                let mut json = serde_json::json!(&settings);
                let val = json[$camelcase_attr].take();
                Ok(HttpResponse::Ok().json(val))
            }

            pub fn resources() -> Resource {
                Resource::new($route)
                    .route(web::get().to(get))
                    .route(web::post().to(update))
                    .route(web::delete().to(delete))
            }
        }
    };
}

make_setting_route!(
    "/indexes/{index_uid}/settings/filterable-attributes",
    std::collections::HashSet<String>,
    filterable_attributes,
    "filterableAttributes"
);

make_setting_route!(
    "/indexes/{index_uid}/settings/displayed-attributes",
    Vec<String>,
    displayed_attributes,
    "displayedAttributes"
);

make_setting_route!(
    "/indexes/{index_uid}/settings/searchable-attributes",
    Vec<String>,
    searchable_attributes,
    "searchableAttributes"
);

make_setting_route!(
    "/indexes/{index_uid}/settings/stop-words",
    std::collections::BTreeSet<String>,
    stop_words,
    "stopWords"
);

make_setting_route!(
    "/indexes/{index_uid}/settings/synonyms",
    std::collections::BTreeMap<String, Vec<String>>,
    synonyms,
    "synonyms"
);

make_setting_route!(
    "/indexes/{index_uid}/settings/distinct-attribute",
    String,
    distinct_attribute,
    "distinctAttribute"
);

make_setting_route!(
    "/indexes/{index_uid}/settings/ranking-rules",
    Vec<String>,
    ranking_rules,
    "rankingRules"
);

macro_rules! create_services {
    ($($mod:ident),*) => {
        pub fn services(cfg: &mut web::ServiceConfig) {
            cfg
                .service(web::resource("/indexes/{index_uid}/settings")
                    .route(web::post().to(update_all))
                    .route(web::get().to(get_all))
                    .route(web::delete().to(delete_all)))
                $(.service($mod::resources()))*;
        }
    };
}

create_services!(
    filterable_attributes,
    displayed_attributes,
    searchable_attributes,
    distinct_attribute,
    stop_words,
    synonyms,
    ranking_rules
);

async fn update_all(
    data: GuardedData<Private, Data>,
    index_uid: web::Path<String>,
    body: web::Json<Settings<Unchecked>>,
) -> Result<HttpResponse, ResponseError> {
    let settings = body.into_inner().check();
    let update_result = data
        .update_settings(index_uid.into_inner(), settings, true)
        .await?;
    let json = serde_json::json!({ "updateId": update_result.id() });
    debug!("returns: {:?}", json);
    Ok(HttpResponse::Accepted().json(json))
}

async fn get_all(
    data: GuardedData<Private, Data>,
    index_uid: web::Path<String>,
) -> Result<HttpResponse, ResponseError> {
    let settings = data.settings(index_uid.into_inner()).await?;
    debug!("returns: {:?}", settings);
    Ok(HttpResponse::Ok().json(settings))
}

async fn delete_all(
    data: GuardedData<Private, Data>,
    index_uid: web::Path<String>,
) -> Result<HttpResponse, ResponseError> {
    let settings = Settings::cleared();
    let update_result = data
        .update_settings(index_uid.into_inner(), settings, false)
        .await?;
    let json = serde_json::json!({ "updateId": update_result.id() });
    debug!("returns: {:?}", json);
    Ok(HttpResponse::Accepted().json(json))
}
