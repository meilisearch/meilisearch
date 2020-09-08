use std::sync::Arc;

use actix_web::{web, HttpResponse};
use actix_web_macros::{delete, get, post};
use meilisearch_core::settings::{Settings, SettingsUpdate, UpdateState, DEFAULT_RANKING_RULES};
use meilisearch_schema::Schema;
use std::collections::{BTreeMap, BTreeSet, HashSet};

use crate::error::{Error, ResponseError};
use crate::helpers::Authentication;
use crate::raft::{Message, Raft};
use crate::Data;

mod attributes_for_faceting;
mod displayed_attributes;
mod distinct_attributes;
mod ranking_rules;
mod searchable_attributes;
mod stop_words;
mod synonyms;

#[macro_export]
macro_rules! make_update_delete_routes {
    ($route:literal, $type:ty, $attr:ident) => {
        #[actix_web_macros::delete($route, wrap = "Authentication::Private")]
        pub async fn delete(
            data: web::Data<Data>,
            index_uid: web::Path<String>,
        ) -> Result<HttpResponse, ResponseError> {
            use meilisearch_core::settings::{SettingsUpdate, UpdateState};
            let settings_update = SettingsUpdate {
                $attr: UpdateState::Clear,
                ..SettingsUpdate::default()
            };
            let response = data.update_settings(index_uid.as_ref(), settings_update)?;
            Ok(HttpResponse::Accepted().json(response))
        }

        #[actix_web_macros::delete($route, wrap = "Authentication::Private")]
        pub async fn delete_raft(
            data: web::Data<std::sync::Arc<crate::raft::Raft>>,
            index_uid: web::Path<String>,
        ) -> Result<HttpResponse, ResponseError> {
            log::warn!("herrrrre");
            use meilisearch_core::settings::{SettingsUpdate, UpdateState};
            let settings_update = SettingsUpdate {
                $attr: UpdateState::Clear,
                ..SettingsUpdate::default()
            };
            let message = crate::raft::Message::SettingsUpdate {
                index_uid: index_uid.into_inner(),
                update: settings_update,
            };
            let response = data
                .propose(message)
                .await
                .map_err(|e| Error::RaftError(e.to_string()))?;
            Ok(HttpResponse::Accepted().json(response))
        }

        #[actix_web_macros::post($route, wrap = "Authentication::Private")]
        pub async fn update(
            data: actix_web::web::Data<Data>,
            index_uid: actix_web::web::Path<String>,
            body: actix_web::web::Json<Option<$type>>,
        ) -> std::result::Result<HttpResponse, ResponseError> {
            use meilisearch_core::settings::Settings;
            let settings = Settings {
                $attr: Some(body.into_inner()),
                ..Settings::default()
            };

            let settings_update = settings.to_update().map_err(Error::bad_request)?;
            let response = data.update_settings(index_uid.as_ref(), settings_update)?;

            Ok(HttpResponse::Accepted().json(response))
        }

        #[actix_web_macros::post($route, wrap = "Authentication::Private")]
        pub async fn update_raft(
            data: web::Data<std::sync::Arc<crate::raft::Raft>>,
            index_uid: actix_web::web::Path<String>,
            body: actix_web::web::Json<Option<$type>>,
        ) -> std::result::Result<HttpResponse, ResponseError> {
            use meilisearch_core::settings::Settings;
            let settings = Settings {
                $attr: Some(body.into_inner()),
                ..Settings::default()
            };

            let settings_update = settings.to_update().map_err(Error::bad_request)?;
            let message = crate::raft::Message::SettingsUpdate {
                index_uid: index_uid.into_inner(),
                update: settings_update,
            };
            let response = data
                .propose(message)
                .await
                .map_err(|e| Error::RaftError(e.to_string()))?;
            Ok(HttpResponse::Accepted().json(response))
        }
    };
}

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

        pub fn services_raft(cfg: &mut web::ServiceConfig) {
            cfg
                .service(update_all_raft)
                .service(get_all)
                .service(delete_all_raft)
                $(
                    .service($mod::get)
                    .service($mod::update_raft)
                    .service($mod::delete_raft)
                )*;
        }
    };
}

create_services!(
    attributes_for_faceting,
    displayed_attributes,
    distinct_attributes,
    ranking_rules,
    searchable_attributes,
    stop_words,
    synonyms
);

#[post("/indexes/{index_uid}/settings", wrap = "Authentication::Private")]
async fn update_all(
    data: web::Data<Data>,
    index_uid: web::Path<String>,
    body: web::Json<Settings>,
) -> Result<HttpResponse, ResponseError> {
    let settings_update = body.to_update().map_err(Error::bad_request)?;
    let response = data.update_settings(index_uid.as_ref(), settings_update)?;
    Ok(HttpResponse::Accepted().json(response))
}

#[post("/indexes/{index_uid}/settings", wrap = "Authentication::Private")]
async fn update_all_raft(
    data: web::Data<Arc<Raft>>,
    index_uid: web::Path<String>,
    body: web::Json<Settings>,
) -> Result<HttpResponse, ResponseError> {
    let settings_update = body.to_update().map_err(Error::bad_request)?;
    let message = crate::raft::Message::SettingsUpdate {
        index_uid: index_uid.into_inner(),
        update: settings_update,
    };
    let response = data
        .propose(message)
        .await
        .map_err(|e| Error::RaftError(e.to_string()))?;
    Ok(HttpResponse::Accepted().json(response))
}

#[get("/indexes/{index_uid}/settings", wrap = "Authentication::Private")]
async fn get_all(
    data: web::Data<Data>,
    index_uid: web::Path<String>,
) -> Result<HttpResponse, ResponseError> {
    let index = data
        .db
        .open_index(&index_uid.as_ref())
        .ok_or(Error::index_not_found(&index_uid.as_ref()))?;

    let reader = data.db.main_read_txn()?;

    let stop_words: BTreeSet<String> = index.main.stop_words(&reader)?.into_iter().collect();

    let synonyms_list = index.main.synonyms(&reader)?;

    let mut synonyms = BTreeMap::new();
    let index_synonyms = &index.synonyms;
    for synonym in synonyms_list {
        let list = index_synonyms.synonyms(&reader, synonym.as_bytes())?;
        synonyms.insert(synonym, list);
    }

    let ranking_rules = index
        .main
        .ranking_rules(&reader)?
        .unwrap_or(DEFAULT_RANKING_RULES.to_vec())
        .into_iter()
        .map(|r| r.to_string())
        .collect();

    let schema = index.main.schema(&reader)?;

    let distinct_attribute = match (index.main.distinct_attribute(&reader)?, &schema) {
        (Some(id), Some(schema)) => schema.name(id).map(str::to_string),
        _ => None,
    };

    let attributes_for_faceting = match (&schema, &index.main.attributes_for_faceting(&reader)?) {
        (Some(schema), Some(attrs)) => attrs
            .iter()
            .filter_map(|&id| schema.name(id))
            .map(str::to_string)
            .collect(),
        _ => vec![],
    };

    let searchable_attributes = schema.as_ref().map(get_indexed_attributes);
    let displayed_attributes = schema.as_ref().map(get_displayed_attributes);

    let settings = Settings {
        ranking_rules: Some(Some(ranking_rules)),
        distinct_attribute: Some(distinct_attribute),
        searchable_attributes: Some(searchable_attributes),
        displayed_attributes: Some(displayed_attributes),
        stop_words: Some(Some(stop_words)),
        synonyms: Some(Some(synonyms)),
        attributes_for_faceting: Some(Some(attributes_for_faceting)),
    };

    Ok(HttpResponse::Ok().json(settings))
}

#[delete("/indexes/{index_uid}/settings", wrap = "Authentication::Private")]
async fn delete_all(
    data: web::Data<Arc<Raft>>,
    index_uid: web::Path<String>,
) -> Result<HttpResponse, ResponseError> {
    let settings_update = SettingsUpdate {
        ranking_rules: UpdateState::Clear,
        distinct_attribute: UpdateState::Clear,
        primary_key: UpdateState::Clear,
        searchable_attributes: UpdateState::Clear,
        displayed_attributes: UpdateState::Clear,
        stop_words: UpdateState::Clear,
        synonyms: UpdateState::Clear,
        attributes_for_faceting: UpdateState::Clear,
    };
    let message = crate::raft::Message::SettingsUpdate {
        index_uid: index_uid.into_inner(),
        update: settings_update,
    };
    let response = data
        .propose(message)
        .await
        .map_err(|e| Error::RaftError(e.to_string()))?;
    Ok(HttpResponse::Accepted().json(response))
}

#[delete("/indexes/{index_uid}/settings", wrap = "Authentication::Private")]
async fn delete_all_raft(
    raft: web::Data<Arc<Raft>>,
    index_uid: web::Path<String>,
) -> Result<HttpResponse, ResponseError> {
    let settings = SettingsUpdate {
        ranking_rules: UpdateState::Clear,
        distinct_attribute: UpdateState::Clear,
        primary_key: UpdateState::Clear,
        searchable_attributes: UpdateState::Clear,
        displayed_attributes: UpdateState::Clear,
        stop_words: UpdateState::Clear,
        synonyms: UpdateState::Clear,
        attributes_for_faceting: UpdateState::Clear,
    };
    let message = Message::SettingsUpdate {
        index_uid: index_uid.into_inner(),
        update: settings,
    };
    let response = raft
        .propose(message)
        .await
        .map_err(|e| Error::RaftError(e.to_string()))?;
    Ok(HttpResponse::Accepted().json(response))
}

fn get_displayed_attributes(schema: &Schema) -> HashSet<String> {
    if schema.is_displayed_all() {
        ["*"].iter().map(|s| s.to_string()).collect()
    } else {
        schema
            .displayed_name()
            .iter()
            .map(|s| s.to_string())
            .collect()
    }
}

fn get_indexed_attributes(schema: &Schema) -> Vec<String> {
    if schema.is_indexed_all() {
        ["*"].iter().map(|s| s.to_string()).collect()
    } else {
        schema
            .indexed_name()
            .iter()
            .map(|s| s.to_string())
            .collect()
    }
}
