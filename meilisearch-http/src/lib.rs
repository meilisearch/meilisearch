//! # MeiliSearch
//! Hello there, future contributors. If you are here and see this code, it's probably because you want to add a super new fancy feature in MeiliSearch or fix a bug and first of all, thank you for that!
//!
//! To help you in this task, we'll try to do a little overview of the project.
//! ## Milli
//! [Milli](https://github.com/meilisearch/milli) is the core library of MeiliSearch. It's where we actually index documents and perform searches. Its purpose is to do these two tasks as fast as possible. You can give an update to milli, and it'll uses as many cores as provided to perform it as fast as possible. Nothing more. You can perform searches at the same time (search only uses one core).
//! As you can see, we're missing quite a lot of features here; milli does not handle multiples indexes, it can't queue updates, it doesn't provide any web / API frontend, it doesn't implement dumps or snapshots, etc...
//!
//! ## `Index` module
//! The [index] module is what encapsulates one milli index. It abstracts over its transaction and isolates a task that can be run into a thread. This is the unit of interaction with milli.
//! If you add a feature to milli, you'll probably need to add it in this module too before exposing it to the rest of meilisearch.
//!
//! ## `IndexController` module
//! To handle multiple indexes, we created an [index_controller]. It's in charge of creating new indexes, keeping references to all its indexes, forward asynchronous updates to its indexes, and provide an API to search in its indexes synchronously.
//! To achieves this goal, we use an [actor model](https://en.wikipedia.org/wiki/Actor_model).
//!
//! ### The actor model
//! Every actor is composed of at least three files:
//! - `mod.rs` declare and import all the files used by the actor. We also describe the interface (= all the methods) used to interact with the actor. If you are not modifying anything inside of an actor, this is usually all you need to see.
//! - `handle_impl.rs` implements the interface described in the `mod.rs`; in reality, there is no code logic in this file. Every method is only wrapping its parameters in a structure that is sent to the actor. This is useful for test and futureproofing.
//! - `message.rs` contains an enum that describes all the interactions you can have with the actor.
//! - `actor.rs` is used to create and execute the actor. It's where we'll write the loop looking for new messages and actually perform the tasks.
//!
//! MeiliSearch currently uses four actors:
//! - [`uuid_resolver`](index_controller/uuid_resolver/index.html) hold the association between the user-provided indexes name and the internal [`uuid`](https://en.wikipedia.org/wiki/Universally_unique_identifier) representation we use.
//! - [`index_actor`](index_controller::index_actor) is our representation of multiples indexes. Any request made to MeiliSearch that needs to talk to milli will pass through this actor.
//! - [`update_actor`](index_controller/update_actor/index.html) is in charge of indexes updates. Since updates can take a long time to receive and  process, we need to:
//!   1. Store them as fast as possible so we can continue to receive other updates even if nothing has been processed
//!   2. Feed the `index_actor` with a new update every time it finished its current job.
//! - [`dump_actor`](index_controller/dump_actor/index.html) this actor handle the  [dumps](https://docs.meilisearch.com/reference/api/dump.html). It needs to contact all the others actors and create a dump of everything that was currently happening.
//!
//! ## Data module
//! The [data] module provide a unified interface to communicate with the index controller and other services (snapshot, dumps, ...), initialize the MeiliSearch instance
//!
//! ## HTTP server
//! To handle the web and API part, we are using [actix-web](https://docs.rs/actix-web/); you can find all routes in the [routes] module.
//! Currently, the configuration of actix-web is made in the [lib.rs](crate).
//! Most of the routes use [extractors] to handle the authentication.

#![allow(rustdoc::private_intra_doc_links)]
pub mod data;
#[macro_use]
pub mod error;
#[macro_use]
pub mod extractors;
#[cfg(all(not(debug_assertions), feature = "analytics"))]
pub mod analytics;
pub mod helpers;
mod index;
mod index_controller;
pub mod option;
pub mod routes;
pub use self::data::Data;
use crate::extractors::authentication::AuthConfig;
pub use option::Opt;

use actix_web::web;

use extractors::authentication::policies::*;
use extractors::payload::PayloadConfig;

pub fn configure_data(config: &mut web::ServiceConfig, data: Data) {
    let http_payload_size_limit = data.http_payload_size_limit();
    config
        .app_data(web::Data::new(data.clone()))
        .app_data(data)
        .app_data(
            web::JsonConfig::default()
                .limit(http_payload_size_limit)
                .content_type(|_mime| true) // Accept all mime types
                .error_handler(|err, _req| error::payload_error_handler(err).into()),
        )
        .app_data(PayloadConfig::new(http_payload_size_limit))
        .app_data(
            web::QueryConfig::default()
                .error_handler(|err, _req| error::payload_error_handler(err).into()),
        );
}

pub fn configure_auth(config: &mut web::ServiceConfig, data: &Data) {
    let keys = data.api_keys();
    let auth_config = if let Some(ref master_key) = keys.master {
        let private_key = keys.private.as_ref().unwrap();
        let public_key = keys.public.as_ref().unwrap();
        let mut policies = init_policies!(Public, Private, Admin);
        create_users!(
            policies,
            master_key.as_bytes() => { Admin, Private, Public },
            private_key.as_bytes() => { Private, Public },
            public_key.as_bytes() => { Public }
        );
        AuthConfig::Auth(policies)
    } else {
        AuthConfig::NoAuth
    };

    config.app_data(auth_config);
}

#[cfg(feature = "mini-dashboard")]
pub fn dashboard(config: &mut web::ServiceConfig, enable_frontend: bool) {
    use actix_web::HttpResponse;
    use actix_web_static_files::Resource;

    mod generated {
        include!(concat!(env!("OUT_DIR"), "/generated.rs"));
    }

    if enable_frontend {
        let generated = generated::generate();
        let mut scope = web::scope("/");
        // Generate routes for mini-dashboard assets
        for (path, resource) in generated.into_iter() {
            let Resource {
                mime_type, data, ..
            } = resource;
            // Redirect index.html to /
            if path == "index.html" {
                config.service(web::resource("/").route(
                    web::get().to(move || HttpResponse::Ok().content_type(mime_type).body(data)),
                ));
            } else {
                scope = scope.service(web::resource(path).route(
                    web::get().to(move || HttpResponse::Ok().content_type(mime_type).body(data)),
                ));
            }
        }
        config.service(scope);
    } else {
        config.service(web::resource("/").route(web::get().to(routes::running)));
    }
}

#[cfg(not(feature = "mini-dashboard"))]
pub fn dashboard(config: &mut web::ServiceConfig, _enable_frontend: bool) {
    config.service(web::resource("/").route(web::get().to(routes::running)));
}

#[macro_export]
macro_rules! create_app {
    ($data:expr, $enable_frontend:expr) => {{
        use actix_cors::Cors;
        use actix_web::middleware::TrailingSlash;
        use actix_web::App;
        use actix_web::{middleware, web};
        use meilisearch_http::routes;
        use meilisearch_http::{configure_auth, configure_data, dashboard};

        App::new()
            .configure(|s| configure_data(s, $data.clone()))
            .configure(|s| configure_auth(s, &$data))
            .configure(routes::configure)
            .configure(|s| dashboard(s, $enable_frontend))
            .wrap(
                Cors::default()
                    .send_wildcard()
                    .allowed_headers(vec!["content-type", "x-meili-api-key"])
                    .allow_any_origin()
                    .allow_any_method()
                    .max_age(86_400), // 24h
            )
            .wrap(middleware::Logger::default())
            .wrap(middleware::Compress::default())
            .wrap(middleware::NormalizePath::new(
                middleware::TrailingSlash::Trim,
            ))
    }};
}
