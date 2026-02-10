use actix_web::web::{self, Json};
use actix_web::HttpResponse;
use serde::{Deserialize, Serialize};
use utoipa::{OpenApi, ToSchema};

#[derive(OpenApi)]
#[openapi(
    paths(post_mochi),
    tags((
        name = "Mochi ze Cat",
        description = "A demo route that proves Mochi is, indeed, a cat.",
    )),
)]
pub struct MochiApi;

pub fn configure(cfg: &mut web::ServiceConfig) {
    cfg.service(web::resource("").route(web::post().to(post_mochi)));
}

#[derive(Debug, Deserialize, ToSchema)]
pub struct MochiRequest {
    /// Is Mochi cute? (spoiler: always yes)
    #[schema(example = true)]
    pub cute: bool,

    /// Does Mochi work? (spoiler: cats don't work)
    #[serde(default = "default_work")]
    #[schema(default = false, example = false)]
    pub work: Option<bool>,
}

fn default_work() -> Option<bool> {
    Some(false)
}


#[derive(Debug, Serialize, ToSchema)]
pub struct MochiResponse {
    /// Current status of Mochi. Spoiler: always a cat.
    #[schema(example = "I'm a cat")]
    pub status: String,

    /// Is Mochi cute?
    #[schema(example = true)]
    pub cute: bool,

    /// Does Mochi work?
    #[schema(example = false)]
    pub work: bool,
}

/// Ask Mochi
///
/// Send a question to Mochi ze Cat and get a status back.
/// This is a **demo route** to show how to document a POST endpoint
/// with a request body using `utoipa`.
#[utoipa::path(
    post,
    path = "",
    tag = "Mochi ze Cat",
    request_body(
        content = MochiRequest,
        description = "Parameters to send to Mochi",
        content_type = "application/json",
        example = json!({
            "cute": true,
            "work": false
        })
    ),
    responses(
        (status = 200, description = "Mochi answered", body = MochiResponse, content_type = "application/json", example = json!(
            {
                "status": "I'm a cat",
                "cute": true,
                "work": false
            }
        )),
    )
)]
pub async fn post_mochi(body: Json<MochiRequest>) -> HttpResponse {
    let cute = body.cute;
    let work = body.work.unwrap_or(false);

    HttpResponse::Ok().json(MochiResponse {
        status: "I'm a cat".to_string(),
        cute,
        work,
    })
}
