use actix_web::{get, post, web, HttpResponse};

use crate::error::ResponseError;
use crate::helpers::Authentication;
use crate::routes::IndexParam;
use crate::Data;
use crate::data::SearchQuery;

pub fn services(cfg: &mut web::ServiceConfig) {
    cfg.service(search_with_post).service(search_with_url_query);
}

//#[derive(Serialize, Deserialize)]
//#[serde(rename_all = "camelCase", deny_unknown_fields)]
//pub struct SearchQuery {
    //q: Option<String>,
    //offset: Option<usize>,
    //limit: Option<usize>,
    //attributes_to_retrieve: Option<String>,
    //attributes_to_crop: Option<String>,
    //crop_length: Option<usize>,
    //attributes_to_highlight: Option<String>,
    //filters: Option<String>,
    //matches: Option<bool>,
    //facet_filters: Option<String>,
    //facets_distribution: Option<String>,
//}

#[get("/indexes/{index_uid}/search", wrap = "Authentication::Public")]
async fn search_with_url_query(
    _data: web::Data<Data>,
    _path: web::Path<IndexParam>,
    _params: web::Query<SearchQuery>,
) -> Result<HttpResponse, ResponseError> {
    todo!()
}

#[post("/indexes/{index_uid}/search", wrap = "Authentication::Public")]
async fn search_with_post(
    data: web::Data<Data>,
    path: web::Path<IndexParam>,
    params: web::Json<SearchQuery>,
) -> Result<HttpResponse, ResponseError> {
    let search_result = data.search(&path.index_uid, params.into_inner());
    match search_result {
        Ok(docs) => {
            let docs = serde_json::to_string(&docs).unwrap();
            Ok(HttpResponse::Ok().body(docs))
        }
        Err(e) => {
            Ok(HttpResponse::BadRequest().body(serde_json::json!({ "error": e.to_string() })))
        }
    }
}
