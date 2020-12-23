use actix_web::{delete, get, post, put};
use actix_web::{web, HttpResponse};
use indexmap::IndexMap;
use serde_json::Value;
use serde::Deserialize;

use crate::Data;
use crate::error::ResponseError;
use crate::helpers::Authentication;
use crate::routes::IndexParam;

type Document = IndexMap<String, Value>;

#[derive(Deserialize)]
struct DocumentParam {
    _index_uid: String,
    _document_id: String,
}

pub fn services(cfg: &mut web::ServiceConfig) {
    cfg.service(get_document)
        .service(delete_document)
        .service(get_all_documents)
        .service(add_documents)
        .service(update_documents)
        .service(delete_documents)
        .service(clear_all_documents);
}

#[get(
    "/indexes/{index_uid}/documents/{document_id}",
    wrap = "Authentication::Public"
)]
async fn get_document(
    _data: web::Data<Data>,
    _path: web::Path<DocumentParam>,
) -> Result<HttpResponse, ResponseError> {
    todo!()
}

#[delete(
    "/indexes/{index_uid}/documents/{document_id}",
    wrap = "Authentication::Private"
)]
async fn delete_document(
    _data: web::Data<Data>,
    _path: web::Path<DocumentParam>,
) -> Result<HttpResponse, ResponseError> {
    todo!()
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct BrowseQuery {
    _offset: Option<usize>,
    _limit: Option<usize>,
    _attributes_to_retrieve: Option<String>,
}

#[get("/indexes/{index_uid}/documents", wrap = "Authentication::Public")]
async fn get_all_documents(
    _data: web::Data<Data>,
    _path: web::Path<IndexParam>,
    _params: web::Query<BrowseQuery>,
) -> Result<HttpResponse, ResponseError> {
    todo!()
}

//fn find_primary_key(document: &IndexMap<String, Value>) -> Option<String> {
    //for key in document.keys() {
        //if key.to_lowercase().contains("id") {
            //return Some(key.to_string());
        //}
    //}
    //None
//}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct UpdateDocumentsQuery {
    _primary_key: Option<String>,
}

async fn update_multiple_documents(
    _data: web::Data<Data>,
    _path: web::Path<IndexParam>,
    _params: web::Query<UpdateDocumentsQuery>,
    _body: web::Json<Vec<Document>>,
    _is_partial: bool,
) -> Result<HttpResponse, ResponseError> {
    todo!()
}

#[post("/indexes/{index_uid}/documents", wrap = "Authentication::Private")]
async fn add_documents(
    data: web::Data<Data>,
    _path: web::Path<IndexParam>,
    _params: web::Query<UpdateDocumentsQuery>,
    body: web::Json<Vec<Document>>,
) -> Result<HttpResponse, ResponseError> {
    todo!()
}

#[put("/indexes/{index_uid}/documents", wrap = "Authentication::Private")]
async fn update_documents(
    data: web::Data<Data>,
    path: web::Path<IndexParam>,
    params: web::Query<UpdateDocumentsQuery>,
    body: web::Json<Vec<Document>>,
) -> Result<HttpResponse, ResponseError> {
    update_multiple_documents(data, path, params, body, true).await
}

#[post(
    "/indexes/{index_uid}/documents/delete-batch",
    wrap = "Authentication::Private"
)]
async fn delete_documents(
    _data: web::Data<Data>,
    _path: web::Path<IndexParam>,
    _body: web::Json<Vec<Value>>,
) -> Result<HttpResponse, ResponseError> {
    todo!()
}

#[delete("/indexes/{index_uid}/documents", wrap = "Authentication::Private")]
async fn clear_all_documents(
    _data: web::Data<Data>,
    _path: web::Path<IndexParam>,
) -> Result<HttpResponse, ResponseError> {
    todo!()
}
