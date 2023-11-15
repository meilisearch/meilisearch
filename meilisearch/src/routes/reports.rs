use actix_web::web::{self, Data};
use actix_web::HttpResponse;
use index_scheduler::{IndexScheduler, Report};
use meilisearch_types::error::ResponseError;
use meilisearch_types::keys::actions;

use crate::extractors::authentication::policies::ActionPolicy;
use crate::extractors::authentication::GuardedData;
use crate::extractors::sequential_extractor::SeqHandler;

pub fn configure(cfg: &mut web::ServiceConfig) {
    cfg.service(web::resource("").route(web::get().to(list_reports))).service(
        web::scope("/{report_uid}")
            .service(web::resource("").route(web::get().to(SeqHandler(get_report)))),
    );
}

pub async fn list_reports(
    index_scheduler: GuardedData<ActionPolicy<{ actions::SETTINGS_ALL }>, Data<IndexScheduler>>,
) -> Result<HttpResponse, ResponseError> {
    let reports = &index_scheduler.reports();
    let reports = &reports.read().unwrap();
    let reports: Vec<&Report> = reports.iter().collect();

    Ok(HttpResponse::Ok().json(reports))
}

pub async fn get_report(
    index_scheduler: GuardedData<ActionPolicy<{ actions::SETTINGS_ALL }>, Data<IndexScheduler>>,
    report_id: web::Path<uuid::Uuid>,
) -> Result<HttpResponse, ResponseError> {
    let reports = &index_scheduler.reports();
    let reports = &reports.read().unwrap();
    let report = reports
        .find(*report_id)
        .ok_or(crate::error::MeilisearchHttpError::ReportNotFound(*report_id))?;

    Ok(HttpResponse::Ok().json(report))
}
