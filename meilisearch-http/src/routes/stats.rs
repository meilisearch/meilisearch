use std::collections::HashMap;

use actix_web::*;
use chrono::{DateTime, Utc};
use log::error;
use pretty_bytes::converter::convert;
use serde::Serialize;
use sysinfo::{NetworkExt, ProcessExt, ProcessorExt, System, SystemExt};
use walkdir::WalkDir;

use crate::Data;
use crate::error::ResponseError;

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct IndexStatsResponse {
    number_of_documents: u64,
    is_indexing: bool,
    fields_frequency: HashMap<String, usize>,
}

#[get("/indexes/{index_uid}/stats")]
pub async fn index_stats(
    data: web::Data<Data>,
    path: web::Path<String>,
) -> Result<web::Json<IndexStatsResponse>> {
    let index = data.db.open_index(path.clone())
        .ok_or(ResponseError::IndexNotFound(path.clone()))?;

    let reader = data.db.main_read_txn()
        .map_err(|_| ResponseError::CreateTransaction)?;

    let number_of_documents = index.main.number_of_documents(&reader)
        .map_err(|err| ResponseError::Internal(err.to_string()))?;

    let fields_frequency = index.main.fields_frequency(&reader)
        .map_err(|err| ResponseError::Internal(err.to_string()))?
        .unwrap_or_default();

    let update_reader = data.db.update_read_txn()
        .map_err(|_| ResponseError::CreateTransaction)?;

    let is_indexing = data
        .is_indexing(&update_reader, &path)
        .map_err(|err| ResponseError::Internal(err.to_string()))?
        .unwrap_or_default();

    Ok(web::Json(IndexStatsResponse {
        number_of_documents,
        is_indexing,
        fields_frequency,
    }))
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct StatsResult {
    database_size: u64,
    last_update: Option<DateTime<Utc>>,
    indexes: HashMap<String, IndexStatsResponse>,
}

#[get("/stats")]
pub async fn get_stats(
    data: web::Data<Data>,
) -> Result<web::Json<StatsResult>> {

    let mut index_list = HashMap::new();

    let reader = data.db.main_read_txn()
        .map_err(|_| ResponseError::CreateTransaction)?;
    let update_reader = data.db.update_read_txn()
        .map_err(|_| ResponseError::CreateTransaction)?;

    let indexes_set = data.db.indexes_uids();
    for index_uid in indexes_set {
        let index = data.db.open_index(&index_uid);
        match index {
            Some(index) => {
                let number_of_documents = index.main.number_of_documents(&reader)
                    .map_err(|err| ResponseError::Internal(err.to_string()))?;

                let fields_frequency = index.main.fields_frequency(&reader)
                    .map_err(|err| ResponseError::Internal(err.to_string()))?
                    .unwrap_or_default();

                let is_indexing = data
                    .is_indexing(&update_reader, &index_uid)
                    .map_err(|err| ResponseError::Internal(err.to_string()))?
                    .unwrap_or_default();

                let response = IndexStatsResponse {
                    number_of_documents,
                    is_indexing,
                    fields_frequency,
                };
                index_list.insert(index_uid, response);
            }
            None => error!(
                "Index {:?} is referenced in the indexes list but cannot be found",
                index_uid
            ),
        }
    }

    let database_size = WalkDir::new(data.db_path.clone())
        .into_iter()
        .filter_map(|entry| entry.ok())
        .filter_map(|entry| entry.metadata().ok())
        .filter(|metadata| metadata.is_file())
        .fold(0, |acc, m| acc + m.len());

    let last_update = data.last_update(&reader)
        .map_err(|err| ResponseError::Internal(err.to_string()))?;

    Ok(web::Json(StatsResult {
        database_size,
        last_update,
        indexes: index_list,
    }))
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct VersionResponse {
    commit_sha: String,
    build_date: String,
    pkg_version: String,
}

#[get("/version")]
pub async fn get_version() -> web::Json<VersionResponse> {
    web::Json(VersionResponse {
        commit_sha: env!("VERGEN_SHA").to_string(),
        build_date: env!("VERGEN_BUILD_TIMESTAMP").to_string(),
        pkg_version: env!("CARGO_PKG_VERSION").to_string(),
    })
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SysGlobal {
    total_memory: u64,
    used_memory: u64,
    total_swap: u64,
    used_swap: u64,
    input_data: u64,
    output_data: u64,
}

impl SysGlobal {
    fn new() -> SysGlobal {
        SysGlobal {
            total_memory: 0,
            used_memory: 0,
            total_swap: 0,
            used_swap: 0,
            input_data: 0,
            output_data: 0,
        }
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SysProcess {
    memory: u64,
    cpu: f32,
}

impl SysProcess {
    fn new() -> SysProcess {
        SysProcess {
            memory: 0,
            cpu: 0.0,
        }
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SysInfo {
    memory_usage: f64,
    processor_usage: Vec<f32>,
    global: SysGlobal,
    process: SysProcess,
}

impl SysInfo {
    fn new() -> SysInfo {
        SysInfo {
            memory_usage: 0.0,
            processor_usage: Vec::new(),
            global: SysGlobal::new(),
            process: SysProcess::new(),
        }
    }
}

#[get("/sys-info")]
pub async fn get_sys_info(
    data: web::Data<Data>,
) -> web::Json<SysInfo> {
    let mut sys = System::new();
    let mut info = SysInfo::new();

    info.memory_usage = sys.get_used_memory() as f64 / sys.get_total_memory() as f64 * 100.0;

    for processor in sys.get_processors() {
        info.processor_usage.push(processor.get_cpu_usage() * 100.0);
    }

    info.global.total_memory = sys.get_total_memory();
    info.global.used_memory = sys.get_used_memory();
    info.global.total_swap = sys.get_total_swap();
    info.global.used_swap = sys.get_used_swap();
    info.global.input_data = sys.get_networks()
        .into_iter()
        .map(|(_, n)| n.get_received())
        .sum::<u64>();
    info.global.output_data = sys.get_networks()
        .into_iter()
        .map(|(_, n)| n.get_transmitted())
        .sum::<u64>();

    if let Some(process) = sys.get_process(data.server_pid) {
        info.process.memory = process.memory();
        info.process.cpu = process.cpu_usage() * 100.0;
    }

    sys.refresh_all();
    web::Json(info)
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SysGlobalPretty {
    total_memory: String,
    used_memory: String,
    total_swap: String,
    used_swap: String,
    input_data: String,
    output_data: String,
}

impl SysGlobalPretty {
    fn new() -> SysGlobalPretty {
        SysGlobalPretty {
            total_memory: "None".to_owned(),
            used_memory: "None".to_owned(),
            total_swap: "None".to_owned(),
            used_swap: "None".to_owned(),
            input_data: "None".to_owned(),
            output_data: "None".to_owned(),
        }
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SysProcessPretty {
    memory: String,
    cpu: String,
}

impl SysProcessPretty {
    fn new() -> SysProcessPretty {
        SysProcessPretty {
            memory: "None".to_owned(),
            cpu: "None".to_owned(),
        }
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SysInfoPretty {
    memory_usage: String,
    processor_usage: Vec<String>,
    global: SysGlobalPretty,
    process: SysProcessPretty,
}

impl SysInfoPretty {
    fn new() -> SysInfoPretty {
        SysInfoPretty {
            memory_usage: "None".to_owned(),
            processor_usage: Vec::new(),
            global: SysGlobalPretty::new(),
            process: SysProcessPretty::new(),
        }
    }
}


#[get("/sys-info/pretty")]
pub async fn get_sys_info_pretty(
    data: web::Data<Data>,
) -> web::Json<SysInfoPretty> {
    let mut sys = System::new();
    let mut info = SysInfoPretty::new();

    info.memory_usage = format!(
        "{:.1} %",
        sys.get_used_memory() as f64 / sys.get_total_memory() as f64 * 100.0
    );

    for processor in sys.get_processors() {
        info.processor_usage
            .push(format!("{:.1} %", processor.get_cpu_usage() * 100.0));
    }

    info.global.total_memory = convert(sys.get_total_memory() as f64 * 1024.0);
    info.global.used_memory = convert(sys.get_used_memory() as f64 * 1024.0);
    info.global.total_swap = convert(sys.get_total_swap() as f64 * 1024.0);
    info.global.used_swap = convert(sys.get_used_swap() as f64 * 1024.0);
    info.global.input_data = convert(sys.get_networks().into_iter().map(|(_, n)| n.get_received()).sum::<u64>() as f64);
    info.global.output_data = convert(sys.get_networks().into_iter().map(|(_, n)| n.get_transmitted()).sum::<u64>() as f64);

    if let Some(process) = sys.get_process(data.server_pid) {
        info.process.memory = convert(process.memory() as f64 * 1024.0);
        info.process.cpu = format!("{:.1} %", process.cpu_usage() * 100.0);
    }

    sys.refresh_all();

    web::Json(info)
}
