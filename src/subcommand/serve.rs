use std::borrow::Cow;
use std::collections::HashSet;
use std::fs::{File, create_dir_all};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Instant;
use std::{mem, io};

use askama_warp::Template;
use flate2::read::GzDecoder;
use futures::stream;
use futures::{FutureExt, StreamExt};
use grenad::CompressionType;
use heed::EnvOpenOptions;
use indexmap::IndexMap;
use once_cell::sync::OnceCell;
use rayon::ThreadPool;
use serde::{Serialize, Deserialize, Deserializer};
use structopt::StructOpt;
use tokio::fs::File as TFile;
use tokio::io::AsyncWriteExt;
use tokio::sync::broadcast;
use warp::filters::ws::Message;
use warp::{Filter, http::Response};

use crate::tokenizer::{simple_tokenizer, TokenType};
use crate::update::{UpdateBuilder, IndexDocumentsMethod, UpdateFormat};
use crate::{Index, UpdateStore, SearchResult};

static GLOBAL_THREAD_POOL: OnceCell<ThreadPool> = OnceCell::new();

#[derive(Debug, StructOpt)]
/// The HTTP main server of the milli project.
pub struct Opt {
    /// The database path where the LMDB database is located.
    /// It is created if it doesn't already exist.
    #[structopt(long = "db", parse(from_os_str))]
    database: PathBuf,

    /// The maximum size the database can take on disk. It is recommended to specify
    /// the whole disk space (value must be a multiple of a page size).
    #[structopt(long = "db-size", default_value = "107374182400")] // 100 GB
    database_size: usize,

    /// The maximum size the database that stores the updates can take on disk. It is recommended
    /// to specify the whole disk space (value must be a multiple of a page size).
    #[structopt(long = "udb-size", default_value = "10737418240")] // 10 GB
    update_database_size: usize,

    /// Disable document highlighting on the dashboard.
    #[structopt(long)]
    disable_highlighting: bool,

    /// Verbose mode (-v, -vv, -vvv, etc.)
    #[structopt(short, long, parse(from_occurrences))]
    verbose: usize,

    /// The ip and port on which the database will listen for HTTP requests.
    #[structopt(short = "l", long, default_value = "127.0.0.1:9700")]
    http_listen_addr: String,

    #[structopt(flatten)]
    indexer: IndexerOpt,
}

#[derive(Debug, Clone, StructOpt)]
pub struct IndexerOpt {
    /// The amount of documents to skip before printing
    /// a log regarding the indexing advancement.
    #[structopt(long, default_value = "1000000")] // 1m
    pub log_every_n: usize,

    /// MTBL max number of chunks in bytes.
    #[structopt(long)]
    pub max_nb_chunks: Option<usize>,

    /// The maximum amount of memory to use for the MTBL buffer. It is recommended
    /// to use something like 80%-90% of the available memory.
    ///
    /// It is automatically split by the number of jobs e.g. if you use 7 jobs
    /// and 7 GB of max memory, each thread will use a maximum of 1 GB.
    #[structopt(long, default_value = "7516192768")] // 7 GB
    pub max_memory: usize,

    /// Size of the linked hash map cache when indexing.
    /// The bigger it is, the faster the indexing is but the more memory it takes.
    #[structopt(long, default_value = "500")]
    pub linked_hash_map_size: usize,

    /// The name of the compression algorithm to use when compressing intermediate
    /// chunks during indexing documents.
    ///
    /// Choosing a fast algorithm will make the indexing faster but may consume more memory.
    #[structopt(long, default_value = "snappy", possible_values = &["snappy", "zlib", "lz4", "lz4hc", "zstd"])]
    pub chunk_compression_type: CompressionType,

    /// The level of compression of the chosen algorithm.
    #[structopt(long, requires = "chunk-compression-type")]
    pub chunk_compression_level: Option<u32>,

    /// The number of bytes to remove from the begining of the chunks while reading/sorting
    /// or merging them.
    ///
    /// File fusing must only be enable on file systems that support the `FALLOC_FL_COLLAPSE_RANGE`,
    /// (i.e. ext4 and XFS). File fusing will only work if the `enable-chunk-fusing` is set.
    #[structopt(long, default_value = "4294967296")] // 4 GB
    pub chunk_fusing_shrink_size: u64,

    /// Enable the chunk fusing or not, this reduces the amount of disk used by a factor of 2.
    #[structopt(long)]
    pub enable_chunk_fusing: bool,

    /// Number of parallel jobs for indexing, defaults to # of CPUs.
    #[structopt(long)]
    pub indexing_jobs: Option<usize>,
}

fn highlight_record(record: &mut IndexMap<String, String>, words: &HashSet<String>) {
    for (_key, value) in record.iter_mut() {
        let old_value = mem::take(value);
        for (token_type, token) in simple_tokenizer(&old_value) {
            if token_type == TokenType::Word {
                let lowercase_token = token.to_lowercase();
                let to_highlight = words.contains(&lowercase_token);
                if to_highlight { value.push_str("<mark>") }
                value.push_str(token);
                if to_highlight { value.push_str("</mark>") }
            } else {
                value.push_str(token);
            }
        }
    }
}

#[derive(Template)]
#[template(path = "index.html")]
struct IndexTemplate {
    db_name: String,
    db_size: usize,
    docs_count: usize,
}

#[derive(Template)]
#[template(path = "updates.html")]
struct UpdatesTemplate<M: Serialize + Send, P: Serialize + Send, N: Serialize + Send> {
    db_name: String,
    db_size: usize,
    docs_count: usize,
    updates: Vec<UpdateStatus<M, P, N>>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type")]
enum UpdateStatus<M, P, N> {
    Pending { update_id: u64, meta: M },
    Progressing { update_id: u64, meta: P },
    Processed { update_id: u64, meta: N },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
enum UpdateMeta {
    DocumentsAddition { method: String, format: String },
    ClearDocuments,
    Settings(Settings),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
enum UpdateMetaProgress {
    DocumentsAddition {
        processed_number_of_documents: usize,
        total_number_of_documents: Option<usize>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Settings {
    #[serde(
        default,
        deserialize_with = "deserialize_some",
        skip_serializing_if = "Option::is_none",
    )]
    displayed_attributes: Option<Option<Vec<String>>>,

    #[serde(
        default,
        deserialize_with = "deserialize_some",
        skip_serializing_if = "Option::is_none",
    )]
    searchable_attributes: Option<Option<Vec<String>>>,
}

// Any value that is present is considered Some value, including null.
fn deserialize_some<'de, T, D>(deserializer: D) -> Result<Option<T>, D::Error>
where T: Deserialize<'de>,
      D: Deserializer<'de>
{
    Deserialize::deserialize(deserializer).map(Some)
}

pub fn run(opt: Opt) -> anyhow::Result<()> {
    stderrlog::new()
        .verbosity(opt.verbose)
        .show_level(false)
        .timestamp(stderrlog::Timestamp::Off)
        .init()?;

    create_dir_all(&opt.database)?;
    let mut options = EnvOpenOptions::new();
    options.map_size(opt.database_size);

    // Setup the global thread pool
    let jobs = opt.indexer.indexing_jobs.unwrap_or(0);
    let pool = rayon::ThreadPoolBuilder::new().num_threads(jobs).build()?;
    GLOBAL_THREAD_POOL.set(pool).unwrap();

    // Open the LMDB database.
    let index = Index::new(options, &opt.database)?;

    // Setup the LMDB based update database.
    let mut update_store_options = EnvOpenOptions::new();
    update_store_options.map_size(opt.update_database_size);

    let update_store_path = opt.database.join("updates.mdb");
    create_dir_all(&update_store_path)?;

    let (update_status_sender, _) = broadcast::channel(100);
    let update_status_sender_cloned = update_status_sender.clone();
    let index_cloned = index.clone();
    let indexer_opt_cloned = opt.indexer.clone();
    let update_store = UpdateStore::open(
        update_store_options,
        update_store_path,
        move |update_id, meta, content| {
            // We prepare the update by using the update builder.
            let mut update_builder = UpdateBuilder::new();
            if let Some(max_nb_chunks) = indexer_opt_cloned.max_nb_chunks {
                update_builder.max_nb_chunks(max_nb_chunks);
            }
            if let Some(chunk_compression_level) = indexer_opt_cloned.chunk_compression_level {
                update_builder.chunk_compression_level(chunk_compression_level);
            }
            update_builder.thread_pool(GLOBAL_THREAD_POOL.get().unwrap());
            update_builder.log_every_n(indexer_opt_cloned.log_every_n);
            update_builder.max_memory(indexer_opt_cloned.max_memory);
            update_builder.linked_hash_map_size(indexer_opt_cloned.linked_hash_map_size);
            update_builder.chunk_compression_type(indexer_opt_cloned.chunk_compression_type);
            update_builder.chunk_fusing_shrink_size(indexer_opt_cloned.chunk_fusing_shrink_size);

            // we extract the update type and execute the update itself.
            let result: anyhow::Result<()> = match meta {
                UpdateMeta::DocumentsAddition { method, format } => {
                    // We must use the write transaction of the update here.
                    let mut wtxn = index_cloned.write_txn()?;
                    let mut builder = update_builder.index_documents(&mut wtxn, &index_cloned);

                    match format.as_str() {
                        "csv" => builder.update_format(UpdateFormat::Csv),
                        "json" => builder.update_format(UpdateFormat::Json),
                        "json-stream" => builder.update_format(UpdateFormat::JsonStream),
                        otherwise => panic!("invalid update format {:?}", otherwise),
                    };

                    match method.as_str() {
                        "replace" => builder.index_documents_method(IndexDocumentsMethod::ReplaceDocuments),
                        "update" => builder.index_documents_method(IndexDocumentsMethod::UpdateDocuments),
                        otherwise => panic!("invalid indexing method {:?}", otherwise),
                    };

                    let gzipped = false;
                    let reader = if gzipped {
                        Box::new(GzDecoder::new(content))
                    } else {
                        Box::new(content) as Box<dyn io::Read>
                    };

                    let result = builder.execute(reader, |count, total| {
                        let _ = update_status_sender_cloned.send(UpdateStatus::Progressing {
                            update_id,
                            meta: UpdateMetaProgress::DocumentsAddition {
                                processed_number_of_documents: count,
                                total_number_of_documents: Some(total),
                            }
                        });
                    });

                    match result {
                        Ok(()) => wtxn.commit().map_err(Into::into),
                        Err(e) => Err(e.into())
                    }
                },
                UpdateMeta::ClearDocuments => {
                    // We must use the write transaction of the update here.
                    let mut wtxn = index_cloned.write_txn()?;
                    let builder = update_builder.clear_documents(&mut wtxn, &index_cloned);

                    match builder.execute() {
                        Ok(_count) => wtxn.commit().map_err(Into::into),
                        Err(e) => Err(e.into())
                    }
                },
                UpdateMeta::Settings(settings) => {
                    // We must use the write transaction of the update here.
                    let mut wtxn = index_cloned.write_txn()?;
                    let mut builder = update_builder.settings(&mut wtxn, &index_cloned);

                    // We transpose the settings JSON struct into a real setting update.
                    if let Some(names) = settings.searchable_attributes {
                        match names {
                            Some(names) => builder.set_searchable_fields(names),
                            None => builder.reset_searchable_fields(),
                        }
                    }

                    // We transpose the settings JSON struct into a real setting update.
                    if let Some(names) = settings.displayed_attributes {
                        match names {
                            Some(names) => builder.set_displayed_fields(names),
                            None => builder.reset_displayed_fields(),
                        }
                    }

                    let result = builder.execute(|count, total| {
                        let _ = update_status_sender_cloned.send(UpdateStatus::Progressing {
                            update_id,
                            meta: UpdateMetaProgress::DocumentsAddition {
                                processed_number_of_documents: count,
                                total_number_of_documents: Some(total),
                            }
                        });
                    });

                    match result {
                        Ok(_count) => wtxn.commit().map_err(Into::into),
                        Err(e) => Err(e.into())
                    }
                }
            };

            let meta = match result {
                Ok(()) => format!("valid update content"),
                Err(e) => format!("error while processing update content: {:?}", e),
            };

            let processed = UpdateStatus::Processed { update_id, meta: meta.clone() };
            let _ = update_status_sender_cloned.send(processed);

            Ok(meta)
        })?;

    // The database name will not change.
    let db_name = opt.database.file_stem().and_then(|s| s.to_str()).unwrap_or("").to_string();
    let lmdb_path = opt.database.join("data.mdb");

    // We run and wait on the HTTP server

    // Expose an HTML page to debug the search in a browser
    let db_name_cloned = db_name.clone();
    let lmdb_path_cloned = lmdb_path.clone();
    let index_cloned = index.clone();
    let dash_html_route = warp::filters::method::get()
        .and(warp::filters::path::end())
        .map(move || {
            // We retrieve the database size.
            let db_size = File::open(lmdb_path_cloned.clone())
                .unwrap()
                .metadata()
                .unwrap()
                .len() as usize;

            // And the number of documents in the database.
            let rtxn = index_cloned.read_txn().unwrap();
            let docs_count = index_cloned.clone().number_of_documents(&rtxn).unwrap() as usize;

            IndexTemplate { db_name: db_name_cloned.clone(), db_size, docs_count }
        });

    let update_store_cloned = update_store.clone();
    let lmdb_path_cloned = lmdb_path.clone();
    let index_cloned = index.clone();
    let updates_list_or_html_route = warp::filters::method::get()
        .and(warp::header("Accept"))
        .and(warp::path!("updates"))
        .map(move |header: String| {
            let update_store = update_store_cloned.clone();
            let mut updates = update_store.iter_metas(|processed, pending| {
                let mut updates = Vec::<UpdateStatus<_, UpdateMetaProgress, _>>::new();
                for result in processed {
                    let (uid, meta) = result?;
                    updates.push(UpdateStatus::Processed { update_id: uid.get(), meta });
                }
                for result in pending {
                    let (uid, meta) = result?;
                    updates.push(UpdateStatus::Pending { update_id: uid.get(), meta });
                }
                Ok(updates)
            }).unwrap();

            if header.contains("text/html") {
                updates.reverse();

                // We retrieve the database size.
                let db_size = File::open(lmdb_path_cloned.clone())
                    .unwrap()
                    .metadata()
                    .unwrap()
                    .len() as usize;

                // And the number of documents in the database.
                let rtxn = index_cloned.read_txn().unwrap();
                let docs_count = index_cloned.clone().number_of_documents(&rtxn).unwrap() as usize;

                let template = UpdatesTemplate {
                    db_name: db_name.clone(),
                    db_size,
                    docs_count,
                    updates,
                };
                Box::new(template) as Box<dyn warp::Reply>
            } else {
                Box::new(warp::reply::json(&updates))
            }
        });

    let dash_bulma_route = warp::filters::method::get()
        .and(warp::path!("bulma.min.css"))
        .map(|| Response::builder()
            .header("content-type", "text/css; charset=utf-8")
            .body(include_str!("../../public/bulma.min.css"))
        );

    let dash_bulma_dark_route = warp::filters::method::get()
        .and(warp::path!("bulma-prefers-dark.min.css"))
        .map(|| Response::builder()
            .header("content-type", "text/css; charset=utf-8")
            .body(include_str!("../../public/bulma-prefers-dark.min.css"))
        );

    let dash_style_route = warp::filters::method::get()
        .and(warp::path!("style.css"))
        .map(|| Response::builder()
            .header("content-type", "text/css; charset=utf-8")
            .body(include_str!("../../public/style.css"))
        );

    let dash_jquery_route = warp::filters::method::get()
        .and(warp::path!("jquery-3.4.1.min.js"))
        .map(|| Response::builder()
            .header("content-type", "application/javascript; charset=utf-8")
            .body(include_str!("../../public/jquery-3.4.1.min.js"))
        );

    let dash_filesize_route = warp::filters::method::get()
        .and(warp::path!("filesize.min.js"))
        .map(|| Response::builder()
            .header("content-type", "application/javascript; charset=utf-8")
            .body(include_str!("../../public/filesize.min.js"))
        );

    let dash_script_route = warp::filters::method::get()
        .and(warp::path!("script.js"))
        .map(|| Response::builder()
            .header("content-type", "application/javascript; charset=utf-8")
            .body(include_str!("../../public/script.js"))
        );

    let updates_script_route = warp::filters::method::get()
        .and(warp::path!("updates-script.js"))
        .map(|| Response::builder()
            .header("content-type", "application/javascript; charset=utf-8")
            .body(include_str!("../../public/updates-script.js"))
        );

    let dash_logo_white_route = warp::filters::method::get()
        .and(warp::path!("logo-white.svg"))
        .map(|| Response::builder()
            .header("content-type", "image/svg+xml")
            .body(include_str!("../../public/logo-white.svg"))
        );

    let dash_logo_black_route = warp::filters::method::get()
        .and(warp::path!("logo-black.svg"))
        .map(|| Response::builder()
            .header("content-type", "image/svg+xml")
            .body(include_str!("../../public/logo-black.svg"))
        );

    #[derive(Deserialize)]
    struct QueryBody {
        query: Option<String>,
    }

    let disable_highlighting = opt.disable_highlighting;
    let query_route = warp::filters::method::post()
        .and(warp::path!("query"))
        .and(warp::body::json())
        .map(move |query: QueryBody| {
            let before_search = Instant::now();
            let rtxn = index.read_txn().unwrap();

            let mut search = index.search(&rtxn);
            if let Some(query) = query.query {
                search.query(query);
            }

            let SearchResult { found_words, documents_ids } = search.execute().unwrap();

            let mut documents = Vec::new();
            let fields_ids_map = index.fields_ids_map(&rtxn).unwrap();
            let displayed_fields = match index.displayed_fields(&rtxn).unwrap() {
                Some(fields) => Cow::Borrowed(fields),
                None => Cow::Owned(fields_ids_map.iter().map(|(id, _)| id).collect()),
            };

            for (_id, record) in index.documents(&rtxn, documents_ids).unwrap() {
                let mut record = displayed_fields.iter()
                    .flat_map(|&id| record.get(id).map(|val| (id, val)))
                    .map(|(key_id, value)| {
                        let key = fields_ids_map.name(key_id).unwrap().to_owned();
                        // TODO we must deserialize a Json Value and highlight it.
                        let value = serde_json::from_slice(value).unwrap();
                        (key, value)
                    })
                    .collect();

                if !disable_highlighting {
                    highlight_record(&mut record, &found_words);
                }

                documents.push(record);
            }

            Response::builder()
                .header("Content-Type", "application/json")
                .header("Time-Ms", before_search.elapsed().as_millis().to_string())
                .body(serde_json::to_string(&documents).unwrap())
        });

    async fn buf_stream(
        update_store: Arc<UpdateStore<UpdateMeta, String>>,
        update_status_sender: broadcast::Sender<UpdateStatus<UpdateMeta, UpdateMetaProgress, String>>,
        update_method: Option<String>,
        update_format: UpdateFormat,
        mut stream: impl futures::Stream<Item=Result<impl bytes::Buf, warp::Error>> + Unpin,
    ) -> Result<impl warp::Reply, warp::Rejection>
    {
        let file = tokio::task::block_in_place(tempfile::tempfile).unwrap();
        let mut file = TFile::from_std(file);

        while let Some(result) = stream.next().await {
            let bytes = result.unwrap().to_bytes();
            file.write_all(&bytes[..]).await.unwrap();
        }

        let file = file.into_std().await;
        let mmap = unsafe { memmap::Mmap::map(&file).unwrap() };

        let method = match update_method.as_deref() {
            Some("replace") => String::from("replace"),
            Some("update") => String::from("update"),
            _ => String::from("replace"),
        };

        let format = match update_format {
            UpdateFormat::Csv => String::from("csv"),
            UpdateFormat::Json => String::from("json"),
            UpdateFormat::JsonStream => String::from("json-stream"),
        };

        let meta = UpdateMeta::DocumentsAddition { method, format };
        let update_id = update_store.register_update(&meta, &mmap[..]).unwrap();
        let _ = update_status_sender.send(UpdateStatus::Pending { update_id, meta });
        eprintln!("update {} registered", update_id);

        Ok(warp::reply())
    }

    #[derive(Deserialize)]
    struct QueryUpdate {
        method: Option<String>,
    }

    let update_store_cloned = update_store.clone();
    let update_status_sender_cloned = update_status_sender.clone();
    let indexing_csv_route = warp::filters::method::post()
        .and(warp::path!("documents"))
        .and(warp::header::exact_ignore_case("content-type", "text/csv"))
        .and(warp::filters::query::query())
        .and(warp::body::stream())
        .and_then(move |params: QueryUpdate, stream| {
            buf_stream(
                update_store_cloned.clone(),
                update_status_sender_cloned.clone(),
                params.method,
                UpdateFormat::Csv,
                stream,
            )
        });

    let update_store_cloned = update_store.clone();
    let update_status_sender_cloned = update_status_sender.clone();
    let indexing_json_route = warp::filters::method::post()
        .and(warp::path!("documents"))
        .and(warp::header::exact_ignore_case("content-type", "application/json"))
        .and(warp::filters::query::query())
        .and(warp::body::stream())
        .and_then(move |params: QueryUpdate, stream| {
            buf_stream(
                update_store_cloned.clone(),
                update_status_sender_cloned.clone(),
                params.method,
                UpdateFormat::Json,
                stream,
            )
        });

    let update_store_cloned = update_store.clone();
    let update_status_sender_cloned = update_status_sender.clone();
    let indexing_json_stream_route = warp::filters::method::post()
        .and(warp::path!("documents"))
        .and(warp::header::exact_ignore_case("content-type", "application/x-ndjson"))
        .and(warp::filters::query::query())
        .and(warp::body::stream())
        .and_then(move |params: QueryUpdate, stream| {
            buf_stream(
                update_store_cloned.clone(),
                update_status_sender_cloned.clone(),
                params.method,
                UpdateFormat::JsonStream,
                stream,
            )
        });

    let update_store_cloned = update_store.clone();
    let update_status_sender_cloned = update_status_sender.clone();
    let clearing_route = warp::filters::method::post()
        .and(warp::path!("clear-documents"))
        .map(move || {
            let meta = UpdateMeta::ClearDocuments;
            let update_id = update_store_cloned.register_update(&meta, &[]).unwrap();
            let _ = update_status_sender_cloned.send(UpdateStatus::Pending { update_id, meta });
            eprintln!("update {} registered", update_id);
            Ok(warp::reply())
        });

    let update_store_cloned = update_store.clone();
    let update_status_sender_cloned = update_status_sender.clone();
    let change_settings_route = warp::filters::method::post()
        .and(warp::path!("settings"))
        .and(warp::body::json())
        .map(move |settings: Settings| {
            let meta = UpdateMeta::Settings(settings);
            let update_id = update_store_cloned.register_update(&meta, &[]).unwrap();
            let _ = update_status_sender_cloned.send(UpdateStatus::Pending { update_id, meta });
            eprintln!("update {} registered", update_id);
            Ok(warp::reply())
        });

    let update_ws_route = warp::ws()
        .and(warp::path!("updates" / "ws"))
        .map(move |ws: warp::ws::Ws| {
            // And then our closure will be called when it completes...
            let update_status_receiver = update_status_sender.subscribe();
            ws.on_upgrade(|websocket| {
                // Just echo all updates messages...
                update_status_receiver
                    .into_stream()
                    .flat_map(|result| {
                        match result {
                            Ok(status) => {
                                let msg = serde_json::to_string(&status).unwrap();
                                stream::iter(Some(Ok(Message::text(msg))))
                            },
                            Err(e) => {
                                eprintln!("channel error: {:?}", e);
                                stream::iter(None)
                            },
                        }
                    })
                    .forward(websocket)
                    .map(|result| {
                        if let Err(e) = result {
                            eprintln!("websocket error: {:?}", e);
                        }
                    })
            })
        });

    let routes = dash_html_route
        .or(updates_list_or_html_route)
        .or(dash_bulma_route)
        .or(dash_bulma_dark_route)
        .or(dash_style_route)
        .or(dash_jquery_route)
        .or(dash_filesize_route)
        .or(dash_script_route)
        .or(updates_script_route)
        .or(dash_logo_white_route)
        .or(dash_logo_black_route)
        .or(query_route)
        .or(indexing_csv_route)
        .or(indexing_json_route)
        .or(indexing_json_stream_route)
        .or(clearing_route)
        .or(change_settings_route)
        .or(update_ws_route);

    let addr = SocketAddr::from_str(&opt.http_listen_addr)?;
    tokio::runtime::Builder::new()
        .threaded_scheduler()
        .enable_all()
        .build()?
        .block_on(async {
            warp::serve(routes).run(addr).await
        });

    Ok(())
}
