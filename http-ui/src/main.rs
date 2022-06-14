mod update_store;

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fmt::Display;
use std::fs::{create_dir_all, File};
use std::io::{BufReader, Cursor, Read};
use std::net::SocketAddr;
use std::num::{NonZeroU32, NonZeroUsize};
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Instant;
use std::{io, mem};

use askama_warp::Template;
use byte_unit::Byte;
use either::Either;
use flate2::read::GzDecoder;
use futures::{stream, FutureExt, StreamExt};
use heed::EnvOpenOptions;
use milli::documents::{DocumentsBatchBuilder, DocumentsBatchReader};
use milli::tokenizer::TokenizerBuilder;
use milli::update::UpdateIndexingStep::*;
use milli::update::{
    ClearDocuments, IndexDocumentsConfig, IndexDocumentsMethod, IndexerConfig, Setting,
};
use milli::{
    obkv_to_json, CompressionType, Filter as MilliFilter, FilterCondition, FormatOptions, Index,
    MatcherBuilder, SearchResult, SortError,
};
use once_cell::sync::OnceCell;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use structopt::StructOpt;
use tokio::fs::File as TFile;
use tokio::io::AsyncWriteExt;
use tokio::sync::broadcast;
use tokio_stream::wrappers::BroadcastStream;
use warp::filters::ws::Message;
use warp::http::Response;
use warp::Filter;

use self::update_store::UpdateStore;

#[cfg(target_os = "linux")]
#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

static GLOBAL_CONFIG: OnceCell<IndexerConfig> = OnceCell::new();

#[derive(Debug, StructOpt)]
/// The HTTP main server of the milli project.
pub struct Opt {
    /// The database path where the LMDB database is located.
    /// It is created if it doesn't already exist.
    #[structopt(long = "db", parse(from_os_str))]
    database: PathBuf,

    /// The maximum size the database can take on disk. It is recommended to specify
    /// the whole disk space (value must be a multiple of a page size).
    #[structopt(long = "db-size", default_value = "100 GiB")]
    database_size: Byte,

    /// The maximum size the database that stores the updates can take on disk. It is recommended
    /// to specify the whole disk space (value must be a multiple of a page size).
    #[structopt(long = "udb-size", default_value = "10 GiB")]
    update_database_size: Byte,

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
    #[structopt(long, default_value = "100000")] // 100k
    pub log_every_n: usize,

    /// MTBL max number of chunks in bytes.
    #[structopt(long)]
    pub max_nb_chunks: Option<usize>,

    /// The maximum amount of memory to use for the MTBL buffer. It is recommended
    /// to use something like 80%-90% of the available memory.
    ///
    /// It is automatically split by the number of jobs e.g. if you use 7 jobs
    /// and 7 GB of max memory, each thread will use a maximum of 1 GB.
    #[structopt(long, default_value = "7 GiB")]
    pub max_memory: Byte,

    /// Size of the linked hash map cache when indexing.
    /// The bigger it is, the faster the indexing is but the more memory it takes.
    #[structopt(long, default_value = "500")]
    pub linked_hash_map_size: usize,

    /// The name of the compression algorithm to use when compressing intermediate
    /// chunks during indexing documents.
    ///
    /// Choosing a fast algorithm will make the indexing faster but may consume more memory.
    #[structopt(long, possible_values = &["snappy", "zlib", "lz4", "lz4hc", "zstd"])]
    pub chunk_compression_type: Option<CompressionType>,

    /// The level of compression of the chosen algorithm.
    #[structopt(long, requires = "chunk-compression-type")]
    pub chunk_compression_level: Option<u32>,

    /// The number of bytes to remove from the begining of the chunks while reading/sorting
    /// or merging them.
    ///
    /// File fusing must only be enable on file systems that support the `FALLOC_FL_COLLAPSE_RANGE`,
    /// (i.e. ext4 and XFS). File fusing will only work if the `enable-chunk-fusing` is set.
    #[structopt(long, default_value = "4 GiB")]
    pub chunk_fusing_shrink_size: Byte,

    /// Enable the chunk fusing or not, this reduces the amount of disk used by a factor of 2.
    #[structopt(long)]
    pub enable_chunk_fusing: bool,

    /// Number of parallel jobs for indexing, defaults to # of CPUs.
    #[structopt(long)]
    pub indexing_jobs: Option<usize>,

    /// Maximum relative position in an attribute for a word to be indexed.
    /// Any value higher than 65535 will be clamped.
    #[structopt(long)]
    pub max_positions_per_attributes: Option<u32>,
}

struct Highlighter<'s, A> {
    matcher_builder: MatcherBuilder<'s, A>,
}

impl<'s, A: AsRef<[u8]>> Highlighter<'s, A> {
    fn new(matcher_builder: MatcherBuilder<'s, A>) -> Self {
        Self { matcher_builder }
    }

    fn highlight_value(&self, value: Value) -> Value {
        match value {
            Value::Null => Value::Null,
            Value::Bool(boolean) => Value::Bool(boolean),
            Value::Number(number) => Value::Number(number),
            Value::String(old_string) => {
                let mut matcher = self.matcher_builder.build(&old_string);

                let format_options = FormatOptions { highlight: true, crop: Some(10) };

                Value::String(matcher.format(format_options).to_string())
            }
            Value::Array(values) => {
                Value::Array(values.into_iter().map(|v| self.highlight_value(v)).collect())
            }
            Value::Object(object) => Value::Object(
                object.into_iter().map(|(k, v)| (k, self.highlight_value(v))).collect(),
            ),
        }
    }

    fn highlight_record(
        &self,
        object: &mut Map<String, Value>,
        attributes_to_highlight: &HashSet<String>,
    ) {
        // TODO do we need to create a string for element that are not and needs to be highlight?
        for (key, value) in object.iter_mut() {
            if attributes_to_highlight.contains(key) {
                let old_value = mem::take(value);
                *value = self.highlight_value(old_value);
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
struct UpdatesTemplate<M: Serialize + Send, P: Serialize + Send, N: Serialize + Send + Display> {
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
    Aborted { update_id: u64, meta: M },
}

impl<M, P, N> UpdateStatus<M, P, N> {
    fn update_id(&self) -> u64 {
        match self {
            UpdateStatus::Pending { update_id, .. } => *update_id,
            UpdateStatus::Progressing { update_id, .. } => *update_id,
            UpdateStatus::Processed { update_id, .. } => *update_id,
            UpdateStatus::Aborted { update_id, .. } => *update_id,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
enum UpdateMeta {
    DocumentsAddition { method: String, format: String, encoding: Option<String> },
    ClearDocuments,
    Settings(Settings),
    Facets(Facets),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
enum UpdateMetaProgress {
    DocumentsAddition { step: usize, total_steps: usize, current: usize, total: Option<usize> },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
struct Settings {
    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    displayed_attributes: Setting<Vec<String>>,

    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    searchable_attributes: Setting<Vec<String>>,

    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    filterable_attributes: Setting<HashSet<String>>,

    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    sortable_attributes: Setting<HashSet<String>>,

    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    criteria: Setting<Vec<String>>,

    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    stop_words: Setting<BTreeSet<String>>,

    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    synonyms: Setting<HashMap<String, Vec<String>>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
struct Facets {
    level_group_size: Option<NonZeroUsize>,
    min_level_size: Option<NonZeroUsize>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
struct WordsPrefixes {
    threshold: Option<f64>,
    max_prefix_length: Option<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
struct WordsLevelPositions {
    level_group_size: Option<NonZeroU32>,
    min_level_size: Option<NonZeroU32>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let opt = Opt::from_args();

    stderrlog::new()
        .verbosity(opt.verbose)
        .show_level(false)
        .timestamp(stderrlog::Timestamp::Off)
        .init()?;

    create_dir_all(&opt.database)?;
    let mut options = EnvOpenOptions::new();
    options.map_size(opt.database_size.get_bytes() as usize);

    // Setup the global thread pool
    let jobs = opt.indexer.indexing_jobs.unwrap_or(0);
    let pool = rayon::ThreadPoolBuilder::new().num_threads(jobs).build()?;

    let config = IndexerConfig {
        max_nb_chunks: opt.indexer.max_nb_chunks,
        chunk_compression_level: opt.indexer.chunk_compression_level,
        max_positions_per_attributes: opt.indexer.max_positions_per_attributes,
        thread_pool: Some(pool),
        log_every_n: Some(opt.indexer.log_every_n),
        max_memory: Some(opt.indexer.max_memory.get_bytes() as usize),
        chunk_compression_type: opt.indexer.chunk_compression_type.unwrap_or(CompressionType::None),
        ..Default::default()
    };

    GLOBAL_CONFIG.set(config).unwrap();

    // Open the LMDB database.
    let index = Index::new(options, &opt.database)?;

    // Setup the LMDB based update database.
    let mut update_store_options = EnvOpenOptions::new();
    update_store_options.map_size(opt.update_database_size.get_bytes() as usize);

    let update_store_path = opt.database.join("updates.mdb");
    create_dir_all(&update_store_path)?;

    let (update_status_sender, _) = broadcast::channel(100);
    let update_status_sender_cloned = update_status_sender.clone();
    let index_cloned = index.clone();
    let update_store = UpdateStore::open(
        update_store_options,
        update_store_path,
        // the type hint is necessary: https://github.com/rust-lang/rust/issues/32600
        move |update_id, meta, content: &_| {
            // We prepare the update by using the update builder.

            let before_update = Instant::now();
            // we extract the update type and execute the update itself.
            let result: anyhow::Result<()> = (|| match meta {
                UpdateMeta::DocumentsAddition { method, format, encoding } => {
                    // We must use the write transaction of the update here.
                    let mut wtxn = index_cloned.write_txn()?;
                    let update_method = match method.as_str() {
                        "replace" => IndexDocumentsMethod::ReplaceDocuments,
                        "update" => IndexDocumentsMethod::UpdateDocuments,
                        otherwise => panic!("invalid indexing method {:?}", otherwise),
                    };
                    let indexing_config = IndexDocumentsConfig {
                        update_method,
                        autogenerate_docids: true,
                        ..Default::default()
                    };

                    let indexing_callback = |indexing_step| {
                        let (current, total) = match indexing_step {
                            RemapDocumentAddition { documents_seen } => (documents_seen, None),
                            ComputeIdsAndMergeDocuments { documents_seen, total_documents } => {
                                (documents_seen, Some(total_documents))
                            }
                            IndexDocuments { documents_seen, total_documents } => {
                                (documents_seen, Some(total_documents))
                            }
                            MergeDataIntoFinalDatabase { databases_seen, total_databases } => {
                                (databases_seen, Some(total_databases))
                            }
                        };
                        let _ = update_status_sender_cloned.send(UpdateStatus::Progressing {
                            update_id,
                            meta: UpdateMetaProgress::DocumentsAddition {
                                step: indexing_step.step(),
                                total_steps: indexing_step.number_of_steps(),
                                current,
                                total,
                            },
                        });
                    };

                    let mut builder = milli::update::IndexDocuments::new(
                        &mut wtxn,
                        &index_cloned,
                        GLOBAL_CONFIG.get().unwrap(),
                        indexing_config,
                        indexing_callback,
                    )?;

                    let reader = match encoding.as_deref() {
                        Some("gzip") => Box::new(GzDecoder::new(content)),
                        None => Box::new(content) as Box<dyn io::Read>,
                        otherwise => panic!("invalid encoding format {:?}", otherwise),
                    };

                    let documents = match format.as_str() {
                        "csv" => documents_from_csv(reader)?,
                        "json" => documents_from_json(reader)?,
                        "jsonl" => documents_from_jsonl(reader)?,
                        otherwise => panic!("invalid update format {:?}", otherwise),
                    };

                    let documents = DocumentsBatchReader::from_reader(Cursor::new(documents))?;

                    builder.add_documents(documents)?;

                    let result = builder.execute();

                    match result {
                        Ok(_) => wtxn.commit().map_err(Into::into),
                        Err(e) => Err(e.into()),
                    }
                }
                UpdateMeta::ClearDocuments => {
                    // We must use the write transaction of the update here.
                    let mut wtxn = index_cloned.write_txn()?;
                    let builder = ClearDocuments::new(&mut wtxn, &index_cloned);

                    match builder.execute() {
                        Ok(_count) => wtxn.commit().map_err(Into::into),
                        Err(e) => Err(e.into()),
                    }
                }
                UpdateMeta::Settings(settings) => {
                    // We must use the write transaction of the update here.
                    let mut wtxn = index_cloned.write_txn()?;
                    let mut builder = milli::update::Settings::new(
                        &mut wtxn,
                        &index_cloned,
                        GLOBAL_CONFIG.get().unwrap(),
                    );

                    // We transpose the settings JSON struct into a real setting update.
                    match settings.searchable_attributes {
                        Setting::Set(searchable_attributes) => {
                            builder.set_searchable_fields(searchable_attributes)
                        }
                        Setting::Reset => builder.reset_searchable_fields(),
                        Setting::NotSet => (),
                    }

                    // We transpose the settings JSON struct into a real setting update.
                    match settings.displayed_attributes {
                        Setting::Set(displayed_attributes) => {
                            builder.set_displayed_fields(displayed_attributes)
                        }
                        Setting::Reset => builder.reset_displayed_fields(),
                        Setting::NotSet => (),
                    }

                    // We transpose the settings JSON struct into a real setting update.
                    match settings.filterable_attributes {
                        Setting::Set(filterable_attributes) => {
                            builder.set_filterable_fields(filterable_attributes)
                        }
                        Setting::Reset => builder.reset_filterable_fields(),
                        Setting::NotSet => (),
                    }

                    // We transpose the settings JSON struct into a real setting update.
                    match settings.sortable_attributes {
                        Setting::Set(sortable_attributes) => {
                            builder.set_sortable_fields(sortable_attributes)
                        }
                        Setting::Reset => builder.reset_sortable_fields(),
                        Setting::NotSet => (),
                    }

                    // We transpose the settings JSON struct into a real setting update.
                    match settings.criteria {
                        Setting::Set(criteria) => builder.set_criteria(criteria),
                        Setting::Reset => builder.reset_criteria(),
                        Setting::NotSet => (),
                    }

                    // We transpose the settings JSON struct into a real setting update.
                    match settings.stop_words {
                        Setting::Set(stop_words) => builder.set_stop_words(stop_words),
                        Setting::Reset => builder.reset_stop_words(),
                        Setting::NotSet => (),
                    }

                    // We transpose the settings JSON struct into a real setting update.
                    match settings.synonyms {
                        Setting::Set(synonyms) => builder.set_synonyms(synonyms),
                        Setting::Reset => builder.reset_synonyms(),
                        Setting::NotSet => (),
                    }

                    let result = builder.execute(|indexing_step| {
                        let (current, total) = match indexing_step {
                            RemapDocumentAddition { documents_seen } => (documents_seen, None),
                            ComputeIdsAndMergeDocuments { documents_seen, total_documents } => {
                                (documents_seen, Some(total_documents))
                            }
                            IndexDocuments { documents_seen, total_documents } => {
                                (documents_seen, Some(total_documents))
                            }
                            MergeDataIntoFinalDatabase { databases_seen, total_databases } => {
                                (databases_seen, Some(total_databases))
                            }
                        };
                        let _ = update_status_sender_cloned.send(UpdateStatus::Progressing {
                            update_id,
                            meta: UpdateMetaProgress::DocumentsAddition {
                                step: indexing_step.step(),
                                total_steps: indexing_step.number_of_steps(),
                                current,
                                total,
                            },
                        });
                    });

                    match result {
                        Ok(_count) => wtxn.commit().map_err(Into::into),
                        Err(e) => Err(e.into()),
                    }
                }
                UpdateMeta::Facets(levels) => {
                    // We must use the write transaction of the update here.
                    let mut wtxn = index_cloned.write_txn()?;
                    let mut builder = milli::update::Facets::new(&mut wtxn, &index_cloned);
                    if let Some(value) = levels.level_group_size {
                        builder.level_group_size(value);
                    }
                    if let Some(value) = levels.min_level_size {
                        builder.min_level_size(value);
                    }
                    match builder.execute() {
                        Ok(()) => wtxn.commit().map_err(Into::into),
                        Err(e) => Err(e.into()),
                    }
                }
            })();

            let meta = match result {
                Ok(()) => {
                    format!("valid update content processed in {:.02?}", before_update.elapsed())
                }
                Err(e) => format!("error while processing update content: {:?}", e),
            };

            let processed = UpdateStatus::Processed { update_id, meta: meta.clone() };
            let _ = update_status_sender_cloned.send(processed);

            Ok(meta)
        },
    )?;

    // The database name will not change.
    let db_name = opt.database.file_stem().and_then(|s| s.to_str()).unwrap_or("").to_string();
    let lmdb_path = opt.database.join("data.mdb");

    // We run and wait on the HTTP server

    // Expose an HTML page to debug the search in a browser
    let db_name_cloned = db_name.clone();
    let lmdb_path_cloned = lmdb_path.clone();
    let index_cloned = index.clone();
    let dash_html_route =
        warp::filters::method::get().and(warp::filters::path::end()).map(move || {
            // We retrieve the database size.
            let db_size =
                File::open(lmdb_path_cloned.clone()).unwrap().metadata().unwrap().len() as usize;

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
            let mut updates = update_store
                .iter_metas(|processed, aborted, pending| {
                    let mut updates = Vec::<UpdateStatus<_, UpdateMetaProgress, _>>::new();
                    for result in processed {
                        let (uid, meta) = result?;
                        updates.push(UpdateStatus::Processed { update_id: uid.get(), meta });
                    }
                    for result in aborted {
                        let (uid, meta) = result?;
                        updates.push(UpdateStatus::Aborted { update_id: uid.get(), meta });
                    }
                    for result in pending {
                        let (uid, meta) = result?;
                        updates.push(UpdateStatus::Pending { update_id: uid.get(), meta });
                    }
                    Ok(updates)
                })
                .unwrap();

            updates.sort_unstable_by(|s1, s2| s1.update_id().cmp(&s2.update_id()).reverse());

            if header.contains("text/html") {
                // We retrieve the database size.
                let db_size =
                    File::open(lmdb_path_cloned.clone()).unwrap().metadata().unwrap().len()
                        as usize;

                // And the number of documents in the database.
                let rtxn = index_cloned.read_txn().unwrap();
                let docs_count = index_cloned.clone().number_of_documents(&rtxn).unwrap() as usize;

                let template =
                    UpdatesTemplate { db_name: db_name.clone(), db_size, docs_count, updates };
                Box::new(template) as Box<dyn warp::Reply>
            } else {
                Box::new(warp::reply::json(&updates))
            }
        });

    let dash_bulma_route =
        warp::filters::method::get().and(warp::path!("bulma.min.css")).map(|| {
            Response::builder()
                .header("content-type", "text/css; charset=utf-8")
                .body(include_str!("../public/bulma.min.css"))
        });

    let dash_bulma_dark_route =
        warp::filters::method::get().and(warp::path!("bulma-prefers-dark.min.css")).map(|| {
            Response::builder()
                .header("content-type", "text/css; charset=utf-8")
                .body(include_str!("../public/bulma-prefers-dark.min.css"))
        });

    let dash_style_route = warp::filters::method::get().and(warp::path!("style.css")).map(|| {
        Response::builder()
            .header("content-type", "text/css; charset=utf-8")
            .body(include_str!("../public/style.css"))
    });

    let dash_jquery_route =
        warp::filters::method::get().and(warp::path!("jquery-3.4.1.min.js")).map(|| {
            Response::builder()
                .header("content-type", "application/javascript; charset=utf-8")
                .body(include_str!("../public/jquery-3.4.1.min.js"))
        });

    let dash_filesize_route =
        warp::filters::method::get().and(warp::path!("filesize.min.js")).map(|| {
            Response::builder()
                .header("content-type", "application/javascript; charset=utf-8")
                .body(include_str!("../public/filesize.min.js"))
        });

    let dash_script_route = warp::filters::method::get().and(warp::path!("script.js")).map(|| {
        Response::builder()
            .header("content-type", "application/javascript; charset=utf-8")
            .body(include_str!("../public/script.js"))
    });

    let updates_script_route =
        warp::filters::method::get().and(warp::path!("updates-script.js")).map(|| {
            Response::builder()
                .header("content-type", "application/javascript; charset=utf-8")
                .body(include_str!("../public/updates-script.js"))
        });

    let dash_logo_white_route =
        warp::filters::method::get().and(warp::path!("logo-white.svg")).map(|| {
            Response::builder()
                .header("content-type", "image/svg+xml")
                .body(include_str!("../public/logo-white.svg"))
        });

    let dash_logo_black_route =
        warp::filters::method::get().and(warp::path!("logo-black.svg")).map(|| {
            Response::builder()
                .header("content-type", "image/svg+xml")
                .body(include_str!("../public/logo-black.svg"))
        });

    #[derive(Debug, Deserialize)]
    #[serde(untagged)]
    enum UntaggedEither<L, R> {
        Left(L),
        Right(R),
    }

    impl<L, R> From<UntaggedEither<L, R>> for Either<L, R> {
        fn from(value: UntaggedEither<L, R>) -> Either<L, R> {
            match value {
                UntaggedEither::Left(left) => Either::Left(left),
                UntaggedEither::Right(right) => Either::Right(right),
            }
        }
    }

    #[derive(Debug, Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    struct QueryBody {
        query: Option<String>,
        filters: Option<String>,
        sort: Option<String>,
        facet_filters: Option<Vec<UntaggedEither<Vec<String>, String>>>,
        facet_distribution: Option<bool>,
        limit: Option<usize>,
    }

    #[derive(Debug, Serialize)]
    #[serde(rename_all = "camelCase")]
    struct Answer {
        documents: Vec<Map<String, Value>>,
        number_of_candidates: u64,
        facets: BTreeMap<String, BTreeMap<String, u64>>,
    }

    let disable_highlighting = opt.disable_highlighting;
    let index_cloned = index.clone();
    let query_route = warp::filters::method::post()
        .and(warp::path!("query"))
        .and(warp::body::json())
        .map(move |query: QueryBody| {
            let before_search = Instant::now();
            let index = index_cloned.clone();
            let rtxn = index.read_txn().unwrap();

            let mut search = index.search(&rtxn);
            if let Some(query) = query.query {
                search.query(query);
            }

            let filters = match query.filters.as_ref() {
                Some(condition) if !condition.trim().is_empty() => {
                    MilliFilter::from_str(condition).unwrap()
                }
                _otherwise => None,
            };

            let facet_filters = match query.facet_filters.as_ref() {
                Some(array) => {
                    let eithers = array.iter().map(|either| match either {
                        UntaggedEither::Left(l) => {
                            Either::Left(l.iter().map(|s| s.as_str()).collect::<Vec<&str>>())
                        }
                        UntaggedEither::Right(r) => Either::Right(r.as_str()),
                    });
                    MilliFilter::from_array(eithers).unwrap()
                }
                _otherwise => None,
            };

            let condition = match (filters, facet_filters) {
                (Some(filters), Some(facet_filters)) => Some(FilterCondition::And(
                    Box::new(filters.into()),
                    Box::new(facet_filters.into()),
                )),
                (Some(condition), None) | (None, Some(condition)) => Some(condition.into()),
                _otherwise => None,
            };

            if let Some(condition) = condition {
                search.filter(condition.into());
            }

            if let Some(limit) = query.limit {
                search.limit(limit);
            }

            if let Some(sort) = query.sort {
                search.sort_criteria(vec![sort.parse().map_err(SortError::from).unwrap()]);
            }

            let SearchResult { matching_words, candidates, documents_ids } =
                search.execute().unwrap();

            let number_of_candidates = candidates.len();
            let facets = if query.facet_distribution == Some(true) {
                Some(index.facets_distribution(&rtxn).candidates(candidates).execute().unwrap())
            } else {
                None
            };

            let mut documents = Vec::new();
            let fields_ids_map = index.fields_ids_map(&rtxn).unwrap();
            let displayed_fields = match index.displayed_fields_ids(&rtxn).unwrap() {
                Some(fields) => fields,
                None => fields_ids_map.iter().map(|(id, _)| id).collect(),
            };
            let attributes_to_highlight = match index.searchable_fields(&rtxn).unwrap() {
                Some(fields) => fields.into_iter().map(String::from).collect(),
                None => fields_ids_map.iter().map(|(_, name)| name).map(String::from).collect(),
            };

            let mut matcher_builder =
                MatcherBuilder::new(matching_words, TokenizerBuilder::default().build());
            matcher_builder.highlight_prefix("<mark>".to_string());
            matcher_builder.highlight_suffix("</mark>".to_string());
            let highlighter = Highlighter::new(matcher_builder);
            for (_id, obkv) in index.documents(&rtxn, documents_ids).unwrap() {
                let mut object = obkv_to_json(&displayed_fields, &fields_ids_map, obkv).unwrap();
                if !disable_highlighting {
                    highlighter.highlight_record(&mut object, &attributes_to_highlight);
                }

                documents.push(object);
            }

            let answer =
                Answer { documents, number_of_candidates, facets: facets.unwrap_or_default() };

            Response::builder()
                .header("Content-Type", "application/json")
                .header("Time-Ms", before_search.elapsed().as_millis().to_string())
                .body(serde_json::to_string(&answer).unwrap())
        });

    let index_cloned = index.clone();
    let document_route = warp::filters::method::get().and(warp::path!("document" / String)).map(
        move |id: String| {
            let index = index_cloned.clone();
            let rtxn = index.read_txn().unwrap();

            let external_documents_ids = index.external_documents_ids(&rtxn).unwrap();
            let fields_ids_map = index.fields_ids_map(&rtxn).unwrap();
            let displayed_fields = match index.displayed_fields_ids(&rtxn).unwrap() {
                Some(fields) => fields,
                None => fields_ids_map.iter().map(|(id, _)| id).collect(),
            };

            match external_documents_ids.get(&id) {
                Some(document_id) => {
                    let document_id = document_id as u32;
                    let (_, obkv) =
                        index.documents(&rtxn, Some(document_id)).unwrap().pop().unwrap();
                    let document = obkv_to_json(&displayed_fields, &fields_ids_map, obkv).unwrap();

                    Response::builder()
                        .header("Content-Type", "application/json")
                        .body(serde_json::to_string(&document).unwrap())
                }
                None => Response::builder()
                    .status(404)
                    .body(format!("Document with id {:?} not found.", id)),
            }
        },
    );

    async fn buf_stream(
        update_store: Arc<UpdateStore<UpdateMeta, String>>,
        update_status_sender: broadcast::Sender<
            UpdateStatus<UpdateMeta, UpdateMetaProgress, String>,
        >,
        update_method: Option<String>,
        format: String,
        encoding: Option<String>,
        mut stream: impl futures::Stream<Item = Result<impl bytes::Buf, warp::Error>> + Unpin,
    ) -> Result<impl warp::Reply, warp::Rejection> {
        let file = tokio::task::block_in_place(tempfile::tempfile).unwrap();
        let mut file = TFile::from_std(file);

        while let Some(result) = stream.next().await {
            let mut bytes = Vec::new();
            result.unwrap().reader().read_to_end(&mut bytes).unwrap();
            file.write_all(&bytes[..]).await.unwrap();
        }

        let file = file.into_std().await;
        let mmap = unsafe { memmap2::Mmap::map(&file).expect("can't map file") };

        let method = match update_method.as_deref() {
            Some("replace") => String::from("replace"),
            Some("update") => String::from("update"),
            _ => String::from("replace"),
        };

        let meta = UpdateMeta::DocumentsAddition { method, format, encoding };
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
    let indexing_route = warp::filters::method::post()
        .and(warp::path!("documents"))
        .and(warp::header::header("content-type"))
        .and(warp::header::optional::<String>("content-encoding"))
        .and(warp::query::query())
        .and(warp::body::stream())
        .and_then(move |content_type: String, content_encoding, params: QueryUpdate, stream| {
            let format = match content_type.as_str() {
                "text/csv" => "csv",
                "application/json" => "json",
                "application/x-ndjson" => "jsonl",
                otherwise => panic!("invalid update format: {}", otherwise),
            };

            buf_stream(
                update_store_cloned.clone(),
                update_status_sender_cloned.clone(),
                params.method,
                format.to_string(),
                content_encoding,
                stream,
            )
        });

    let update_store_cloned = update_store.clone();
    let update_status_sender_cloned = update_status_sender.clone();
    let clearing_route =
        warp::filters::method::post().and(warp::path!("clear-documents")).map(move || {
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

    let update_store_cloned = update_store.clone();
    let update_status_sender_cloned = update_status_sender.clone();
    let change_facet_levels_route = warp::filters::method::post()
        .and(warp::path!("facet-level-sizes"))
        .and(warp::body::json())
        .map(move |levels: Facets| {
            let meta = UpdateMeta::Facets(levels);
            let update_id = update_store_cloned.register_update(&meta, &[]).unwrap();
            let _ = update_status_sender_cloned.send(UpdateStatus::Pending { update_id, meta });
            eprintln!("update {} registered", update_id);
            warp::reply()
        });

    let update_store_cloned = update_store.clone();
    let update_status_sender_cloned = update_status_sender.clone();
    let abort_update_id_route = warp::filters::method::delete()
        .and(warp::path!("update" / u64))
        .map(move |update_id: u64| {
            if let Some(meta) = update_store_cloned.abort_update(update_id).unwrap() {
                let _ = update_status_sender_cloned.send(UpdateStatus::Aborted { update_id, meta });
                eprintln!("update {} aborted", update_id);
            }
            warp::reply()
        });

    let update_store_cloned = update_store.clone();
    let update_status_sender_cloned = update_status_sender.clone();
    let abort_pending_updates_route =
        warp::filters::method::delete().and(warp::path!("updates")).map(move || {
            let updates = update_store_cloned.abort_pendings().unwrap();
            for (update_id, meta) in updates {
                let _ = update_status_sender_cloned.send(UpdateStatus::Aborted { update_id, meta });
                eprintln!("update {} aborted", update_id);
            }
            warp::reply()
        });

    let update_ws_route =
        warp::ws().and(warp::path!("updates" / "ws")).map(move |ws: warp::ws::Ws| {
            // And then our closure will be called when it completes...
            let update_status_receiver = update_status_sender.subscribe();
            ws.on_upgrade(|websocket| {
                // Just echo all updates messages...
                BroadcastStream::new(update_status_receiver)
                    .flat_map(|result| match result {
                        Ok(status) => {
                            let msg = serde_json::to_string(&status).unwrap();
                            stream::iter(Some(Ok(Message::text(msg))))
                        }
                        Err(e) => {
                            eprintln!("channel error: {:?}", e);
                            stream::iter(None)
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

    let die_route = warp::filters::method::get().and(warp::path!("die")).map(move || {
        eprintln!("Killed by an HTTP request received on the die route");
        std::process::exit(0);
        #[allow(unreachable_code)]
        warp::reply()
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
        .or(document_route)
        .or(indexing_route)
        .or(abort_update_id_route)
        .or(abort_pending_updates_route)
        .or(clearing_route)
        .or(change_settings_route)
        .or(change_facet_levels_route)
        .or(update_ws_route)
        .or(die_route);

    let addr = SocketAddr::from_str(&opt.http_listen_addr)?;
    warp::serve(routes).run(addr).await;
    Ok(())
}

fn documents_from_jsonl(reader: impl Read) -> anyhow::Result<Vec<u8>> {
    let mut documents = DocumentsBatchBuilder::new(Vec::new());
    let reader = BufReader::new(reader);

    for result in serde_json::Deserializer::from_reader(reader).into_iter::<Map<String, Value>>() {
        let object = result?;
        documents.append_json_object(&object)?;
    }

    documents.into_inner().map_err(Into::into)
}

fn documents_from_json(reader: impl Read) -> anyhow::Result<Vec<u8>> {
    let mut documents = DocumentsBatchBuilder::new(Vec::new());
    let list: Vec<Map<String, Value>> = serde_json::from_reader(reader)?;

    for object in list {
        documents.append_json_object(&object)?;
    }

    documents.into_inner().map_err(Into::into)
}

fn documents_from_csv(reader: impl Read) -> anyhow::Result<Vec<u8>> {
    let csv = csv::Reader::from_reader(reader);

    let mut documents = DocumentsBatchBuilder::new(Vec::new());
    documents.append_csv(csv)?;

    documents.into_inner().map_err(Into::into)
}

#[cfg(test)]
mod tests {
    use maplit::{btreeset, hashmap, hashset};
    use milli::update::Setting;
    use serde_test::{assert_tokens, Token};

    use crate::Settings;

    #[test]
    fn serde_settings_set() {
        let settings = Settings {
            displayed_attributes: Setting::Set(vec!["name".to_string()]),
            searchable_attributes: Setting::Set(vec!["age".to_string()]),
            filterable_attributes: Setting::Set(hashset! { "age".to_string() }),
            sortable_attributes: Setting::Set(hashset! { "age".to_string() }),
            criteria: Setting::Set(vec!["age:asc".to_string()]),
            stop_words: Setting::Set(btreeset! { "and".to_string() }),
            synonyms: Setting::Set(hashmap! { "alex".to_string() => vec!["alexey".to_string()] }),
        };

        assert_tokens(
            &settings,
            &[
                Token::Struct { name: "Settings", len: 7 },
                Token::Str("displayedAttributes"),
                Token::Some,
                Token::Seq { len: Some(1) },
                Token::Str("name"),
                Token::SeqEnd,
                Token::Str("searchableAttributes"),
                Token::Some,
                Token::Seq { len: Some(1) },
                Token::Str("age"),
                Token::SeqEnd,
                Token::Str("filterableAttributes"),
                Token::Some,
                Token::Seq { len: Some(1) },
                Token::Str("age"),
                Token::SeqEnd,
                Token::Str("sortableAttributes"),
                Token::Some,
                Token::Seq { len: Some(1) },
                Token::Str("age"),
                Token::SeqEnd,
                Token::Str("criteria"),
                Token::Some,
                Token::Seq { len: Some(1) },
                Token::Str("age:asc"),
                Token::SeqEnd,
                Token::Str("stopWords"),
                Token::Some,
                Token::Seq { len: Some(1) },
                Token::Str("and"),
                Token::SeqEnd,
                Token::Str("synonyms"),
                Token::Some,
                Token::Map { len: Some(1) },
                Token::Str("alex"),
                Token::Seq { len: Some(1) },
                Token::Str("alexey"),
                Token::SeqEnd,
                Token::MapEnd,
                Token::StructEnd,
            ],
        );
    }

    #[test]
    fn serde_settings_reset() {
        let settings = Settings {
            displayed_attributes: Setting::Reset,
            searchable_attributes: Setting::Reset,
            filterable_attributes: Setting::Reset,
            sortable_attributes: Setting::Reset,
            criteria: Setting::Reset,
            stop_words: Setting::Reset,
            synonyms: Setting::Reset,
        };

        assert_tokens(
            &settings,
            &[
                Token::Struct { name: "Settings", len: 7 },
                Token::Str("displayedAttributes"),
                Token::None,
                Token::Str("searchableAttributes"),
                Token::None,
                Token::Str("filterableAttributes"),
                Token::None,
                Token::Str("sortableAttributes"),
                Token::None,
                Token::Str("criteria"),
                Token::None,
                Token::Str("stopWords"),
                Token::None,
                Token::Str("synonyms"),
                Token::None,
                Token::StructEnd,
            ],
        );
    }

    #[test]
    fn serde_settings_notset() {
        let settings = Settings {
            displayed_attributes: Setting::NotSet,
            searchable_attributes: Setting::NotSet,
            filterable_attributes: Setting::NotSet,
            sortable_attributes: Setting::NotSet,
            criteria: Setting::NotSet,
            stop_words: Setting::NotSet,
            synonyms: Setting::NotSet,
        };

        assert_tokens(&settings, &[Token::Struct { name: "Settings", len: 0 }, Token::StructEnd]);
    }
}
