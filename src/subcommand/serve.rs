use std::collections::HashSet;
use std::fs::{File, create_dir_all};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use askama_warp::Template;
use futures::{FutureExt, StreamExt};
use futures::stream;
use heed::EnvOpenOptions;
use serde::Deserialize;
use structopt::StructOpt;
use tokio::fs::File as TFile;
use tokio::io::AsyncWriteExt;
use tokio::sync::broadcast;
use warp::filters::ws::Message;
use warp::{Filter, http::Response};

use crate::tokenizer::{simple_tokenizer, TokenType};
use crate::{Index, UpdateStore, SearchResult};

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
}

fn highlight_record(record: &csv::StringRecord, words: &HashSet<String>) -> csv::StringRecord {
    let mut output_record = csv::StringRecord::new();
    let mut buffer = String::new();
    for field in record {
        buffer.clear();
        for (token_type, token) in simple_tokenizer(field) {
            if token_type == TokenType::Word {
                let lowercase_token = token.to_lowercase();
                let to_highlight = words.contains(&lowercase_token);
                if to_highlight { buffer.push_str("<mark>") }
                buffer.push_str(token);
                if to_highlight { buffer.push_str("</mark>") }
            } else {
                buffer.push_str(token);
            }
        }
        output_record.push_field(&buffer);
    }
    output_record
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
struct UpdatesTemplate {
    db_name: String,
    updates: Vec<String>,
}

pub fn run(opt: Opt) -> anyhow::Result<()> {
    stderrlog::new()
        .verbosity(opt.verbose)
        .show_level(false)
        .timestamp(stderrlog::Timestamp::Off)
        .init()?;

    let env = EnvOpenOptions::new()
        .map_size(opt.database_size)
        .max_dbs(10)
        .open(&opt.database)?;

    // Open the LMDB database.
    let index = Index::new(&env)?;

    // Setup the LMDB based update database.
    let mut update_store_options = EnvOpenOptions::new();
    update_store_options.map_size(opt.update_database_size);

    let update_store_path = opt.database.join("updates.mdb");
    create_dir_all(&update_store_path)?;

    let (update_status_sender, _) = broadcast::channel(100);
    let update_status_sender_cloned = update_status_sender.clone();
    let update_store = UpdateStore::open(
        update_store_options,
        update_store_path,
        move |uid, meta: String, _content| {
            let _ = update_status_sender_cloned.send(format!("processing update {}", uid));
            std::thread::sleep(Duration::from_secs(3));
            let _ = update_status_sender_cloned.send(format!("update {} processed", uid));
            Ok(meta)
        })?;

    // Retrieve the database the file stem (w/o the extension),
    // the disk file size and the number of documents in the database.
    let db_name = opt.database.file_stem().and_then(|s| s.to_str()).unwrap_or("").to_string();
    let db_size = File::open(opt.database.join("data.mdb"))?.metadata()?.len() as usize;

    let rtxn = env.read_txn()?;
    let docs_count = index.number_of_documents(&rtxn)? as usize;
    drop(rtxn);

    // We run and wait on the HTTP server

    // Expose an HTML page to debug the search in a browser
    let db_name_cloned = db_name.clone();
    let dash_html_route = warp::filters::method::get()
        .and(warp::filters::path::end())
        .map(move || IndexTemplate { db_name: db_name_cloned.clone(), db_size, docs_count });

    let update_store_cloned = update_store.clone();
    let updates_list_or_html_route = warp::filters::method::get()
        .and(warp::header("Accept"))
        .and(warp::path!("updates"))
        .map(move |header: String| {
            let update_store = update_store_cloned.clone();
            let mut updates = update_store.iter_metas(|processed, pending| {
                let mut updates = Vec::new();
                for result in processed {
                    let (id, _) = result?;
                    updates.push(format!("update {} processed", id.get()));
                }
                for result in pending {
                    let (id, _) = result?;
                    updates.push(format!("update {} pending", id.get()));
                }
                Ok(updates)
            }).unwrap();

            if header.contains("text/html") {
                updates.reverse();
                let template = UpdatesTemplate { db_name: db_name.clone(), updates };
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

    let dash_papaparse_route = warp::filters::method::get()
        .and(warp::path!("papaparse.min.js"))
        .map(|| Response::builder()
            .header("content-type", "application/javascript; charset=utf-8")
            .body(include_str!("../../public/papaparse.min.js"))
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

    let env_cloned = env.clone();
    let disable_highlighting = opt.disable_highlighting;
    let query_route = warp::filters::method::post()
        .and(warp::path!("query"))
        .and(warp::body::json())
        .map(move |query: QueryBody| {
            let before_search = Instant::now();
            let rtxn = env_cloned.read_txn().unwrap();

            let mut search = index.search(&rtxn);
            if let Some(query) = query.query {
                search.query(query);
            }

            let SearchResult { found_words, documents_ids } = search.execute().unwrap();

            let body = match index.headers(&rtxn).unwrap() {
                Some(headers) => {
                    let mut wtr = csv::Writer::from_writer(Vec::new());

                    // We write the headers
                    wtr.write_record(&headers).unwrap();

                    let documents = index.documents(&rtxn, documents_ids).unwrap();
                    for (_id, record) in documents {
                        let record = if disable_highlighting {
                            record
                        } else {
                            highlight_record(&record, &found_words)
                        };
                        wtr.write_record(&record).unwrap();
                    }

                    wtr.into_inner().unwrap()
                },
                None => Vec::new(),
            };

            Response::builder()
                .header("Content-Type", "text/csv")
                .header("Time-Ms", before_search.elapsed().as_millis().to_string())
                .body(String::from_utf8(body).unwrap())
        });

    async fn buf_stream(
        update_store: Arc<UpdateStore<String>>,
        update_status_sender: broadcast::Sender<String>,
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

        let meta = String::from("I am the metadata");
        let uid = update_store.register_update(&meta, &mmap[..]).unwrap();
        update_status_sender.send(format!("update {} pending", uid)).unwrap();
        eprintln!("Registering update {}", uid);

        Ok(warp::reply())
    }

    let update_store_cloned = update_store.clone();
    let update_status_sender_cloned = update_status_sender.clone();
    let indexing_route = warp::filters::method::post()
        .and(warp::path!("documents"))
        .and(warp::body::stream())
        .and_then(move |stream| {
            buf_stream(update_store_cloned.clone(), update_status_sender_cloned.clone(), stream)
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
                        match result{
                            Ok(msg) => stream::iter(Some(Ok(Message::text(msg)))),
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
        .or(dash_papaparse_route)
        .or(dash_filesize_route)
        .or(dash_script_route)
        .or(updates_script_route)
        .or(dash_logo_white_route)
        .or(dash_logo_black_route)
        .or(query_route)
        .or(indexing_route)
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
