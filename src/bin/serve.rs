use std::borrow::Cow;
use std::collections::HashSet;
use std::fs::File;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::str::FromStr;
use std::time::Instant;

use askama_warp::Template;
use heed::EnvOpenOptions;
use serde::Deserialize;
use slice_group_by::StrGroupBy;
use structopt::StructOpt;
use warp::{Filter, http::Response};

use milli::{Index, SearchResult};

#[cfg(target_os = "linux")]
#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

#[derive(Debug, StructOpt)]
#[structopt(name = "milli", about = "The server binary of the milli project.")]
struct Opt {
    /// The database path where the LMDB database is located.
    /// It is created if it doesn't already exist.
    #[structopt(long = "db", parse(from_os_str))]
    database: PathBuf,

    /// The maximum size the database can take on disk. It is recommended to specify
    /// the whole disk space (value must be a multiple of a page size).
    #[structopt(long = "db-size", default_value = "107374182400")] // 100 GB
    database_size: usize,

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

fn highlight_string(string: &str, words: &HashSet<String>) -> String {
    let mut output = String::new();
    for token in string.linear_group_by_key(|c| c.is_alphanumeric()) {
        let lowercase_token = token.to_lowercase();
        let to_highlight = words.contains(&lowercase_token);
        if to_highlight { output.push_str("<mark>") }
        output.push_str(token);
        if to_highlight { output.push_str("</mark>") }
    }
    output
}

#[derive(Template)]
#[template(path = "index.html")]
struct IndexTemplate {
    db_name: String,
    db_size: usize,
    docs_count: usize,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let opt = Opt::from_args();

    stderrlog::new()
        .verbosity(opt.verbose)
        .show_level(false)
        .timestamp(stderrlog::Timestamp::Off)
        .init()?;

    std::fs::create_dir_all(&opt.database)?;
    let env = EnvOpenOptions::new()
        .map_size(opt.database_size)
        .max_dbs(10)
        .open(&opt.database)?;

    // Open the LMDB database.
    let index = Index::new(&env)?;

    // Retrieve the database the file stem (w/o the extension),
    // the disk file size and the number of documents in the database.
    let db_name = opt.database.file_stem().and_then(|s| s.to_str()).unwrap_or("").to_string();
    let db_size = File::open(opt.database.join("data.mdb"))?.metadata()?.len() as usize;

    let rtxn = env.read_txn()?;
    let docs_count = index.number_of_documents(&rtxn)? as usize;
    drop(rtxn);

    // We run and wait on the HTTP server

    // Expose an HTML page to debug the search in a browser
    let dash_html_route = warp::filters::method::get()
        .and(warp::filters::path::end())
        .map(move || IndexTemplate { db_name: db_name.clone(), db_size, docs_count });

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
        query: String,
    }

    let env_cloned = env.clone();
    let disable_highlighting = opt.disable_highlighting;
    let query_route = warp::filters::method::post()
        .and(warp::path!("query"))
        .and(warp::body::json())
        .map(move |query: QueryBody| {
            let before_search = Instant::now();
            let rtxn = env_cloned.read_txn().unwrap();

            let SearchResult { found_words, documents_ids } = index.search(&rtxn)
                .query(query.query)
                .execute()
                .unwrap();

            let mut body = Vec::new();
            if let Some(headers) = index.headers(&rtxn).unwrap() {
                // We write the headers
                body.extend_from_slice(headers);
                let documents = index.documents(&rtxn, documents_ids).unwrap();

                for (_id, content) in documents {
                    let content = std::str::from_utf8(content.as_ref()).unwrap();
                    let content = if disable_highlighting {
                        Cow::from(content)
                    } else {
                        Cow::from(highlight_string(content, &found_words))
                    };

                    body.extend_from_slice(content.as_bytes());
                }
            }

            Response::builder()
                .header("Content-Type", "text/csv")
                .header("Time-Ms", before_search.elapsed().as_millis().to_string())
                .body(String::from_utf8(body).unwrap())
        });

    let routes = dash_html_route
        .or(dash_bulma_route)
        .or(dash_bulma_dark_route)
        .or(dash_style_route)
        .or(dash_jquery_route)
        .or(dash_papaparse_route)
        .or(dash_filesize_route)
        .or(dash_script_route)
        .or(dash_logo_white_route)
        .or(dash_logo_black_route)
        .or(query_route);

    let addr = SocketAddr::from_str(&opt.http_listen_addr).unwrap();
    warp::serve(routes).run(addr).await;

    Ok(())
}
