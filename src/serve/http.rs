use std::str::from_utf8_unchecked;
use std::io::{self, Write};
use std::net::SocketAddr;
use std::error::Error;
use std::sync::Arc;

use rocksdb::{DB, DBOptions, IngestExternalFileOptions};
use raptor::{automaton, Metadata};
use raptor::rank::RankedStream;
use fst::Streamer;
use warp::Filter;

use crate::serve::http_feature::CommandHttp;
use crate::common_words::{self, CommonWords};

#[derive(Debug, Serialize)]
struct Document<'a> {
    id: u64,
    title: &'a str,
    description: &'a str,
    image: &'a str,
}

#[derive(Debug, Deserialize)]
struct SearchQuery { q: String }

pub struct HttpServer {
    listen_addr: SocketAddr,
    common_words: Arc<CommonWords>,
    metadata: Arc<Metadata>,
    db: Arc<DB>,
}

impl HttpServer {
    pub fn from_command(command: CommandHttp) -> io::Result<HttpServer> {
        let common_words = common_words::from_file(command.stop_words)?;

        let meta_name = command.meta_name.display();
        let map_file = format!("{}.map", meta_name);
        let idx_file = format!("{}.idx", meta_name);
        let sst_file = format!("{}.sst", meta_name);
        let metadata = unsafe { Metadata::from_paths(map_file, idx_file).unwrap() };

        let rocksdb = "rocksdb/storage";
        let db = DB::open_default(rocksdb).unwrap();
        db.ingest_external_file(&IngestExternalFileOptions::new(), &[&sst_file]).unwrap();
        drop(db);
        let db = DB::open_for_read_only(DBOptions::default(), rocksdb, false).unwrap();

        Ok(HttpServer {
            listen_addr: command.listen_addr,
            common_words: Arc::new(common_words),
            metadata: Arc::new(metadata),
            db: Arc::new(db),
        })
    }

    pub fn serve(self) {
        let HttpServer { listen_addr, common_words, metadata, db } = self;

        let routes = warp::path("search")
            .and(warp::query())
            .map(move |query: SearchQuery| {
                let body = search(metadata.clone(), db.clone(), common_words.clone(), &query.q).unwrap();
                body
            })
            .with(warp::reply::with::header("Content-Type", "application/json"))
            .with(warp::reply::with::header("Access-Control-Allow-Origin", "*"));

        warp::serve(routes).run(listen_addr)
    }
}

fn search<M, D, C>(metadata: M, database: D, common_words: C, query: &str) -> Result<String, Box<Error>>
where M: AsRef<Metadata>,
      D: AsRef<DB>,
      C: AsRef<CommonWords>,
{
    let mut automatons = Vec::new();
    for query in query.split_whitespace().map(str::to_lowercase) {
        if common_words.as_ref().contains(&query) { continue }
        let lev = automaton::build(&query);
        automatons.push(lev);
    }

    let mut stream = RankedStream::new(metadata.as_ref(), automatons, 20);
    let mut body = Vec::new();
    write!(&mut body, "[")?;

    let mut first = true;
    while let Some(document) = stream.next() {
        let title_key = format!("{}-title", document.document_id);
        let title = database.as_ref().get(title_key.as_bytes()).unwrap().unwrap();
        let title = unsafe { from_utf8_unchecked(&title) };

        let description_key = format!("{}-description", document.document_id);
        let description = database.as_ref().get(description_key.as_bytes()).unwrap().unwrap();
        let description = unsafe { from_utf8_unchecked(&description) };

        let image_key = format!("{}-image", document.document_id);
        let image = database.as_ref().get(image_key.as_bytes()).unwrap().unwrap();
        let image = unsafe { from_utf8_unchecked(&image) };

        let document = Document {
            id: document.document_id,
            title: title,
            description: description,
            image: image,
        };

        if !first { write!(&mut body, ",")? }
        serde_json::to_writer(&mut body, &document)?;

        first = false;
    }

    write!(&mut body, "]")?;

    Ok(String::from_utf8(body)?)
}
