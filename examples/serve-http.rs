#[macro_use] extern crate serde_derive;

use std::str::from_utf8_unchecked;
use std::io::{self, Write};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::error::Error;
use std::sync::Arc;

use raptor::rank::{criterion, RankedStreamBuilder};
use raptor::{automaton, Metadata, CommonWords};
use rocksdb::{DB, DBOptions, IngestExternalFileOptions};
use warp::Filter;

use structopt::StructOpt;

#[derive(Debug, StructOpt)]
pub struct CommandHttp {
    /// The address and port to bind the server to.
    #[structopt(short = "l", default_value = "127.0.0.1:3030")]
    pub listen_addr: SocketAddr,

    /// The stop word file, each word must be separated by a newline.
    #[structopt(long = "stop-words", parse(from_os_str))]
    pub stop_words: PathBuf,

    /// Meta file name (e.g. relaxed-colden).
    #[structopt(parse(from_os_str))]
    pub meta_name: PathBuf,
}

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
        let common_words = CommonWords::from_file(command.stop_words)?;

        let map_file = command.meta_name.with_extension("map");
        let idx_file = command.meta_name.with_extension("idx");
        let sst_file = command.meta_name.with_extension("sst");
        let metadata = unsafe { Metadata::from_paths(map_file, idx_file).unwrap() };

        let rocksdb = "rocksdb/storage";
        let db = DB::open_default(rocksdb).unwrap();
        let sst_file = sst_file.to_str().unwrap();
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

    let mut builder = RankedStreamBuilder::new(metadata.as_ref(), automatons);
    builder.criteria(criterion::default());

    let mut stream = builder.build();
    let documents = stream.retrieve_documents(20);

    let mut body = Vec::new();
    write!(&mut body, "[")?;

    let mut first = true;
    for document in documents {
        let title_key = format!("{}-title", document.id);
        let title = database.as_ref().get(title_key.as_bytes()).unwrap().unwrap();
        let title = unsafe { from_utf8_unchecked(&title) };

        let description_key = format!("{}-description", document.id);
        let description = database.as_ref().get(description_key.as_bytes()).unwrap().unwrap();
        let description = unsafe { from_utf8_unchecked(&description) };

        let image_key = format!("{}-image", document.id);
        let image = database.as_ref().get(image_key.as_bytes()).unwrap().unwrap();
        let image = unsafe { from_utf8_unchecked(&image) };

        let document = Document {
            id: document.id,
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

fn main() {
    let command = CommandHttp::from_args();
    let server = HttpServer::from_command(command).unwrap();
    server.serve();
}
