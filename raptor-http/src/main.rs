#[macro_use] extern crate serde_derive;

use std::env;
use std::fs::File;
use std::path::Path;
use std::collections::hash_set::HashSet;
use std::io::{self, BufReader, BufRead, Write};
use std::sync::Arc;
use std::error::Error;
use std::str::from_utf8_unchecked;
use rocksdb::{DB, DBOptions, IngestExternalFileOptions};
use raptor::{automaton, Metadata, RankedStream};
use fst::Streamer;
use warp::Filter;

#[derive(Debug, Deserialize)]
struct SearchQuery { query: String }

#[derive(Debug, Serialize)]
struct Document<'a> {
    id: u64,
    title: &'a str,
    description: &'a str,
}

type CommonWords = HashSet<String>;

fn common_words<P>(path: P) -> io::Result<CommonWords>
where P: AsRef<Path>,
{
    let file = File::open(path)?;
    let file = BufReader::new(file);
    let mut set = HashSet::new();
    for line in file.lines().filter_map(|l| l.ok()) {
        for word in line.split_whitespace() {
            set.insert(word.to_owned());
        }
    }
    Ok(set)
}

fn search<M, D>(metadata: M, database: D, common_words: &CommonWords, query: &str) -> Result<String, Box<Error>>
where M: AsRef<Metadata>,
      D: AsRef<DB>,
{
    let mut automatons = Vec::new();
    for query in query.split_whitespace().filter(|q| !common_words.contains(*q)) {
        let lev = automaton::build(query);
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

        let document = Document {
            id: document.document_id,
            title: title,
            description: description,
        };

        if !first { write!(&mut body, ",")? }
        serde_json::to_writer(&mut body, &document)?;

        first = false;
    }

    write!(&mut body, "]")?;

    Ok(String::from_utf8(body)?)
}

fn main() {
    let name = env::args().nth(1).expect("Missing meta file name (e.g. lucid-ptolemy)");
    let map_file = format!("{}.map", name);
    let idx_file = format!("{}.idx", name);
    let sst_file = format!("{}.sst", name);

    let rocksdb = "rocksdb/storage";

    let meta = unsafe { Metadata::from_paths(map_file, idx_file).unwrap() };
    let meta = Arc::new(meta);

    let db = DB::open_default(rocksdb).unwrap();
    db.ingest_external_file(&IngestExternalFileOptions::new(), &[&sst_file]).unwrap();
    drop(db);
    let db = DB::open_for_read_only(DBOptions::default(), rocksdb, false).unwrap();
    let db = Arc::new(db);

    let common_words = common_words("fr.stopwords.txt").unwrap();

    let routes = warp::path("search")
        .and(warp::query())
        .map(move |query: SearchQuery| {
            let body = search(meta.clone(), db.clone(), &common_words, &query.query).unwrap();
            body
        })
        .with(warp::reply::with::header("Content-Type", "application/json"));

    warp::serve(routes).run(([127, 0, 0, 1], 3030));
}
