extern crate env_logger;
extern crate rocksdb;
extern crate fst;
extern crate raptor;
extern crate elapsed;

use std::env;
use std::str::from_utf8_unchecked;
use std::io::{self, Write};
use elapsed::measure_time;
use fst::Streamer;
use rocksdb::{DB, DBOptions};
use raptor::{load_map, DocIndexMap, RankedStream, LevBuilder};

fn search(map: &DocIndexMap, lev_builder: &LevBuilder, db: &DB, query: &str) {
    let mut automatons = Vec::new();
    for query in query.split_whitespace() {
        let lev = lev_builder.get_automaton(query);
        automatons.push(lev);
    }

    let mut stream = RankedStream::new(&map, map.values(), automatons, 20);
    while let Some(document) = stream.next() {
        print!("{:?} ", document.document_id);

        let title_key = format!("{}-title", document.document_id);
        let title = db.get(title_key.as_bytes()).unwrap().unwrap();
        let title = unsafe { from_utf8_unchecked(&title) };
        print!("{:?}", title);

        println!();
    }
}

fn main() {
    drop(env_logger::init());

    let (elapsed, map) = measure_time(|| load_map("map.fst", "values.vecs").unwrap());
    println!("{} to load the map", elapsed);

    let (elapsed, lev_builder) = measure_time(|| LevBuilder::new());
    println!("{} to load the levenshtein automaton", elapsed);

    let (elapsed, db) = measure_time(|| {
        let opts = DBOptions::new();
        let error_if_log_file_exist = false;
        DB::open_for_read_only(opts, "rocksdb/storage", error_if_log_file_exist).unwrap()
    });
    println!("{} to load the rocksdb DB", elapsed);

    match env::args().nth(1) {
        Some(query) => {
            println!("Searching for: {:?}", query);
            let query = query.to_lowercase();
            let (elapsed, _) = measure_time(|| search(&map, &lev_builder, &db, &query));
            println!("Finished in {}", elapsed);
        },
        None => loop {
            print!("Searching for: ");
            io::stdout().flush().unwrap();

            let mut query = String::new();
            io::stdin().read_line(&mut query).unwrap();
            let query = query.trim().to_lowercase();

            if query.is_empty() { break }

            let (elapsed, _) = measure_time(|| search(&map, &lev_builder, &db, &query));
            println!("Finished in {}", elapsed);
        },
    }
}
