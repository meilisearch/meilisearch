extern crate env_logger;
extern crate fst;
extern crate raptor;

use std::path::Path;
use std::{fs, env, io};
use fst::Streamer;
use raptor::{load_map, DocIndexMap, RankedStream, LevBuilder};

fn main() {
    drop(env_logger::init());

    let lev_builder = LevBuilder::new();
    let map = load_map("map.fst", "values.vecs").unwrap();

    let query = env::args().nth(1).expect("Please enter query words!");
    let query = query.to_lowercase();

    println!("Searching for: {:?}", query);

    let mut automatons = Vec::new();
    for query in query.split_whitespace() {
        let lev = lev_builder.build_automaton(query);
        automatons.push(lev);
    }

    let mut limit: Option<usize> = env::var("RAPTOR_OUTPUT_LIMIT").ok().and_then(|x| x.parse().ok());
    let mut stream = RankedStream::new(&map, map.values(), automatons);
    while let Some(document_id) = stream.next() {
        if limit == Some(0) { println!("..."); break }

        println!("{:?}", document_id);

        if let Some(ref mut limit) = limit { *limit -= 1 }
    }
}
