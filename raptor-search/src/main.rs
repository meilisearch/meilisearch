extern crate env_logger;
extern crate fst;
extern crate raptor;

use std::{fs, env};
use fst::Streamer;
use raptor::{DocIndexMap, RankedStream, LevBuilder};

fn main() {
    drop(env_logger::init());

    let lev_builder = LevBuilder::new();
    let map = {
        let fst = fs::read("map.fst").unwrap();
        let values = fs::read("values.vecs").unwrap();
        DocIndexMap::from_bytes(fst, &values).unwrap()
    };

    let query = env::args().nth(1).expect("Please enter query words!");
    let query = query.to_lowercase();

    println!("Searching for: {:?}", query);

    let mut automatons = Vec::new();
    for query in query.split_whitespace() {
        let lev = lev_builder.build_automaton(query);
        automatons.push(lev);
    }

    let mut stream = RankedStream::new(&map, map.values(), automatons);
    while let Some(document_id) = stream.next() {
        println!("{:?}", document_id);
    }
}
