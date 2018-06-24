#![feature(test)]

extern crate test;
extern crate fst;
extern crate raptor;

use std::path::Path;
use std::{fs, env, io};
use fst::Streamer;
use raptor::{load_map, DocIndexMap, RankedStream, LevBuilder};

#[bench]
fn chauve_souris(b: &mut test::Bencher) {
    let lev_builder = LevBuilder::new();
    let map = load_map("map.fst", "values.vecs").unwrap();

    let query = "chauve souris";

    b.iter(|| {
        let mut automatons = Vec::new();
        for query in query.split_whitespace() {
            let lev = lev_builder.build_automaton(query);
            automatons.push(lev);
        }

        let mut stream = RankedStream::new(&map, &map.values(), automatons);
        while let Some(document_id) = stream.next() {
            test::black_box(document_id);
        }
    })
}
