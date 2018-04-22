// TODO make the raptor binary expose multiple subcommand
//      make only one binary

extern crate fst;
extern crate raptor;
extern crate serde_json;
#[macro_use] extern crate serde_derive;

use std::fs::File;
use std::io::{BufReader, BufRead};

use fst::Streamer;
use serde_json::from_str;

use raptor::{MultiMapBuilder, MultiMap};

#[derive(Debug, Deserialize)]
struct Product {
    product_id: u64,
    title: String,
    ft: String,
}

fn main() {
    let data = File::open("products.json_lines").unwrap();
    let data = BufReader::new(data);

    let mut builder = MultiMapBuilder::new();
    for line in data.lines() {
        let line = line.unwrap();

        let product: Product = from_str(&line).unwrap();

        // TODO filter words here !!!
        let title = product.title.split_whitespace();
        let description = product.ft.split_whitespace();
        let words = title.chain(description);

        for word in words {
            builder.insert(word, product.product_id);
        }
    }

    let map = File::create("map.fst").unwrap();
    let values = File::create("values.vecs").unwrap();
    let (map, values) = builder.build(map, values).unwrap();

    let map = unsafe { MultiMap::from_paths("map.fst", "values.vecs").unwrap() };

    let mut stream = map.stream();
    while let Some(x) = stream.next() {
        println!("{:?}", x);
    }
}
