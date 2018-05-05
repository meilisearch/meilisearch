// TODO make the raptor binary expose multiple subcommand
//      make only one binary

extern crate fst;
extern crate raptor;
extern crate serde_json;
#[macro_use] extern crate serde_derive;

use std::collections::HashSet;
use std::fs::File;
use std::io::{BufReader, BufRead};

use fst::Streamer;
use serde_json::from_str;

use raptor::{FstMapBuilder, FstMap};

#[derive(Debug, Deserialize)]
struct Product {
    product_id: u64,
    title: String,
    ft: String,
}

fn main() {
    let data = File::open("products.json_lines").unwrap();
    let data = BufReader::new(data);

    let common_words = {
        // TODO don't break if doesn't exist
        let file = File::open("fr.stopwords.txt").unwrap();
        let file = BufReader::new(file);
        let mut set = HashSet::new();

        for line in file.lines() {
            let words = line.unwrap();
            for word in words.split_whitespace() {
                set.insert(word.to_owned());
            }
        }

        set
    };

    let mut builder = FstMapBuilder::new();
    for line in data.lines() {
        let line = line.unwrap();

        // TODO if possible remove String allocation of Product here...
        let product: Product = from_str(&line).unwrap();

        let title = product.title.split_whitespace();
        let description = product.ft.split_whitespace().filter(|&s| s != "Description");
        let words = title.chain(description)
                         .filter(|&s| s.chars().any(|c| c.is_alphabetic())) // remove that ?
                         .map(|s| s.trim_matches(|c: char| !c.is_alphabetic()).to_lowercase())
                         .filter(|s| !common_words.contains(s));

        for word in words {
            builder.insert(word, product.product_id);
        }
    }

    let map = File::create("map.fst").unwrap();
    let values = File::create("values.vecs").unwrap();
    let (map, values) = builder.build(map, values).unwrap();

    eprintln!("Checking the dump consistency...");
    unsafe { FstMap::<u64>::from_paths("map.fst", "values.vecs").unwrap() };
}
