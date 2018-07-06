extern crate env_logger;
extern crate fst;
extern crate raptor;
extern crate elapsed;

use std::env;
use std::io::{self, Write};
use elapsed::measure_time;
use fst::Streamer;
use raptor::{load_map, DocIndexMap, RankedStream, LevBuilder};

fn search(map: &DocIndexMap, lev_builder: &LevBuilder, query: &str) {
    let mut automatons = Vec::new();
    for query in query.split_whitespace() {
        let lev = lev_builder.get_automaton(query);
        automatons.push(lev);
    }

    let mut stream = RankedStream::new(&map, map.values(), automatons, 20);
    while let Some(document_id) = stream.next() {
        print!("{:?}", document_id);

        // /* only here to debug !
        use std::{fs, process::Command};
        if let Ok(_) = fs::File::open("products.json_lines") {
            let output = Command::new("rg")
                                .arg(document_id.to_string())
                                .arg("products.json_lines")
                                .output();
            if let Ok(Ok(output)) = output.map(|o| String::from_utf8(o.stdout)) {
                if let Some(line) = output.lines().next() {
                    let pattern = "\"title\":";
                    if let Some(index) = line.find(pattern) {
                        let line: String = line[index..].chars().skip(pattern.len()).take(100).collect();
                        print!(" => {}", line);
                    }
                }
            }
        }
        // */

        println!();
    }
}

fn main() {
    drop(env_logger::init());

    let (elapsed, map) = measure_time(|| load_map("map.fst", "values.vecs").unwrap());
    println!("{} to load the map", elapsed);

    let (elapsed, lev_builder) = measure_time(|| LevBuilder::new());
    println!("{} to load the levenshtein automaton", elapsed);

    match env::args().nth(1) {
        Some(query) => {
            println!("Searching for: {:?}", query);
            let query = query.to_lowercase();
            let (elapsed, _) = measure_time(|| search(&map, &lev_builder, &query));
            println!("Finished in {}", elapsed);
        },
        None => loop {
            print!("Searching for: ");
            io::stdout().flush().unwrap();

            let mut query = String::new();
            io::stdin().read_line(&mut query).unwrap();
            let query = query.trim().to_lowercase();

            if query.is_empty() { break }

            let (elapsed, _) = measure_time(|| search(&map, &lev_builder, &query));
            println!("Finished in {}", elapsed);
        },
    }
}
