use std::env;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::PathBuf;

use fst::SetBuilder;

fn main() {
    let chinese_words_txt = "chinese-words.txt";
    let out_dir = env::var("OUT_DIR").unwrap();
    let chinese_words_fst = PathBuf::from(out_dir).join("chinese-words.fst");

    // Tell Cargo that if the given file changes, to rerun this build script.
    println!("cargo:rerun-if-changed={}", chinese_words_txt);

    let chinese_words_txt = File::open(chinese_words_txt).map(BufReader::new).unwrap();
    let chinese_words_fst = File::create(chinese_words_fst).unwrap();

    let mut builder = SetBuilder::new(chinese_words_fst).unwrap();
    for result in chinese_words_txt.lines() {
        let line = result.unwrap();
        if let Some(s) = line.split(' ').next() {
            builder.insert(s).unwrap();
        }
    }
    builder.finish().unwrap();
}
