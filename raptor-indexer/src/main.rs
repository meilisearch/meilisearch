// TODO make the raptor binary expose multiple subcommand
//      make only one binary

extern crate raptor;
extern crate rocksdb;
extern crate serde_json;
#[macro_use] extern crate serde_derive;
extern crate unidecode;

use std::path::Path;
use std::collections::HashSet;
use std::fs::{self, File};
use std::io::{self, BufReader, BufRead};
use std::iter;

use raptor::{DocIndexMapBuilder, DocIndexMap, DocIndex};
use rocksdb::{DB, WriteBatch, Writable};
use serde_json::from_str;
use unidecode::unidecode;

#[derive(Debug, Deserialize)]
struct Product {
    title: String,
    product_id: u64,
    ft: String,
}

fn set_readonly<P>(path: P, readonly: bool) -> io::Result<()>
where P: AsRef<Path>
{
    let mut perms = fs::metadata(&path)?.permissions();
    perms.set_readonly(readonly);
    fs::set_permissions(&path, perms)
}

fn is_readonly<P>(path: P) -> io::Result<bool>
where P: AsRef<Path>
{
    fs::metadata(&path).map(|m| m.permissions().readonly())
}

fn main() {
    let data = File::open("products.json_lines").unwrap();
    let data = BufReader::new(data);

    let common_words = {
        match File::open("fr.stopwords.txt") {
            Ok(file) => {
                let file = BufReader::new(file);
                let mut set = HashSet::new();
                for line in file.lines().filter_map(|l| l.ok()) {
                    for word in line.split_whitespace() {
                        set.insert(word.to_owned());
                    }
                }
                set
            },
            Err(e) => {
                eprintln!("{:?}", e);
                HashSet::new()
            },
        }
    };

    let map_file = "map.fst";
    let values_file = "values.vecs";
    let rocksdb_file = "rocksdb/storage";

    for file in &[map_file, values_file, rocksdb_file] {
        match is_readonly(file) {
            Ok(true) => panic!("the {:?} file is readonly, please make it writeable", file),
            Err(ref e) if e.kind() == io::ErrorKind::NotFound => (),
            Err(e) => panic!("{:?}", e),
            _ => (),
        }
    }

    fs::remove_file(rocksdb_file);
    let db = DB::open_default(rocksdb_file).unwrap();

    let mut builder = DocIndexMapBuilder::new();
    for line in data.lines() {
        let line = line.unwrap();

        let product: Product = from_str(&line).unwrap();

        let title = iter::repeat(0).zip(product.title.split_whitespace()).filter(|&(_, w)| !common_words.contains(w)).enumerate();
        let description = iter::repeat(1).zip(product.ft.split_whitespace()).filter(|&(_, w)| !common_words.contains(w)).enumerate();

        let mut batch = WriteBatch::new();

        let title_key = format!("{}-title", product.product_id);
        let _ = batch.put(title_key.as_bytes(), product.title.as_bytes());

        let description_key = format!("{}-description", product.product_id);
        let _ = batch.put(description_key.as_bytes(), product.ft.as_bytes());

        db.write(batch).unwrap();

        let words = title.chain(description);
        for (i, (attr, word)) in words {
            let doc_index = DocIndex {
                document: product.product_id,
                attribute: attr,
                attribute_index: i as u32,
            };
            // insert the exact representation
            let word_lower = word.to_lowercase();

            // and the unidecoded lowercased version
            let word_unidecoded = unidecode(word).to_lowercase();
            if word_lower != word_unidecoded {
                builder.insert(word_unidecoded, doc_index);
            }

            builder.insert(word_lower, doc_index);
        }
    }

    let map = File::create(map_file).unwrap();
    let values = File::create(values_file).unwrap();

    let (map, values) = builder.build(map, values).unwrap();

    set_readonly(map_file, true).unwrap();
    set_readonly(values_file, true).unwrap();
    set_readonly(rocksdb_file, true).unwrap();

    println!("Checking the dump consistency...");
    unsafe { DocIndexMap::from_paths("map.fst", "values.vecs").unwrap() };
}
