// TODO make the raptor binary expose multiple subcommand
//      make only one binary

#[macro_use] extern crate serde_derive;

use std::path::Path;
use std::collections::{HashSet, BTreeMap};
use std::io::{self, BufReader, BufRead};
use std::fs::File;
use std::{env, iter};

use raptor::{MetadataBuilder, DocIndex};
use rocksdb::{SstFileWriter, EnvOptions, ColumnFamilyOptions};
use serde_json::from_str;
use unidecode::unidecode;

#[derive(Debug, Deserialize)]
struct Product {
    title: String,
    product_id: u64,
    ft: String,
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

fn main() {
    let data_path = env::args().nth(1).expect("Missing data json lines file (e.g. products.json_lines)");
    let data = File::open(data_path).unwrap();
    let data = BufReader::new(data);

    let common_path = "fr.stopwords.txt";
    let common_words = common_words(common_path).unwrap_or_else(|e| {
        println!("{:?}: {:?}", common_path, e);
        HashSet::new()
    });

    // TODO add a subcommand to pack these files in a tar.xxx archive
    let random_name = moby_name_gen::random_name();
    let map_file = format!("{}.map", random_name);
    let idx_file = format!("{}.idx", random_name);
    let sst_file = format!("{}.sst", random_name);

    let env_options = EnvOptions::new();
    let cf_options = ColumnFamilyOptions::new();
    let mut sst_file_writer = SstFileWriter::new(env_options, cf_options);
    sst_file_writer.open(&sst_file).expect("open the sst file");

    let map = File::create(&map_file).unwrap();
    let indexes = File::create(&idx_file).unwrap();
    let mut builder = MetadataBuilder::new(map, indexes);
    let mut fields = BTreeMap::new();

    for line in data.lines() {
        let line = line.unwrap();

        let product: Product = from_str(&line).unwrap();

        {
            let title = iter::repeat(0).zip(product.title.split_whitespace()).filter(|&(_, w)| !common_words.contains(w)).enumerate();
            let description = iter::repeat(1).zip(product.ft.split_whitespace()).filter(|&(_, w)| !common_words.contains(w)).enumerate();

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

        // TODO simplify this by using functions and
        //      use the MetadataBuilder internal BTreeMap ?
        let key = format!("{}-title", product.product_id);
        let value = product.title;
        fields.insert(key, value);

        let key = format!("{}-description", product.product_id);
        let value = product.ft;
        fields.insert(key, value);
    }

    for (key, value) in fields {
        sst_file_writer.put(key.as_bytes(), value.as_bytes()).unwrap();
    }
    let _sst_file_info = sst_file_writer.finish().unwrap();

    builder.finish().unwrap();

    println!("Succesfully created {:?} dump.", random_name);
}
