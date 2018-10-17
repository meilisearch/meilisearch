#[macro_use] extern crate serde_derive;

use std::collections::BTreeMap;
use std::io::{self, BufReader, BufRead};
use std::fs::File;
use std::path::PathBuf;

use serde_json::from_str;
use rocksdb::{SstFileWriter, EnvOptions, ColumnFamilyOptions};
use raptor::{MetadataBuilder, DocIndex, Tokenizer, CommonWords};
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
pub struct CommandJsonLines {
    /// The stop word file, each word must be separated by a newline.
    #[structopt(long = "stop-words", parse(from_os_str))]
    pub stop_words: PathBuf,

    /// The csv file to index.
    #[structopt(parse(from_os_str))]
    pub products: PathBuf,
}

#[derive(Debug, Deserialize)]
struct Product {
    id: u64,
    title: String,
    description: String,
    image: String,
}

#[derive(Debug)]
pub struct JsonLinesIndexer {
    common_words: CommonWords,
    products: PathBuf,
}

impl JsonLinesIndexer {
    pub fn from_command(command: CommandJsonLines) -> io::Result<JsonLinesIndexer> {
        let common_words = CommonWords::from_file(command.stop_words)?;
        let products = command.products;

        Ok(JsonLinesIndexer { common_words, products })
    }

    pub fn index(self) {
        let data = File::open(&self.products).unwrap();
        let data = BufReader::new(data);

        // TODO add a subcommand to pack these files in a tar.xxx archive
        let random_name = PathBuf::from(moby_name_gen::random_name());
        let map_file = random_name.with_extension("map");
        let idx_file = random_name.with_extension("idx");
        let sst_file = random_name.with_extension("sst");

        let env_options = EnvOptions::new();
        let cf_options = ColumnFamilyOptions::new();
        let mut sst_file_writer = SstFileWriter::new(env_options, cf_options);
        let sst_file = sst_file.to_str().unwrap();
        sst_file_writer.open(&sst_file).expect("open the sst file");

        let map = File::create(&map_file).unwrap();
        let indexes = File::create(&idx_file).unwrap();
        let mut builder = MetadataBuilder::new(map, indexes);
        let mut fields = BTreeMap::new();
        let mut errors = 0;

        for result in data.lines() {
            let product: Product = match result {
                Ok(product) => match from_str(&product) {
                    Ok(product) => product,
                    Err(e) => { eprintln!("{:?}", e); errors += 1; continue },
                },
                Err(e) => { eprintln!("{:?}", e); errors += 1; continue },
            };

            {
                let string_id = product.id.to_string();
                insert_document_words(&mut builder, product.id, 0, Some((0, string_id.as_str())));

                let key = format!("{}-id", product.id);
                let value = string_id;
                fields.insert(key, value);
            }

            {
                let title = Tokenizer::new(&product.title);
                let title = title.iter().filter(|&(_, w)| !self.common_words.contains(w));
                insert_document_words(&mut builder, product.id, 1, title);

                let key = format!("{}-title", product.id);
                let value = product.title;
                fields.insert(key, value);
            }

            {
                let description = Tokenizer::new(&product.description);
                let description = description.iter().filter(|&(_, w)| !self.common_words.contains(w));
                insert_document_words(&mut builder, product.id, 2, description);

                let key = format!("{}-description", product.id);
                let value = product.description;
                fields.insert(key, value);
            }

            {
                let key = format!("{}-image", product.id);
                let value = product.image;
                fields.insert(key, value);
            }
        }

        for (key, value) in fields {
            sst_file_writer.put(key.as_bytes(), value.as_bytes()).unwrap();
        }
        let _sst_file_info = sst_file_writer.finish().unwrap();

        builder.finish().unwrap();

        println!("Found {} errorneous lines", errors);
        println!("Succesfully created {:?} dump.", random_name);
    }
}

fn insert_document_words<'a, I, A, B>(builder: &mut MetadataBuilder<A, B>, doc_id: u64, attr: u8, words: I)
where A: io::Write,
      B: io::Write,
      I: IntoIterator<Item=(usize, &'a str)>,
{
    for (index, word) in words {
        let doc_index = DocIndex {
            document_id: doc_id,
            attribute: attr,
            attribute_index: index as u32,
        };
        // insert the exact representation
        let word_lower = word.to_lowercase();

        // and the unidecoded lowercased version
        let word_unidecoded = unidecode::unidecode(word).to_lowercase();
        if word_lower != word_unidecoded {
            builder.insert(word_unidecoded, doc_index);
        }

        builder.insert(word_lower, doc_index);
    }
}

fn main() {
    let command = CommandJsonLines::from_args();
    let indexer = JsonLinesIndexer::from_command(command).unwrap();
    indexer.index();
}
