// TODO make the raptor binary expose multiple subcommand
//      make only one binary

#[macro_use] extern crate serde_derive;

use std::path::{Path, PathBuf};
use std::collections::{HashSet, BTreeMap};
use std::io::{self, BufReader, BufRead};
use std::fs::File;

use csv::ReaderBuilder;
use structopt::StructOpt;
use raptor::{MetadataBuilder, DocIndex, Tokenizer};
use rocksdb::{SstFileWriter, EnvOptions, ColumnFamilyOptions};
use unidecode::unidecode;

#[derive(Debug, StructOpt)]
#[structopt(name = "raptor-indexer-csv", about = "A Raptor binary to index csv stored products.")]
struct Opt {
    /// The stop word file, each word must be separated by a newline.
    #[structopt(long = "stop-words", parse(from_os_str))]
    stop_words: PathBuf,

    /// The csv file to index.
    #[structopt(parse(from_os_str))]
    products: PathBuf,
}

#[derive(Debug, Deserialize)]
struct Product {
    #[serde(rename = "_unit_id")]
    id: u64,
    #[serde(rename = "product_title")]
    title: String,
    #[serde(rename = "product_image")]
    image: String,
    #[serde(rename = "product_description")]
    description: String,
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

fn insert_document_words<'a, I, A, B>(builder: &mut MetadataBuilder<A, B>, doc_index: u64, attr: u8, words: I)
where A: io::Write,
      B: io::Write,
      I: IntoIterator<Item=(usize, &'a str)>,
{
    for (index, word) in words {
        let doc_index = DocIndex {
            document: doc_index,
            attribute: attr,
            attribute_index: index as u32,
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

fn main() {
    let opt = Opt::from_args();

    let common_words = common_words(opt.stop_words).expect("reading stop words");

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

    let mut rdr = ReaderBuilder::new().from_path(opt.products).expect("reading product file");
    let mut errors = 0;

    for result in rdr.deserialize() {
        let product: Product = match result {
            Ok(product) => product,
            Err(e) => { eprintln!("{:?}", e); errors += 1; continue },
        };

        let title = Tokenizer::new(&product.title);
        let title = title.iter().filter(|&(_, w)| !common_words.contains(w));
        insert_document_words(&mut builder, product.id, 0, title);

        let description = Tokenizer::new(&product.description);
        let description = description.iter().filter(|&(_, w)| !common_words.contains(w));
        insert_document_words(&mut builder, product.id, 1, description);

        // TODO simplify this by using functions and
        //      use the MetadataBuilder internal BTreeMap ?
        let key = format!("{}-title", product.id);
        let value = product.title;
        fields.insert(key, value);

        let key = format!("{}-description", product.id);
        let value = product.description;
        fields.insert(key, value);

        let key = format!("{}-image", product.id);
        let value = product.image;
        fields.insert(key, value);
    }

    for (key, value) in fields {
        sst_file_writer.put(key.as_bytes(), value.as_bytes()).unwrap();
    }
    let _sst_file_info = sst_file_writer.finish().unwrap();

    builder.finish().unwrap();

    println!("Found {} errorneous lines", errors);
    println!("Succesfully created {:?} dump.", random_name);
}
