#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

use std::collections::{HashMap, HashSet};
use std::io::{self, BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::time::Instant;
use std::error::Error;
use std::borrow::Cow;
use std::fs::File;

use diskus::Walk;
use sysinfo::{SystemExt, ProcessExt};
use serde::{Serialize, Deserialize};
use structopt::StructOpt;

use meilidb_data::Database;
use meilidb_schema::Schema;

#[derive(Debug, StructOpt)]
pub struct Opt {
    /// The destination where the database must be created.
    #[structopt(parse(from_os_str))]
    pub database_path: PathBuf,

    /// The csv file to index.
    #[structopt(parse(from_os_str))]
    pub csv_data_path: PathBuf,

    /// The path to the schema.
    #[structopt(long = "schema", parse(from_os_str))]
    pub schema_path: PathBuf,

    /// The file with the synonyms.
    #[structopt(long = "synonyms", parse(from_os_str))]
    pub synonyms: Option<PathBuf>,

    /// The path to the list of stop words (one by line).
    #[structopt(long = "stop-words", parse(from_os_str))]
    pub stop_words: Option<PathBuf>,

    #[structopt(long = "update-group-size")]
    pub update_group_size: Option<usize>,
}

#[derive(Serialize, Deserialize)]
struct Document<'a> (
    #[serde(borrow)]
    HashMap<Cow<'a, str>, Cow<'a, str>>
);

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Synonym {
    OneWay(SynonymOneWay),
    MultiWay { synonyms: Vec<String> },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SynonymOneWay {
    pub search_terms: String,
    pub synonyms: Synonyms,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Synonyms {
    Multiple(Vec<String>),
    Single(String),
}

fn read_synomys(path: &Path) -> Result<Vec<Synonym>, Box<dyn Error>> {
    let file = File::open(path)?;
    let synonyms = serde_json::from_reader(file)?;
    Ok(synonyms)
}

fn index(
    schema: Schema,
    database_path: &Path,
    csv_data_path: &Path,
    update_group_size: Option<usize>,
    stop_words: &HashSet<String>,
    synonyms: Vec<Synonym>,
) -> Result<Database, Box<dyn Error>>
{
    let database = Database::start_default(database_path)?;

    let mut wtr = csv::Writer::from_path("./stats.csv").unwrap();
    wtr.write_record(&["NumberOfDocuments", "DiskUsed", "MemoryUsed"])?;

    let mut system = sysinfo::System::new();

    let index = database.create_index("test", schema.clone())?;

    let mut synonyms_adder = index.synonyms_addition();
    for synonym in synonyms {
        match synonym {
            Synonym::OneWay(SynonymOneWay { search_terms, synonyms }) => {
                let alternatives = match synonyms {
                    Synonyms::Multiple(alternatives) => alternatives,
                    Synonyms::Single(alternative) => vec![alternative],
                };
                synonyms_adder.add_synonym(search_terms, alternatives);
            },
            Synonym::MultiWay { mut synonyms } => {
                for _ in 0..synonyms.len() {
                    if let Some((synonym, alternatives)) = synonyms.split_first() {
                        synonyms_adder.add_synonym(synonym, alternatives);
                    }
                    synonyms.rotate_left(1);
                }
            },
        }
    }
    synonyms_adder.finalize()?;

    let mut rdr = csv::Reader::from_path(csv_data_path)?;
    let mut raw_record = csv::StringRecord::new();
    let headers = rdr.headers()?.clone();

    let mut i = 0;
    let mut end_of_file = false;

    while !end_of_file {
        let mut update = index.documents_addition();

        loop {
            end_of_file = !rdr.read_record(&mut raw_record)?;
            if end_of_file { break }

            let document: Document = match raw_record.deserialize(Some(&headers)) {
                Ok(document) => document,
                Err(e) => {
                    eprintln!("{:?}", e);
                    continue;
                }
            };

            update.update_document(&document)?;

            print!("\rindexing document {}", i);
            i += 1;

            if let Some(group_size) = update_group_size {
                if i % group_size == 0 { break }
            }
        }

        println!();

        println!("committing update...");
        update.finalize()?;

        // write stats
        let directory_size = Walk::new(&[database_path.to_owned()], 4).run();
        system.refresh_all();
        let memory = system.get_process(sysinfo::get_current_pid()).unwrap().memory(); // in kb
        wtr.write_record(&[i.to_string(), directory_size.to_string(), memory.to_string()])?;
        wtr.flush()?;
    }

    Ok(database)
}

fn retrieve_stop_words(path: &Path) -> io::Result<HashSet<String>> {
    let f = File::open(path)?;
    let reader = BufReader::new(f);
    let mut words = HashSet::new();

    for line in reader.lines() {
        let line = line?;
        let word = line.trim().to_string();
        words.insert(word);
    }

    Ok(words)
}

fn main() -> Result<(), Box<dyn Error>> {
    let _ = env_logger::init();
    let opt = Opt::from_args();

    let schema = {
        let file = File::open(&opt.schema_path)?;
        Schema::from_toml(file)?
    };

    let stop_words = match opt.stop_words {
        Some(ref path) => retrieve_stop_words(path)?,
        None           => HashSet::new(),
    };

    let synonyms = match opt.synonyms {
        Some(ref path) => read_synomys(path)?,
        None           => Vec::new(),
    };

    let start = Instant::now();
    let result = index(
        schema,
        &opt.database_path,
        &opt.csv_data_path,
        opt.update_group_size,
        &stop_words,
        synonyms,
    );

    if let Err(e) = result {
        return Err(e.into())
    }

    println!("database created in {:.2?} at: {:?}", start.elapsed(), opt.database_path);
    Ok(())
}
