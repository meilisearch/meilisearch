#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

use std::path::{Path, PathBuf};
use std::error::Error;
use std::borrow::Cow;
use std::fs::File;

use hashbrown::HashMap;
use serde_derive::{Serialize, Deserialize};
use structopt::StructOpt;

use meilidb::database::{Database, Schema, UpdateBuilder};
use meilidb::tokenizer::DefaultBuilder;

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
}

#[derive(Serialize, Deserialize)]
struct Document<'a> (
    #[serde(borrow)]
    HashMap<Cow<'a, str>, Cow<'a, str>>
);

fn index(schema: Schema, database_path: &Path, csv_data_path: &Path) -> Result<Database, Box<Error>> {
    let database = Database::create(database_path, &schema)?;

    println!("start indexing...");

    let tokenizer_builder = DefaultBuilder::new();
    let update_path = tempfile::NamedTempFile::new()?;
    let mut update = UpdateBuilder::new(update_path.path().to_path_buf(), schema);

    let mut rdr = csv::Reader::from_path(csv_data_path)?;
    let mut raw_record = csv::StringRecord::new();
    let headers = rdr.headers()?.clone();

    while rdr.read_record(&mut raw_record)? {
        let document: Document = match raw_record.deserialize(Some(&headers)) {
            Ok(document) => document,
            Err(e) => {
                eprintln!("{:?}", e);
                continue;
            }
        };

        update.update_document(&document, &tokenizer_builder)?;
    }

    let update = update.build()?;
    database.ingest_update_file(update)?;

    Ok(database)
}

fn main() -> Result<(), Box<Error>> {
    let _ = env_logger::init();
    let opt = Opt::from_args();

    let schema = {
        let file = File::open(&opt.schema_path)?;
        Schema::from_toml(file)?
    };

    let (elapsed, result) = elapsed::measure_time(|| {
        index(schema, &opt.database_path, &opt.csv_data_path)
    });

    if let Err(e) = result {
        return Err(e.into())
    }

    println!("database created in {} at: {:?}", elapsed, opt.database_path);
    Ok(())
}
