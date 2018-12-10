use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::io::{self, Write};
use std::path::PathBuf;
use std::error::Error;

use serde_derive::{Serialize, Deserialize};
use structopt::StructOpt;

use meilidb::database::Database;

#[derive(Debug, StructOpt)]
pub struct Opt {
    /// The destination where the database must be created
    #[structopt(parse(from_os_str))]
    pub database_path: PathBuf,
}

#[derive(Debug, Serialize, Deserialize)]
#[allow(non_snake_case)]
struct Document {
    skuId: String,
    fr_FR_commercialName: String,
    en_GB_commercialName: String,
    maketingColorInternalName: String,
    materialInternalName: String,
    fr_FR_description: String,
    en_GB_description: String,
}

fn calculate_hash<T: Hash>(t: &T) -> u64 {
    let mut s = DefaultHasher::new();
    t.hash(&mut s);
    s.finish()
}

fn main() -> Result<(), Box<Error>> {
    let opt = Opt::from_args();

    let (elapsed, result) = elapsed::measure_time(|| Database::open(&opt.database_path));
    let database = result?;
    println!("database opened in {}", elapsed);

    let mut buffer = String::new();
    let input = io::stdin();

    loop {
        print!("Search: ");
        io::stdout().flush()?;

        if input.read_line(&mut buffer)? == 0 { break }

        let view = database.view();

        let (elapsed, documents) = elapsed::measure_time(|| {
            let builder = view.query_builder().unwrap();
            builder.query(&buffer, 10)
        });

        let mut full_documents = Vec::with_capacity(documents.len());

        for document in documents {
            match view.retrieve_document::<Document>(document.id) {
                Ok(document) => full_documents.push(document),
                Err(e) => eprintln!("{}", e),
            }
        }

        println!("{:#?}", full_documents);
        println!("{}", elapsed);

        buffer.clear();
    }

    Ok(())
}
