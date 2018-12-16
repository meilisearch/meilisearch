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

    /// The number of returned results
    #[structopt(short = "n", long = "number-results", default_value = "10")]
    pub number_results: usize,
}

#[derive(Debug, Serialize, Deserialize)]
struct Document {
    id: String,
    title: String,
    description: String,
    image: String,
}

fn main() -> Result<(), Box<Error>> {
    let opt = Opt::from_args();

    let (elapsed, result) = elapsed::measure_time(|| Database::open(&opt.database_path));
    let database = result?;
    println!("database prepared for you in {}", elapsed);

    let mut buffer = String::new();
    let input = io::stdin();

    loop {
        print!("Searching for: ");
        io::stdout().flush()?;

        if input.read_line(&mut buffer)? == 0 { break }

        let view = database.view();

        let (elapsed, documents) = elapsed::measure_time(|| {
            let builder = view.query_builder().unwrap();
            builder.query(&buffer, 0..opt.number_results)
        });

        let mut full_documents = Vec::with_capacity(documents.len());

        for document in documents {
            match view.retrieve_document::<Document>(document.id) {
                Ok(document) => full_documents.push(document),
                Err(e) => eprintln!("{}", e),
            }
        }

        println!("{:#?}", full_documents);
        println!("Found {} results in {}", full_documents.len(), elapsed);

        buffer.clear();
    }

    Ok(())
}
