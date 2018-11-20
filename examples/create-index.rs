use std::path::Path;
use std::error::Error;
use std::path::PathBuf;
use std::io::{self, Write};

use elapsed::measure_time;
use moby_name_gen::random_name;
use structopt::StructOpt;

use pentium::index::update::Update;
use pentium::index::Index;

#[derive(Debug, StructOpt)]
pub struct Cmd {
    /// csv file to index
    #[structopt(parse(from_os_str))]
    pub csv_file: PathBuf,
}

fn generate_update_from_csv(path: &Path) -> Result<Update, Box<Error>> {
    unimplemented!()
}

fn main() -> Result<(), Box<Error>> {
    let command = Cmd::from_args();

    let path = random_name();

    println!("generating the update...");
    let update = generate_update_from_csv(&command.csv_file)?;

    println!("creating the index");
    let index = Index::open(&path)?;

    println!("ingesting the changes in the index");
    index.ingest_update(update)?;

    println!("the index {:?} has been created!", path);

    Ok(())
}
