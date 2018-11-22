use std::error::Error;
use std::path::PathBuf;
use std::io::{self, Write};

use elapsed::measure_time;
use structopt::StructOpt;
use pentium::index::Index;

#[derive(Debug, StructOpt)]
pub struct Cmd {
    /// Index path (e.g. relaxed-colden).
    #[structopt(parse(from_os_str))]
    pub index_path: PathBuf,
}

fn main() -> Result<(), Box<Error>> {
    let command = Cmd::from_args();
    let index = Index::open(command.index_path)?;

    loop {
        print!("Searching for: ");
        io::stdout().flush()?;

        let mut query = String::new();
        io::stdin().read_line(&mut query)?;

        if query.is_empty() { break }

        let (elapsed, result) = measure_time(|| index.search(&query));
        match result {
            Ok(documents) => {
                println!("{:?}", documents);
                println!("Finished in {}", elapsed)
            },
            Err(e) => panic!("{}", e),
        }
    }

    Ok(())
}
