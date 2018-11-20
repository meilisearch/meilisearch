use std::error::Error;
use std::str::from_utf8_unchecked;
use std::io::{self, Write};
use structopt::StructOpt;
use std::path::PathBuf;

use elapsed::measure_time;
use rocksdb::{DB, DBOptions, IngestExternalFileOptions};
use pentium::index::Index;
use pentium::rank::{criterion, Config, RankedStream};
use pentium::{automaton, DocumentId};

#[derive(Debug, StructOpt)]
pub struct CommandConsole {
    /// Meta file name (e.g. relaxed-colden).
    #[structopt(parse(from_os_str))]
    pub index_path: PathBuf,
}

pub struct ConsoleSearch {
    index: Index,
}

impl ConsoleSearch {
    pub fn from_command(command: CommandConsole) -> Result<ConsoleSearch, Box<Error>> {
        let index = Index::open(command.index_path)?;
        Ok(ConsoleSearch { index })
    }

    pub fn serve(self) {
        loop {
            print!("Searching for: ");
            io::stdout().flush().unwrap();

            let mut query = String::new();
            io::stdin().read_line(&mut query).unwrap();

            if query.is_empty() { break }

            let (elapsed, _) = measure_time(|| search(&self.index, &query));
            println!("Finished in {}", elapsed);
        }
    }
}

fn search(index: &Index, query: &str) {
    let mut automatons = Vec::new();
    for query in query.split_whitespace().map(str::to_lowercase) {
        let lev = automaton::build_prefix_dfa(&query);
        automatons.push(lev);
    }

    let distinct_by_title_first_four_chars = |id: &DocumentId| {
        let title_key = format!("{}-title", id);
        match database.get(title_key.as_bytes()) {
            Ok(Some(value)) => {
                value.to_utf8().map(|s| s.chars().take(4).collect::<String>())
            },
            Ok(None) => None,
            Err(err) => {
                eprintln!("{:?}", err);
                None
            }
        }
    };

    let index: Index = unimplemented!();

    // "Sony" "PlayStation 4 500GB"
    let config = Config {
        blobs: &index.blobs().unwrap(),
        automatons: automatons,
        criteria: criterion::default(),
        distinct: (distinct_by_title_first_four_chars, 1),
    };
    let stream = RankedStream::new(config);

    let documents = stream.retrieve_distinct_documents(0..20);
    // let documents = stream.retrieve_documents(0..20);

    for document in documents {
        let id_key = format!("{}-id", document.id);
        let id = database.get(id_key.as_bytes()).unwrap().unwrap();
        let id = unsafe { from_utf8_unchecked(&id) };
        print!("{} ", id);

        let title_key = format!("{}-title", document.id);
        let title = database.get(title_key.as_bytes()).unwrap().unwrap();
        let title = unsafe { from_utf8_unchecked(&title) };
        println!("{:?}", title);
    }
}

fn main() {
    let command = CommandConsole::from_args();
    let console = ConsoleSearch::from_command(command).unwrap();
    console.serve()
}
