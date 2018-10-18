use std::str::from_utf8_unchecked;
use std::io::{self, Write};
use structopt::StructOpt;
use std::path::PathBuf;

use elapsed::measure_time;
use rocksdb::{DB, DBOptions, IngestExternalFileOptions};
use raptor::rank::{criterion, Config, RankedStream, Document};
use raptor::{automaton, DocumentId, Metadata, CommonWords};

#[derive(Debug, StructOpt)]
pub struct CommandConsole {
    /// The stop word file, each word must be separated by a newline.
    #[structopt(long = "stop-words", parse(from_os_str))]
    pub stop_words: PathBuf,

    /// Meta file name (e.g. relaxed-colden).
    #[structopt(parse(from_os_str))]
    pub meta_name: PathBuf,
}

pub struct ConsoleSearch {
    common_words: CommonWords,
    metadata: Metadata,
    db: DB,
}

impl ConsoleSearch {
    pub fn from_command(command: CommandConsole) -> io::Result<ConsoleSearch> {
        let common_words = CommonWords::from_file(command.stop_words)?;

        let map_file = command.meta_name.with_extension("map");
        let idx_file = command.meta_name.with_extension("idx");
        let sst_file = command.meta_name.with_extension("sst");

        let metadata = unsafe { Metadata::from_paths(map_file, idx_file).unwrap() };

        let rocksdb = "rocksdb/storage";
        let db = DB::open_default(rocksdb).unwrap();
        let sst_file = sst_file.to_str().unwrap();
        db.ingest_external_file(&IngestExternalFileOptions::new(), &[sst_file]).unwrap();
        drop(db);
        let db = DB::open_for_read_only(DBOptions::default(), rocksdb, false).unwrap();

        Ok(ConsoleSearch { common_words, metadata, db })
    }

    pub fn serve(self) {
        loop {
            print!("Searching for: ");
            io::stdout().flush().unwrap();

            let mut query = String::new();
            io::stdin().read_line(&mut query).unwrap();
            let query = query.trim().to_lowercase();

            if query.is_empty() { break }

            let (elapsed, _) = measure_time(|| search(&self.metadata, &self.db, &self.common_words, &query));
            println!("Finished in {}", elapsed);
        }
    }
}

fn search(metadata: &Metadata, database: &DB, common_words: &CommonWords, query: &str) {
    let mut automatons = Vec::new();
    for query in query.split_whitespace().filter(|q| !common_words.contains(*q)) {
        let lev = automaton::build(query);
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

    // "Sony" "PlayStation 4 500GB"
    let config = Config {
        metadata: metadata,
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
