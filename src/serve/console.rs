use std::str::from_utf8_unchecked;
use std::io::{self, Write};

use fst::Streamer;
use elapsed::measure_time;
use rocksdb::{DB, DBOptions, IngestExternalFileOptions};
use raptor::{automaton, Metadata, RankedStream};

use crate::serve::console_feature::CommandConsole;
use crate::common_words::{self, CommonWords};

pub struct ConsoleSearch {
    common_words: CommonWords,
    metadata: Metadata,
    db: DB,
}

impl ConsoleSearch {
    pub fn from_command(command: CommandConsole) -> io::Result<ConsoleSearch> {
        let common_words = common_words::from_file(command.stop_words)?;

        let meta_name = command.meta_name.display();
        let map_file = format!("{}.map", meta_name);
        let idx_file = format!("{}.idx", meta_name);
        let sst_file = format!("{}.sst", meta_name);
        let metadata = unsafe { Metadata::from_paths(map_file, idx_file).unwrap() };

        let rocksdb = "rocksdb/storage";
        let db = DB::open_default(rocksdb).unwrap();
        db.ingest_external_file(&IngestExternalFileOptions::new(), &[&sst_file]).unwrap();
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

    let mut stream = RankedStream::new(&metadata, automatons, 20);
    while let Some(document) = stream.next() {
        print!("{:?}", document.document_id);

        let title_key = format!("{}-title", document.document_id);
        let title = database.get(title_key.as_bytes()).unwrap().unwrap();
        let title = unsafe { from_utf8_unchecked(&title) };
        print!(" {:?}", title);

        println!();
    }
}
