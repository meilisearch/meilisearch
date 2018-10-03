#[cfg(feature = "index-csv")]
mod csv;

#[cfg(feature = "index-jsonlines")]
mod json_lines;

use structopt::StructOpt;

#[derive(Debug, StructOpt)]
pub enum CommandIndex {

    #[cfg(feature = "index-jsonlines")]
    /// Index files encoded as json lines.
    #[structopt(name = "json-lines")]
    JsonLines(self::jsonlines_feature::CommandJsonLines),

    #[cfg(feature = "index-csv")]
    /// Index files encoded as csv.
    #[structopt(name = "csv")]
    Csv(self::csv_feature::CommandCsv),
}

#[cfg(feature = "index-jsonlines")]
pub mod jsonlines_feature {
    use std::error;
    use std::path::PathBuf;
    use structopt::StructOpt;

    #[derive(Debug, StructOpt)]
    pub struct CommandJsonLines {
        /// The stop word file, each word must be separated by a newline.
        #[structopt(long = "stop-words", parse(from_os_str))]
        pub stop_words: PathBuf,

        /// The csv file to index.
        #[structopt(parse(from_os_str))]
        pub products: PathBuf,
    }

    pub fn json_lines(command: CommandJsonLines) -> Result<(), Box<error::Error>> {
        use super::json_lines::JsonLinesIndexer;

        let indexer = JsonLinesIndexer::from_command(command)?;
        Ok(indexer.index())
    }
}

#[cfg(feature = "index-csv")]
pub mod csv_feature {
    use std::error;
    use std::path::PathBuf;
    use structopt::StructOpt;

    #[derive(Debug, StructOpt)]
    pub struct CommandCsv {
        /// The stop word file, each word must be separated by a newline.
        #[structopt(long = "stop-words", parse(from_os_str))]
        pub stop_words: PathBuf,

        /// The csv file to index.
        #[structopt(parse(from_os_str))]
        pub products: PathBuf,
    }

    pub fn csv(command: CommandCsv) -> Result<(), Box<error::Error>> {
        use super::csv::CsvIndexer;

        let indexer = CsvIndexer::from_command(command)?;
        Ok(indexer.index())
    }
}
