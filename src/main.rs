#[macro_use] extern crate serde_derive;

#[cfg(feature = "index")]
mod index;
#[cfg(feature = "serve")]
mod serve;
mod common_words;

use structopt::StructOpt;

#[derive(Debug, StructOpt)]
#[structopt(name = "raptor-cli", about = "A command line to do raptor operations.")]
enum Command {
    #[cfg(feature = "index")]
    /// Index files of different format.
    #[structopt(name = "index")]
    Index(index::CommandIndex),

    #[cfg(feature = "serve")]
    /// Serve indexes.
    #[structopt(name = "serve")]
    Serve(serve::CommandServe),
}

fn main() {
    let ret = match Command::from_args() {

        #[cfg(feature = "index")]
        Command::Index(i) => match i {

            #[cfg(feature = "index-jsonlines")]
            index::CommandIndex::JsonLines(command) => index::jsonlines_feature::json_lines(command),

            #[cfg(feature = "index-csv")]
            index::CommandIndex::Csv(command) => index::csv_feature::csv(command),
        },

        #[cfg(feature = "serve")]
        Command::Serve(s) => match s {

            #[cfg(feature = "serve-http")]
            serve::CommandServe::Http(command) => serve::http_feature::http(command),

            #[cfg(feature = "serve-console")]
            serve::CommandServe::Console(command) => serve::console_feature::console(command),
        },
    };

    if let Err(e) = ret { eprintln!("{}", e) }
}
