use structopt::StructOpt;

use milli::subcommand::indexer::{self, Opt as IndexerOpt};
use milli::subcommand::infos::{self, Opt as InfosOpt};
use milli::subcommand::serve::{self, Opt as ServeOpt};
use milli::subcommand::search::{self, Opt as SearchOpt};

#[cfg(target_os = "linux")]
#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

#[derive(Debug, StructOpt)]
#[structopt(name = "milli", about = "The milli project.")]
enum Command {
    Serve(ServeOpt),
    Indexer(IndexerOpt),
    Infos(InfosOpt),
    Search(SearchOpt),
}

fn main() -> anyhow::Result<()> {
    match Command::from_args() {
        Command::Serve(opt) => serve::run(opt),
        Command::Indexer(opt) => indexer::run(opt),
        Command::Infos(opt) => infos::run(opt),
        Command::Search(opt) => search::run(opt),
    }
}
