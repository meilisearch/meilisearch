use std::path::PathBuf;

use byte_unit::Byte;
use heed::{CompactionOption, Env, EnvOpenOptions};
use structopt::StructOpt;
use Command::*;

#[cfg(target_os = "linux")]
#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

#[derive(Debug, StructOpt)]
/// Some helpers commands for milli.
pub struct Opt {
    /// The database path where the database is located.
    /// It is created if it doesn't already exist.
    #[structopt(long = "db", parse(from_os_str))]
    database: PathBuf,

    /// The maximum size the database can take on disk. It is recommended to specify
    /// the whole disk space (value must be a multiple of a page size).
    #[structopt(long = "db-size", default_value = "100 GiB")]
    database_size: Byte,

    /// Verbose mode (-v, -vv, -vvv, etc.)
    #[structopt(short, long, parse(from_occurrences))]
    verbose: usize,

    #[structopt(subcommand)]
    command: Command,
}

#[derive(Debug, StructOpt)]
enum Command {
    /// Outputs the main LMDB database to stdout.
    CopyMainDatabase {
        /// Wether to enable or not the compaction of the database.
        #[structopt(long, short = "c")]
        enable_compaction: bool,
    },
}

fn main() -> anyhow::Result<()> {
    let opt = Opt::from_args();

    stderrlog::new()
        .verbosity(opt.verbose)
        .show_level(false)
        .timestamp(stderrlog::Timestamp::Off)
        .init()?;

    let mut options = EnvOpenOptions::new();
    options.map_size(opt.database_size.get_bytes() as usize);

    // Return an error if the database does not exist.
    if !opt.database.exists() {
        anyhow::bail!("The database ({}) does not exist.", opt.database.display());
    }

    let env = options.open(opt.database)?;

    match opt.command {
        CopyMainDatabase { enable_compaction } => {
            use CompactionOption::*;
            let compaction = if enable_compaction { Enabled } else { Disabled };
            copy_main_database_to_stdout(env, compaction)
        }
    }
}

#[cfg(target_family = "unix")]
fn copy_main_database_to_stdout(env: Env, compaction: CompactionOption) -> anyhow::Result<()> {
    use std::os::unix::io::AsRawFd;

    let stdout = std::io::stdout().as_raw_fd();
    unsafe { env.copy_to_fd(stdout, compaction).map_err(Into::into) }
}

#[cfg(target_family = "windows")]
fn copy_main_database_to_stdout(env: Env, compaction: CompactionOption) -> anyhow::Result<()> {
    use std::os::windows::io::AsRawHandle;

    let stdout = std::io::stdout().as_raw_handle();
    unsafe { env.copy_to_fd(stdout, compaction).map_err(Into::into) }
}
