use core::fmt;
use std::{convert::TryFrom, num::ParseIntError, ops::Deref, str::FromStr};

use byte_unit::{Byte, ByteError};
use clap::Parser;
use milli::update::IndexerConfig;
use serde::Serialize;
use sysinfo::{RefreshKind, System, SystemExt};

#[derive(Debug, Clone, Parser, Serialize)]
pub struct IndexerOpts {
    /// The amount of documents to skip before printing
    /// a log regarding the indexing advancement.
    #[serde(skip)]
    #[clap(long, default_value = "100000", hide = true)] // 100k
    pub log_every_n: usize,

    /// Grenad max number of chunks in bytes.
    #[serde(skip)]
    #[clap(long, hide = true)]
    pub max_nb_chunks: Option<usize>,

    /// The maximum amount of memory the indexer will use. It defaults to 2/3
    /// of the available memory. It is recommended to use something like 80%-90%
    /// of the available memory, no more.
    ///
    /// In case the engine is unable to retrieve the available memory the engine will
    /// try to use the memory it needs but without real limit, this can lead to
    /// Out-Of-Memory issues and it is recommended to specify the amount of memory to use.
    #[clap(long, env = "MEILI_MAX_INDEXING_MEMORY", default_value_t)]
    pub max_indexing_memory: MaxMemory,

    /// The maximum number of threads the indexer will use.
    /// If the number set is higher than the real number of cores available in the machine,
    /// it will use the maximum number of available cores.
    ///
    /// It defaults to half of the available threads.
    #[clap(long, env = "MEILI_MAX_INDEXING_THREADS", default_value_t)]
    pub max_indexing_threads: MaxThreads,
}

#[derive(Debug, Clone, Parser, Default, Serialize)]
pub struct SchedulerConfig {
    /// The engine will disable task auto-batching,
    /// and will sequencialy compute each task one by one.
    #[clap(long, env = "DISABLE_AUTO_BATCHING")]
    pub disable_auto_batching: bool,
}

impl TryFrom<&IndexerOpts> for IndexerConfig {
    type Error = anyhow::Error;

    fn try_from(other: &IndexerOpts) -> Result<Self, Self::Error> {
        let thread_pool = rayon::ThreadPoolBuilder::new()
            .num_threads(*other.max_indexing_threads)
            .build()?;

        Ok(Self {
            log_every_n: Some(other.log_every_n),
            max_nb_chunks: other.max_nb_chunks,
            max_memory: other.max_indexing_memory.map(|b| b.get_bytes() as usize),
            thread_pool: Some(thread_pool),
            max_positions_per_attributes: None,
            ..Default::default()
        })
    }
}

impl Default for IndexerOpts {
    fn default() -> Self {
        Self {
            log_every_n: 100_000,
            max_nb_chunks: None,
            max_indexing_memory: MaxMemory::default(),
            max_indexing_threads: MaxThreads::default(),
        }
    }
}

/// A type used to detect the max memory available and use 2/3 of it.
#[derive(Debug, Clone, Copy, Serialize)]
pub struct MaxMemory(Option<Byte>);

impl FromStr for MaxMemory {
    type Err = ByteError;

    fn from_str(s: &str) -> Result<MaxMemory, ByteError> {
        Byte::from_str(s).map(Some).map(MaxMemory)
    }
}

impl Default for MaxMemory {
    fn default() -> MaxMemory {
        MaxMemory(
            total_memory_bytes()
                .map(|bytes| bytes * 2 / 3)
                .map(Byte::from_bytes),
        )
    }
}

impl fmt::Display for MaxMemory {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self.0 {
            Some(memory) => write!(f, "{}", memory.get_appropriate_unit(true)),
            None => f.write_str("unknown"),
        }
    }
}

impl Deref for MaxMemory {
    type Target = Option<Byte>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl MaxMemory {
    pub fn unlimited() -> Self {
        Self(None)
    }
}

/// Returns the total amount of bytes available or `None` if this system isn't supported.
fn total_memory_bytes() -> Option<u64> {
    if System::IS_SUPPORTED {
        let memory_kind = RefreshKind::new().with_memory();
        let mut system = System::new_with_specifics(memory_kind);
        system.refresh_memory();
        Some(system.total_memory() * 1024) // KiB into bytes
    } else {
        None
    }
}

#[derive(Debug, Clone, Copy, Serialize)]
pub struct MaxThreads(usize);

impl FromStr for MaxThreads {
    type Err = ParseIntError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        usize::from_str(s).map(Self)
    }
}

impl Default for MaxThreads {
    fn default() -> Self {
        MaxThreads(num_cpus::get() / 2)
    }
}

impl fmt::Display for MaxThreads {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl Deref for MaxThreads {
    type Target = usize;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
