use core::fmt;
use std::{ops::Deref, str::FromStr};

use byte_unit::{Byte, ByteError};
use milli::CompressionType;
use structopt::StructOpt;
use sysinfo::{RefreshKind, System, SystemExt};

#[derive(Debug, Clone, StructOpt)]
pub struct IndexerOpts {
    /// The amount of documents to skip before printing
    /// a log regarding the indexing advancement.
    #[structopt(long, default_value = "100000")] // 100k
    pub log_every_n: usize,

    /// Grenad max number of chunks in bytes.
    #[structopt(long)]
    pub max_nb_chunks: Option<usize>,

    /// The maximum amount of memory the indexer will use. It defaults to 2/3
    /// of the available memory. It is recommended to use something like 80%-90%
    /// of the available memory, no more.
    ///
    /// In case the engine is unable to retrieve the available memory the engine will
    /// try to use the memory it needs but without real limit, this can lead to
    /// Out-Of-Memory issues and it is recommended to specify the amount of memory to use.
    #[structopt(long, default_value)]
    pub max_memory: MaxMemory,

    /// The name of the compression algorithm to use when compressing intermediate
    /// Grenad chunks while indexing documents.
    ///
    /// Choosing a fast algorithm will make the indexing faster but may consume more memory.
    #[structopt(long, default_value = "snappy", possible_values = &["snappy", "zlib", "lz4", "lz4hc", "zstd"])]
    pub chunk_compression_type: CompressionType,

    /// The level of compression of the chosen algorithm.
    #[structopt(long, requires = "chunk-compression-type")]
    pub chunk_compression_level: Option<u32>,

    /// Number of parallel jobs for indexing, defaults to # of CPUs.
    #[structopt(long)]
    pub indexing_jobs: Option<usize>,
}

impl Default for IndexerOpts {
    fn default() -> Self {
        Self {
            log_every_n: 100_000,
            max_nb_chunks: None,
            max_memory: MaxMemory::default(),
            chunk_compression_type: CompressionType::None,
            chunk_compression_level: None,
            indexing_jobs: None,
        }
    }
}

/// A type used to detect the max memory available and use 2/3 of it.
#[derive(Debug, Clone, Copy)]
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
