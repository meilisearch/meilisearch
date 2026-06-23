use std::io::BufWriter;

use fst::{Set, SetBuilder};
use memmap2::Mmap;
use tempfile::tempfile;

use super::fst_merger_builder::FstMergerBuilder;
use crate::index::PrefixSettings;
use crate::update::del_add::DelAdd;
use crate::{InternalError, Prefix, Result};

pub struct WordFstBuilder<'a> {
    word_fst_builder: FstMergerBuilder<'a>,
    prefix_fst_builder: Option<PrefixFstBuilder>,
    registered_words: usize,
}

impl<'a> WordFstBuilder<'a> {
    pub fn new(words_fst: &'a Set<std::borrow::Cow<'a, [u8]>>) -> Result<Self> {
        Ok(Self {
            word_fst_builder: FstMergerBuilder::new(Some(words_fst))?,
            prefix_fst_builder: None,
            registered_words: 0,
        })
    }

    pub fn with_prefix_settings(&mut self, prefix_settings: PrefixSettings) -> &Self {
        self.prefix_fst_builder = PrefixFstBuilder::new(prefix_settings);
        self
    }

    pub fn register_word(&mut self, deladd: DelAdd, right: &[u8]) -> Result<()> {
        if deladd == DelAdd::Addition {
            self.registered_words += 1;
        }

        self.word_fst_builder.register(deladd, right, &mut |bytes, deladd| {
            if let Some(prefix_fst_builder) = &mut self.prefix_fst_builder {
                prefix_fst_builder.insert_word(bytes, deladd)
            } else {
                Ok(())
            }
        })?;

        Ok(())
    }

    pub fn build(mut self) -> Result<(Mmap, Option<PrefixData>)> {
        let words_fst_mmap = self.word_fst_builder.build(&mut |bytes, deladd| {
            if let Some(prefix_fst_builder) = &mut self.prefix_fst_builder {
                prefix_fst_builder.insert_word(bytes, deladd)
            } else {
                Ok(())
            }
        })?;

        let prefix_data = self
            .prefix_fst_builder
            .map(|prefix_fst_builder| prefix_fst_builder.build())
            .transpose()?;

        Ok((words_fst_mmap, prefix_data))
    }
}

pub struct PrefixData {
    pub prefixes_fst_mmap: Mmap,
}

struct PrefixFstBuilder {
    prefix_count_threshold: usize,
    max_prefix_length: usize,
    /// TODO: Replace the full memory allocation
    prefix_fst_builders: Vec<SetBuilder<Vec<u8>>>,
    current_prefix: Vec<Prefix>,
    current_prefix_count: Vec<usize>,
}

impl PrefixFstBuilder {
    pub fn new(prefix_settings: PrefixSettings) -> Option<Self> {
        let PrefixSettings { prefix_count_threshold, max_prefix_length, compute_prefixes } =
            prefix_settings;

        if compute_prefixes != crate::index::PrefixSearch::IndexingTime {
            return None;
        }

        let mut prefix_fst_builders = Vec::new();
        for _ in 0..max_prefix_length {
            prefix_fst_builders.push(SetBuilder::memory());
        }

        Some(Self {
            prefix_count_threshold,
            max_prefix_length,
            prefix_fst_builders,
            current_prefix: vec![Prefix::new(); max_prefix_length],
            current_prefix_count: vec![0; max_prefix_length],
        })
    }

    fn insert_word(&mut self, bytes: &[u8], deladd: DelAdd) -> Result<()> {
        for n in 0..self.max_prefix_length {
            let current_prefix = &mut self.current_prefix[n];
            let current_prefix_count = &mut self.current_prefix_count[n];
            let builder = &mut self.prefix_fst_builders[n];

            // We try to get the first n bytes out of this string but we only want
            // to split at valid characters bounds. If we try to split in the middle of
            // a character we ignore this word and go to the next one.
            let word = std::str::from_utf8(bytes)?;
            let prefix = match word.get(..=n) {
                Some(prefix) => prefix,
                None => continue,
            };

            // This is the first iteration of the loop,
            // or the current word doesn't starts with the current prefix.
            if *current_prefix_count == 0 || prefix != current_prefix.as_str() {
                *current_prefix = Prefix::from(prefix);
                *current_prefix_count = 0;
            }

            if deladd == DelAdd::Addition {
                *current_prefix_count += 1;
            }

            // There is enough words corresponding to this prefix to add it to the cache.
            if *current_prefix_count == self.prefix_count_threshold {
                builder.insert(prefix)?;
            }
        }

        Ok(())
    }

    fn build(self) -> Result<PrefixData> {
        // We merge all of the previously computed prefixes into on final set.
        let mut prefix_fsts = Vec::new();
        for builder in self.prefix_fst_builders.into_iter() {
            let prefix_fst = builder.into_set();
            prefix_fsts.push(prefix_fst);
        }
        let op = fst::set::OpBuilder::from_iter(prefix_fsts.iter());
        let mut builder = SetBuilder::new(BufWriter::new(tempfile()?))?;
        builder.extend_stream(op.r#union())?;
        let prefix_fst_file = builder.into_inner()?.into_inner().map_err(|_| {
            InternalError::IndexingMergingKeys { process: "building-words-prefixes-fst" }
        })?;
        let prefix_fst_mmap = unsafe { Mmap::map(&prefix_fst_file)? };
        Ok(PrefixData { prefixes_fst_mmap: prefix_fst_mmap })
    }
}
