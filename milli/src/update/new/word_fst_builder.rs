use std::{fs::File, io::BufWriter};

use fst::{Set, SetBuilder, Streamer};
use memmap2::Mmap;
use std::collections::HashSet;
use tempfile::tempfile;

use crate::{index::PrefixSettings, update::del_add::DelAdd, InternalError, Prefix, Result};

pub struct WordFstBuilder<'a> {
    stream: Option<fst::set::Stream<'a>>,
    word_fst_builder: SetBuilder<BufWriter<File>>,
    last_word: Option<Vec<u8>>,
    prefix_fst_builder: Option<PrefixFstBuilder>,
    inserted_words: usize,
    registered_words: usize,
}

impl<'a> WordFstBuilder<'a> {
    pub fn new(words_fst: &'a Set<std::borrow::Cow<'a, [u8]>>) -> Result<Self> {
        Ok(Self {
            stream: Some(words_fst.stream()),
            word_fst_builder: SetBuilder::new(BufWriter::new(tempfile()?))?,
            prefix_fst_builder: None,
            last_word: None,
            inserted_words: 0,
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

        if let Some(left) = self.last_word.take() {
            let (left_inserted, right_inserted) =
                self.compare_and_insert(deladd, left.as_slice(), right)?;

            // left was not inserted, so we keep it for the next iteration
            if !left_inserted {
                self.last_word = Some(left);
            }

            // right was inserted, so we can stop
            if right_inserted {
                return Ok(());
            }
        }

        if let Some(mut stream) = self.stream.take() {
            while let Some(left) = stream.next() {
                let (left_inserted, right_inserted) =
                    self.compare_and_insert(deladd, left, right)?;

                // left was not inserted, so we keep it for the next iteration
                if !left_inserted {
                    self.last_word = Some(left.to_vec());
                }

                // right was inserted, so we can stop
                if right_inserted {
                    self.stream = Some(stream);
                    return Ok(());
                }
            }

            // If we reach this point, it means that the stream is empty
            // and we need to insert the incoming word
            self.insert_word(right, deladd, true)?;

            self.stream = Some(stream);
        }

        Ok(())
    }

    pub fn compare_and_insert(
        &mut self,
        deladd: DelAdd,
        left: &[u8],
        right: &[u8],
    ) -> Result<(bool, bool)> {
        let mut left_inserted = false;
        let mut right_inserted = false;
        match left.cmp(right) {
            std::cmp::Ordering::Less => {
                // We need to insert the last word from the current fst
                self.insert_word(left, DelAdd::Addition, false)?;

                left_inserted = true;
            }
            std::cmp::Ordering::Equal => {
                self.insert_word(right, deladd, true)?;

                left_inserted = true;
                right_inserted = true;
            }
            std::cmp::Ordering::Greater => {
                self.insert_word(right, deladd, true)?;

                right_inserted = true;
            }
        }

        Ok((left_inserted, right_inserted))
    }

    fn insert_word(&mut self, bytes: &[u8], deladd: DelAdd, is_modified: bool) -> Result<()> {
        // Addition: We insert the word
        // Deletion: We delete the word by not inserting it
        if deladd == DelAdd::Addition {
            self.inserted_words += 1;
            self.word_fst_builder.insert(bytes)?;
        }

        if let Some(prefix_fst_builder) = self.prefix_fst_builder.as_mut() {
            prefix_fst_builder.insert_word(bytes, deladd, is_modified)?;
        }

        Ok(())
    }

    fn drain_stream(&mut self) -> Result<()> {
        if let Some(mut stream) = self.stream.take() {
            while let Some(current) = stream.next() {
                self.insert_word(current, DelAdd::Addition, false)?;
            }
        }

        Ok(())
    }

    pub fn build(
        mut self,
        index: &crate::Index,
        rtxn: &heed::RoTxn,
    ) -> Result<(Mmap, Option<PrefixData>)> {
        self.drain_stream()?;

        let words_fst_file =
            self.word_fst_builder.into_inner()?.into_inner().map_err(|_| {
                InternalError::IndexingMergingKeys { process: "building-words-fst" }
            })?;
        let words_fst_mmap = unsafe { Mmap::map(&words_fst_file)? };

        let prefix_data = self
            .prefix_fst_builder
            .map(|prefix_fst_builder| prefix_fst_builder.build(index, rtxn))
            .transpose()?;

        Ok((words_fst_mmap, prefix_data))
    }
}

pub struct PrefixData {
    pub prefixes_fst_mmap: Mmap,
    pub prefix_delta: PrefixDelta,
}

#[derive(Debug)]
pub struct PrefixDelta {
    pub modified: HashSet<Prefix>,
    pub deleted: HashSet<Prefix>,
}

struct PrefixFstBuilder {
    prefix_count_threshold: u64,
    max_prefix_length: usize,
    /// TODO: Replace the full memory allocation
    prefix_fst_builders: Vec<SetBuilder<Vec<u8>>>,
    current_prefix: Vec<Prefix>,
    current_prefix_count: Vec<u64>,
    modified_prefixes: HashSet<Prefix>,
    current_prefix_is_modified: Vec<bool>,
}

impl PrefixFstBuilder {
    pub fn new(prefix_settings: PrefixSettings) -> Option<Self> {
        let PrefixSettings { prefix_count_threshold, max_prefix_length, compute_prefixes } =
            prefix_settings;

        if !compute_prefixes {
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
            modified_prefixes: HashSet::new(),
            current_prefix_is_modified: vec![false; max_prefix_length],
        })
    }

    fn insert_word(&mut self, bytes: &[u8], deladd: DelAdd, is_modified: bool) -> Result<()> {
        for n in 0..self.max_prefix_length {
            let current_prefix = &mut self.current_prefix[n];
            let current_prefix_count = &mut self.current_prefix_count[n];
            let builder = &mut self.prefix_fst_builders[n];
            let current_prefix_is_modified = &mut self.current_prefix_is_modified[n];

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
                *current_prefix_is_modified = false;
            }

            if deladd == DelAdd::Addition {
                *current_prefix_count += 1;
            }

            if is_modified && !*current_prefix_is_modified {
                if *current_prefix_count > self.prefix_count_threshold {
                    self.modified_prefixes.insert(current_prefix.clone());
                }

                *current_prefix_is_modified = true;
            }

            // There is enough words corresponding to this prefix to add it to the cache.
            if *current_prefix_count == self.prefix_count_threshold {
                builder.insert(prefix)?;

                if *current_prefix_is_modified {
                    self.modified_prefixes.insert(current_prefix.clone());
                }
            }
        }

        Ok(())
    }

    fn build(self, index: &crate::Index, rtxn: &heed::RoTxn) -> Result<PrefixData> {
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
        let new_prefix_fst = Set::new(&prefix_fst_mmap)?;
        let old_prefix_fst = index.words_prefixes_fst(rtxn)?;
        let mut deleted_prefixes = HashSet::new();
        {
            let mut deleted_prefixes_stream = old_prefix_fst.op().add(&new_prefix_fst).difference();
            while let Some(prefix) = deleted_prefixes_stream.next() {
                deleted_prefixes.insert(Prefix::from(std::str::from_utf8(prefix)?));
            }
        }

        Ok(PrefixData {
            prefixes_fst_mmap: prefix_fst_mmap,
            prefix_delta: PrefixDelta {
                modified: self.modified_prefixes,
                deleted: deleted_prefixes,
            },
        })
    }
}
