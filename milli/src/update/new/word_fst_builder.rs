use std::{fs::File, io::BufWriter};

use fst::{Set, SetBuilder, Streamer};
use memmap2::Mmap;
use tempfile::tempfile;

use crate::{update::del_add::DelAdd, Result, SmallString32};

pub struct WordFstBuilder<'a> {
    stream: Option<fst::set::Stream<'a>>,
    word_fst_builder: SetBuilder<BufWriter<File>>,
    /// TODO: Replace the full memory allocation
    prefix_fst_builders: Vec<SetBuilder<Vec<u8>>>,
    max_prefix_length: usize,
    last_word: Option<Vec<u8>>,
    current_prefix: Vec<SmallString32>,
    current_prefix_count: Vec<u64>,
    prefix_count_threshold: u64,
    inserted_words: usize,
    registered_words: usize,
    base_set_length: usize,
}

impl<'a> WordFstBuilder<'a> {
    pub fn new(
        words_fst: &'a Set<std::borrow::Cow<'a, [u8]>>,
        max_prefix_length: usize,
    ) -> Result<Self> {
        let mut prefix_fst_builders = Vec::new();
        for _ in 0..max_prefix_length {
            prefix_fst_builders.push(SetBuilder::memory());
        }

        Ok(Self {
            stream: Some(words_fst.stream()),
            word_fst_builder: SetBuilder::new(BufWriter::new(tempfile()?))?,
            prefix_fst_builders,
            max_prefix_length,
            last_word: None,
            current_prefix: vec![SmallString32::new(); max_prefix_length],
            current_prefix_count: vec![0; max_prefix_length],
            prefix_count_threshold: 100,
            inserted_words: 0,
            registered_words: 0,
            base_set_length: words_fst.len(),
        })
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
            self.insert_word(right)?;

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
                self.insert_word(left)?;

                left_inserted = true;
            }
            std::cmp::Ordering::Equal => {
                // Addition: We insert the word
                // Deletion: We delete the word by not inserting it
                if deladd == DelAdd::Addition {
                    self.insert_word(right)?;
                }

                left_inserted = true;
                right_inserted = true;
            }
            std::cmp::Ordering::Greater => {
                // Addition: We insert the word and keep the last word
                // Deletion: We keep the current word until the left word to delete is greater or equal
                if deladd == DelAdd::Addition {
                    self.insert_word(right)?;
                }

                right_inserted = true;
            }
        }

        Ok((left_inserted, right_inserted))
    }

    fn insert_word(&mut self, bytes: &[u8]) -> Result<()> {
        self.inserted_words += 1;
        self.word_fst_builder.insert(bytes)?;

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
                *current_prefix = SmallString32::from(prefix);
                *current_prefix_count = 0;
            }

            *current_prefix_count += 1;

            // There is enough words corresponding to this prefix to add it to the cache.
            /// TODO: (LEGACY) Replace this by `==` to avoid inserting several times the same prefix?
            if *current_prefix_count >= self.prefix_count_threshold {
                builder.insert(prefix)?;
            }
        }

        Ok(())
    }

    fn drain_stream(&mut self) -> Result<()> {
        if let Some(mut stream) = self.stream.take() {
            while let Some(current) = stream.next() {
                self.insert_word(current)?;
            }
        }

        Ok(())
    }

    pub fn build(mut self) -> Result<(Mmap, Mmap)> {
        self.drain_stream()?;

        /// TODO: ugly unwrap
        let words_fst_file = self.word_fst_builder.into_inner()?.into_inner().unwrap();
        let words_fst_mmap = unsafe { Mmap::map(&words_fst_file)? };

        // We merge all of the previously computed prefixes into on final set.
        let mut prefix_fsts = Vec::new();
        for builder in self.prefix_fst_builders {
            prefix_fsts.push(builder.into_set());
        }
        let op = fst::set::OpBuilder::from_iter(prefix_fsts.iter());
        let mut builder = SetBuilder::new(BufWriter::new(tempfile()?))?;
        builder.extend_stream(op.r#union())?;
        /// TODO: ugly unwrap
        let prefix_fst_file = builder.into_inner()?.into_inner().unwrap();
        let prefix_fst_mmap = unsafe { Mmap::map(&prefix_fst_file)? };

        eprintln!("================================================");
        eprintln!(
            "inserted words: {}, registered words: {}, base set len: {}",
            self.inserted_words, self.registered_words, self.base_set_length
        );
        eprintln!("================================================");

        Ok((words_fst_mmap, prefix_fst_mmap))
    }
}
