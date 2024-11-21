use std::fs::File;
use std::io::BufWriter;

use fst::{Set, SetBuilder, Streamer};
use memmap2::Mmap;
use tempfile::tempfile;

use crate::update::del_add::DelAdd;
use crate::{InternalError, Result};

pub struct FstMergerBuilder<'a> {
    stream: Option<fst::set::Stream<'a>>,
    fst_builder: SetBuilder<BufWriter<File>>,
    last: Option<Vec<u8>>,
    inserted_words: usize,
}

impl<'a> FstMergerBuilder<'a> {
    pub fn new<D: AsRef<[u8]>>(fst: Option<&'a Set<D>>) -> Result<Self> {
        Ok(Self {
            stream: fst.map(|fst| fst.stream()),
            fst_builder: SetBuilder::new(BufWriter::new(tempfile()?))?,
            last: None,
            inserted_words: 0,
        })
    }

    pub fn register(
        &mut self,
        deladd: DelAdd,
        right: &[u8],
        insertion_callback: &mut impl FnMut(&[u8], DelAdd, bool) -> Result<()>,
    ) -> Result<()> {
        if let Some(left) = self.last.take() {
            let (left_inserted, right_inserted) =
                self.compare_and_insert(deladd, left.as_slice(), right, insertion_callback)?;

            // left was not inserted, so we keep it for the next iteration
            if !left_inserted {
                self.last = Some(left);
            }

            // right was inserted, so we can stop
            if right_inserted {
                return Ok(());
            }
        }

        if let Some(mut stream) = self.stream.take() {
            while let Some(left) = stream.next() {
                let (left_inserted, right_inserted) =
                    self.compare_and_insert(deladd, left, right, insertion_callback)?;

                // left was not inserted, so we keep it for the next iteration
                if !left_inserted {
                    self.last = Some(left.to_vec());
                }

                // right was inserted, so we can stop
                if right_inserted {
                    self.stream = Some(stream);
                    return Ok(());
                }
            }
        }

        // If we reach this point, it means that the stream is empty
        // and we need to insert the incoming word
        self.insert(right, deladd, true, insertion_callback)?;

        Ok(())
    }

    fn compare_and_insert(
        &mut self,
        deladd: DelAdd,
        left: &[u8],
        right: &[u8],
        insertion_callback: &mut impl FnMut(&[u8], DelAdd, bool) -> Result<()>,
    ) -> Result<(bool, bool)> {
        let mut left_inserted = false;
        let mut right_inserted = false;
        match left.cmp(right) {
            std::cmp::Ordering::Less => {
                // We need to insert the last word from the current fst
                self.insert(left, DelAdd::Addition, false, insertion_callback)?;

                left_inserted = true;
            }
            std::cmp::Ordering::Equal => {
                self.insert(right, deladd, true, insertion_callback)?;

                left_inserted = true;
                right_inserted = true;
            }
            std::cmp::Ordering::Greater => {
                self.insert(right, deladd, true, insertion_callback)?;

                right_inserted = true;
            }
        }

        Ok((left_inserted, right_inserted))
    }

    fn insert(
        &mut self,
        bytes: &[u8],
        deladd: DelAdd,
        is_modified: bool,
        insertion_callback: &mut impl FnMut(&[u8], DelAdd, bool) -> Result<()>,
    ) -> Result<()> {
        // Addition: We insert the word
        // Deletion: We delete the word by not inserting it
        if deladd == DelAdd::Addition {
            self.inserted_words += 1;
            self.fst_builder.insert(bytes)?;
        }

        insertion_callback(bytes, deladd, is_modified)?;

        Ok(())
    }

    fn drain_stream(
        &mut self,
        insertion_callback: &mut impl FnMut(&[u8], DelAdd, bool) -> Result<()>,
    ) -> Result<()> {
        if let Some(last) = self.last.take() {
            self.insert(last.as_slice(), DelAdd::Addition, false, insertion_callback)?;
        }

        if let Some(mut stream) = self.stream.take() {
            while let Some(current) = stream.next() {
                self.insert(current, DelAdd::Addition, false, insertion_callback)?;
            }
        }

        Ok(())
    }

    pub fn build(
        mut self,
        insertion_callback: &mut impl FnMut(&[u8], DelAdd, bool) -> Result<()>,
    ) -> Result<Mmap> {
        self.drain_stream(insertion_callback)?;

        let fst_file = self
            .fst_builder
            .into_inner()?
            .into_inner()
            .map_err(|_| InternalError::IndexingMergingKeys { process: "building-fst" })?;
        let fst_mmap = unsafe { Mmap::map(&fst_file)? };

        Ok(fst_mmap)
    }
}
