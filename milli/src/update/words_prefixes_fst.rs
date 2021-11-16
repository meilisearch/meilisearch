use std::iter::FromIterator;
use std::str;

use fst::Streamer;

use crate::{Index, Result, SmallString32};

pub struct WordsPrefixesFst<'t, 'u, 'i> {
    wtxn: &'t mut heed::RwTxn<'i, 'u>,
    index: &'i Index,
    threshold: u32,
    max_prefix_length: usize,
}

impl<'t, 'u, 'i> WordsPrefixesFst<'t, 'u, 'i> {
    pub fn new(
        wtxn: &'t mut heed::RwTxn<'i, 'u>,
        index: &'i Index,
    ) -> WordsPrefixesFst<'t, 'u, 'i> {
        WordsPrefixesFst { wtxn, index, threshold: 100, max_prefix_length: 4 }
    }

    /// Set the number of words required to make a prefix be part of the words prefixes
    /// database. If a word prefix is supposed to match more than this number of words in the
    /// dictionnary, therefore this prefix is added to the words prefixes datastructures.
    ///
    /// Default value is 100. This value must be higher than 50 and will be clamped
    /// to this bound otherwise.
    pub fn threshold(&mut self, value: u32) -> &mut Self {
        self.threshold = value.max(50);
        self
    }

    /// Set the maximum length of prefixes in bytes.
    ///
    /// Default value is `4` bytes. This value must be between 1 and 25 will be clamped
    /// to these bounds, otherwise.
    pub fn max_prefix_length(&mut self, value: usize) -> &mut Self {
        self.max_prefix_length = value.min(25).max(1); // clamp [1, 25]
        self
    }

    #[logging_timer::time("WordsPrefixesFst::{}")]
    pub fn execute(self) -> Result<()> {
        let words_fst = self.index.words_fst(&self.wtxn)?;

        let mut prefix_fsts = Vec::with_capacity(self.max_prefix_length);
        for n in 1..=self.max_prefix_length {
            let mut current_prefix = SmallString32::new();
            let mut current_prefix_count = 0;
            let mut builder = fst::SetBuilder::memory();

            let mut stream = words_fst.stream();
            while let Some(bytes) = stream.next() {
                // We try to get the first n bytes out of this string but we only want
                // to split at valid characters bounds. If we try to split in the middle of
                // a character we ignore this word and go to the next one.
                let word = str::from_utf8(bytes)?;
                let prefix = match word.get(..n) {
                    Some(prefix) => prefix,
                    None => continue,
                };

                // This is the first iteration of the loop,
                // or the current word doesn't starts with the current prefix.
                if current_prefix_count == 0 || prefix != current_prefix.as_str() {
                    current_prefix = SmallString32::from(prefix);
                    current_prefix_count = 0;
                }

                current_prefix_count += 1;

                // There is enough words corresponding to this prefix to add it to the cache.
                if current_prefix_count >= self.threshold {
                    builder.insert(prefix)?;
                }
            }

            // We construct the final set for prefixes of size n.
            prefix_fsts.push(builder.into_set());
        }

        // We merge all of the previously computed prefixes into on final set.
        let op = fst::set::OpBuilder::from_iter(prefix_fsts.iter());
        let mut builder = fst::SetBuilder::memory();
        builder.extend_stream(op.r#union())?;
        let prefix_fst = builder.into_set();

        // Set the words prefixes FST in the dtabase.
        self.index.put_words_prefixes_fst(self.wtxn, &prefix_fst)?;

        Ok(())
    }
}
