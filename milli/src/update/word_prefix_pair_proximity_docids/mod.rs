use grenad::CompressionType;
use heed::types::ByteSlice;

use heed::BytesDecode;
use log::debug;

use std::borrow::Cow;
use std::collections::HashSet;
use std::io::BufReader;
use std::time::Instant;

use crate::update::index_documents::{
    create_writer, merge_cbo_roaring_bitmaps, CursorClonableMmap,
};
use crate::{Index, Result, UncheckedStrStrU8Codec};

pub struct WordPrefixPairProximityDocids<'t, 'u, 'i> {
    wtxn: &'t mut heed::RwTxn<'i, 'u>,
    index: &'i Index,
    pub(crate) chunk_compression_type: CompressionType,
    pub(crate) chunk_compression_level: Option<u32>,
    pub(crate) max_nb_chunks: Option<usize>,
    pub(crate) max_memory: Option<usize>,
    max_proximity: u8,
    max_prefix_length: usize,
}

impl<'t, 'u, 'i> WordPrefixPairProximityDocids<'t, 'u, 'i> {
    pub fn new(
        wtxn: &'t mut heed::RwTxn<'i, 'u>,
        index: &'i Index,
    ) -> WordPrefixPairProximityDocids<'t, 'u, 'i> {
        WordPrefixPairProximityDocids {
            wtxn,
            index,
            chunk_compression_type: CompressionType::None,
            chunk_compression_level: None,
            max_nb_chunks: None,
            max_memory: None,
            max_proximity: 4,
            max_prefix_length: 2,
        }
    }

    /// Set the maximum proximity required to make a prefix be part of the words prefixes
    /// database. If two words are too far from the threshold the associated documents will
    /// not be part of the prefix database.
    ///
    /// Default value is 4. This value must be lower or equal than 7 and will be clamped
    /// to this bound otherwise.
    pub fn max_proximity(&mut self, value: u8) -> &mut Self {
        self.max_proximity = value.max(7);
        self
    }

    /// Set the maximum length the prefix of a word pair is allowed to have to be part of the words
    /// prefixes database. If the prefix length is higher than the threshold, the associated documents
    /// will not be part of the prefix database.
    ///
    /// Default value is 2.
    pub fn max_prefix_length(&mut self, value: usize) -> &mut Self {
        self.max_prefix_length = value;
        self
    }

    #[logging_timer::time("WordPrefixPairProximityDocids::{}")]
    pub fn execute<'a>(
        mut self,
        new_word_pair_proximity_docids: grenad::Reader<CursorClonableMmap>,
        new_prefix_fst_words: &'a [String],
        common_prefix_fst_words: &[&'a [String]],
        del_prefix_fst_words: &HashSet<Vec<u8>>,
    ) -> Result<()> {
        debug!("Computing and writing the word prefix pair proximity docids into LMDB on disk...");

        // All of the word prefix pairs in the database that have a w2
        // that is contained in the `suppr_pw` set must be removed as well.
        if !del_prefix_fst_words.is_empty() {
            let mut iter = self
                .index
                .word_prefix_pair_proximity_docids
                .remap_data_type::<ByteSlice>()
                .iter_mut(self.wtxn)?;
            while let Some(((_, w2, _), _)) = iter.next().transpose()? {
                if del_prefix_fst_words.contains(w2.as_bytes()) {
                    // Delete this entry as the w2 prefix is no more in the words prefix fst.
                    unsafe { iter.del_current()? };
                }
            }
        }

        // We construct a Trie of all the prefixes that are smaller than the max prefix length
        // This is an optimisation that allows us to iterate over all prefixes of a word quickly.
        let new_prefix_fst_words = PrefixTrieNode::from_sorted_prefixes(
            new_prefix_fst_words
                .into_iter()
                .map(|s| s.as_str())
                .filter(|s| s.len() <= self.max_prefix_length),
        );

        let common_prefix_fst_words = PrefixTrieNode::from_sorted_prefixes(
            common_prefix_fst_words
                .into_iter()
                .map(|s| s.into_iter())
                .flatten()
                .map(|s| s.as_str())
                .filter(|s| s.len() <= self.max_prefix_length),
        );

        let mut allocations = Allocations::default();
        let mut batch = PrefixAndProximityBatch::default();

        if !common_prefix_fst_words.is_empty() {
            let mut cursor = new_word_pair_proximity_docids.into_cursor()?;

            while let Some((key, data)) = cursor.move_on_next()? {
                let (word1, word2, proximity) =
                    UncheckedStrStrU8Codec::bytes_decode(key).ok_or(heed::Error::Decoding)?;

                if proximity <= self.max_proximity {
                    batch.flush_if_necessary(
                        word1,
                        word2,
                        &mut allocations,
                        &mut |key, value| {
                            insert_into_database(
                                &mut self.wtxn,
                                *self.index.word_prefix_pair_proximity_docids.as_polymorph(),
                                key,
                                value,
                            )
                        },
                    )?;
                    self.insert_word_prefix_pair_proximity_docids_into_batch(
                        word2,
                        proximity,
                        data,
                        &common_prefix_fst_words,
                        &mut batch,
                        &mut allocations,
                    )?;
                }
            }
            batch.flush(&mut allocations, &mut |key, value| {
                insert_into_database(
                    &mut self.wtxn,
                    *self.index.word_prefix_pair_proximity_docids.as_polymorph(),
                    key,
                    value,
                )
            })?;
        }

        if !new_prefix_fst_words.is_empty() {
            let mut db_iter = self
                .index
                .word_pair_proximity_docids
                .remap_key_type::<UncheckedStrStrU8Codec>()
                .remap_data_type::<ByteSlice>()
                .iter(self.wtxn)?;

            let mut writer = create_writer(
                self.chunk_compression_type,
                self.chunk_compression_level,
                tempfile::tempfile()?,
            );

            while let Some(((word1, word2, proximity), data)) = db_iter.next().transpose()? {
                if proximity <= self.max_proximity {
                    batch.flush_if_necessary(
                        word1,
                        word2,
                        &mut allocations,
                        &mut |key, value| writer.insert(key, value).map_err(|e| e.into()),
                    )?;
                    self.insert_word_prefix_pair_proximity_docids_into_batch(
                        word2,
                        proximity,
                        data,
                        &new_prefix_fst_words,
                        &mut batch,
                        &mut allocations,
                    )?;
                }
            }
            batch.flush(&mut allocations, &mut |key, value| {
                writer.insert(key, value).map_err(|e| e.into())
            })?;

            drop(db_iter);
            writer_into_lmdb_database(
                self.wtxn,
                *self.index.word_prefix_pair_proximity_docids.as_polymorph(),
                writer,
            )?;
        }

        Ok(())
    }

    fn insert_word_prefix_pair_proximity_docids_into_batch<'b, 'c>(
        &self,
        word2: &[u8],
        proximity: u8,
        data: &'b [u8],
        prefixes: &'c PrefixTrieNode,
        writer: &'b mut PrefixAndProximityBatch,
        allocations: &mut Allocations,
    ) -> Result<()> {
        let mut prefix_buffer = allocations.take_byte_vector();
        prefixes.for_each_prefix_of(word2, &mut prefix_buffer, |prefix| {
            let mut value = allocations.take_byte_vector();
            value.extend_from_slice(&data);
            writer.insert(prefix, proximity, value, allocations);
        });
        allocations.reclaim_byte_vector(prefix_buffer);
        Ok(())
    }
}

/**
A map structure whose keys are (prefix, proximity) and whose values are vectors of bitstrings (serialized roaring bitmaps).
The keys are sorted and conflicts are resolved by merging the vectors of bitstrings together.

It is used to ensure that all ((word1, prefix, proximity), docids) are inserted into the database in sorted order and efficiently.

A batch is valid only for a specific `word1`. Also, all prefixes stored in the batch start with the same letter. Make sure to
call [`self.flush_if_necessary`](Self::flush_if_necessary) before inserting a list of sorted `(prefix, proximity)` (and where each
`prefix` starts with the same letter) in order to uphold these invariants.

The batch is flushed as often as possible, when we are sure that every (word1, prefix, proximity) key derived from its content
can be inserted into the database in sorted order. When it is flushed, it calls a user-provided closure with the following arguments:
- key   : (word1, prefix, proximity) as bytes
- value : merged roaring bitmaps from all values associated with (prefix, proximity) in the batch, serialised to bytes
*/
#[derive(Default)]
struct PrefixAndProximityBatch {
    batch: Vec<(Vec<u8>, Vec<Cow<'static, [u8]>>)>,
    word1: Vec<u8>,
    word2_start: u8,
}

impl PrefixAndProximityBatch {
    fn insert(
        &mut self,
        new_prefix: &[u8],
        new_proximity: u8,
        new_value: Vec<u8>,
        allocations: &mut Allocations,
    ) {
        let mut key = allocations.take_byte_vector();
        key.extend_from_slice(new_prefix);
        key.push(0);
        key.push(new_proximity);

        if let Some(position) = self.batch.iter().position(|(k, _)| k >= &key) {
            let (existing_key, existing_data) = &mut self.batch[position];
            if existing_key == &key {
                existing_data.push(Cow::Owned(new_value));
            } else {
                let mut mergeable_data = allocations.take_mergeable_data_vector();
                mergeable_data.push(Cow::Owned(new_value));
                self.batch.insert(position, (key, mergeable_data));
            }
        } else {
            let mut mergeable_data = allocations.take_mergeable_data_vector();
            mergeable_data.push(Cow::Owned(new_value));
            self.batch.push((key, mergeable_data));
        }
    }

    /// Call [`self.flush`](Self::flush) if `word1` changed or if `word2` begins with a different letter than the
    /// previous word2. Update `prev_word1` and `prev_word2_start` with the new values from `word1` and `word2`.
    fn flush_if_necessary(
        &mut self,
        word1: &[u8],
        word2: &[u8],
        allocations: &mut Allocations,
        insert: &mut impl for<'buffer> FnMut(&'buffer [u8], &'buffer [u8]) -> Result<()>,
    ) -> Result<()> {
        let word2_start = word2[0];
        if word1 != self.word1 {
            self.flush(allocations, insert)?;
            self.word1.clear();
            self.word1.extend_from_slice(word1);
            if word2_start != self.word2_start {
                self.word2_start = word2_start;
            }
        }
        if word2_start != self.word2_start {
            self.flush(allocations, insert)?;
            self.word2_start = word2_start;
        }
        Ok(())
    }

    /// Empties the batch, calling `insert` on each element.
    ///
    /// The key given to insert is `(word1, prefix, proximity)` and the value is the associated merged roaring bitmap.
    fn flush(
        &mut self,
        allocations: &mut Allocations,
        insert: &mut impl for<'buffer> FnMut(&'buffer [u8], &'buffer [u8]) -> Result<()>,
    ) -> Result<()> {
        let PrefixAndProximityBatch { batch, word1: prev_word1, word2_start: _ } = self;
        let mut buffer = allocations.take_byte_vector();
        buffer.extend_from_slice(prev_word1.as_slice());
        buffer.push(0);

        for (key, mergeable_data) in batch.drain(..) {
            buffer.truncate(prev_word1.len() + 1);
            buffer.extend_from_slice(key.as_slice());
            let data = merge_cbo_roaring_bitmaps(&buffer, &mergeable_data)?;
            insert(buffer.as_slice(), &data)?;

            allocations.reclaim_byte_vector(key);
            allocations.reclaim_mergeable_data_vector(mergeable_data);
        }
        Ok(())
    }
}

fn insert_into_database(
    wtxn: &mut heed::RwTxn,
    database: heed::PolyDatabase,
    new_key: &[u8],
    new_value: &[u8],
) -> Result<()> {
    let mut iter = database.prefix_iter_mut::<_, ByteSlice, ByteSlice>(wtxn, new_key)?;
    match iter.next().transpose()? {
        Some((key, old_val)) if new_key == key => {
            let val =
                merge_cbo_roaring_bitmaps(key, &[Cow::Borrowed(old_val), Cow::Borrowed(new_value)])
                    .map_err(|_| {
                        // TODO just wrap this error?
                        crate::error::InternalError::IndexingMergingKeys {
                            process: "get-put-merge",
                        }
                    })?;
            // safety: we don't keep references from inside the LMDB database.
            unsafe { iter.put_current(key, &val)? };
        }
        _ => {
            drop(iter);
            database.put::<_, ByteSlice, ByteSlice>(wtxn, new_key, new_value)?;
        }
    }
    Ok(())
}

// This is adapted from `sorter_into_lmdb_database`
pub fn writer_into_lmdb_database(
    wtxn: &mut heed::RwTxn,
    database: heed::PolyDatabase,
    writer: grenad::Writer<std::fs::File>,
) -> Result<()> {
    let file = writer.into_inner()?;
    let reader = grenad::Reader::new(BufReader::new(file))?;

    let before = Instant::now();

    if database.is_empty(wtxn)? {
        let mut out_iter = database.iter_mut::<_, ByteSlice, ByteSlice>(wtxn)?;
        let mut cursor = reader.into_cursor()?;
        while let Some((k, v)) = cursor.move_on_next()? {
            // safety: we don't keep references from inside the LMDB database.
            unsafe { out_iter.append(k, v)? };
        }
    } else {
        let mut cursor = reader.into_cursor()?;
        while let Some((k, v)) = cursor.move_on_next()? {
            insert_into_database(wtxn, database, k, v)?;
        }
    }

    debug!("MTBL sorter writen in {:.02?}!", before.elapsed());
    Ok(())
}

struct Allocations {
    byte_vectors: Vec<Vec<u8>>,
    mergeable_data_vectors: Vec<Vec<Cow<'static, [u8]>>>,
}
impl Default for Allocations {
    fn default() -> Self {
        Self {
            byte_vectors: Vec::with_capacity(65_536),
            mergeable_data_vectors: Vec::with_capacity(4096),
        }
    }
}
impl Allocations {
    fn take_byte_vector(&mut self) -> Vec<u8> {
        self.byte_vectors.pop().unwrap_or_else(|| Vec::with_capacity(16))
    }
    fn take_mergeable_data_vector(&mut self) -> Vec<Cow<'static, [u8]>> {
        self.mergeable_data_vectors.pop().unwrap_or_else(|| Vec::with_capacity(8))
    }

    fn reclaim_byte_vector(&mut self, mut data: Vec<u8>) {
        data.clear();
        self.byte_vectors.push(data);
    }
    fn reclaim_mergeable_data_vector(&mut self, mut data: Vec<Cow<'static, [u8]>>) {
        data.clear();
        self.mergeable_data_vectors.push(data);
    }
}

#[derive(Default, Debug)]
struct PrefixTrieNode {
    children: Vec<(PrefixTrieNode, u8)>,
    is_end_node: bool,
}

impl PrefixTrieNode {
    fn is_empty(&self) -> bool {
        self.children.is_empty()
    }
    fn from_sorted_prefixes<'a>(prefixes: impl Iterator<Item = &'a str>) -> Self {
        let mut node = PrefixTrieNode::default();
        for prefix in prefixes {
            node.insert_sorted_prefix(prefix.as_bytes().into_iter());
        }
        node
    }
    fn insert_sorted_prefix(&mut self, mut prefix: std::slice::Iter<u8>) {
        if let Some(&c) = prefix.next() {
            if let Some((node, byte)) = self.children.last_mut() {
                if *byte == c {
                    node.insert_sorted_prefix(prefix);
                    return;
                }
            }
            let mut new_node = PrefixTrieNode::default();
            new_node.insert_sorted_prefix(prefix);
            self.children.push((new_node, c));
        } else {
            self.is_end_node = true;
        }
    }
    fn for_each_prefix_of(&self, word: &[u8], buffer: &mut Vec<u8>, mut do_fn: impl FnMut(&[u8])) {
        let mut cur_node = self;
        for &byte in word {
            buffer.push(byte);
            if let Some((child_node, _)) = cur_node.children.iter().find(|(_, c)| *c == byte) {
                cur_node = child_node;
                if cur_node.is_end_node {
                    do_fn(buffer.as_slice());
                }
            } else {
                break;
            }
        }
    }
    // fn print(&self, buffer: &mut String, ident: usize) {
    //     let mut spaces = String::new();
    //     for _ in 0..ident {
    //         spaces.push(' ')
    //     }
    //     for (child, c) in &self.children {
    //         buffer.push(char::from_u32(*c as u32).unwrap());
    //         println!("{spaces}{buffer}:");
    //         child.print(buffer, ident + 4);
    //         buffer.pop();
    //     }
    // }
}
