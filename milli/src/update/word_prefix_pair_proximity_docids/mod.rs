use grenad::CompressionType;
use heed::types::ByteSlice;

use heed::BytesDecode;
use log::debug;

use std::borrow::Cow;
use std::cmp::Ordering;
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
        let mut allocations = Allocations::default();

        let mut count = 0;

        let prefixes = PrefixTrieNode::from_sorted_prefixes(
            common_prefix_fst_words
                .into_iter()
                .map(|s| s.into_iter())
                .flatten()
                .map(|s| s.as_str())
                .filter(|s| s.len() <= self.max_prefix_length),
        );

        if !prefixes.is_empty() {
            let mut cursor = new_word_pair_proximity_docids.into_cursor()?;
            Self::execute_on_word_pairs_and_prefixes(
                &mut cursor,
                |cursor| {
                    if let Some((key, value)) = cursor.move_on_next()? {
                        let (word1, word2, proximity) = UncheckedStrStrU8Codec::bytes_decode(key)
                            .ok_or(heed::Error::Decoding)?;
                        Ok(Some(((word1, word2, proximity), value)))
                    } else {
                        Ok(None)
                    }
                },
                &prefixes,
                &mut allocations,
                self.max_proximity,
                |key, value| {
                    count += 1;
                    insert_into_database(
                        &mut self.wtxn,
                        *self.index.word_prefix_pair_proximity_docids.as_polymorph(),
                        key,
                        value,
                    )
                },
            )?;
        }
        dbg!(count);

        let prefixes = PrefixTrieNode::from_sorted_prefixes(
            new_prefix_fst_words
                .into_iter()
                .map(|s| s.as_str())
                .filter(|s| s.len() <= self.max_prefix_length),
        );

        if !prefixes.is_empty() {
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

            Self::execute_on_word_pairs_and_prefixes(
                &mut db_iter,
                |db_iter| db_iter.next().transpose().map_err(|e| e.into()),
                &prefixes,
                &mut allocations,
                self.max_proximity,
                |key, value| writer.insert(key, value).map_err(|e| e.into()),
            )?;
            drop(db_iter);
            writer_into_lmdb_database(
                self.wtxn,
                *self.index.word_prefix_pair_proximity_docids.as_polymorph(),
                writer,
            )?;
        }

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

        Ok(())
    }

    fn execute_on_word_pairs_and_prefixes<Iter>(
        iter: &mut Iter,
        mut next_word_pair_proximity: impl for<'a> FnMut(
            &'a mut Iter,
        ) -> Result<
            Option<((&'a [u8], &'a [u8], u8), &'a [u8])>,
        >,
        prefixes: &PrefixTrieNode,
        allocations: &mut Allocations,
        max_proximity: u8,
        mut insert: impl for<'a> FnMut(&'a [u8], &'a [u8]) -> Result<()>,
    ) -> Result<()> {
        let mut batch = PrefixAndProximityBatch::default();
        let mut prev_word2_start = 0;

        let mut prefix_search_start = PrefixTrieNodeSearchStart(0);
        let mut empty_prefixes = false;

        let mut prefix_buffer = allocations.take_byte_vector();

        while let Some(((word1, word2, proximity), data)) = next_word_pair_proximity(iter)? {
            if proximity > max_proximity {
                continue;
            };
            let word2_start_different_than_prev = word2[0] != prev_word2_start;
            if empty_prefixes && !word2_start_different_than_prev {
                continue;
            }
            let word1_different_than_prev = word1 != batch.word1;
            if word1_different_than_prev || word2_start_different_than_prev {
                batch.flush(allocations, &mut insert)?;
                if word1_different_than_prev {
                    prefix_search_start.0 = 0;
                    batch.word1.clear();
                    batch.word1.extend_from_slice(word1);
                }
                if word2_start_different_than_prev {
                    // word2_start_different_than_prev == true
                    prev_word2_start = word2[0];
                }
                empty_prefixes = !prefixes.set_search_start(word2, &mut prefix_search_start);
            }

            if !empty_prefixes {
                prefixes.for_each_prefix_of(
                    word2,
                    &mut prefix_buffer,
                    &prefix_search_start,
                    |prefix_buffer| {
                        let mut value = allocations.take_byte_vector();
                        value.extend_from_slice(&data);
                        let prefix_len = prefix_buffer.len();
                        prefix_buffer.push(0);
                        prefix_buffer.push(proximity);
                        batch.insert(&prefix_buffer, value, allocations);
                        prefix_buffer.truncate(prefix_len);
                    },
                );
                prefix_buffer.clear();
            }
        }
        batch.flush(allocations, &mut insert)?;
        Ok(())
    }
}

/**
A map structure whose keys are (prefix, proximity) and whose values are vectors of bitstrings (serialized roaring bitmaps).
The keys are sorted and conflicts are resolved by merging the vectors of bitstrings together.

It is used to ensure that all ((word1, prefix, proximity), docids) are inserted into the database in sorted order and efficiently.

The batch is flushed as often as possible, when we are sure that every (word1, prefix, proximity) key derived from its content
can be inserted into the database in sorted order. When it is flushed, it calls a user-provided closure with the following arguments:
- key   : (word1, prefix, proximity) as bytes
- value : merged roaring bitmaps from all values associated with (prefix, proximity) in the batch, serialised to bytes
*/
#[derive(Default)]
struct PrefixAndProximityBatch {
    word1: Vec<u8>,
    batch: Vec<(Vec<u8>, Vec<Cow<'static, [u8]>>)>,
}

impl PrefixAndProximityBatch {
    fn insert(&mut self, new_key: &[u8], new_value: Vec<u8>, allocations: &mut Allocations) {
        // this is a macro instead of a closure because the borrow checker will complain
        // about the closure moving `new_value`
        macro_rules! insert_new_key_value {
            () => {
                let mut key = allocations.take_byte_vector();
                key.extend_from_slice(new_key);
                let mut mergeable_data = allocations.take_mergeable_data_vector();
                mergeable_data.push(Cow::Owned(new_value));
                self.batch.push((key, mergeable_data));
            };
            ($idx:expr) => {
                let mut key = allocations.take_byte_vector();
                key.extend_from_slice(new_key);
                let mut mergeable_data = allocations.take_mergeable_data_vector();
                mergeable_data.push(Cow::Owned(new_value));
                self.batch.insert($idx, (key, mergeable_data));
            };
        }

        if self.batch.is_empty() {
            insert_new_key_value!();
        } else if self.batch.len() == 1 {
            let (existing_key, existing_data) = &mut self.batch[0];
            match new_key.cmp(&existing_key) {
                Ordering::Less => {
                    insert_new_key_value!(0);
                }
                Ordering::Equal => {
                    existing_data.push(Cow::Owned(new_value));
                }
                Ordering::Greater => {
                    insert_new_key_value!();
                }
            }
        } else {
            match self.batch.binary_search_by_key(&new_key, |(k, _)| k.as_slice()) {
                Ok(position) => {
                    self.batch[position].1.push(Cow::Owned(new_value));
                }
                Err(position) => {
                    insert_new_key_value!(position);
                }
            }
        }
    }

    /// Empties the batch, calling `insert` on each element.
    ///
    /// The key given to `insert` is `(word1, prefix, proximity)` and the value is the associated merged roaring bitmap.
    fn flush(
        &mut self,
        allocations: &mut Allocations,
        insert: &mut impl for<'buffer> FnMut(&'buffer [u8], &'buffer [u8]) -> Result<()>,
    ) -> Result<()> {
        let PrefixAndProximityBatch { word1, batch } = self;
        if batch.is_empty() {
            return Ok(());
        }

        let mut buffer = allocations.take_byte_vector();
        buffer.extend_from_slice(word1);
        buffer.push(0);

        for (key, mergeable_data) in batch.drain(..) {
            buffer.truncate(word1.len() + 1);
            buffer.extend_from_slice(key.as_slice());
            let merged;
            let data = if mergeable_data.len() > 1 {
                merged = merge_cbo_roaring_bitmaps(&buffer, &mergeable_data)?;
                &merged
            } else {
                &mergeable_data[0]
            };
            insert(buffer.as_slice(), data)?;
            allocations.reclaim_byte_vector(key);
            allocations.reclaim_mergeable_data_vector(mergeable_data);
        }

        Ok(())
    }
}

// This is adapted from `sorter_into_lmdb_database`
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
    let len = reader.len();
    dbg!(len);
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

#[derive(Debug)]
struct PrefixTrieNodeSearchStart(usize);

impl PrefixTrieNode {
    fn is_empty(&self) -> bool {
        self.children.is_empty()
    }

    /// Returns false if the trie does not contain a prefix of the given word.
    /// Returns true if the trie *may* contain a prefix of the given word.
    ///
    /// Moves the search start to the first node equal to the first letter of the word,
    /// or to 0 otherwise.
    fn set_search_start(&self, word: &[u8], search_start: &mut PrefixTrieNodeSearchStart) -> bool {
        let byte = word[0];
        if self.children[search_start.0].1 == byte {
            return true;
        } else if let Some(position) =
            self.children[search_start.0..].iter().position(|(_, c)| *c >= byte)
        {
            let (_, c) = self.children[search_start.0 + position];
            // dbg!(position, c, byte);
            if c == byte {
                // dbg!();
                search_start.0 += position;
                true
            } else {
                // dbg!();
                search_start.0 = 0;
                false
            }
        } else {
            // dbg!();
            search_start.0 = 0;
            false
        }
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
    fn for_each_prefix_of(
        &self,
        word: &[u8],
        buffer: &mut Vec<u8>,
        search_start: &PrefixTrieNodeSearchStart,
        mut do_fn: impl FnMut(&mut Vec<u8>),
    ) {
        let first_byte = word[0];
        let mut cur_node = self;
        buffer.push(first_byte);
        if let Some((child_node, c)) =
            cur_node.children[search_start.0..].iter().find(|(_, c)| *c >= first_byte)
        {
            if *c == first_byte {
                cur_node = child_node;
                if cur_node.is_end_node {
                    do_fn(buffer);
                }
                for &byte in &word[1..] {
                    buffer.push(byte);
                    if let Some((child_node, c)) =
                        cur_node.children.iter().find(|(_, c)| *c >= byte)
                    {
                        if *c == byte {
                            cur_node = child_node;
                            if cur_node.is_end_node {
                                do_fn(buffer);
                            }
                        } else {
                            break;
                        }
                    } else {
                        break;
                    }
                }
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
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_trie() {
        let trie = PrefixTrieNode::from_sorted_prefixes(IntoIterator::into_iter([
            "1", "19", "2", "a", "ab", "ac", "ad", "al", "am", "an", "ap", "ar", "as", "at", "au",
            "b", "ba", "bar", "be", "bi", "bl", "bla", "bo", "br", "bra", "bri", "bro", "bu", "c",
            "ca", "car", "ce", "ch", "cha", "che", "chi", "ci", "cl", "cla", "co", "col", "com",
            "comp", "con", "cons", "cont", "cor", "cou", "cr", "cu", "d", "da", "de", "dec", "des",
            "di", "dis", "do", "dr", "du", "e", "el", "em", "en", "es", "ev", "ex", "exp", "f",
            "fa", "fe", "fi", "fl", "fo", "for", "fr", "fra", "fre", "fu", "g", "ga", "ge", "gi",
            "gl", "go", "gr", "gra", "gu", "h", "ha", "har", "he", "hea", "hi", "ho", "hu", "i",
            "im", "imp", "in", "ind", "ins", "int", "inte", "j", "ja", "je", "jo", "ju", "k", "ka",
            "ke", "ki", "ko", "l", "la", "le", "li", "lo", "lu", "m", "ma", "mal", "man", "mar",
            "mat", "mc", "me", "mi", "min", "mis", "mo", "mon", "mor", "mu", "n", "na", "ne", "ni",
            "no", "o", "or", "ou", "ov", "ove", "over", "p", "pa", "par", "pe", "per", "ph", "pi",
            "pl", "po", "pr", "pre", "pro", "pu", "q", "qu", "r", "ra", "re", "rec", "rep", "res",
            "ri", "ro", "ru", "s", "sa", "san", "sc", "sch", "se", "sh", "sha", "shi", "sho", "si",
            "sk", "sl", "sn", "so", "sp", "st", "sta", "ste", "sto", "str", "su", "sup", "sw", "t",
            "ta", "te", "th", "ti", "to", "tr", "tra", "tri", "tu", "u", "un", "v", "va", "ve",
            "vi", "vo", "w", "wa", "we", "wh", "wi", "wo", "y", "yo", "z",
        ]));
        // let mut buffer = String::new();
        // trie.print(&mut buffer, 0);
        // buffer.clear();
        let mut search_start = PrefixTrieNodeSearchStart(0);
        let mut buffer = vec![];

        let is_empty = !trie.set_search_start("affair".as_bytes(), &mut search_start);
        println!("{search_start:?}");
        println!("is empty: {is_empty}");
        trie.for_each_prefix_of("affair".as_bytes(), &mut buffer, &search_start, |x| {
            let s = std::str::from_utf8(x).unwrap();
            println!("{s}");
        });
        buffer.clear();
        trie.for_each_prefix_of("trans".as_bytes(), &mut buffer, &search_start, |x| {
            let s = std::str::from_utf8(x).unwrap();
            println!("{s}");
        });
        buffer.clear();

        trie.for_each_prefix_of("affair".as_bytes(), &mut buffer, &search_start, |x| {
            let s = std::str::from_utf8(x).unwrap();
            println!("{s}");
        });
        buffer.clear();
        // trie.for_each_prefix_of("1", |x| {
        //     println!("{x}");
        // });
        // trie.for_each_prefix_of("19", |x| {
        //     println!("{x}");
        // });
        // trie.for_each_prefix_of("21", |x| {
        //     println!("{x}");
        // });
        // let mut buffer = vec![];
        // trie.for_each_prefix_of("integ", &mut buffer, |x| {
        //     println!("{x}");
        // });
    }
}
