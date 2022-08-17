/*!
 ## What is WordPrefixPairProximityDocids?
The word-prefix-pair-proximity-docids database is a database whose keys are of
the form (`word`, `prefix`, `proximity`) and the values are roaring bitmaps of
the documents which contain `word` followed by another word starting with
`prefix` at a distance of `proximity`.

The prefixes present in this database are only those that correspond to many
different words in the documents.

## How is it created/updated? (simplified version)
To compute it, we have access to (mainly) two inputs:

* a list of sorted prefixes, such as:
```text
c
ca
cat
d
do
dog
```
Note that only prefixes which correspond to more than a certain number of
different words from the database are included in this list.

* a sorted list of word pairs and the distance between them (i.e. proximity),
* associated with a roaring bitmap, such as:
```text
good dog   3         -> docids1: [2, 5, 6]
good doggo 1         -> docids2: [8]
good dogma 1         -> docids3: [7, 19, 20]
good ghost 2         -> docids4: [1]
horror cathedral 4   -> docids5: [1, 2]
```

I illustrate a simplified version of the algorithm to create the word-prefix
pair-proximity database below:

1. **Outer loop:** First, we iterate over each word pair and its proximity:
```text
word1    : good
word2    : dog
proximity: 3
```
2. **Inner loop:** Then, we iterate over all the prefixes of `word2` that are
in the list of sorted prefixes. And we insert the key (`prefix`, `proximity`)
and the value (`docids`) to a sorted map which we call the “batch”. For example,
at the end of the first inner loop, we may have:
```text
Outer loop 1:
------------------------------
word1    : good
word2    : dog
proximity: 3
docids   : docids1

prefixes: [d, do, dog]

batch: [
    (d, 3)   -> [docids1]
    (do, 3)  -> [docids1]
    (dog, 3) -> [docids1]
]
```
3. For illustration purpose, let's run through a second iteration of the outer loop:
```text
Outer loop 2:
------------------------------
word1    : good
word2    : doggo
proximity: 1
docids   : docids2

prefixes: [d, do, dog]

batch: [
    (d, 1)   -> [docids2]
    (d, 3)   -> [docids1]
    (do, 1)  -> [docids2]
    (do, 3)  -> [docids1]
    (dog, 1) -> [docids2]
    (dog, 3) -> [docids1]
]
```
Notice that the batch had to re-order some (`prefix`, `proximity`) keys: some
of the elements inserted in the second iteration of the outer loop appear
*before* elements from the first iteration.

4. And a third:
```text
Outer loop 3:
------------------------------
word1    : good
word2    : dogma
proximity: 1
docids   : docids3

prefixes: [d, do, dog]

batch: [
    (d, 1)   -> [docids2, docids3]
    (d, 3)   -> [docids1]
    (do, 1)  -> [docids2, docids3]
    (do, 3)  -> [docids1]
    (dog, 1) -> [docids2, docids3]
    (dog, 3) -> [docids1]
]
```
Notice that there were some conflicts which were resolved by merging the
conflicting values together.

5. On the fourth iteration of the outer loop, we have:
```text
Outer loop 4:
------------------------------
word1    : good
word2    : ghost
proximity: 2
```
Because `word2` begins with a different letter than the previous `word2`,
we know that:

1. All the prefixes of `word2` are greater than the prefixes of the previous word2
2. And therefore, every instance of (`word2`, `prefix`) will be greater than
any element in the batch.

Therefore, we know that we can insert every element from the batch into the
database before proceeding any further. This operation is called
“flushing the batch”. Flushing the batch should also be done whenever `word1`
is different than the previous `word1`.

6. **Flushing the batch:** to flush the batch, we look at the `word1` and
iterate over the elements of the batch in sorted order:
```text
Flushing Batch loop 1:
------------------------------
word1    : good
word2    : d
proximity: 1
docids   : [docids2, docids3]
```
We then merge the array of `docids` (of type `Vec<Vec<u8>>`) using
`merge_cbo_roaring_bitmap` in order to get a single byte vector representing a
roaring bitmap of all the document ids where `word1` is followed by `prefix`
at a distance of `proximity`.
Once we have done that, we insert (`word1`, `prefix`, `proximity`) -> `merged_docids`
into the database.

7. That's it! ... except...

## How is it created/updated (continued)

I lied a little bit about the input data. In reality, we get two sets of the
inputs described above, which come from different places:

* For the list of sorted prefixes, we have:
    1. `new_prefixes`, which are all the prefixes that were not present in the
    database before the insertion of the new documents

    2. `common_prefixes` which are the prefixes that are present both in the
    database and in the newly added documents

* For the list of word pairs and proximities, we have:
    1. `new_word_pairs`, which is the list of word pairs and their proximities
    present in the newly added documents

    2. `word_pairs_db`, which is the list of word pairs from the database.
    This list includes all elements in `new_word_pairs` since `new_word_pairs`
    was added to the database prior to calling the `WordPrefixPairProximityDocIds::execute`
    function.

To update the prefix database correctly, we call the algorithm described earlier first
on (`common_prefixes`, `new_word_pairs`) and then on (`new_prefixes`, `word_pairs_db`).
Thus:

1. For all the word pairs that were already present in the DB, we insert them
again with the `new_prefixes`. Calling the algorithm on them with the
`common_prefixes` would not result in any new data.

2. For all the new word pairs, we insert them twice: first with the `common_prefixes`,
and then, because they are part of `word_pairs_db`, with the `new_prefixes`.

Note, also, that since we read data from the database when iterating over
`word_pairs_db`, we cannot insert the computed word-prefix-pair-proximity-
docids from the batch directly into the database (we would have a concurrent
reader and writer). Therefore, when calling the algorithm on
(`new_prefixes`, `word_pairs_db`), we insert the computed
((`word`, `prefix`, `proximity`), `docids`) elements in an intermediary grenad
Writer instead of the DB. At the end of the outer loop, we finally read from
the grenad and insert its elements in the database.



*/
use std::borrow::Cow;
use std::collections::HashSet;
use std::io::BufReader;

use grenad::CompressionType;
use heed::types::ByteSlice;
use heed::BytesDecode;
use log::debug;

use crate::update::index_documents::{
    create_writer, merge_cbo_roaring_bitmaps, CursorClonableMmap,
};
use crate::{CboRoaringBitmapCodec, Index, Result, UncheckedStrStrU8Codec};

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

        // Make a prefix trie from the common prefixes that are shorter than self.max_prefix_length
        let prefixes = PrefixTrieNode::from_sorted_prefixes(
            common_prefix_fst_words
                .into_iter()
                .map(|s| s.into_iter())
                .flatten()
                .map(|s| s.as_str())
                .filter(|s| s.len() <= self.max_prefix_length),
        );

        // If the prefix trie is not empty, then we can iterate over all new
        // word pairs to look for new (word1, common_prefix, proximity) elements
        // to insert in the DB
        if !prefixes.is_empty() {
            let mut cursor = new_word_pair_proximity_docids.into_cursor()?;
            // This is the core of the algorithm
            execute_on_word_pairs_and_prefixes(
                // the first two arguments tell how to iterate over the new word pairs
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
                self.max_proximity,
                // and this argument tells what to do with each new key (word1, prefix, proximity) and value (roaring bitmap)
                |key, value| {
                    insert_into_database(
                        &mut self.wtxn,
                        *self.index.word_prefix_pair_proximity_docids.as_polymorph(),
                        key,
                        value,
                    )
                },
            )?;
        }

        // Now we do the same thing with the new prefixes and all word pairs in the DB

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

            // Since we read the DB, we can't write to it directly, so we add each new (word1, prefix, proximity)
            // element in an intermediary grenad
            let mut writer = create_writer(
                self.chunk_compression_type,
                self.chunk_compression_level,
                tempfile::tempfile()?,
            );

            execute_on_word_pairs_and_prefixes(
                &mut db_iter,
                |db_iter| db_iter.next().transpose().map_err(|e| e.into()),
                &prefixes,
                self.max_proximity,
                |key, value| writer.insert(key, value).map_err(|e| e.into()),
            )?;
            drop(db_iter);

            // and then we write the grenad into the DB
            // Since the grenad contains only new prefixes, we know in advance that none
            // of its elements already exist in the DB, thus there is no need to specify
            // how to merge conflicting elements
            write_into_lmdb_database_without_merging(
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
}

/// This is the core of the algorithm to initialise the Word Prefix Pair Proximity Docids database.
///
/// Its main arguments are:
/// 1. a sorted iterator over ((word1, word2, proximity), docids) elements
/// 2. a prefix trie
/// 3. a closure to describe how to handle the new computed (word1, prefix, proximity) elements
///
/// For more information about what this function does, read the module documentation.
fn execute_on_word_pairs_and_prefixes<I>(
    iter: &mut I,
    mut next_word_pair_proximity: impl for<'a> FnMut(
        &'a mut I,
    ) -> Result<
        Option<((&'a [u8], &'a [u8], u8), &'a [u8])>,
    >,
    prefixes: &PrefixTrieNode,
    max_proximity: u8,
    mut insert: impl for<'a> FnMut(&'a [u8], &'a [u8]) -> Result<()>,
) -> Result<()> {
    let mut batch = PrefixAndProximityBatch::default();
    let mut prev_word2_start = 0;

    // Optimisation: the index at the root of the prefix trie where to search for
    let mut prefix_search_start = PrefixTrieNodeSearchStart(0);

    // Optimisation: true if there are no potential prefixes for the current word2 based on its first letter
    let mut empty_prefixes = false;

    let mut prefix_buffer = Vec::with_capacity(8);
    let mut merge_buffer = Vec::with_capacity(65_536);

    while let Some(((word1, word2, proximity), data)) = next_word_pair_proximity(iter)? {
        // skip this iteration if the proximity is over the threshold
        if proximity > max_proximity {
            continue;
        };
        let word2_start_different_than_prev = word2[0] != prev_word2_start;
        // if there were no potential prefixes for the previous word2 based on its first letter,
        // and if the current word2 starts with the same letter, then there is also no potential
        // prefixes for the current word2, and we can skip to the next iteration
        if empty_prefixes && !word2_start_different_than_prev {
            continue;
        }

        // if word1 is different than the previous word1 OR if the start of word2 is different
        // than the previous start of word2, then we'll need to flush the batch
        let word1_different_than_prev = word1 != batch.word1;
        if word1_different_than_prev || word2_start_different_than_prev {
            batch.flush(&mut merge_buffer, &mut insert)?;
            // don't forget to reset the value of batch.word1 and prev_word2_start
            if word1_different_than_prev {
                prefix_search_start.0 = 0;
                batch.word1.clear();
                batch.word1.extend_from_slice(word1);
            }
            if word2_start_different_than_prev {
                // word2_start_different_than_prev == true
                prev_word2_start = word2[0];
            }
            // Optimisation: find the search start in the prefix trie to iterate over the prefixes of word2
            empty_prefixes = !prefixes.set_search_start(word2, &mut prefix_search_start);
        }

        if !empty_prefixes {
            // All conditions are satisfied, we can now insert each new prefix of word2 into the batch
            prefixes.for_each_prefix_of(
                word2,
                &mut prefix_buffer,
                &prefix_search_start,
                |prefix_buffer| {
                    let prefix_len = prefix_buffer.len();
                    prefix_buffer.push(0);
                    prefix_buffer.push(proximity);
                    batch.insert(&prefix_buffer, data.to_vec());
                    prefix_buffer.truncate(prefix_len);
                },
            );
            prefix_buffer.clear();
        }
    }
    batch.flush(&mut merge_buffer, &mut insert)?;
    Ok(())
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
    /// Insert the new key and value into the batch
    fn insert(&mut self, new_key: &[u8], new_value: Vec<u8>) {
        match self.batch.binary_search_by_key(&new_key, |(k, _)| k.as_slice()) {
            Ok(position) => {
                self.batch[position].1.push(Cow::Owned(new_value));
            }
            Err(position) => {
                self.batch.insert(position, (new_key.to_vec(), vec![Cow::Owned(new_value)]));
            }
        }
    }

    /// Empties the batch, calling `insert` on each element.
    ///
    /// The key given to `insert` is `(word1, prefix, proximity)` and the value is the associated merged roaring bitmap.
    fn flush(
        &mut self,
        merge_buffer: &mut Vec<u8>,
        insert: &mut impl for<'buffer> FnMut(&'buffer [u8], &'buffer [u8]) -> Result<()>,
    ) -> Result<()> {
        let PrefixAndProximityBatch { word1, batch } = self;
        if batch.is_empty() {
            return Ok(());
        }
        merge_buffer.clear();

        let mut buffer = Vec::with_capacity(word1.len() + 1 + 6 + 1);
        buffer.extend_from_slice(word1);
        buffer.push(0);

        for (key, mergeable_data) in batch.drain(..) {
            buffer.truncate(word1.len() + 1);
            buffer.extend_from_slice(key.as_slice());

            let data = if mergeable_data.len() > 1 {
                CboRoaringBitmapCodec::merge_into(&mergeable_data, merge_buffer)?;
                merge_buffer.as_slice()
            } else {
                &mergeable_data[0]
            };
            insert(buffer.as_slice(), data)?;
            merge_buffer.clear();
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
            // safety: we use the new_key, not the one from the database iterator, to avoid undefined behaviour
            unsafe { iter.put_current(new_key, &val)? };
        }
        _ => {
            drop(iter);
            database.put::<_, ByteSlice, ByteSlice>(wtxn, new_key, new_value)?;
        }
    }
    Ok(())
}

// This is adapted from `sorter_into_lmdb_database` and `write_into_lmdb_database`,
// but it uses `append` if the database is empty, and it assumes that the values in the
// writer don't conflict with values in the database.
pub fn write_into_lmdb_database_without_merging(
    wtxn: &mut heed::RwTxn,
    database: heed::PolyDatabase,
    writer: grenad::Writer<std::fs::File>,
) -> Result<()> {
    let file = writer.into_inner()?;
    let reader = grenad::Reader::new(BufReader::new(file))?;
    if database.is_empty(wtxn)? {
        let mut out_iter = database.iter_mut::<_, ByteSlice, ByteSlice>(wtxn)?;
        let mut cursor = reader.into_cursor()?;
        while let Some((k, v)) = cursor.move_on_next()? {
            // safety: the key comes from the grenad reader, not the database
            unsafe { out_iter.append(k, v)? };
        }
    } else {
        let mut cursor = reader.into_cursor()?;
        while let Some((k, v)) = cursor.move_on_next()? {
            database.put::<_, ByteSlice, ByteSlice>(wtxn, k, v)?;
        }
    }
    Ok(())
}

/** A prefix trie. Used to iterate quickly over the prefixes of a word that are
within a set.

## Structure
The trie is made of nodes composed of:
1. a byte character (e.g. 'a')
2. whether the node is an end node or not
3. a list of children nodes, sorted by their byte character

For example, the trie that stores the strings `[ac, ae, ar, ch, cei, cel, ch, r, rel, ri]`
is drawn below. Nodes with a double border are "end nodes".

┌──────────────────────┐ ┌──────────────────────┐ ╔══════════════════════╗
│          a           │ │          c           │ ║          r           ║
└──────────────────────┘ └──────────────────────┘ ╚══════════════════════╝
╔══════╗╔══════╗╔══════╗ ┌─────────┐  ╔═════════╗ ┌─────────┐ ╔══════════╗
║  c   ║║  e   ║║  r   ║ │    e    │  ║    h    ║ │    e    │ ║    i     ║
╚══════╝╚══════╝╚══════╝ └─────────┘  ╚═════════╝ └─────────┘ ╚══════════╝
                         ╔═══╗ ╔═══╗                 ╔═══╗
                         ║ i ║ ║ l ║                 ║ l ║
                         ╚═══╝ ╚═══╝                 ╚═══╝
*/
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
        } else {
            match self.children[search_start.0..].binary_search_by_key(&byte, |x| x.1) {
                Ok(position) => {
                    search_start.0 += position;
                    true
                }
                Err(_) => {
                    search_start.0 = 0;
                    false
                }
            }
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

    /// Call the given closure on each prefix of the word contained in the prefix trie.
    ///
    /// The search starts from the given `search_start`.
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
}
#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use roaring::RoaringBitmap;

    use super::*;
    use crate::documents::{DocumentsBatchBuilder, DocumentsBatchReader};
    use crate::index::tests::TempIndex;
    use crate::{db_snap, CboRoaringBitmapCodec, StrStrU8Codec};

    fn documents_with_enough_different_words_for_prefixes(prefixes: &[&str]) -> Vec<crate::Object> {
        let mut documents = Vec::new();
        for prefix in prefixes {
            for i in 0..50 {
                documents.push(
                    serde_json::json!({
                        "text": format!("{prefix}{i:x}"),
                    })
                    .as_object()
                    .unwrap()
                    .clone(),
                )
            }
        }
        documents
    }

    #[test]
    fn test_update() {
        let mut index = TempIndex::new();
        index.index_documents_config.words_prefix_threshold = Some(50);
        index.index_documents_config.autogenerate_docids = true;

        index
            .update_settings(|settings| {
                settings.set_searchable_fields(vec!["text".to_owned()]);
            })
            .unwrap();

        let batch_reader_from_documents = |documents| {
            let mut builder = DocumentsBatchBuilder::new(Vec::new());
            for object in documents {
                builder.append_json_object(&object).unwrap();
            }
            DocumentsBatchReader::from_reader(Cursor::new(builder.into_inner().unwrap())).unwrap()
        };

        let mut documents = documents_with_enough_different_words_for_prefixes(&["a", "be"]);
        // now we add some documents where the text should populate the word_prefix_pair_proximity_docids database
        documents.push(
            serde_json::json!({
                "text": "At an amazing and beautiful house"
            })
            .as_object()
            .unwrap()
            .clone(),
        );
        documents.push(
            serde_json::json!({
                "text": "The bell rings at 5 am"
            })
            .as_object()
            .unwrap()
            .clone(),
        );

        let documents = batch_reader_from_documents(documents);
        index.add_documents(documents).unwrap();

        db_snap!(index, word_prefix_pair_proximity_docids, "initial");

        let mut documents = documents_with_enough_different_words_for_prefixes(&["am", "an"]);
        documents.push(
            serde_json::json!({
                "text": "At an extraordinary house"
            })
            .as_object()
            .unwrap()
            .clone(),
        );
        let documents = batch_reader_from_documents(documents);
        index.add_documents(documents).unwrap();

        db_snap!(index, word_prefix_pair_proximity_docids, "update");
    }

    fn check_prefixes(
        trie: &PrefixTrieNode,
        search_start: &PrefixTrieNodeSearchStart,
        word: &str,
        expected_prefixes: &[&str],
    ) {
        let mut actual_prefixes = vec![];
        trie.for_each_prefix_of(word.as_bytes(), &mut Vec::new(), &search_start, |x| {
            let s = String::from_utf8(x.to_owned()).unwrap();
            actual_prefixes.push(s);
        });
        assert_eq!(actual_prefixes, expected_prefixes);
    }

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

        let mut search_start = PrefixTrieNodeSearchStart(0);

        let is_empty = !trie.set_search_start("affair".as_bytes(), &mut search_start);
        assert!(!is_empty);
        assert_eq!(search_start.0, 2);

        check_prefixes(&trie, &search_start, "affair", &["a"]);
        check_prefixes(&trie, &search_start, "shampoo", &["s", "sh", "sha"]);

        let is_empty = !trie.set_search_start("unique".as_bytes(), &mut search_start);
        assert!(!is_empty);
        assert_eq!(trie.children[search_start.0].1, b'u');

        check_prefixes(&trie, &search_start, "unique", &["u", "un"]);

        // NOTE: this should fail, because the search start is already beyong 'a'
        let is_empty = trie.set_search_start("abba".as_bytes(), &mut search_start);
        assert!(!is_empty);
        // search start is reset
        assert_eq!(search_start.0, 0);

        let trie = PrefixTrieNode::from_sorted_prefixes(IntoIterator::into_iter([
            "arb", "arbre", "cat", "catto",
        ]));
        check_prefixes(&trie, &search_start, "arbres", &["arb", "arbre"]);
        check_prefixes(&trie, &search_start, "cattos", &["cat", "catto"]);
    }

    #[test]
    fn test_execute_on_word_pairs_and_prefixes() {
        let prefixes = PrefixTrieNode::from_sorted_prefixes(IntoIterator::into_iter([
            "arb", "arbre", "cat", "catto",
        ]));

        let mut serialised_bitmap123 = vec![];
        let mut bitmap123 = RoaringBitmap::new();
        bitmap123.insert(1);
        bitmap123.insert(2);
        bitmap123.insert(3);
        CboRoaringBitmapCodec::serialize_into(&bitmap123, &mut serialised_bitmap123);

        let mut serialised_bitmap456 = vec![];
        let mut bitmap456 = RoaringBitmap::new();
        bitmap456.insert(4);
        bitmap456.insert(5);
        bitmap456.insert(6);
        CboRoaringBitmapCodec::serialize_into(&bitmap456, &mut serialised_bitmap456);

        let mut serialised_bitmap789 = vec![];
        let mut bitmap789 = RoaringBitmap::new();
        bitmap789.insert(7);
        bitmap789.insert(8);
        bitmap789.insert(9);
        CboRoaringBitmapCodec::serialize_into(&bitmap789, &mut serialised_bitmap789);

        let mut serialised_bitmap_ranges = vec![];
        let mut bitmap_ranges = RoaringBitmap::new();
        bitmap_ranges.insert_range(63_000..65_000);
        bitmap_ranges.insert_range(123_000..128_000);
        CboRoaringBitmapCodec::serialize_into(&bitmap_ranges, &mut serialised_bitmap_ranges);

        let word_pairs = [
            // 1, 3:  (healthy arb 2) and (healthy arbre 2) with (bitmap123 | bitmap456)
            (("healthy", "arbre", 2), &serialised_bitmap123),
            //          not inserted because 3 > max_proximity
            (("healthy", "arbre", 3), &serialised_bitmap456),
            // 0, 2:  (healthy arb 1) and (healthy arbre 1) with (bitmap123)
            (("healthy", "arbres", 1), &serialised_bitmap123),
            // 1, 3:
            (("healthy", "arbres", 2), &serialised_bitmap456),
            //          not be inserted because 3 > max_proximity
            (("healthy", "arbres", 3), &serialised_bitmap789),
            //          not inserted because no prefixes for boat
            (("healthy", "boat", 1), &serialised_bitmap123),
            //          not inserted because no prefixes for ca
            (("healthy", "ca", 1), &serialised_bitmap123),
            // 4: (healthy cat 1) with (bitmap456 + bitmap123)
            (("healthy", "cats", 1), &serialised_bitmap456),
            // 5: (healthy cat 2) with (bitmap789 + bitmap_ranges)
            (("healthy", "cats", 2), &serialised_bitmap789),
            // 4 + 6: (healthy catto 1) with (bitmap123)
            (("healthy", "cattos", 1), &serialised_bitmap123),
            // 5 + 7: (healthy catto 2) with (bitmap_ranges)
            (("healthy", "cattos", 2), &serialised_bitmap_ranges),
            // 8: (jittery cat 1) with (bitmap123 | bitmap456 | bitmap789 | bitmap_ranges)
            (("jittery", "cat", 1), &serialised_bitmap123),
            // 8:
            (("jittery", "cata", 1), &serialised_bitmap456),
            // 8:
            (("jittery", "catb", 1), &serialised_bitmap789),
            // 8:
            (("jittery", "catc", 1), &serialised_bitmap_ranges),
        ];

        let expected_result = [
            // first batch:
            (("healthy", "arb", 1), bitmap123.clone()),
            (("healthy", "arb", 2), &bitmap123 | &bitmap456),
            (("healthy", "arbre", 1), bitmap123.clone()),
            (("healthy", "arbre", 2), &bitmap123 | &bitmap456),
            // second batch:
            (("healthy", "cat", 1), &bitmap456 | &bitmap123),
            (("healthy", "cat", 2), &bitmap789 | &bitmap_ranges),
            (("healthy", "catto", 1), bitmap123.clone()),
            (("healthy", "catto", 2), bitmap_ranges.clone()),
            // third batch
            (("jittery", "cat", 1), (&bitmap123 | &bitmap456 | &bitmap789 | &bitmap_ranges)),
        ];

        let mut result = vec![];

        let mut iter =
            IntoIterator::into_iter(word_pairs).map(|((word1, word2, proximity), data)| {
                ((word1.as_bytes(), word2.as_bytes(), proximity), data.as_slice())
            });
        execute_on_word_pairs_and_prefixes(
            &mut iter,
            |iter| Ok(iter.next()),
            &prefixes,
            2,
            |k, v| {
                let (word1, prefix, proximity) = StrStrU8Codec::bytes_decode(k).unwrap();
                let bitmap = CboRoaringBitmapCodec::bytes_decode(v).unwrap();
                result.push(((word1.to_owned(), prefix.to_owned(), proximity.to_owned()), bitmap));
                Ok(())
            },
        )
        .unwrap();

        for (x, y) in result.into_iter().zip(IntoIterator::into_iter(expected_result)) {
            let ((actual_word1, actual_prefix, actual_proximity), actual_bitmap) = x;
            let ((expected_word1, expected_prefix, expected_proximity), expected_bitmap) = y;

            assert_eq!(actual_word1, expected_word1);
            assert_eq!(actual_prefix, expected_prefix);
            assert_eq!(actual_proximity, expected_proximity);
            assert_eq!(actual_bitmap, expected_bitmap);
        }
    }
}
