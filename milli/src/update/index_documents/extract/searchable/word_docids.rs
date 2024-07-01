use std::collections::hash_map::Entry::{Occupied, Vacant};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fs::File;
use std::hash::Hash;
use std::io::BufReader;
use std::mem::size_of;

use roaring::RoaringBitmap;

use crate::update::del_add::KvWriterDelAdd;
use crate::update::index_documents::extract::searchable::DelAdd;
use crate::update::index_documents::{create_writer, writer_into_reader, GrenadParameters};
use crate::update::settings::InnerIndexSettingsDiff;
use crate::{CboRoaringBitmapCodec, DocumentId, FieldId, Result};

pub struct WordDocidsExtractor<'a> {
    word_fid_docids: RevertedIndex<(u32, FieldId)>,
    settings_diff: &'a InnerIndexSettingsDiff,
}

impl<'a> WordDocidsExtractor<'a> {
    pub fn new(settings_diff: &'a InnerIndexSettingsDiff) -> Self {
        Self { word_fid_docids: RevertedIndex::new(), settings_diff }
    }
    pub fn insert(&mut self, wordid: u32, fieldid: FieldId, docid: DocumentId, del_add: DelAdd) {
        self.word_fid_docids.insert((wordid, fieldid), docid, del_add);
    }

    pub fn rough_size_estimate(&self) -> usize {
        self.word_fid_docids.rough_size_estimate()
    }

    pub fn dump(
        &mut self,
        word_map: &BTreeMap<String, u32>,
        fields: &BTreeSet<FieldId>,
        indexer: GrenadParameters,
    ) -> Result<WordDocidsDump> {
        let mut word_fid_docids_writer = create_writer(
            indexer.chunk_compression_type,
            indexer.chunk_compression_level,
            tempfile::tempfile()?,
        );

        let mut word_docids_writer = create_writer(
            indexer.chunk_compression_type,
            indexer.chunk_compression_level,
            tempfile::tempfile()?,
        );

        let mut exact_word_docids_writer = create_writer(
            indexer.chunk_compression_type,
            indexer.chunk_compression_level,
            tempfile::tempfile()?,
        );

        let mut exact_word_deletion = RoaringBitmap::new();
        let mut exact_word_addition = RoaringBitmap::new();
        let mut word_deletion = RoaringBitmap::new();
        let mut word_addition = RoaringBitmap::new();
        let mut key_buffer = Vec::new();
        let mut bitmap_buffer = Vec::new();
        let mut obkv_buffer = Vec::new();
        for (word, wid) in word_map {
            exact_word_deletion.clear();
            exact_word_addition.clear();
            word_deletion.clear();
            word_addition.clear();
            for fid in fields {
                if let Some((deletion, addition)) = self.word_fid_docids.inner.get(&(*wid, *fid)) {
                    if self.settings_diff.old.exact_attributes.contains(&fid) {
                        exact_word_deletion |= deletion;
                    } else {
                        word_deletion |= deletion;
                    }

                    if self.settings_diff.new.exact_attributes.contains(&fid) {
                        exact_word_addition |= addition;
                    } else {
                        word_addition |= addition;
                    }

                    if deletion != addition {
                        key_buffer.clear();
                        key_buffer.extend_from_slice(word.as_bytes());
                        key_buffer.push(0);
                        key_buffer.extend_from_slice(&fid.to_be_bytes());
                        let value = bitmaps_into_deladd_obkv(
                            deletion,
                            addition,
                            &mut obkv_buffer,
                            &mut bitmap_buffer,
                        )?;
                        word_fid_docids_writer.insert(&key_buffer, value)?;
                    }
                }
            }

            key_buffer.clear();
            key_buffer.extend_from_slice(word.as_bytes());
            if exact_word_deletion != exact_word_addition {
                let value = bitmaps_into_deladd_obkv(
                    &exact_word_deletion,
                    &exact_word_addition,
                    &mut obkv_buffer,
                    &mut bitmap_buffer,
                )?;
                exact_word_docids_writer.insert(&key_buffer, value)?;
            }

            if word_deletion != word_addition {
                let value = bitmaps_into_deladd_obkv(
                    &word_deletion,
                    &word_addition,
                    &mut obkv_buffer,
                    &mut bitmap_buffer,
                )?;
                word_docids_writer.insert(&key_buffer, value)?;
            }
        }

        self.word_fid_docids.clear();

        Ok(WordDocidsDump {
            word_fid_docids: writer_into_reader(word_fid_docids_writer)?,
            word_docids: writer_into_reader(word_docids_writer)?,
            exact_word_docids: writer_into_reader(exact_word_docids_writer)?,
        })
    }
}

fn bitmaps_into_deladd_obkv<'a>(
    deletion: &RoaringBitmap,
    addition: &RoaringBitmap,
    obkv_buffer: &'a mut Vec<u8>,
    bitmap_buffer: &mut Vec<u8>,
) -> Result<&'a mut Vec<u8>> {
    obkv_buffer.clear();
    let mut value_writer = KvWriterDelAdd::new(obkv_buffer);
    if !deletion.is_empty() {
        bitmap_buffer.clear();
        CboRoaringBitmapCodec::serialize_into(deletion, bitmap_buffer);
        value_writer.insert(DelAdd::Deletion, &*bitmap_buffer)?;
    }
    if !addition.is_empty() {
        bitmap_buffer.clear();
        CboRoaringBitmapCodec::serialize_into(addition, bitmap_buffer);
        value_writer.insert(DelAdd::Addition, &*bitmap_buffer)?;
    }
    Ok(value_writer.into_inner()?)
}

#[derive(Debug)]
struct RevertedIndex<K> {
    inner: HashMap<K, (RoaringBitmap, RoaringBitmap)>,
    max_value_size: usize,
}

impl<K: PartialEq + Eq + Hash> RevertedIndex<K> {
    pub fn insert(&mut self, key: K, docid: DocumentId, del_add: DelAdd) {
        let size = match self.inner.entry(key) {
            Occupied(mut entry) => {
                let (ref mut del, ref mut add) = entry.get_mut();
                match del_add {
                    DelAdd::Deletion => del.insert(docid),
                    DelAdd::Addition => add.insert(docid),
                };
                del.serialized_size() + add.serialized_size()
            }
            Vacant(entry) => {
                let mut bitmap = RoaringBitmap::new();
                bitmap.insert(docid);
                let size = bitmap.serialized_size();
                match del_add {
                    DelAdd::Deletion => entry.insert((bitmap, RoaringBitmap::new())),
                    DelAdd::Addition => entry.insert((RoaringBitmap::new(), bitmap)),
                };
                size * 2
            }
        };

        self.max_value_size = self.max_value_size.max(size);
    }

    pub fn new() -> Self {
        Self { inner: HashMap::new(), max_value_size: 0 }
    }

    pub fn rough_size_estimate(&self) -> usize {
        self.inner.len() * size_of::<K>() + self.inner.len() * self.max_value_size
    }

    fn clear(&mut self) {
        self.max_value_size = 0;
        self.inner.clear();
    }
}

pub struct WordDocidsDump {
    pub word_fid_docids: grenad::Reader<BufReader<File>>,
    pub word_docids: grenad::Reader<BufReader<File>>,
    pub exact_word_docids: grenad::Reader<BufReader<File>>,
}
