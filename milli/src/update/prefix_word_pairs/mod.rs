use std::borrow::Cow;
use std::collections::HashSet;
use std::io::BufReader;

use grenad::CompressionType;
use heed::types::ByteSlice;

use super::index_documents::{merge_cbo_roaring_bitmaps, CursorClonableMmap};
use crate::{Index, Result};

mod prefix_word;
mod word_prefix;

pub use prefix_word::index_prefix_word_database;
pub use word_prefix::index_word_prefix_database;

pub struct PrefixWordPairsProximityDocids<'t, 'u, 'i> {
    wtxn: &'t mut heed::RwTxn<'i, 'u>,
    index: &'i Index,
    max_proximity: u8,
    max_prefix_length: usize,
    chunk_compression_type: CompressionType,
    chunk_compression_level: Option<u32>,
}
impl<'t, 'u, 'i> PrefixWordPairsProximityDocids<'t, 'u, 'i> {
    pub fn new(
        wtxn: &'t mut heed::RwTxn<'i, 'u>,
        index: &'i Index,
        chunk_compression_type: CompressionType,
        chunk_compression_level: Option<u32>,
    ) -> Self {
        Self {
            wtxn,
            index,
            max_proximity: 4,
            max_prefix_length: 2,
            chunk_compression_type,
            chunk_compression_level,
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
        self,
        new_word_pair_proximity_docids: grenad::Reader<CursorClonableMmap>,
        new_prefix_fst_words: &'a [String],
        common_prefix_fst_words: &[&'a [String]],
        del_prefix_fst_words: &HashSet<Vec<u8>>,
    ) -> Result<()> {
        index_word_prefix_database(
            self.wtxn,
            self.index.word_pair_proximity_docids,
            self.index.word_prefix_pair_proximity_docids,
            self.max_proximity,
            self.max_prefix_length,
            new_word_pair_proximity_docids.clone(),
            new_prefix_fst_words,
            common_prefix_fst_words,
            del_prefix_fst_words,
            self.chunk_compression_type,
            self.chunk_compression_level,
        )?;

        index_prefix_word_database(
            self.wtxn,
            self.index.word_pair_proximity_docids,
            self.index.prefix_word_pair_proximity_docids,
            self.max_proximity,
            self.max_prefix_length,
            new_word_pair_proximity_docids,
            new_prefix_fst_words,
            common_prefix_fst_words,
            del_prefix_fst_words,
            self.chunk_compression_type,
            self.chunk_compression_level,
        )?;

        Ok(())
    }
}

// This is adapted from `sorter_into_lmdb_database`
pub fn insert_into_database(
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

#[cfg(test)]
mod tests {
    use std::io::Cursor;
    use std::iter::FromIterator;

    use roaring::RoaringBitmap;

    use crate::db_snap;
    use crate::documents::{DocumentsBatchBuilder, DocumentsBatchReader};
    use crate::index::tests::TempIndex;
    use crate::update::{DeleteDocuments, IndexDocumentsMethod};

    fn documents_with_enough_different_words_for_prefixes(
        prefixes: &[&str],
        start_id: usize,
    ) -> Vec<crate::Object> {
        let mut documents = Vec::new();
        let mut id = start_id;
        for prefix in prefixes {
            for i in 0..50 {
                documents.push(
                    serde_json::json!({
                        "id": id,
                        "text": format!("{prefix}{i:x}"),
                    })
                    .as_object()
                    .unwrap()
                    .clone(),
                );
                id += 1;
            }
        }
        documents
    }

    #[test]
    fn add_new_documents() {
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

        let mut documents = documents_with_enough_different_words_for_prefixes(&["a", "be"], 0);
        // now we add some documents where the text should populate the word_prefix_pair_proximity_docids database
        documents.push(
            serde_json::json!({
                "id": "9000",
                "text": "At an amazing and beautiful house"
            })
            .as_object()
            .unwrap()
            .clone(),
        );
        documents.push(
            serde_json::json!({
                "id": "9001",
                "text": "The bell rings at 5 am"
            })
            .as_object()
            .unwrap()
            .clone(),
        );

        let documents = batch_reader_from_documents(documents);
        index.add_documents(documents).unwrap();

        db_snap!(index, word_prefix_pair_proximity_docids, "initial");
        db_snap!(index, prefix_word_pair_proximity_docids, "initial");

        let mut documents = documents_with_enough_different_words_for_prefixes(&["am", "an"], 100);
        documents.push(
            serde_json::json!({
                "id": "9002",
                "text": "At an extraordinary house"
            })
            .as_object()
            .unwrap()
            .clone(),
        );
        let documents = batch_reader_from_documents(documents);
        index.add_documents(documents).unwrap();

        db_snap!(index, word_pair_proximity_docids, "update");
        db_snap!(index, word_prefix_pair_proximity_docids, "update");
        db_snap!(index, prefix_word_pair_proximity_docids, "update");
    }
    #[test]
    fn batch_bug_3043() {
        // https://github.com/meilisearch/meilisearch/issues/3043
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

        let mut documents = documents_with_enough_different_words_for_prefixes(&["y"], 0);
        // now we add some documents where the text should populate the word_prefix_pair_proximity_docids database
        documents.push(
            serde_json::json!({
                "text": "x y"
            })
            .as_object()
            .unwrap()
            .clone(),
        );
        documents.push(
            serde_json::json!({
                "text": "x a y"
            })
            .as_object()
            .unwrap()
            .clone(),
        );

        let documents = batch_reader_from_documents(documents);
        index.add_documents(documents).unwrap();

        db_snap!(index, word_pair_proximity_docids);
        db_snap!(index, word_prefix_pair_proximity_docids);
        db_snap!(index, prefix_word_pair_proximity_docids);
    }

    #[test]
    fn hard_delete_and_reupdate() {
        let mut index = TempIndex::new();
        index.index_documents_config.words_prefix_threshold = Some(50);

        index
            .update_settings(|settings| {
                settings.set_primary_key("id".to_owned());
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

        let mut documents = documents_with_enough_different_words_for_prefixes(&["a"], 0);
        // now we add some documents where the text should populate the word_prefix_pair_proximity_docids database
        documents.push(
            serde_json::json!({
                "id": 9000,
                "text": "At an amazing and beautiful house"
            })
            .as_object()
            .unwrap()
            .clone(),
        );
        documents.push(
            serde_json::json!({
                "id": 9001,
                "text": "The bell rings at 5 am"
            })
            .as_object()
            .unwrap()
            .clone(),
        );

        let documents = batch_reader_from_documents(documents);
        index.add_documents(documents).unwrap();

        db_snap!(index, documents_ids, "initial");
        db_snap!(index, word_docids, "initial");
        db_snap!(index, word_prefix_pair_proximity_docids, "initial");
        db_snap!(index, prefix_word_pair_proximity_docids, "initial");

        let mut wtxn = index.write_txn().unwrap();
        let mut delete = DeleteDocuments::new(&mut wtxn, &index).unwrap();
        delete.disable_soft_deletion(true);
        delete.delete_documents(&RoaringBitmap::from_iter([50]));
        delete.execute().unwrap();
        wtxn.commit().unwrap();

        db_snap!(index, documents_ids, "first_delete");
        db_snap!(index, word_docids, "first_delete");
        db_snap!(index, word_prefix_pair_proximity_docids, "first_delete");
        db_snap!(index, prefix_word_pair_proximity_docids, "first_delete");

        let mut wtxn = index.write_txn().unwrap();
        let mut delete = DeleteDocuments::new(&mut wtxn, &index).unwrap();
        delete.disable_soft_deletion(true);
        delete.delete_documents(&RoaringBitmap::from_iter(0..50));
        delete.execute().unwrap();
        wtxn.commit().unwrap();

        db_snap!(index, documents_ids, "second_delete");
        db_snap!(index, word_docids, "second_delete");
        db_snap!(index, word_prefix_pair_proximity_docids, "second_delete");
        db_snap!(index, prefix_word_pair_proximity_docids, "second_delete");

        let documents = documents_with_enough_different_words_for_prefixes(&["b"], 1000);
        // now we add some documents where the text should populate the word_prefix_pair_proximity_docids database

        index.add_documents(batch_reader_from_documents(documents)).unwrap();

        db_snap!(index, documents_ids, "reupdate");
        db_snap!(index, word_docids, "reupdate");
        db_snap!(index, word_prefix_pair_proximity_docids, "reupdate");
        db_snap!(index, prefix_word_pair_proximity_docids, "reupdate");
    }

    #[test]
    fn soft_delete_and_reupdate() {
        let mut index = TempIndex::new();
        index.index_documents_config.words_prefix_threshold = Some(50);

        index
            .update_settings(|settings| {
                settings.set_primary_key("id".to_owned());
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

        let mut documents = documents_with_enough_different_words_for_prefixes(&["a"], 0);
        // now we add some documents where the text should populate the word_prefix_pair_proximity_docids database
        documents.push(
            serde_json::json!({
                "id": 9000,
                "text": "At an amazing and beautiful house"
            })
            .as_object()
            .unwrap()
            .clone(),
        );
        documents.push(
            serde_json::json!({
                "id": 9001,
                "text": "The bell rings at 5 am"
            })
            .as_object()
            .unwrap()
            .clone(),
        );

        let documents = batch_reader_from_documents(documents);
        index.add_documents(documents).unwrap();

        db_snap!(index, documents_ids, "initial");
        db_snap!(index, word_docids, "initial");
        db_snap!(index, word_prefix_pair_proximity_docids, "initial");
        db_snap!(index, prefix_word_pair_proximity_docids, "initial");

        let mut wtxn = index.write_txn().unwrap();
        let mut delete = DeleteDocuments::new(&mut wtxn, &index).unwrap();
        delete.delete_documents(&RoaringBitmap::from_iter([50]));
        delete.execute().unwrap();
        wtxn.commit().unwrap();

        db_snap!(index, documents_ids, "first_delete");
        db_snap!(index, word_docids, "first_delete");
        db_snap!(index, word_prefix_pair_proximity_docids, "first_delete");
        db_snap!(index, prefix_word_pair_proximity_docids, "first_delete");

        let mut wtxn = index.write_txn().unwrap();
        let mut delete = DeleteDocuments::new(&mut wtxn, &index).unwrap();
        delete.delete_documents(&RoaringBitmap::from_iter(0..50));
        delete.execute().unwrap();
        wtxn.commit().unwrap();

        db_snap!(index, documents_ids, "second_delete");
        db_snap!(index, word_docids, "second_delete");
        db_snap!(index, word_prefix_pair_proximity_docids, "second_delete");
        db_snap!(index, prefix_word_pair_proximity_docids, "second_delete");

        let documents = documents_with_enough_different_words_for_prefixes(&["b"], 1000);
        // now we add some documents where the text should populate the word_prefix_pair_proximity_docids database

        index.add_documents(batch_reader_from_documents(documents)).unwrap();

        db_snap!(index, documents_ids, "reupdate");
        db_snap!(index, word_docids, "reupdate");
        db_snap!(index, word_prefix_pair_proximity_docids, "reupdate");
        db_snap!(index, prefix_word_pair_proximity_docids, "reupdate");
    }

    #[test]
    fn replace_soft_deletion() {
        let mut index = TempIndex::new();
        index.index_documents_config.words_prefix_threshold = Some(50);
        index.index_documents_config.update_method = IndexDocumentsMethod::ReplaceDocuments;

        index
            .update_settings(|settings| {
                settings.set_primary_key("id".to_owned());
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

        let mut documents = documents_with_enough_different_words_for_prefixes(&["a"], 0);
        // now we add some documents where the text should populate the word_prefix_pair_proximity_docids database
        documents.push(
            serde_json::json!({
                "id": 9000,
                "text": "At an amazing house"
            })
            .as_object()
            .unwrap()
            .clone(),
        );
        documents.push(
            serde_json::json!({
                "id": 9001,
                "text": "The bell rings"
            })
            .as_object()
            .unwrap()
            .clone(),
        );

        let documents = batch_reader_from_documents(documents);
        index.add_documents(documents).unwrap();

        db_snap!(index, documents_ids, "initial");
        db_snap!(index, word_docids, "initial");
        db_snap!(index, word_prefix_pair_proximity_docids, "initial");
        db_snap!(index, prefix_word_pair_proximity_docids, "initial");

        let documents = documents_with_enough_different_words_for_prefixes(&["b"], 0);
        index.add_documents(batch_reader_from_documents(documents)).unwrap();

        db_snap!(index, documents_ids, "replaced");
        db_snap!(index, word_docids, "replaced");
        db_snap!(index, word_prefix_pair_proximity_docids, "replaced");
        db_snap!(index, prefix_word_pair_proximity_docids, "replaced");
        db_snap!(index, soft_deleted_documents_ids, "replaced", @"[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, ]");
    }

    #[test]
    fn replace_hard_deletion() {
        let mut index = TempIndex::new();
        index.index_documents_config.words_prefix_threshold = Some(50);
        index.index_documents_config.disable_soft_deletion = true;
        index.index_documents_config.update_method = IndexDocumentsMethod::ReplaceDocuments;

        index
            .update_settings(|settings| {
                settings.set_primary_key("id".to_owned());
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

        let mut documents = documents_with_enough_different_words_for_prefixes(&["a"], 0);
        // now we add some documents where the text should populate the word_prefix_pair_proximity_docids database
        documents.push(
            serde_json::json!({
                "id": 9000,
                "text": "At an amazing house"
            })
            .as_object()
            .unwrap()
            .clone(),
        );
        documents.push(
            serde_json::json!({
                "id": 9001,
                "text": "The bell rings"
            })
            .as_object()
            .unwrap()
            .clone(),
        );

        let documents = batch_reader_from_documents(documents);
        index.add_documents(documents).unwrap();

        db_snap!(index, documents_ids, "initial");
        db_snap!(index, word_docids, "initial");
        db_snap!(index, word_prefix_pair_proximity_docids, "initial");
        db_snap!(index, prefix_word_pair_proximity_docids, "initial");

        let documents = documents_with_enough_different_words_for_prefixes(&["b"], 0);
        index.add_documents(batch_reader_from_documents(documents)).unwrap();

        db_snap!(index, documents_ids, "replaced");
        db_snap!(index, word_docids, "replaced");
        db_snap!(index, word_prefix_pair_proximity_docids, "replaced");
        db_snap!(index, prefix_word_pair_proximity_docids, "replaced");
        db_snap!(index, soft_deleted_documents_ids, "replaced", @"[]");
    }
}
