use std::borrow::Cow;
use std::convert::TryFrom;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};

use anyhow::Context;
use crate::{FieldsIdsMap, AvailableDocumentsIds};
use fst::{IntoStreamer, Streamer};
use grenad::{Writer, Sorter, CompressionType};
use roaring::RoaringBitmap;

pub struct TransformOutput {
    pub fields_ids_map: FieldsIdsMap,
    pub users_ids_documents_ids: fst::Map<Vec<u8>>,
    pub new_documents_ids: RoaringBitmap,
    pub replaced_documents_ids: RoaringBitmap,
    pub documents_count: usize,
    pub documents_file: File,
}

pub struct Transform<A> {
    pub fields_ids_map: FieldsIdsMap,
    pub available_documents_ids: AvailableDocumentsIds,
    pub users_ids_documents_ids: fst::Map<A>,
    pub compression_type: CompressionType,
    pub compression_level: u32,
    pub enable_file_fuzing: bool,
}

impl<A: AsRef<[u8]>> Transform<A> {
    /// Extract the users ids, deduplicate and compute the new internal documents ids
    /// and fields ids, writing all the documents under their internal ids into a final file.
    ///
    /// Outputs the new `FieldsIdsMap`, the new `UsersIdsDocumentsIds` map, the new documents ids,
    /// the replaced documents ids, the number of documents in this update and the file
    /// containing all those documents.
    pub fn from_csv<R: Read>(mut self, reader: R) -> anyhow::Result<TransformOutput> {
        let mut csv = csv::Reader::from_reader(reader);
        let headers = csv.headers()?.clone();
        let user_id_pos = headers.iter().position(|h| h == "id").context(r#"missing "id" header"#)?;

        // Generate the new fields ids based on the current fields ids and this CSV headers.
        let mut fields_ids = Vec::new();
        for header in headers.iter() {
            let id = self.fields_ids_map.insert(header)
                .context("impossible to generate a field id (limit reached)")?;
            fields_ids.push(id);
        }

        /// The last value associated with an id is kept.
        fn merge_last_win(_key: &[u8], vals: &[Cow<[u8]>]) -> anyhow::Result<Vec<u8>> {
            vals.last().context("no last value").map(|last| last.clone().into_owned())
        }

        // We initialize the sorter with the user indexing settings.
        let mut sorter_builder = Sorter::builder(merge_last_win);
        sorter_builder.chunk_compression_type(self.compression_type);
        sorter_builder.chunk_compression_level(self.compression_level);
        if self.enable_file_fuzing {
            sorter_builder.enable_fusing();
        }

        // We write into the sorter to merge and deduplicate the documents
        // based on the users ids.
        let mut sorter = sorter_builder.build();
        let mut json_buffer = Vec::new();
        let mut obkv_buffer = Vec::new();
        let mut record = csv::StringRecord::new();
        while csv.read_record(&mut record)? {

            obkv_buffer.clear();
            let mut writer = obkv::KvWriter::new(&mut obkv_buffer);

            // We retrieve the field id based on the CSV header position
            // and zip it with the record value.
            for (key, field) in fields_ids.iter().copied().zip(&record) {
                // We serialize the attribute values as JSON strings.
                json_buffer.clear();
                serde_json::to_writer(&mut json_buffer, &field)?;
                writer.insert(key, &json_buffer)?;
            }

            // We extract the user id and use it as the key for this document.
            // TODO we must validate the user id (i.e. [a-zA-Z0-9\-_]).
            let user_id = &record[user_id_pos];
            sorter.insert(user_id, &obkv_buffer)?;
        }

        // Once we have sort and deduplicated the documents we write them into a final file.
        let file = tempfile::tempfile()?;
        let mut writer_builder = Writer::builder();
        writer_builder.compression_type(self.compression_type);
        writer_builder.compression_level(self.compression_level);

        let mut writer = writer_builder.build(file)?;
        let mut new_users_ids_documents_ids_builder = fst::MapBuilder::memory();
        let mut replaced_documents_ids = RoaringBitmap::new();
        let mut new_documents_ids = RoaringBitmap::new();

        // While we write into final file we get or generate the internal documents ids.
        let mut documents_count = 0;
        let mut iter = sorter.into_iter()?;
        while let Some((user_id, obkv)) = iter.next()? {

            let docid = match self.users_ids_documents_ids.get(user_id) {
                Some(docid) => {
                    // If we find the user id in the current users ids documents ids map
                    // we use it and insert it in the list of replaced documents.
                    let docid = u32::try_from(docid).expect("valid document id");
                    replaced_documents_ids.insert(docid);
                    docid
                },
                None => {
                    // If this user id is new we add it to the users ids documents ids map
                    // for new ids and into the list of new documents.
                    let new_docid = self.available_documents_ids.next()
                        .context("no more available documents ids")?;
                    new_users_ids_documents_ids_builder.insert(user_id, new_docid as u64)?;
                    new_documents_ids.insert(new_docid);
                    new_docid
                },
            };

            // We insert the document under the documents ids map into the final file.
            writer.insert(docid.to_be_bytes(), obkv)?;
            documents_count += 1;
        }

        // Once we have written all the documents into the final file, we extract it
        // from the writer and reset the seek to be able to read it again.
        let mut documents_file = writer.into_inner()?;
        documents_file.seek(SeekFrom::Start(0))?;

        // We create the union between the existing users ids documents ids with the new ones.
        let new_users_ids_documents_ids = new_users_ids_documents_ids_builder.into_map();
        let union_ = fst::map::OpBuilder::new()
            .add(&self.users_ids_documents_ids)
            .add(&new_users_ids_documents_ids)
            .r#union();

        // We stream and merge the new users ids documents ids map with the existing one.
        let mut users_ids_documents_ids_builder = fst::MapBuilder::memory();
        let mut iter = union_.into_stream();
        while let Some((user_id, vals)) = iter.next() {
            assert_eq!(vals.len(), 1, "there must be exactly one document id");
            users_ids_documents_ids_builder.insert(user_id, vals[0].value)?;
        }

        Ok(TransformOutput {
            fields_ids_map: self.fields_ids_map,
            users_ids_documents_ids: users_ids_documents_ids_builder.into_map(),
            new_documents_ids,
            replaced_documents_ids,
            documents_count,
            documents_file,
        })
    }
}
