use std::collections::BTreeMap;
use std::path::PathBuf;
use std::error::Error;
use std::fmt::Write;

use ::rocksdb::rocksdb_options;

use crate::index::schema::{SchemaProps, Schema, SchemaField};
use crate::index::update::{FIELD_BLOBS_ORDER, Update};
use crate::tokenizer::TokenizerBuilder;
use crate::index::blob_name::BlobName;
use crate::blob::PositiveBlobBuilder;
use crate::{DocIndex, DocumentId};

pub enum NewState {
    Updated {
        value: String,
        props: SchemaProps,
    },
    Removed,
}

pub struct PositiveUpdateBuilder<B> {
    path: PathBuf,
    schema: Schema,
    tokenizer_builder: B,
    new_states: BTreeMap<(DocumentId, SchemaField), NewState>,
}

impl<B> PositiveUpdateBuilder<B> {
    pub fn new<P: Into<PathBuf>>(path: P, schema: Schema, tokenizer_builder: B) -> PositiveUpdateBuilder<B> {
        PositiveUpdateBuilder {
            path: path.into(),
            schema: schema,
            tokenizer_builder: tokenizer_builder,
            new_states: BTreeMap::new(),
        }
    }

    // TODO value must be a field that can be indexed
    pub fn update_field(&mut self, id: DocumentId, field: SchemaField, value: String) {
        let state = NewState::Updated { value, props: self.schema.props(field) };
        self.new_states.insert((id, field), state);
    }

    pub fn remove_field(&mut self, id: DocumentId, field: SchemaField) {
        self.new_states.insert((id, field), NewState::Removed);
    }
}

impl<B> PositiveUpdateBuilder<B>
where B: TokenizerBuilder
{
    pub fn build(self) -> Result<Update, Box<Error>> {
        let blob_name = BlobName::new();

        let env_options = rocksdb_options::EnvOptions::new();
        let column_family_options = rocksdb_options::ColumnFamilyOptions::new();
        let mut file_writer = rocksdb::SstFileWriter::new(env_options, column_family_options);

        file_writer.open(&self.path.to_string_lossy())?;

        // TODO the blob-name must be written in bytes (16 bytes)
        //      along with the sign
        unimplemented!("write the blob sign and name");

        // write the blob name to be merged
        let blob_name = blob_name.to_string();
        file_writer.put(FIELD_BLOBS_ORDER.as_bytes(), blob_name.as_bytes())?;

        let mut builder = PositiveBlobBuilder::new(Vec::new(), Vec::new());
        for ((document_id, field), state) in &self.new_states {
            let value = match state {
                NewState::Updated { value, props } if props.is_indexed() => value,
                _ => continue,
            };

            for (index, word) in self.tokenizer_builder.build(value) {
                let doc_index = DocIndex {
                    document_id: *document_id,
                    attribute: field.as_u32() as u8,
                    attribute_index: index as u32,
                };
                // insert the exact representation
                let word_lower = word.to_lowercase();

                // and the unidecoded lowercased version
                let word_unidecoded = unidecode::unidecode(word).to_lowercase();
                if word_lower != word_unidecoded {
                    builder.insert(word_unidecoded, doc_index);
                }

                builder.insert(word_lower, doc_index);
            }
        }
        let (blob_fst_map, blob_doc_idx) = builder.into_inner()?;

        // write the fst
        let blob_key = format!("0b-{}-fst", blob_name);
        file_writer.put(blob_key.as_bytes(), &blob_fst_map)?;

        // write the doc-idx
        let blob_key = format!("0b-{}-doc-idx", blob_name);
        file_writer.put(blob_key.as_bytes(), &blob_doc_idx)?;

        // write all the documents fields updates
        let mut key = String::from("5d-");
        let prefix_len = key.len();

        for ((id, field), state) in self.new_states {
            key.truncate(prefix_len);
            write!(&mut key, "{}-{}", id, field)?;
            match state {
                NewState::Updated { value, props } => if props.is_stored() {
                    file_writer.put(key.as_bytes(), value.as_bytes())?
                },
                NewState::Removed => file_writer.delete(key.as_bytes())?,
            }
        }

        file_writer.finish()?;
        Update::open(self.path)
    }
}
