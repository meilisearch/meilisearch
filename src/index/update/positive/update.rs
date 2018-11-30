use std::collections::BTreeMap;
use std::path::PathBuf;
use std::error::Error;

use ::rocksdb::rocksdb_options;

use crate::index::update::positive::unordered_builder::UnorderedPositiveBlobBuilder;
use crate::index::schema::{SchemaProps, Schema, SchemaAttr};
use crate::index::update::{Update, raw_document_key_attr};
use crate::blob::positive::PositiveBlob;
use crate::tokenizer::TokenizerBuilder;
use crate::{DocumentId, DocIndex};
use crate::index::DATA_INDEX;
use crate::blob::Blob;

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
    new_states: BTreeMap<(DocumentId, SchemaAttr), NewState>,
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
    pub fn update_field(&mut self, id: DocumentId, field: SchemaAttr, value: String) {
        let state = NewState::Updated { value, props: self.schema.props(field) };
        self.new_states.insert((id, field), state);
    }

    pub fn remove_field(&mut self, id: DocumentId, field: SchemaAttr) {
        self.new_states.insert((id, field), NewState::Removed);
    }
}

impl<B> PositiveUpdateBuilder<B>
where B: TokenizerBuilder
{
    pub fn build(self) -> Result<Update, Box<Error>> {
        let env_options = rocksdb_options::EnvOptions::new();
        let column_family_options = rocksdb_options::ColumnFamilyOptions::new();
        let mut file_writer = rocksdb::SstFileWriter::new(env_options, column_family_options);
        file_writer.open(&self.path.to_string_lossy())?;

        let mut builder = UnorderedPositiveBlobBuilder::memory();
        for ((document_id, attr), state) in &self.new_states {
            let value = match state {
                NewState::Updated { value, props } if props.is_indexed() => value,
                _ => continue,
            };

            for (index, word) in self.tokenizer_builder.build(value) {
                let doc_index = DocIndex {
                    document_id: *document_id,
                    attribute: attr.as_u32() as u8,
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
        let positive_blob = PositiveBlob::from_bytes(blob_fst_map, blob_doc_idx)?;
        let blob = Blob::Positive(positive_blob);

        // write the data-index aka positive blob
        let bytes = bincode::serialize(&blob)?;
        file_writer.merge(DATA_INDEX, &bytes)?;

        // write all the documents fields updates
        for ((id, attr), state) in self.new_states {
            let key = raw_document_key_attr(id, attr);
            match state {
                NewState::Updated { value, props } => if props.is_stored() {
                    file_writer.put(&key, value.as_bytes())?
                },
                NewState::Removed => file_writer.delete(&key)?,
            }
        }

        file_writer.finish()?;
        Update::open(self.path)
    }
}
