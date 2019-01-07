use std::path::PathBuf;
use std::error::Error;

use hashbrown::HashSet;
use serde::Serialize;

use crate::database::serde::serializer::Serializer;
use crate::database::serde::SerializerError;
use crate::tokenizer::TokenizerBuilder;
use crate::database::Schema;

use crate::DocumentId;
use super::{Update, RawUpdateBuilder};

pub struct UpdateBuilder {
    schema: Schema,
    raw_builder: RawUpdateBuilder,
}

impl UpdateBuilder {
    pub fn new(path: PathBuf, schema: Schema) -> UpdateBuilder {
        UpdateBuilder {
            schema: schema,
            raw_builder: RawUpdateBuilder::new(path),
        }
    }

    pub fn update_document<T, B>(
        &mut self,
        document: T,
        tokenizer_builder: &B,
        stop_words: &HashSet<String>,
    ) -> Result<DocumentId, SerializerError>
    where T: Serialize,
          B: TokenizerBuilder,
    {
        let document_id = self.schema.document_id(&document)?;
        let update = self.raw_builder.document_update(document_id);

        let serializer = Serializer {
            schema: &self.schema,
            document_id: document_id,
            tokenizer_builder: tokenizer_builder,
            update: update,
            stop_words: stop_words,
        };

        document.serialize(serializer)?;

        Ok(document_id)
    }

    pub fn remove_document<T>(&mut self, document: T) -> Result<DocumentId, SerializerError>
    where T: Serialize,
    {
        let document_id = self.schema.document_id(&document)?;
        self.raw_builder.document_update(document_id).remove();
        Ok(document_id)
    }

    pub fn build(self) -> Result<Update, Box<Error>> {
        self.raw_builder.build()
    }
}
