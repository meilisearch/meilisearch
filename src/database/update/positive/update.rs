use std::collections::BTreeMap;
use std::path::PathBuf;
use std::error::Error;
use std::fmt;

use ::rocksdb::rocksdb_options;
use serde::ser::{self, Serialize};

use crate::database::update::positive::unordered_builder::UnorderedPositiveBlobBuilder;
use crate::database::blob::positive::PositiveBlob;
use crate::database::schema::{Schema, SchemaAttr};
use crate::tokenizer::{TokenizerBuilder, Token};
use crate::database::DocumentKeyAttr;
use crate::database::update::Update;
use crate::database::DATA_INDEX;
use crate::database::blob::Blob;
use crate::{DocumentId, DocIndex, Attribute, WordArea};

pub enum NewState {
    Updated { value: Vec<u8> },
    Removed,
}

pub struct PositiveUpdateBuilder<B> {
    path: PathBuf,
    schema: Schema,
    tokenizer_builder: B,
    builder: UnorderedPositiveBlobBuilder<Vec<u8>, Vec<u8>>,
    new_states: BTreeMap<DocumentKeyAttr, NewState>,
}

impl<B> PositiveUpdateBuilder<B> {
    pub fn new<P: Into<PathBuf>>(path: P, schema: Schema, tokenizer_builder: B) -> PositiveUpdateBuilder<B> {
        PositiveUpdateBuilder {
            path: path.into(),
            schema: schema,
            tokenizer_builder: tokenizer_builder,
            builder: UnorderedPositiveBlobBuilder::memory(),
            new_states: BTreeMap::new(),
        }
    }

    pub fn update<T: Serialize>(&mut self, document: &T) -> Result<DocumentId, SerializerError>
    where B: TokenizerBuilder
    {
        let document_id = self.schema.document_id(document)?;

        let serializer = Serializer {
            schema: &self.schema,
            tokenizer_builder: &self.tokenizer_builder,
            document_id: document_id,
            builder: &mut self.builder,
            new_states: &mut self.new_states
        };
        document.serialize(serializer)?;

        Ok(document_id)
    }

    // TODO value must be a field that can be indexed
    pub fn update_field(&mut self, id: DocumentId, attr: SchemaAttr, value: String) {
        let value = bincode::serialize(&value).unwrap();
        self.new_states.insert(DocumentKeyAttr::new(id, attr), NewState::Updated { value });
    }

    pub fn remove_field(&mut self, id: DocumentId, attr: SchemaAttr) {
        self.new_states.insert(DocumentKeyAttr::new(id, attr), NewState::Removed);
    }
}

#[derive(Debug)]
pub enum SerializerError {
    DocumentIdNotFound,
    UnserializableType { name: &'static str },
    Custom(String),
}

impl ser::Error for SerializerError {
    fn custom<T: fmt::Display>(msg: T) -> Self {
        SerializerError::Custom(msg.to_string())
    }
}

impl fmt::Display for SerializerError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            SerializerError::DocumentIdNotFound => {
                write!(f, "serialized document does not have an id according to the schema")
            }
            SerializerError::UnserializableType { name } => {
                write!(f, "Only struct and map types are considered valid documents and
                           can be serialized, not {} types directly.", name)
            },
            SerializerError::Custom(s) => f.write_str(&s),
        }
    }
}

impl Error for SerializerError {}

struct Serializer<'a, B> {
    schema: &'a Schema,
    tokenizer_builder: &'a B,
    document_id: DocumentId,
    builder: &'a mut UnorderedPositiveBlobBuilder<Vec<u8>, Vec<u8>>,
    new_states: &'a mut BTreeMap<DocumentKeyAttr, NewState>,
}

impl<'a, B> ser::Serializer for Serializer<'a, B>
where B: TokenizerBuilder
{
    type Ok = ();
    type Error = SerializerError;
    type SerializeSeq = ser::Impossible<Self::Ok, Self::Error>;
    type SerializeTuple = ser::Impossible<Self::Ok, Self::Error>;
    type SerializeTupleStruct = ser::Impossible<Self::Ok, Self::Error>;
    type SerializeTupleVariant = ser::Impossible<Self::Ok, Self::Error>;
    type SerializeMap = ser::Impossible<Self::Ok, Self::Error>;
    type SerializeStruct = StructSerializer<'a, B>;
    type SerializeStructVariant = ser::Impossible<Self::Ok, Self::Error>;

    forward_to_unserializable_type! {
        bool => serialize_bool,
        char => serialize_char,

        i8  => serialize_i8,
        i16 => serialize_i16,
        i32 => serialize_i32,
        i64 => serialize_i64,

        u8  => serialize_u8,
        u16 => serialize_u16,
        u32 => serialize_u32,
        u64 => serialize_u64,

        f32 => serialize_f32,
        f64 => serialize_f64,
    }

    fn serialize_str(self, _v: &str) -> Result<Self::Ok, Self::Error> {
        Err(SerializerError::UnserializableType { name: "str" })
    }

    fn serialize_bytes(self, _v: &[u8]) -> Result<Self::Ok, Self::Error> {
        Err(SerializerError::UnserializableType { name: "&[u8]" })
    }

    fn serialize_none(self) -> Result<Self::Ok, Self::Error> {
        Err(SerializerError::UnserializableType { name: "Option" })
    }

    fn serialize_some<T: ?Sized>(self, _value: &T) -> Result<Self::Ok, Self::Error>
    where T: Serialize,
    {
        Err(SerializerError::UnserializableType { name: "Option" })
    }

    fn serialize_unit(self) -> Result<Self::Ok, Self::Error> {
        Err(SerializerError::UnserializableType { name: "()" })
    }

    fn serialize_unit_struct(self, _name: &'static str) -> Result<Self::Ok, Self::Error> {
        Err(SerializerError::UnserializableType { name: "unit struct" })
    }

    fn serialize_unit_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str
    ) -> Result<Self::Ok, Self::Error>
    {
        Err(SerializerError::UnserializableType { name: "unit variant" })
    }

    fn serialize_newtype_struct<T: ?Sized>(
        self,
        _name: &'static str,
        value: &T
    ) -> Result<Self::Ok, Self::Error>
    where T: Serialize,
    {
        value.serialize(self)
    }

    fn serialize_newtype_variant<T: ?Sized>(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _value: &T
    ) -> Result<Self::Ok, Self::Error>
    where T: Serialize,
    {
        Err(SerializerError::UnserializableType { name: "newtype variant" })
    }

    fn serialize_seq(self, _len: Option<usize>) -> Result<Self::SerializeSeq, Self::Error> {
        Err(SerializerError::UnserializableType { name: "sequence" })
    }

    fn serialize_tuple(self, _len: usize) -> Result<Self::SerializeTuple, Self::Error> {
        Err(SerializerError::UnserializableType { name: "tuple" })
    }

    fn serialize_tuple_struct(
        self,
        _name: &'static str,
        _len: usize
    ) -> Result<Self::SerializeTupleStruct, Self::Error>
    {
        Err(SerializerError::UnserializableType { name: "tuple struct" })
    }

    fn serialize_tuple_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _len: usize
    ) -> Result<Self::SerializeTupleVariant, Self::Error>
    {
        Err(SerializerError::UnserializableType { name: "tuple variant" })
    }

    fn serialize_map(self, _len: Option<usize>) -> Result<Self::SerializeMap, Self::Error> {
        // Ok(MapSerializer {
        //     schema: self.schema,
        //     document_id: self.document_id,
        //     new_states: self.new_states,
        // })
        Err(SerializerError::UnserializableType { name: "map" })
    }

    fn serialize_struct(
        self,
        _name: &'static str,
        _len: usize
    ) -> Result<Self::SerializeStruct, Self::Error>
    {
        Ok(StructSerializer {
            schema: self.schema,
            tokenizer_builder: self.tokenizer_builder,
            document_id: self.document_id,
            builder: self.builder,
            new_states: self.new_states,
        })
    }

    fn serialize_struct_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _len: usize
    ) -> Result<Self::SerializeStructVariant, Self::Error>
    {
        Err(SerializerError::UnserializableType { name: "struct variant" })
    }
}

struct StructSerializer<'a, B> {
    schema: &'a Schema,
    tokenizer_builder: &'a B,
    document_id: DocumentId,
    builder: &'a mut UnorderedPositiveBlobBuilder<Vec<u8>, Vec<u8>>,
    new_states: &'a mut BTreeMap<DocumentKeyAttr, NewState>,
}

impl<'a, B> ser::SerializeStruct for StructSerializer<'a, B>
where B: TokenizerBuilder
{
    type Ok = ();
    type Error = SerializerError;

    fn serialize_field<T: ?Sized>(
        &mut self,
        key: &'static str,
        value: &T
    ) -> Result<(), Self::Error>
    where T: Serialize,
    {
        if let Some(attr) = self.schema.attribute(key) {
            let props = self.schema.props(attr);
            if props.is_stored() {
                let value = bincode::serialize(value).unwrap();
                let key = DocumentKeyAttr::new(self.document_id, attr);
                self.new_states.insert(key, NewState::Updated { value });
            }
            if props.is_indexed() {
                let serializer = IndexerSerializer {
                    builder: self.builder,
                    tokenizer_builder: self.tokenizer_builder,
                    document_id: self.document_id,
                    attribute: attr,
                };
                value.serialize(serializer)?;
            }
        }

        Ok(())
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(())
    }
}

struct IndexerSerializer<'a, B> {
    tokenizer_builder: &'a B,
    builder: &'a mut UnorderedPositiveBlobBuilder<Vec<u8>, Vec<u8>>,
    document_id: DocumentId,
    attribute: SchemaAttr,
}

impl<'a, B> ser::Serializer for IndexerSerializer<'a, B>
where B: TokenizerBuilder
{
    type Ok = ();
    type Error = SerializerError;
    type SerializeSeq = ser::Impossible<Self::Ok, Self::Error>;
    type SerializeTuple = ser::Impossible<Self::Ok, Self::Error>;
    type SerializeTupleStruct = ser::Impossible<Self::Ok, Self::Error>;
    type SerializeTupleVariant = ser::Impossible<Self::Ok, Self::Error>;
    type SerializeMap = ser::Impossible<Self::Ok, Self::Error>;
    type SerializeStruct = ser::Impossible<Self::Ok, Self::Error>;
    type SerializeStructVariant = ser::Impossible<Self::Ok, Self::Error>;

    forward_to_unserializable_type! {
        bool => serialize_bool,
        char => serialize_char,

        i8  => serialize_i8,
        i16 => serialize_i16,
        i32 => serialize_i32,
        i64 => serialize_i64,

        u8  => serialize_u8,
        u16 => serialize_u16,
        u32 => serialize_u32,
        u64 => serialize_u64,

        f32 => serialize_f32,
        f64 => serialize_f64,
    }

    fn serialize_str(self, v: &str) -> Result<Self::Ok, Self::Error> {
        for Token { word, word_index, char_index } in self.tokenizer_builder.build(v) {
            let doc_index = DocIndex {
                document_id: self.document_id,
                attribute: Attribute::new(self.attribute.0, word_index as u32),
                word_area: WordArea::new(char_index as u32, word.len() as u16),
            };

            // insert the exact representation
            let word_lower = word.to_lowercase();

            // and the unidecoded lowercased version
            let word_unidecoded = unidecode::unidecode(word).to_lowercase();
            if word_lower != word_unidecoded {
                self.builder.insert(word_unidecoded, doc_index);
            }

            self.builder.insert(word_lower, doc_index);
        }
        Ok(())
    }

    fn serialize_bytes(self, _v: &[u8]) -> Result<Self::Ok, Self::Error> {
        Err(SerializerError::UnserializableType { name: "&[u8]" })
    }

    fn serialize_none(self) -> Result<Self::Ok, Self::Error> {
        Err(SerializerError::UnserializableType { name: "Option" })
    }

    fn serialize_some<T: ?Sized>(self, _value: &T) -> Result<Self::Ok, Self::Error>
    where T: Serialize,
    {
        Err(SerializerError::UnserializableType { name: "Option" })
    }

    fn serialize_unit(self) -> Result<Self::Ok, Self::Error> {
        Err(SerializerError::UnserializableType { name: "()" })
    }

    fn serialize_unit_struct(self, _name: &'static str) -> Result<Self::Ok, Self::Error> {
        Err(SerializerError::UnserializableType { name: "unit struct" })
    }

    fn serialize_unit_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str
    ) -> Result<Self::Ok, Self::Error>
    {
        Err(SerializerError::UnserializableType { name: "unit variant" })
    }

    fn serialize_newtype_struct<T: ?Sized>(
        self,
        _name: &'static str,
        value: &T
    ) -> Result<Self::Ok, Self::Error>
    where T: Serialize,
    {
        value.serialize(self)
    }

    fn serialize_newtype_variant<T: ?Sized>(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _value: &T
    ) -> Result<Self::Ok, Self::Error>
    where T: Serialize,
    {
        Err(SerializerError::UnserializableType { name: "newtype variant" })
    }

    fn serialize_seq(self, _len: Option<usize>) -> Result<Self::SerializeSeq, Self::Error> {
        Err(SerializerError::UnserializableType { name: "seq" })
    }

    fn serialize_tuple(self, _len: usize) -> Result<Self::SerializeTuple, Self::Error> {
        Err(SerializerError::UnserializableType { name: "tuple" })
    }

    fn serialize_tuple_struct(
        self,
        _name: &'static str,
        _len: usize
    ) -> Result<Self::SerializeTupleStruct, Self::Error>
    {
        Err(SerializerError::UnserializableType { name: "tuple struct" })
    }

    fn serialize_tuple_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _len: usize
    ) -> Result<Self::SerializeTupleVariant, Self::Error>
    {
        Err(SerializerError::UnserializableType { name: "tuple variant" })
    }

    fn serialize_map(self, _len: Option<usize>) -> Result<Self::SerializeMap, Self::Error> {
        Err(SerializerError::UnserializableType { name: "map" })
    }

    fn serialize_struct(
        self,
        _name: &'static str,
        _len: usize
    ) -> Result<Self::SerializeStruct, Self::Error>
    {
        Err(SerializerError::UnserializableType { name: "struct" })
    }

    fn serialize_struct_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _len: usize
    ) -> Result<Self::SerializeStructVariant, Self::Error>
    {
        Err(SerializerError::UnserializableType { name: "struct variant" })
    }
}

impl<B> PositiveUpdateBuilder<B> {
    pub fn build(self) -> Result<Update, Box<Error>> {
        let env_options = rocksdb_options::EnvOptions::new();
        let column_family_options = rocksdb_options::ColumnFamilyOptions::new();
        let mut file_writer = rocksdb::SstFileWriter::new(env_options, column_family_options);
        file_writer.open(&self.path.to_string_lossy())?;

        let (blob_fst_map, blob_doc_idx) = self.builder.into_inner()?;
        let positive_blob = PositiveBlob::from_bytes(blob_fst_map, blob_doc_idx)?;
        let blob = Blob::Positive(positive_blob);

        // write the data-index aka positive blob
        let mut bytes = Vec::new();
        blob.write_to_bytes(&mut bytes);
        file_writer.merge(DATA_INDEX, &bytes)?;

        // write all the documents fields updates
        for (key, state) in self.new_states {
            match state {
                NewState::Updated { value } => {
                    file_writer.put(key.as_ref(), &value)?
                },
                NewState::Removed => file_writer.delete(key.as_ref())?,
            }
        }

        file_writer.finish()?;
        Update::open(self.path)
    }
}
