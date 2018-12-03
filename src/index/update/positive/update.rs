use std::collections::BTreeMap;
use std::path::PathBuf;
use std::error::Error;
use std::fmt;

use ::rocksdb::rocksdb_options;
use serde::ser::{self, Serialize};

use crate::index::update::positive::unordered_builder::UnorderedPositiveBlobBuilder;
use crate::index::schema::{SchemaProps, Schema, SchemaAttr};
use crate::index::update::Update;
use crate::database::{DocumentKey, DocumentKeyAttr};
use crate::blob::positive::PositiveBlob;
use crate::tokenizer::TokenizerBuilder;
use crate::{DocumentId, DocIndex};
use crate::index::DATA_INDEX;
use crate::blob::Blob;

pub enum NewState {
    Updated { value: String },
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

    pub fn update<T: Serialize>(&mut self, id: DocumentId, document: &T) -> Result<(), Box<Error>> {
        let serializer = Serializer {
            schema: &self.schema,
            document_id: id,
            new_states: &mut self.new_states
        };

        Ok(ser::Serialize::serialize(document, serializer)?)
    }

    // TODO value must be a field that can be indexed
    pub fn update_field(&mut self, id: DocumentId, field: SchemaAttr, value: String) {
        self.new_states.insert((id, field), NewState::Updated { value });
    }

    pub fn remove_field(&mut self, id: DocumentId, field: SchemaAttr) {
        self.new_states.insert((id, field), NewState::Removed);
    }
}

#[derive(Debug)]
pub enum SerializerError {
    SchemaDontMatch { attribute: String },
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
            SerializerError::SchemaDontMatch { attribute } => {
                write!(f, "serialized document try to specify the \
                           {:?} attribute that is not known by the schema", attribute)
            },
            SerializerError::UnserializableType { name } => {
                write!(f, "Only struct and map types are considered valid documents and
                           can be serialized, not {} types directly.", name)
            },
            SerializerError::Custom(s) => f.write_str(&s),
        }
    }
}

impl Error for SerializerError {}

struct Serializer<'a> {
    schema: &'a Schema,
    document_id: DocumentId,
    new_states: &'a mut BTreeMap<(DocumentId, SchemaAttr), NewState>,
}

macro_rules! forward_to_unserializable_type {
    ($($ty:ident => $se_method:ident,)*) => {
        $(
            fn $se_method(self, v: $ty) -> Result<Self::Ok, Self::Error> {
                Err(SerializerError::UnserializableType { name: "$ty" })
            }
        )*
    }
}

impl<'a> ser::Serializer for Serializer<'a> {
    type Ok = ();
    type Error = SerializerError;
    type SerializeSeq = ser::Impossible<Self::Ok, Self::Error>;
    type SerializeTuple = ser::Impossible<Self::Ok, Self::Error>;
    type SerializeTupleStruct = ser::Impossible<Self::Ok, Self::Error>;
    type SerializeTupleVariant = ser::Impossible<Self::Ok, Self::Error>;
    type SerializeMap = MapSerializer<'a>;
    type SerializeStruct = StructSerializer<'a>;
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
        Err(SerializerError::UnserializableType { name: "str" })
    }

    fn serialize_bytes(self, v: &[u8]) -> Result<Self::Ok, Self::Error> {
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
        Ok(MapSerializer {
            schema: self.schema,
            document_id: self.document_id,
            new_states: self.new_states,
        })
    }

    fn serialize_struct(
        self,
        _name: &'static str,
        _len: usize
    ) -> Result<Self::SerializeStruct, Self::Error>
    {
        Ok(StructSerializer {
            schema: self.schema,
            document_id: self.document_id,
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

fn serialize_field<T: ?Sized>(
    schema: &Schema,
    document_id: DocumentId,
    new_states: &mut BTreeMap<(DocumentId, SchemaAttr), NewState>,
    name: &str,
    value: &T
) -> Result<(), SerializerError>
where T: Serialize,
{
    match schema.attribute(name) {
        Some(attr) => {
            if schema.props(attr).is_stored() {
                let value = unimplemented!();
                new_states.insert((document_id, attr), NewState::Updated { value });
            }
            Ok(())
        },
        None => Err(SerializerError::SchemaDontMatch { attribute: name.to_owned() }),
    }
}

struct StructSerializer<'a> {
    schema: &'a Schema,
    document_id: DocumentId,
    new_states: &'a mut BTreeMap<(DocumentId, SchemaAttr), NewState>,
}

impl<'a> ser::SerializeStruct for StructSerializer<'a> {
    type Ok = ();
    type Error = SerializerError;

    fn serialize_field<T: ?Sized>(
        &mut self,
        key: &'static str,
        value: &T
    ) -> Result<(), Self::Error>
    where T: Serialize,
    {
        serialize_field(self.schema, self.document_id, self.new_states, key, value)
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(())
    }
}

struct MapSerializer<'a> {
    schema: &'a Schema,
    document_id: DocumentId,
    new_states: &'a mut BTreeMap<(DocumentId, SchemaAttr), NewState>,
    // pending_key: Option<String>,
}

impl<'a> ser::SerializeMap for MapSerializer<'a> {
    type Ok = ();
    type Error = SerializerError;

    fn serialize_key<T: ?Sized>(&mut self, key: &T) -> Result<(), Self::Error>
    where T: Serialize
    {
        Err(SerializerError::UnserializableType { name: "setmap" })
    }

    fn serialize_value<T: ?Sized>(&mut self, value: &T) -> Result<(), Self::Error>
    where T: Serialize
    {
        unimplemented!()
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(())
    }

    fn serialize_entry<K: ?Sized, V: ?Sized>(
        &mut self,
        key: &K,
        value: &V
    ) -> Result<(), Self::Error>
    where K: Serialize, V: Serialize,
    {
        let key = unimplemented!();
        serialize_field(self.schema, self.document_id, self.new_states, key, value)
    }
}

// struct MapKeySerializer;

// impl ser::Serializer for MapKeySerializer {
//     type Ok = String;
//     type Error = SerializerError;

//     #[inline]
//     fn serialize_str(self, value: &str) -> Result<()> {
//         unimplemented!()
//     }
// }

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
            let props = self.schema.props(*attr);
            let value = match state {
                NewState::Updated { value } if props.is_indexed() => value,
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
            let key = DocumentKeyAttr::new(id, attr);
            let props = self.schema.props(attr);
            match state {
                NewState::Updated { value } => if props.is_stored() {
                    file_writer.put(key.as_ref(), value.as_bytes())?
                },
                NewState::Removed => file_writer.delete(key.as_ref())?,
            }
        }

        file_writer.finish()?;
        Update::open(self.path)
    }
}
