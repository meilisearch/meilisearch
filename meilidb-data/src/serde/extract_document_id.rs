use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use meilidb_core::DocumentId;
use serde::Serialize;
use serde::ser;

use super::{SerializerError, ConvertToString};

pub fn extract_document_id<D>(
    identifier: &str,
    document: &D,
) -> Result<Option<DocumentId>, SerializerError>
where D: serde::Serialize,
{
    let serializer = ExtractDocumentId { identifier };
    document.serialize(serializer)
}

fn calculate_hash<T: Hash>(t: &T) -> u64 {
    let mut s = DefaultHasher::new();
    t.hash(&mut s);
    s.finish()
}

struct ExtractDocumentId<'a> {
    identifier: &'a str,
}

impl<'a> ser::Serializer for ExtractDocumentId<'a> {
    type Ok = Option<DocumentId>;
    type Error = SerializerError;
    type SerializeSeq = ser::Impossible<Self::Ok, Self::Error>;
    type SerializeTuple = ser::Impossible<Self::Ok, Self::Error>;
    type SerializeTupleStruct = ser::Impossible<Self::Ok, Self::Error>;
    type SerializeTupleVariant = ser::Impossible<Self::Ok, Self::Error>;
    type SerializeMap = ExtractDocumentIdMapSerializer<'a>;
    type SerializeStruct = ExtractDocumentIdStructSerializer<'a>;
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

    fn serialize_str(self, _value: &str) -> Result<Self::Ok, Self::Error> {
        Err(SerializerError::UnserializableType { type_name: "str" })
    }

    fn serialize_bytes(self, _value: &[u8]) -> Result<Self::Ok, Self::Error> {
        Err(SerializerError::UnserializableType { type_name: "&[u8]" })
    }

    fn serialize_none(self) -> Result<Self::Ok, Self::Error> {
        Err(SerializerError::UnserializableType { type_name: "Option" })
    }

    fn serialize_some<T: ?Sized>(self, _value: &T) -> Result<Self::Ok, Self::Error>
    where T: Serialize,
    {
        Err(SerializerError::UnserializableType { type_name: "Option" })
    }

    fn serialize_unit(self) -> Result<Self::Ok, Self::Error> {
        Err(SerializerError::UnserializableType { type_name: "()" })
    }

    fn serialize_unit_struct(self, _name: &'static str) -> Result<Self::Ok, Self::Error> {
        Err(SerializerError::UnserializableType { type_name: "unit struct" })
    }

    fn serialize_unit_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str
    ) -> Result<Self::Ok, Self::Error>
    {
        Err(SerializerError::UnserializableType { type_name: "unit variant" })
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
        Err(SerializerError::UnserializableType { type_name: "newtype variant" })
    }

    fn serialize_seq(self, _len: Option<usize>) -> Result<Self::SerializeSeq, Self::Error> {
        Err(SerializerError::UnserializableType { type_name: "sequence" })
    }

    fn serialize_tuple(self, _len: usize) -> Result<Self::SerializeTuple, Self::Error> {
        Err(SerializerError::UnserializableType { type_name: "tuple" })
    }

    fn serialize_tuple_struct(
        self,
        _name: &'static str,
        _len: usize
    ) -> Result<Self::SerializeTupleStruct, Self::Error>
    {
        Err(SerializerError::UnserializableType { type_name: "tuple struct" })
    }

    fn serialize_tuple_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _len: usize
    ) -> Result<Self::SerializeTupleVariant, Self::Error>
    {
        Err(SerializerError::UnserializableType { type_name: "tuple variant" })
    }

    fn serialize_map(self, _len: Option<usize>) -> Result<Self::SerializeMap, Self::Error> {
        let serializer = ExtractDocumentIdMapSerializer {
            identifier: self.identifier,
            document_id: None,
            current_key_name: None,
        };

        Ok(serializer)
    }

    fn serialize_struct(
        self,
        _name: &'static str,
        _len: usize
    ) -> Result<Self::SerializeStruct, Self::Error>
    {
        let serializer = ExtractDocumentIdStructSerializer {
            identifier: self.identifier,
            document_id: None,
        };

        Ok(serializer)
    }

    fn serialize_struct_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _len: usize
    ) -> Result<Self::SerializeStructVariant, Self::Error>
    {
        Err(SerializerError::UnserializableType { type_name: "struct variant" })
    }
}

pub struct ExtractDocumentIdMapSerializer<'a> {
    identifier: &'a str,
    document_id: Option<DocumentId>,
    current_key_name: Option<String>,
}

impl<'a> ser::SerializeMap for ExtractDocumentIdMapSerializer<'a> {
    type Ok = Option<DocumentId>;
    type Error = SerializerError;

    fn serialize_key<T: ?Sized>(&mut self, key: &T) -> Result<(), Self::Error>
    where T: Serialize,
    {
        let key = key.serialize(ConvertToString)?;
        self.current_key_name = Some(key);
        Ok(())
    }

    fn serialize_value<T: ?Sized>(&mut self, value: &T) -> Result<(), Self::Error>
    where T: Serialize,
    {
        let key = self.current_key_name.take().unwrap();
        self.serialize_entry(&key, value)
    }

    fn serialize_entry<K: ?Sized, V: ?Sized>(
        &mut self,
        key: &K,
        value: &V
    ) -> Result<(), Self::Error>
    where K: Serialize, V: Serialize,
    {
        let key = key.serialize(ConvertToString)?;

        if self.identifier == key {
            // TODO is it possible to have multiple ids?
            let id = bincode::serialize(value).unwrap();
            let hash = calculate_hash(&id);
            self.document_id = Some(DocumentId(hash));
        }

        Ok(())
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(self.document_id)
    }
}

pub struct ExtractDocumentIdStructSerializer<'a> {
    identifier: &'a str,
    document_id: Option<DocumentId>,
}

impl<'a> ser::SerializeStruct for ExtractDocumentIdStructSerializer<'a> {
    type Ok = Option<DocumentId>;
    type Error = SerializerError;

    fn serialize_field<T: ?Sized>(
        &mut self,
        key: &'static str,
        value: &T
    ) -> Result<(), Self::Error>
    where T: Serialize,
    {
        if self.identifier == key {
            // TODO can it be possible to have multiple ids?
            let id = bincode::serialize(value).unwrap();
            let hash = calculate_hash(&id);
            self.document_id = Some(DocumentId(hash));
        }

        Ok(())
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(self.document_id)
    }
}
