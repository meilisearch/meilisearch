use serde::Serialize;
use serde::ser;

use crate::database::serde::key_to_string::KeyToStringSerializer;
use crate::database::serde::{SerializerError, calculate_hash};
use crate::DocumentId;

pub struct FindDocumentIdSerializer<'a> {
    pub id_attribute_name: &'a str,
}

impl<'a> ser::Serializer for FindDocumentIdSerializer<'a> {
    type Ok = DocumentId;
    type Error = SerializerError;
    type SerializeSeq = ser::Impossible<Self::Ok, Self::Error>;
    type SerializeTuple = ser::Impossible<Self::Ok, Self::Error>;
    type SerializeTupleStruct = ser::Impossible<Self::Ok, Self::Error>;
    type SerializeTupleVariant = ser::Impossible<Self::Ok, Self::Error>;
    type SerializeMap = FindDocumentIdMapSerializer<'a>;
    type SerializeStruct = FindDocumentIdStructSerializer<'a>;
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
        Ok(FindDocumentIdMapSerializer {
            id_attribute_name: self.id_attribute_name,
            document_id: None,
            current_key_name: None,
        })
    }

    fn serialize_struct(
        self,
        _name: &'static str,
        _len: usize
    ) -> Result<Self::SerializeStruct, Self::Error>
    {
        Ok(FindDocumentIdStructSerializer {
            id_attribute_name: self.id_attribute_name,
            document_id: None,
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

pub struct FindDocumentIdMapSerializer<'a> {
    id_attribute_name: &'a str,
    document_id: Option<DocumentId>,
    current_key_name: Option<String>,
}

impl<'a> ser::SerializeMap for FindDocumentIdMapSerializer<'a> {
    type Ok = DocumentId;
    type Error = SerializerError;

    fn serialize_key<T: ?Sized>(&mut self, key: &T) -> Result<(), Self::Error>
    where T: Serialize,
    {
        let key = key.serialize(KeyToStringSerializer)?;
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
        let key = key.serialize(KeyToStringSerializer)?;

        if self.id_attribute_name == key {
            // TODO is it possible to have multiple ids?
            let id = bincode::serialize(value).unwrap();
            let hash = calculate_hash(&id);
            self.document_id = Some(DocumentId(hash));
        }

        Ok(())
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        match self.document_id {
            Some(document_id) => Ok(document_id),
            None => Err(SerializerError::DocumentIdNotFound)
        }
    }
}

pub struct FindDocumentIdStructSerializer<'a> {
    id_attribute_name: &'a str,
    document_id: Option<DocumentId>,
}

impl<'a> ser::SerializeStruct for FindDocumentIdStructSerializer<'a> {
    type Ok = DocumentId;
    type Error = SerializerError;

    fn serialize_field<T: ?Sized>(
        &mut self,
        key: &'static str,
        value: &T
    ) -> Result<(), Self::Error>
    where T: Serialize,
    {
        if self.id_attribute_name == key {
            // TODO can it be possible to have multiple ids?
            let id = bincode::serialize(value).unwrap();
            let hash = calculate_hash(&id);
            self.document_id = Some(DocumentId(hash));
        }

        Ok(())
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        match self.document_id {
            Some(document_id) => Ok(document_id),
            None => Err(SerializerError::DocumentIdNotFound)
        }
    }
}
