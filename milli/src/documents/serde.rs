use std::collections::BTreeMap;
use std::convert::TryInto;
use std::io::Cursor;
use std::{fmt, io};

use byteorder::{BigEndian, WriteBytesExt};
use obkv::KvWriter;
use serde::ser::{Impossible, Serialize, SerializeMap, SerializeSeq, Serializer};
use serde_json::Value;

use super::{ByteCounter, DocumentsBatchIndex, Error};
use crate::FieldId;

pub struct DocumentSerializer<W> {
    pub writer: ByteCounter<W>,
    pub buffer: Vec<u8>,
    pub index: DocumentsBatchIndex,
    pub count: usize,
    pub allow_seq: bool,
}

impl<'a, W: io::Write> Serializer for &'a mut DocumentSerializer<W> {
    type Ok = ();

    type Error = Error;

    type SerializeSeq = SeqSerializer<'a, W>;
    type SerializeTuple = Impossible<(), Self::Error>;
    type SerializeTupleStruct = Impossible<(), Self::Error>;
    type SerializeTupleVariant = Impossible<(), Self::Error>;
    type SerializeMap = MapSerializer<'a, &'a mut ByteCounter<W>>;
    type SerializeStruct = Impossible<(), Self::Error>;
    type SerializeStructVariant = Impossible<(), Self::Error>;
    fn serialize_map(self, _len: Option<usize>) -> Result<Self::SerializeMap, Self::Error> {
        self.buffer.clear();
        let cursor = io::Cursor::new(&mut self.buffer);
        self.count += 1;
        let map_serializer = MapSerializer {
            map: KvWriter::new(cursor),
            index: &mut self.index,
            writer: &mut self.writer,
            mapped_documents: BTreeMap::new(),
        };

        Ok(map_serializer)
    }

    fn serialize_seq(self, _len: Option<usize>) -> Result<Self::SerializeSeq, Self::Error> {
        if self.allow_seq {
            // Only allow sequence of documents of depth 1.
            self.allow_seq = false;
            Ok(SeqSerializer { serializer: self })
        } else {
            Err(Error::InvalidDocumentFormat)
        }
    }

    fn serialize_bool(self, _v: bool) -> Result<Self::Ok, Self::Error> {
        Err(Error::InvalidDocumentFormat)
    }

    fn serialize_i8(self, _v: i8) -> Result<Self::Ok, Self::Error> {
        Err(Error::InvalidDocumentFormat)
    }

    fn serialize_i16(self, _v: i16) -> Result<Self::Ok, Self::Error> {
        Err(Error::InvalidDocumentFormat)
    }

    fn serialize_i32(self, _v: i32) -> Result<Self::Ok, Self::Error> {
        Err(Error::InvalidDocumentFormat)
    }

    fn serialize_i64(self, _v: i64) -> Result<Self::Ok, Self::Error> {
        Err(Error::InvalidDocumentFormat)
    }

    fn serialize_u8(self, _v: u8) -> Result<Self::Ok, Self::Error> {
        Err(Error::InvalidDocumentFormat)
    }

    fn serialize_u16(self, _v: u16) -> Result<Self::Ok, Self::Error> {
        Err(Error::InvalidDocumentFormat)
    }

    fn serialize_u32(self, _v: u32) -> Result<Self::Ok, Self::Error> {
        Err(Error::InvalidDocumentFormat)
    }

    fn serialize_u64(self, _v: u64) -> Result<Self::Ok, Self::Error> {
        Err(Error::InvalidDocumentFormat)
    }

    fn serialize_f32(self, _v: f32) -> Result<Self::Ok, Self::Error> {
        Err(Error::InvalidDocumentFormat)
    }

    fn serialize_f64(self, _v: f64) -> Result<Self::Ok, Self::Error> {
        Err(Error::InvalidDocumentFormat)
    }

    fn serialize_char(self, _v: char) -> Result<Self::Ok, Self::Error> {
        Err(Error::InvalidDocumentFormat)
    }

    fn serialize_str(self, _v: &str) -> Result<Self::Ok, Self::Error> {
        Err(Error::InvalidDocumentFormat)
    }

    fn serialize_bytes(self, _v: &[u8]) -> Result<Self::Ok, Self::Error> {
        Err(Error::InvalidDocumentFormat)
    }

    fn serialize_none(self) -> Result<Self::Ok, Self::Error> {
        Err(Error::InvalidDocumentFormat)
    }

    fn serialize_some<T: ?Sized>(self, _value: &T) -> Result<Self::Ok, Self::Error>
    where
        T: Serialize,
    {
        Err(Error::InvalidDocumentFormat)
    }

    fn serialize_unit(self) -> Result<Self::Ok, Self::Error> {
        Err(Error::InvalidDocumentFormat)
    }

    fn serialize_unit_struct(self, _name: &'static str) -> Result<Self::Ok, Self::Error> {
        Err(Error::InvalidDocumentFormat)
    }

    fn serialize_unit_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
    ) -> Result<Self::Ok, Self::Error> {
        Err(Error::InvalidDocumentFormat)
    }

    fn serialize_newtype_struct<T: ?Sized>(
        self,
        _name: &'static str,
        _value: &T,
    ) -> Result<Self::Ok, Self::Error>
    where
        T: Serialize,
    {
        Err(Error::InvalidDocumentFormat)
    }

    fn serialize_newtype_variant<T: ?Sized>(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _value: &T,
    ) -> Result<Self::Ok, Self::Error>
    where
        T: Serialize,
    {
        Err(Error::InvalidDocumentFormat)
    }

    fn serialize_tuple(self, _len: usize) -> Result<Self::SerializeTuple, Self::Error> {
        Err(Error::InvalidDocumentFormat)
    }

    fn serialize_tuple_struct(
        self,
        _name: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleStruct, Self::Error> {
        Err(Error::InvalidDocumentFormat)
    }

    fn serialize_tuple_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleVariant, Self::Error> {
        Err(Error::InvalidDocumentFormat)
    }

    fn serialize_struct(
        self,
        _name: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStruct, Self::Error> {
        Err(Error::InvalidDocumentFormat)
    }

    fn serialize_struct_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStructVariant, Self::Error> {
        Err(Error::InvalidDocumentFormat)
    }
}

pub struct SeqSerializer<'a, W> {
    serializer: &'a mut DocumentSerializer<W>,
}

impl<'a, W: io::Write> SerializeSeq for SeqSerializer<'a, W> {
    type Ok = ();
    type Error = Error;

    fn serialize_element<T: ?Sized>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: Serialize,
    {
        value.serialize(&mut *self.serializer)?;
        Ok(())
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(())
    }
}

pub struct MapSerializer<'a, W> {
    map: KvWriter<io::Cursor<&'a mut Vec<u8>>, FieldId>,
    index: &'a mut DocumentsBatchIndex,
    writer: W,
    mapped_documents: BTreeMap<FieldId, Value>,
}

/// This implementation of SerializeMap uses serilialize_entry instead of seriliaze_key and
/// serialize_value, therefore these to methods remain unimplemented.
impl<'a, W: io::Write> SerializeMap for MapSerializer<'a, W> {
    type Ok = ();
    type Error = Error;

    fn serialize_key<T: ?Sized + Serialize>(&mut self, _key: &T) -> Result<(), Self::Error> {
        unreachable!()
    }

    fn serialize_value<T: ?Sized>(&mut self, _value: &T) -> Result<(), Self::Error> {
        unreachable!()
    }

    fn end(mut self) -> Result<Self::Ok, Self::Error> {
        let mut buf = Vec::new();
        for (key, value) in self.mapped_documents {
            buf.clear();
            let mut cursor = Cursor::new(&mut buf);
            serde_json::to_writer(&mut cursor, &value).map_err(Error::JsonError)?;
            self.map.insert(key, cursor.into_inner()).map_err(Error::Io)?;
        }

        let data = self.map.into_inner().map_err(Error::Io)?.into_inner();
        let data_len: u32 = data.len().try_into().map_err(|_| Error::DocumentTooLarge)?;

        self.writer.write_u32::<BigEndian>(data_len).map_err(Error::Io)?;
        self.writer.write_all(&data).map_err(Error::Io)?;

        Ok(())
    }

    fn serialize_entry<K: ?Sized, V: ?Sized>(
        &mut self,
        key: &K,
        value: &V,
    ) -> Result<(), Self::Error>
    where
        K: Serialize,
        V: Serialize,
    {
        let field_serializer = FieldSerializer { index: &mut self.index };
        let field_id: FieldId = key.serialize(field_serializer)?;

        let value = serde_json::to_value(value).map_err(Error::JsonError)?;

        self.mapped_documents.insert(field_id, value);

        Ok(())
    }
}

struct FieldSerializer<'a> {
    index: &'a mut DocumentsBatchIndex,
}

impl<'a> serde::Serializer for FieldSerializer<'a> {
    type Ok = FieldId;

    type Error = Error;

    type SerializeSeq = Impossible<FieldId, Self::Error>;
    type SerializeTuple = Impossible<FieldId, Self::Error>;
    type SerializeTupleStruct = Impossible<FieldId, Self::Error>;
    type SerializeTupleVariant = Impossible<FieldId, Self::Error>;
    type SerializeMap = Impossible<FieldId, Self::Error>;
    type SerializeStruct = Impossible<FieldId, Self::Error>;
    type SerializeStructVariant = Impossible<FieldId, Self::Error>;

    fn serialize_str(self, ws: &str) -> Result<Self::Ok, Self::Error> {
        let field_id = match self.index.get_by_right(ws) {
            Some(field_id) => *field_id,
            None => {
                let field_id = self.index.len() as FieldId;
                self.index.insert(field_id, ws.to_string());
                field_id
            }
        };

        Ok(field_id)
    }

    fn serialize_bool(self, _v: bool) -> Result<Self::Ok, Self::Error> {
        Err(Error::InvalidDocumentFormat)
    }

    fn serialize_i8(self, _v: i8) -> Result<Self::Ok, Self::Error> {
        Err(Error::InvalidDocumentFormat)
    }

    fn serialize_i16(self, _v: i16) -> Result<Self::Ok, Self::Error> {
        Err(Error::InvalidDocumentFormat)
    }

    fn serialize_i32(self, _v: i32) -> Result<Self::Ok, Self::Error> {
        Err(Error::InvalidDocumentFormat)
    }

    fn serialize_i64(self, _v: i64) -> Result<Self::Ok, Self::Error> {
        Err(Error::InvalidDocumentFormat)
    }

    fn serialize_u8(self, _v: u8) -> Result<Self::Ok, Self::Error> {
        Err(Error::InvalidDocumentFormat)
    }

    fn serialize_u16(self, _v: u16) -> Result<Self::Ok, Self::Error> {
        Err(Error::InvalidDocumentFormat)
    }

    fn serialize_u32(self, _v: u32) -> Result<Self::Ok, Self::Error> {
        Err(Error::InvalidDocumentFormat)
    }

    fn serialize_u64(self, _v: u64) -> Result<Self::Ok, Self::Error> {
        Err(Error::InvalidDocumentFormat)
    }

    fn serialize_f32(self, _v: f32) -> Result<Self::Ok, Self::Error> {
        Err(Error::InvalidDocumentFormat)
    }

    fn serialize_f64(self, _v: f64) -> Result<Self::Ok, Self::Error> {
        Err(Error::InvalidDocumentFormat)
    }

    fn serialize_char(self, _v: char) -> Result<Self::Ok, Self::Error> {
        Err(Error::InvalidDocumentFormat)
    }

    fn serialize_bytes(self, _v: &[u8]) -> Result<Self::Ok, Self::Error> {
        Err(Error::InvalidDocumentFormat)
    }

    fn serialize_none(self) -> Result<Self::Ok, Self::Error> {
        Err(Error::InvalidDocumentFormat)
    }

    fn serialize_some<T: ?Sized>(self, _value: &T) -> Result<Self::Ok, Self::Error>
    where
        T: Serialize,
    {
        Err(Error::InvalidDocumentFormat)
    }

    fn serialize_unit(self) -> Result<Self::Ok, Self::Error> {
        Err(Error::InvalidDocumentFormat)
    }

    fn serialize_unit_struct(self, _name: &'static str) -> Result<Self::Ok, Self::Error> {
        Err(Error::InvalidDocumentFormat)
    }

    fn serialize_unit_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
    ) -> Result<Self::Ok, Self::Error> {
        Err(Error::InvalidDocumentFormat)
    }

    fn serialize_newtype_struct<T: ?Sized>(
        self,
        _name: &'static str,
        _value: &T,
    ) -> Result<Self::Ok, Self::Error>
    where
        T: Serialize,
    {
        Err(Error::InvalidDocumentFormat)
    }

    fn serialize_newtype_variant<T: ?Sized>(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _value: &T,
    ) -> Result<Self::Ok, Self::Error>
    where
        T: Serialize,
    {
        Err(Error::InvalidDocumentFormat)
    }

    fn serialize_seq(self, _len: Option<usize>) -> Result<Self::SerializeSeq, Self::Error> {
        Err(Error::InvalidDocumentFormat)
    }

    fn serialize_tuple(self, _len: usize) -> Result<Self::SerializeTuple, Self::Error> {
        Err(Error::InvalidDocumentFormat)
    }

    fn serialize_tuple_struct(
        self,
        _name: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleStruct, Self::Error> {
        Err(Error::InvalidDocumentFormat)
    }

    fn serialize_tuple_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleVariant, Self::Error> {
        Err(Error::InvalidDocumentFormat)
    }

    fn serialize_map(self, _len: Option<usize>) -> Result<Self::SerializeMap, Self::Error> {
        Err(Error::InvalidDocumentFormat)
    }

    fn serialize_struct(
        self,
        _name: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStruct, Self::Error> {
        Err(Error::InvalidDocumentFormat)
    }

    fn serialize_struct_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStructVariant, Self::Error> {
        Err(Error::InvalidDocumentFormat)
    }
}

impl serde::ser::Error for Error {
    fn custom<T: fmt::Display>(msg: T) -> Self {
        Error::Custom(msg.to_string())
    }
}
