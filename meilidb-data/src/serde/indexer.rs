use meilidb_core::DocumentId;
use serde::ser;
use serde::Serialize;

use crate::indexer::Indexer as RawIndexer;
use crate::schema::SchemaAttr;
use super::{SerializerError, ConvertToString};

pub struct Indexer<'a> {
    pub attribute: SchemaAttr,
    pub indexer: &'a mut RawIndexer,
    pub document_id: DocumentId,
}

impl<'a> ser::Serializer for Indexer<'a> {
    type Ok = ();
    type Error = SerializerError;
    type SerializeSeq = SeqIndexer<'a>;
    type SerializeTuple = TupleIndexer<'a>;
    type SerializeTupleStruct = ser::Impossible<Self::Ok, Self::Error>;
    type SerializeTupleVariant = ser::Impossible<Self::Ok, Self::Error>;
    type SerializeMap = MapIndexer<'a>;
    type SerializeStruct = StructSerializer<'a>;
    type SerializeStructVariant = ser::Impossible<Self::Ok, Self::Error>;

    fn serialize_bool(self, _value: bool) -> Result<Self::Ok, Self::Error> {
        Err(SerializerError::UnindexableType { type_name: "boolean" })
    }

    fn serialize_char(self, value: char) -> Result<Self::Ok, Self::Error> {
        let text = value.serialize(ConvertToString)?;
        self.serialize_str(&text)
    }

    fn serialize_i8(self, value: i8) -> Result<Self::Ok, Self::Error> {
        let text = value.serialize(ConvertToString)?;
        self.serialize_str(&text)
    }

    fn serialize_i16(self, value: i16) -> Result<Self::Ok, Self::Error> {
        let text = value.serialize(ConvertToString)?;
        self.serialize_str(&text)
    }

    fn serialize_i32(self, value: i32) -> Result<Self::Ok, Self::Error> {
        let text = value.serialize(ConvertToString)?;
        self.serialize_str(&text)
    }

    fn serialize_i64(self, value: i64) -> Result<Self::Ok, Self::Error> {
        let text = value.serialize(ConvertToString)?;
        self.serialize_str(&text)
    }

    fn serialize_u8(self, value: u8) -> Result<Self::Ok, Self::Error> {
        let text = value.serialize(ConvertToString)?;
        self.serialize_str(&text)
    }

    fn serialize_u16(self, value: u16) -> Result<Self::Ok, Self::Error> {
        let text = value.serialize(ConvertToString)?;
        self.serialize_str(&text)
    }

    fn serialize_u32(self, value: u32) -> Result<Self::Ok, Self::Error> {
        let text = value.serialize(ConvertToString)?;
        self.serialize_str(&text)
    }

    fn serialize_u64(self, value: u64) -> Result<Self::Ok, Self::Error> {
        let text = value.serialize(ConvertToString)?;
        self.serialize_str(&text)
    }

    fn serialize_f32(self, value: f32) -> Result<Self::Ok, Self::Error> {
        let text = value.serialize(ConvertToString)?;
        self.serialize_str(&text)
    }

    fn serialize_f64(self, value: f64) -> Result<Self::Ok, Self::Error> {
        let text = value.serialize(ConvertToString)?;
        self.serialize_str(&text)
    }

    fn serialize_str(self, text: &str) -> Result<Self::Ok, Self::Error> {
        self.indexer.index_text(self.document_id, self.attribute, text);
        Ok(())
    }

    fn serialize_bytes(self, _v: &[u8]) -> Result<Self::Ok, Self::Error> {
        Err(SerializerError::UnindexableType { type_name: "&[u8]" })
    }

    fn serialize_none(self) -> Result<Self::Ok, Self::Error> {
        Err(SerializerError::UnindexableType { type_name: "Option" })
    }

    fn serialize_some<T: ?Sized>(self, value: &T) -> Result<Self::Ok, Self::Error>
    where T: ser::Serialize,
    {
        let text = value.serialize(ConvertToString)?;
        self.indexer.index_text(self.document_id, self.attribute, &text);
        Ok(())
    }

    fn serialize_unit(self) -> Result<Self::Ok, Self::Error> {
        Err(SerializerError::UnindexableType { type_name: "()" })
    }

    fn serialize_unit_struct(self, _name: &'static str) -> Result<Self::Ok, Self::Error> {
        Err(SerializerError::UnindexableType { type_name: "unit struct" })
    }

    fn serialize_unit_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str
    ) -> Result<Self::Ok, Self::Error>
    {
        Err(SerializerError::UnindexableType { type_name: "unit variant" })
    }

    fn serialize_newtype_struct<T: ?Sized>(
        self,
        _name: &'static str,
        value: &T
    ) -> Result<Self::Ok, Self::Error>
    where T: ser::Serialize,
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
    where T: ser::Serialize,
    {
        Err(SerializerError::UnindexableType { type_name: "newtype variant" })
    }

    fn serialize_seq(self, _len: Option<usize>) -> Result<Self::SerializeSeq, Self::Error> {
        let indexer = SeqIndexer {
            attribute: self.attribute,
            document_id: self.document_id,
            indexer: self.indexer,
            texts: Vec::new(),
        };

        Ok(indexer)
    }

    fn serialize_tuple(self, _len: usize) -> Result<Self::SerializeTuple, Self::Error> {
        let indexer = TupleIndexer {
            attribute: self.attribute,
            document_id: self.document_id,
            indexer: self.indexer,
            texts: Vec::new(),
        };

        Ok(indexer)
    }

    fn serialize_tuple_struct(
        self,
        _name: &'static str,
        _len: usize
    ) -> Result<Self::SerializeTupleStruct, Self::Error>
    {
        Err(SerializerError::UnindexableType { type_name: "tuple struct" })
    }

    fn serialize_tuple_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _len: usize
    ) -> Result<Self::SerializeTupleVariant, Self::Error>
    {
        Err(SerializerError::UnindexableType { type_name: "tuple variant" })
    }

    fn serialize_map(self, _len: Option<usize>) -> Result<Self::SerializeMap, Self::Error> {
        let indexer = MapIndexer {
            attribute: self.attribute,
            document_id: self.document_id,
            indexer: self.indexer,
            texts: Vec::new(),
        };

        Ok(indexer)
    }

    fn serialize_struct(
        self,
        _name: &'static str,
        _len: usize
    ) -> Result<Self::SerializeStruct, Self::Error>
    {
        Err(SerializerError::UnindexableType { type_name: "struct" })
    }

    fn serialize_struct_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _len: usize
    ) -> Result<Self::SerializeStructVariant, Self::Error>
    {
        Err(SerializerError::UnindexableType { type_name: "struct variant" })
    }
}

pub struct SeqIndexer<'a> {
    attribute: SchemaAttr,
    document_id: DocumentId,
    indexer: &'a mut RawIndexer,
    texts: Vec<String>,
}

impl<'a> ser::SerializeSeq for SeqIndexer<'a> {
    type Ok = ();
    type Error = SerializerError;

    fn serialize_element<T: ?Sized>(&mut self, value: &T) -> Result<(), Self::Error>
    where T: ser::Serialize
    {
        let text = value.serialize(ConvertToString)?;
        self.texts.push(text);
        Ok(())
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        let texts = self.texts.iter().map(String::as_str);
        self.indexer.index_text_seq(self.document_id, self.attribute, texts);
        Ok(())
    }
}

pub struct MapIndexer<'a> {
    attribute: SchemaAttr,
    document_id: DocumentId,
    indexer: &'a mut RawIndexer,
    texts: Vec<String>,
}

impl<'a> ser::SerializeMap for MapIndexer<'a> {
    type Ok = ();
    type Error = SerializerError;

    fn serialize_key<T: ?Sized>(&mut self, key: &T) -> Result<(), Self::Error>
    where T: ser::Serialize,
    {
        let text = key.serialize(ConvertToString)?;
        self.texts.push(text);
        Ok(())
    }

    fn serialize_value<T: ?Sized>(&mut self, value: &T) -> Result<(), Self::Error>
    where T: ser::Serialize,
    {
        let text = value.serialize(ConvertToString)?;
        self.texts.push(text);
        Ok(())
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        let texts = self.texts.iter().map(String::as_str);
        self.indexer.index_text_seq(self.document_id, self.attribute, texts);
        Ok(())
    }
}

pub struct StructSerializer<'a> {
    attribute: SchemaAttr,
    document_id: DocumentId,
    indexer: &'a mut RawIndexer,
    texts: Vec<String>,
}

impl<'a> ser::SerializeStruct for StructSerializer<'a> {
    type Ok = ();
    type Error = SerializerError;

    fn serialize_field<T: ?Sized>(
        &mut self,
        key: &'static str,
        value: &T,
    ) -> Result<(), Self::Error>
    where T: ser::Serialize,
    {
        let key_text = key.to_owned();
        let value_text = value.serialize(ConvertToString)?;
        self.texts.push(key_text);
        self.texts.push(value_text);
        Ok(())
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        let texts = self.texts.iter().map(String::as_str);
        self.indexer.index_text_seq(self.document_id, self.attribute, texts);
        Ok(())
    }
}

pub struct TupleIndexer<'a> {
    attribute: SchemaAttr,
    document_id: DocumentId,
    indexer: &'a mut RawIndexer,
    texts: Vec<String>,
}

impl<'a> ser::SerializeTuple for TupleIndexer<'a> {
    type Ok = ();
    type Error = SerializerError;

    fn serialize_element<T: ?Sized>(&mut self, value: &T) -> Result<(), Self::Error>
    where T: Serialize
    {
        let text = value.serialize(ConvertToString)?;
        self.texts.push(text);
        Ok(())
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        let texts = self.texts.iter().map(String::as_str);
        self.indexer.index_text_seq(self.document_id, self.attribute, texts);
        Ok(())
    }
}
