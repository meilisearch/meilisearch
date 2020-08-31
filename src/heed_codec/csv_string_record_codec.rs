use std::borrow::Cow;
use csv::{StringRecord, Writer, ReaderBuilder};

pub struct CsvStringRecordCodec;

impl heed::BytesDecode<'_> for CsvStringRecordCodec {
    type DItem = StringRecord;

    fn bytes_decode(bytes: &[u8]) -> Option<Self::DItem> {
        let mut reader = ReaderBuilder::new()
            .has_headers(false)
            .buffer_capacity(bytes.len()) // we will just read this record
            .from_reader(bytes);
        reader.records().next()?.ok() // it return an Option of Result
    }
}

impl heed::BytesEncode<'_> for CsvStringRecordCodec {
    type EItem = StringRecord;

    fn bytes_encode(item: &Self::EItem) -> Option<Cow<[u8]>> {
        let mut writer = Writer::from_writer(Vec::new());
        writer.write_record(item).ok()?;
        writer.into_inner().ok().map(Cow::Owned)
    }
}
