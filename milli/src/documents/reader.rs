use std::io;

use flate2::bufread::GzDecoder;
use serde_json::de::IoRead;
use serde_json::{Deserializer, StreamDeserializer};

use crate::Object;

/// The `DocumentsBatchReader` provides a way to iterate over documents that have been created with
/// a `DocumentsBatchWriter`.
///
/// The documents are returned in the form of `obkv::Reader` where each field is identified with a
/// `FieldId`. The mapping between the field ids and the field names is done thanks to the index.
pub struct DocumentsBatchReader<R: io::BufRead>(
    StreamDeserializer<'static, IoRead<GzDecoder<R>>, Object>,
);

impl<R: io::BufRead> DocumentsBatchReader<R> {
    /// Construct a `DocumentsReader` from a reader.
    pub fn new(reader: R) -> DocumentsBatchReader<R> {
        let decoder = GzDecoder::new(reader);
        let deserializer = Deserializer::from_reader(decoder);
        let iterator = deserializer.into_iter();
        DocumentsBatchReader(iterator)
    }
}

impl<R: io::BufRead> Iterator for DocumentsBatchReader<R> {
    type Item = serde_json::Result<Object>;

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next()
    }
}
