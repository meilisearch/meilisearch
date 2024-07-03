use std::borrow::Cow;
use std::io;
use std::io::ErrorKind;

use heed::BoxedError;
use obkv::KvReaderU16;
use zstd::bulk::{Compressor, Decompressor};
use zstd::dict::{DecoderDictionary, EncoderDictionary};

// TODO move that elsewhere
pub const COMPRESSION_LEVEL: i32 = 12;

pub struct CompressedObkvCodec;

impl<'a> heed::BytesDecode<'a> for CompressedObkvCodec {
    type DItem = CompressedKvReaderU16<'a>;

    fn bytes_decode(bytes: &'a [u8]) -> Result<Self::DItem, BoxedError> {
        Ok(CompressedKvReaderU16(bytes))
    }
}

impl heed::BytesEncode<'_> for CompressedObkvCodec {
    type EItem = CompressedKvWriterU16;

    fn bytes_encode(item: &Self::EItem) -> Result<Cow<[u8]>, BoxedError> {
        Ok(Cow::Borrowed(&item.0))
    }
}

pub struct CompressedKvReaderU16<'a>(&'a [u8]);

impl<'a> CompressedKvReaderU16<'a> {
    /// Decompresses the KvReader into the buffer using the provided dictionnary.
    pub fn decompress_with<'b>(
        &self,
        buffer: &'b mut Vec<u8>,
        dictionary: &DecoderDictionary,
    ) -> io::Result<KvReaderU16<'b>> {
        const TWO_GIGABYTES: usize = 2 * 1024 * 1024 * 1024;

        let mut decompressor = Decompressor::with_prepared_dictionary(dictionary)?;
        let mut max_size = self.0.len() * 4;
        let size = loop {
            buffer.resize(max_size, 0);
            match decompressor.decompress_to_buffer(self.0, &mut buffer[..max_size]) {
                Ok(size) => break size,
                // TODO don't do that !!! But what should I do?
                Err(e) if e.kind() == ErrorKind::Other && max_size <= TWO_GIGABYTES => {
                    max_size *= 2
                }
                Err(e) => return Err(e),
            }
        };
        Ok(KvReaderU16::new(&buffer[..size]))
    }

    /// Returns the KvReader like it is not compressed.
    /// Happends when there is no dictionary yet.
    pub fn as_non_compressed(&self) -> KvReaderU16<'a> {
        KvReaderU16::new(self.0)
    }

    /// Decompresses this KvReader if necessary.
    pub fn decompress_with_optional_dictionary<'b>(
        &'b self,
        buffer: &'b mut Vec<u8>,
        dictionary: Option<&DecoderDictionary>,
    ) -> io::Result<KvReaderU16<'b>> {
        match dictionary {
            Some(dict) => self.decompress_with(buffer, dict),
            None => Ok(self.as_non_compressed()),
        }
    }
}

pub struct CompressedKvWriterU16(Vec<u8>);

impl CompressedKvWriterU16 {
    // TODO ask for a KvReaderU16 here
    pub fn new_with_dictionary(input: &[u8], dictionary: &EncoderDictionary) -> io::Result<Self> {
        let mut compressor = Compressor::with_prepared_dictionary(dictionary)?;
        compressor.compress(input).map(CompressedKvWriterU16)
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}
