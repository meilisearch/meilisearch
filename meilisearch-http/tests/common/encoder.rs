use std::io::{Read, Write};

use actix_http::header::TryIntoHeaderPair;
use bytes::Bytes;
use flate2::read::{GzDecoder, ZlibDecoder};
use flate2::write::{GzEncoder, ZlibEncoder};
use flate2::Compression;

#[derive(Clone, Copy)]
pub enum Encoder {
    Plain,
    Gzip,
    Deflate,
    Brotli,
}

impl Encoder {
    pub fn encode(self: &Encoder, body: impl Into<Bytes>) -> impl Into<Bytes> {
        match self {
            Self::Gzip => {
                let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
                encoder.write_all(&body.into()).expect("Failed to encode request body");
                encoder.finish().expect("Failed to encode request body")
            }
            Self::Deflate => {
                let mut encoder = ZlibEncoder::new(Vec::new(), Compression::default());
                encoder.write_all(&body.into()).expect("Failed to encode request body");
                encoder.finish().unwrap()
            }
            Self::Plain => Vec::from(body.into()),
            Self::Brotli => {
                let mut encoder = brotli::CompressorWriter::new(Vec::new(), 32 * 1024, 3, 22);
                encoder.write_all(&body.into()).expect("Failed to encode request body");
                encoder.flush().expect("Failed to encode request body");
                encoder.into_inner()
            }
        }
    }

    pub fn decode(self: &Encoder, bytes: impl Into<Bytes>) -> impl Into<Bytes> {
        let mut buffer = Vec::new();
        let input = bytes.into();
        match self {
            Self::Gzip => {
                GzDecoder::new(input.as_ref())
                    .read_to_end(&mut buffer)
                    .expect("Invalid gzip stream");
            }
            Self::Deflate => {
                ZlibDecoder::new(input.as_ref())
                    .read_to_end(&mut buffer)
                    .expect("Invalid zlib stream");
            }
            Self::Plain => {
                buffer.write_all(input.as_ref()).expect("Unexpected memory copying issue");
            }
            Self::Brotli => {
                brotli::Decompressor::new(input.as_ref(), 4096)
                    .read_to_end(&mut buffer)
                    .expect("Invalid brotli stream");
            }
        };
        buffer
    }

    pub fn header(self: &Encoder) -> Option<impl TryIntoHeaderPair> {
        match self {
            Self::Plain => None,
            Self::Gzip => Some(("Content-Encoding", "gzip")),
            Self::Deflate => Some(("Content-Encoding", "deflate")),
            Self::Brotli => Some(("Content-Encoding", "br")),
        }
    }

    pub fn iterator() -> impl Iterator<Item = Self> {
        [Self::Plain, Self::Gzip, Self::Deflate, Self::Brotli].iter().copied()
    }
}
