use std::io::{self, BufRead, Read};
use std::mem;

use byteorder::{LittleEndian, ReadBytesExt};
use heed::BoxedError;

use crate::heed_codec::BytesDecodeOwned;

const SERIAL_COOKIE_NO_RUNCONTAINER: u32 = 12346;
const SERIAL_COOKIE: u16 = 12347;
const NO_OFFSET_THRESHOLD: usize = 4;

pub struct RoaringBitmapLenCodec;

impl RoaringBitmapLenCodec {
    // FIXME should be exported in the RoaringBitmap crate
    // From RoaringBitmap specification
    // https://github.com/RoaringBitmap/RoaringFormatSpec
    fn deserialize_from_slice(mut bytes: &[u8]) -> io::Result<u64> {
        let (size, has_offsets, run_bitmap) = {
            let cookie = bytes.read_u32::<LittleEndian>()?;
            if cookie == SERIAL_COOKIE_NO_RUNCONTAINER {
                (bytes.read_u32::<LittleEndian>()? as usize, true, None)
            } else if (cookie as u16) == SERIAL_COOKIE {
                let size = ((cookie >> 16) as usize) + 1;
                let run_bitmap_size = (size + 7) / 8;
                let mut run_bitmap = vec![0u8; run_bitmap_size];
                bytes.read_exact(&mut run_bitmap)?;
                (size, size >= NO_OFFSET_THRESHOLD, Some(run_bitmap))
            } else {
                // TODO: temporary fix since there are some
                // locations in the codebase that are still writing
                // data in lmdb byte order format rather than writing in roaring bitmap format
                // Some merge operations produce byte order data that reach this code
                let mut entire_bytes = Vec::with_capacity(mem::size_of::<u32>() + bytes.len());
                entire_bytes.extend_from_slice(&cookie.to_le_bytes());
                entire_bytes.extend_from_slice(bytes);
                return Ok((entire_bytes.len() / mem::size_of::<u32>()) as u64);
                // return Err(io::Error::new(io::ErrorKind::Other, "unknown cookie value"));
            }
        };

        if size > u16::MAX as usize + 1 {
            return Err(io::Error::other("size is greater than supported"));
        }

        let mut description_bytes = vec![0u8; size * 4];
        bytes.read_exact(&mut description_bytes)?;
        let description_bytes = &mut &description_bytes[..];

        if has_offsets {
            bytes.consume(size * 4);
        }

        let mut length = 0;
        for i in 0..size {
            let _key = description_bytes.read_u16::<LittleEndian>()?;
            let len = u64::from(description_bytes.read_u16::<LittleEndian>()?) + 1;
            length += len;

            let is_run_container = if let Some(ref run_bitmap) = run_bitmap {
                let container_index = i / 8;
                let offset = i % 8;
                run_bitmap.get(container_index).map(|byte| (byte >> offset) & 1 != 0).ok_or(
                    io::Error::new(io::ErrorKind::Other, "bitmap for run container is flawed"),
                )?
            } else {
                false
            };

            if is_run_container {
                let num_runs = bytes.read_u16::<LittleEndian>()?;
                bytes.consume(num_runs as usize * 2 * mem::size_of::<u16>());
            } else if len <= 4096 {
                // Array container
                bytes.consume(len as usize * mem::size_of::<u16>());
            } else {
                // Bitset container
                bytes.consume(1024 * mem::size_of::<u64>());
            }
        }

        Ok(length)
    }
}

impl heed::BytesDecode<'_> for RoaringBitmapLenCodec {
    type DItem = u64;

    fn bytes_decode(bytes: &[u8]) -> Result<Self::DItem, BoxedError> {
        RoaringBitmapLenCodec::deserialize_from_slice(bytes).map_err(Into::into)
    }
}

impl BytesDecodeOwned for RoaringBitmapLenCodec {
    type DItem = u64;

    fn bytes_decode_owned(bytes: &[u8]) -> Result<Self::DItem, BoxedError> {
        RoaringBitmapLenCodec::deserialize_from_slice(bytes).map_err(Into::into)
    }
}

#[cfg(test)]
mod tests {
    use std::u16;

    use heed::BytesEncode;
    use roaring::RoaringBitmap;

    use super::*;
    use crate::heed_codec::RoaringBitmapCodec;

    #[test]
    fn deserialize_roaring_bitmap_length() {
        let bitmaps: Vec<RoaringBitmap> = vec![
            // With run containers
            (0..500).chain(800..800_000).chain(920_056..930_032).collect(),
            // No containers
            RoaringBitmap::new(),
            // No run containers with an array container
            (1..2).chain(900_000..900_005).collect(),
            // No run containers with bitset containers
            ((0..u16::MAX as u32)
                .step_by(2)
                .chain(u16::MAX as u32..2 * u16::MAX as u32)
                .step_by(2))
            .collect(),
        ];
        for bitmap in bitmaps {
            let bytes = RoaringBitmapCodec::bytes_encode(&bitmap).unwrap();
            let len = RoaringBitmapLenCodec::deserialize_from_slice(&bytes).unwrap();
            assert_eq!(bitmap.len(), len);
        }
    }
}
