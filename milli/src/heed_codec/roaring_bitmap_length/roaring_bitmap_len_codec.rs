use std::io::{self, BufRead, Read};
use std::mem;

use byteorder::{LittleEndian, ReadBytesExt};

const SERIAL_COOKIE_NO_RUNCONTAINER: u32 = 12346;
const SERIAL_COOKIE: u16 = 12347;

pub struct RoaringBitmapLenCodec;

impl RoaringBitmapLenCodec {
    // FIXME should be exported in the RoaringBitmap crate
    fn deserialize_from_slice(mut bytes: &[u8]) -> io::Result<u64> {
        let (size, has_offsets) = {
            let cookie = bytes.read_u32::<LittleEndian>()?;
            if cookie == SERIAL_COOKIE_NO_RUNCONTAINER {
                (bytes.read_u32::<LittleEndian>()? as usize, true)
            } else if (cookie as u16) == SERIAL_COOKIE {
                return Err(io::Error::new(io::ErrorKind::Other, "run containers are unsupported"));
            } else {
                return Err(io::Error::new(io::ErrorKind::Other, "unknown cookie value"));
            }
        };

        if size > u16::max_value() as usize + 1 {
            return Err(io::Error::new(io::ErrorKind::Other, "size is greater than supported"));
        }

        let mut description_bytes = vec![0u8; size * 4];
        bytes.read_exact(&mut description_bytes)?;
        let description_bytes = &mut &description_bytes[..];

        if has_offsets {
            bytes.consume(size * 4);
        }

        let mut length = 0;
        for _ in 0..size {
            let _key = description_bytes.read_u16::<LittleEndian>()?;
            let len = u64::from(description_bytes.read_u16::<LittleEndian>()?) + 1;
            length += len;

            if len <= 4096 {
                bytes.consume(len as usize * mem::size_of::<u16>());
            } else {
                bytes.consume(1024 * mem::size_of::<u64>())
            }
        }

        Ok(length)
    }
}

impl heed::BytesDecode<'_> for RoaringBitmapLenCodec {
    type DItem = u64;

    fn bytes_decode(bytes: &[u8]) -> Option<Self::DItem> {
        RoaringBitmapLenCodec::deserialize_from_slice(bytes).ok()
    }
}

#[cfg(test)]
mod tests {
    use heed::BytesEncode;
    use roaring::RoaringBitmap;

    use super::*;
    use crate::heed_codec::RoaringBitmapCodec;

    #[test]
    fn deserialize_roaring_bitmap_length() {
        let bitmap: RoaringBitmap = (0..500).chain(800..800_000).chain(920_056..930_032).collect();
        let bytes = RoaringBitmapCodec::bytes_encode(&bitmap).unwrap();
        let len = RoaringBitmapLenCodec::deserialize_from_slice(&bytes).unwrap();
        assert_eq!(bitmap.len(), len);
    }
}
