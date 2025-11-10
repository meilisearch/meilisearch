use std::{io, mem::size_of};

use bitpacking::{BitPacker, BitPacker1x, BitPacker4x, BitPacker8x};
use roaring::RoaringBitmap;

/// The magic header for our custom encoding format
const MAGIC_HEADER: u8 = 178;

pub struct DeBitmapCodec;

// TODO reintroduce:
//  - serialized_size?
//  - serialize_into_vec
//  - intersection_with_serialized
//  - merge_into
//  - merge_deladd_into
impl DeBitmapCodec {
    pub fn serialize_into<W: io::Write>(bitmap: &RoaringBitmap, writer: W) -> io::Result<()> {
        let mut tmp_buffer = Vec::new();
        Self::serialize_into_with_tmp_buffer(bitmap, writer, &mut tmp_buffer)
    }

    /// Returns the delta-encoded compressed version of the given roaring bitmap.
    pub fn serialize_into_with_tmp_buffer<W: io::Write>(
        bitmap: &RoaringBitmap,
        mut writer: W,
        tmp_buffer: &mut Vec<u32>,
    ) -> io::Result<()> {
        // Insert the magic header
        // TODO switch to a native endian u16
        writer.write_all(&[MAGIC_HEADER])?;

        let bitpacker8x = BitPacker8x::new();
        let bitpacker4x = BitPacker4x::new();
        let bitpacker1x = BitPacker1x::new();

        // This temporary buffer is used to store each chunk of decompressed and
        // compressed and delta-encoded u32s. We need room for the decompressed
        // u32s coming from the roaring bitmap, the compressed output that can
        // be as large as the decompressed u32s, and the chunk header.
        tmp_buffer.resize((BitPacker8x::BLOCK_LEN * 2) + 1, 0u32);
        let (decompressed, compressed) = tmp_buffer.split_at_mut(BitPacker8x::BLOCK_LEN);
        let compressed = bytemuck::cast_slice_mut(compressed);

        let mut buffer_index = 0;
        let mut initial = None;
        // We initially collect all the integers into a flat buffer of the size
        // of the largest bitpacker. We encode them with it until we don't have
        // enough of them...
        for n in bitmap {
            decompressed[buffer_index] = n;
            buffer_index += 1;
            if buffer_index == BitPacker8x::BLOCK_LEN {
                let output = encode_with_packer(&bitpacker8x, decompressed, initial, compressed);
                writer.write_all(output)?;
                initial = Some(n);
                buffer_index = 0;
            }
        }

        // ...We then switch to a smaller bitpacker to encode the remaining chunks...
        let decompressed = &decompressed[..buffer_index];
        let mut chunks = decompressed.chunks_exact(BitPacker4x::BLOCK_LEN);
        for decompressed in chunks.by_ref() {
            let output = encode_with_packer(&bitpacker4x, &decompressed, initial, compressed);
            writer.write_all(output)?;
            initial = decompressed.iter().last().copied();
        }

        // ...And so on...
        let decompressed = chunks.remainder();
        let mut chunks = decompressed.chunks_exact(BitPacker1x::BLOCK_LEN);
        for decompressed in chunks.by_ref() {
            let output = encode_with_packer(&bitpacker1x, &decompressed, initial, compressed);
            writer.write_all(output)?;
            initial = decompressed.iter().last().copied();
        }

        // Until we don't have any small enough bitpacker. We put them raw
        // at the end of out buffer with a header indicating the matter.
        let decompressed = chunks.remainder();
        if !decompressed.is_empty() {
            let header = encode_bitpacker_level_and_num_bits(BitPackerLevel::None, u32::BITS as u8);
            // Note: Not convinced about the performance of writing a single
            //       byte followed by a larger write. However, we will use this
            //       codec with a BufWriter or directly with a Vec of bytes.
            writer.write_all(&[header])?;
            writer.write_all(bytemuck::cast_slice(decompressed))?;
        }

        Ok(())
    }

    pub fn deserialize_from(compressed: &[u8]) -> io::Result<RoaringBitmap> {
        let mut tmp_buffer = Vec::new();
        Self::deserialize_from_with_tmp_buffer(compressed, &mut tmp_buffer)
    }

    // TODO do not panic and return error messages
    pub fn deserialize_from_with_tmp_buffer(
        compressed: &[u8],
        tmp_buffer: &mut Vec<u32>,
    ) -> io::Result<RoaringBitmap> {
        let (&header, mut compressed) =
            compressed.split_first().expect("compressed must not be empty");

        assert_eq!(
            header, MAGIC_HEADER,
            "Invalid header. Found 0x{:x}, expecting 0x{:x}",
            header, MAGIC_HEADER
        );

        let bitpacker8x = BitPacker8x::new();
        let bitpacker4x = BitPacker4x::new();
        let bitpacker1x = BitPacker1x::new();

        let mut bitmap = RoaringBitmap::new();
        tmp_buffer.resize(BitPacker8x::BLOCK_LEN, 0u32);
        let decompressed = &mut tmp_buffer[..];
        let mut initial = None;

        while let Some((&chunk_header, encoded)) = compressed.split_first() {
            let (level, num_bits) = decode_bitpacker_level_and_num_bits(chunk_header);
            let (bytes_read, decompressed) = match level {
                BitPackerLevel::None => {
                    assert_eq!(num_bits, u32::BITS as u8);

                    let integers = encoded
                        .chunks_exact(size_of::<u32>())
                        // safety: This unwrap cannot happen as
                        //         the size of u32 is set correctly.
                        .map(|b| b.try_into().unwrap())
                        .map(u32::from_ne_bytes);

                    // TODO: It is possible that a bad encoding generates
                    //       non-strictly sorted integers.
                    bitmap.append(integers).unwrap();

                    // This is basically always the last chunk that exists in
                    // this delta-encoded format as the raw u32s are appended
                    // when there is not enough of them to fit in a bitpacker.
                    break;
                }
                BitPackerLevel::BitPacker1x => {
                    decode_with_packer(&bitpacker1x, decompressed, initial, encoded, num_bits)
                }
                BitPackerLevel::BitPacker4x => {
                    decode_with_packer(&bitpacker4x, decompressed, initial, encoded, num_bits)
                }
                BitPackerLevel::BitPacker8x => {
                    decode_with_packer(&bitpacker8x, decompressed, initial, encoded, num_bits)
                }
            };

            initial = decompressed.iter().last().copied();
            // TODO investigate perf
            // QUESTION: Is it possible that a bad encoding generates
            //           non-strictly sorted integers? I don't think so.
            bitmap.append(decompressed.iter().copied()).unwrap();
            // What the delta-decoding read plus the chunk header size
            compressed = &compressed[bytes_read + 1..];
        }

        Ok(bitmap)
    }
}

/// Takes a strickly sorted list of u32s and outputs delta-encoded bytes
/// with a chunk header.
fn encode_with_packer<'c, B: BitPackerExt>(
    bitpacker: &B,
    decompressed: &[u32],
    initial: Option<u32>,
    output: &'c mut [u8],
) -> &'c [u8] {
    let num_bits = bitpacker.num_bits_strictly_sorted(initial, decompressed);
    let compressed_len = B::compressed_block_size(num_bits);
    let chunk_header = encode_bitpacker_level_and_num_bits(B::level(), num_bits);
    let buffer = &mut output[..compressed_len + 1];
    let (header_in_buffer, encoded) = buffer.split_first_mut().unwrap();
    *header_in_buffer = chunk_header;
    bitpacker.compress_strictly_sorted(initial, decompressed, encoded, num_bits);
    buffer
}

/// Returns the number of bytes read and the decoded unsigned integers.
fn decode_with_packer<'d, B: BitPacker>(
    bitpacker: &B,
    decompressed: &'d mut [u32],
    initial: Option<u32>,
    compressed: &[u8],
    num_bits: u8,
) -> (usize, &'d [u32]) {
    let decompressed = &mut decompressed[..B::BLOCK_LEN];
    let read = bitpacker.decompress_strictly_sorted(initial, compressed, decompressed, num_bits);
    (read, decompressed)
}

#[derive(Debug, PartialEq, Eq)]
#[repr(u8)]
enum BitPackerLevel {
    /// The remaining bytes are raw little endian encoded u32s.
    None,
    /// The remaining bits are encoded using a `BitPacker1x`.
    BitPacker1x,
    /// The remaining bits are encoded using a `BitPacker4x`.
    BitPacker4x,
    /// The remaining bits are encoded using a `BitPacker8x`.
    BitPacker8x,
}

// TODO: never panic in this function and rather return a result
fn encode_bitpacker_level_and_num_bits(level: BitPackerLevel, num_bits: u8) -> u8 {
    assert!(num_bits as u32 <= 2_u32.pow(6));
    let level = level as u8;
    assert!(level <= 3);
    num_bits | (level << 6)
}

// TODO: never panic in this function and rather return a result
fn decode_bitpacker_level_and_num_bits(data: u8) -> (BitPackerLevel, u8) {
    let num_bits = data & 0b00111111;
    let level = match data >> 6 {
        0 => BitPackerLevel::None,
        1 => BitPackerLevel::BitPacker1x,
        2 => BitPackerLevel::BitPacker4x,
        3 => BitPackerLevel::BitPacker8x,
        invalid => panic!("Invalid bitpacker level: {invalid}"),
    };
    assert!(num_bits as u32 <= 2_u32.pow(6));
    (level, num_bits)
}

trait BitPackerExt: BitPacker {
    fn level() -> BitPackerLevel;
}

impl BitPackerExt for BitPacker8x {
    fn level() -> BitPackerLevel {
        BitPackerLevel::BitPacker8x
    }
}

impl BitPackerExt for BitPacker4x {
    fn level() -> BitPackerLevel {
        BitPackerLevel::BitPacker4x
    }
}

impl BitPackerExt for BitPacker1x {
    fn level() -> BitPackerLevel {
        BitPackerLevel::BitPacker1x
    }
}

#[cfg(test)]
mod tests {
    use quickcheck::quickcheck;
    use roaring::RoaringBitmap;

    use crate::heed_codec::de_bitmap::DeBitmapCodec;

    quickcheck! {
        fn qc_random(xs: Vec<u32>) -> bool {
            let bitmap = RoaringBitmap::from_iter(xs);
            let mut compressed = Vec::new();
            DeBitmapCodec::serialize_into(&bitmap, &mut compressed).unwrap();
            let decompressed = DeBitmapCodec::deserialize_from(&compressed[..]).unwrap();
            decompressed == bitmap
        }
    }
}
