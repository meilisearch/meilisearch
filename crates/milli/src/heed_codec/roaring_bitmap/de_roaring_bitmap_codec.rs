use std::io::{self, ErrorKind};
use std::mem::{self, size_of, size_of_val};

use bitpacking::{BitPacker, BitPacker1x, BitPacker4x, BitPacker8x};
use roaring::RoaringBitmap;

/// The magic header for our custom encoding format
const MAGIC_HEADER: u16 = 36869;

pub struct DeRoaringBitmapCodec;

// TODO reintroduce:
//  - serialized_size?
//  - serialize_into_vec
//  - intersection_with_serialized
//  - merge_into
//  - merge_deladd_into
impl DeRoaringBitmapCodec {
    /// Returns the serialized size of the given roaring bitmap with the delta encoding format.
    pub fn serialized_size_with_tmp_buffer(
        bitmap: &RoaringBitmap,
        tmp_buffer: &mut Vec<u32>,
    ) -> usize {
        let mut size = 2; // u16 magic header

        let bitpacker8x = BitPacker8x::new();
        let bitpacker4x = BitPacker4x::new();
        let bitpacker1x = BitPacker1x::new();

        // This temporary buffer is used to store each chunk of decompressed u32s.
        tmp_buffer.resize(BitPacker8x::BLOCK_LEN, 0u32);
        let decompressed = &mut tmp_buffer[..];

        let mut buffer_index = 0;
        let mut initial = None;
        // We initially collect all the integers into a flat buffer of the size
        // of the largest bitpacker. We encode them with it until we don't have
        // enough of them...
        for n in bitmap {
            decompressed[buffer_index] = n;
            buffer_index += 1;
            if buffer_index == BitPacker8x::BLOCK_LEN {
                let num_bits = bitpacker8x.num_bits_strictly_sorted(initial, decompressed);
                let compressed_len = BitPacker8x::compressed_block_size(num_bits);
                size += 1; // u8 chunk header
                size += compressed_len; // compressed data length
                initial = Some(n);
                buffer_index = 0;
            }
        }

        // ...We then switch to a smaller bitpacker to encode the remaining chunks...
        let decompressed = &decompressed[..buffer_index];
        let mut chunks = decompressed.chunks_exact(BitPacker4x::BLOCK_LEN);
        for decompressed in chunks.by_ref() {
            let num_bits = bitpacker4x.num_bits_strictly_sorted(initial, decompressed);
            let compressed_len = BitPacker4x::compressed_block_size(num_bits);
            size += 1; // u8 chunk header
            size += compressed_len; // compressed data length
            initial = decompressed.iter().last().copied();
        }

        // ...And so on...
        let decompressed = chunks.remainder();
        let mut chunks = decompressed.chunks_exact(BitPacker1x::BLOCK_LEN);
        for decompressed in chunks.by_ref() {
            let num_bits = bitpacker1x.num_bits_strictly_sorted(initial, decompressed);
            let compressed_len = BitPacker1x::compressed_block_size(num_bits);
            size += 1; // u8 chunk header
            size += compressed_len; // compressed data length
            initial = decompressed.iter().last().copied();
        }

        // ...Until we don't have any small enough bitpacker. We put them raw
        // at the end of out buffer with a header indicating the matter.
        let decompressed = chunks.remainder();
        if !decompressed.is_empty() {
            size += 1; // u8 chunk header
            size += mem::size_of_val(decompressed); // remaining uncompressed u32s
        }

        size
    }

    /// Writes the delta-encoded compressed version of the given roaring bitmap
    /// into the provided writer. Accepts a buffer to avoid allocating one.
    pub fn serialize_into_with_tmp_buffer<W: io::Write>(
        bitmap: &RoaringBitmap,
        mut writer: W,
        tmp_buffer: &mut Vec<u32>,
    ) -> io::Result<()> {
        // Insert the magic header
        writer.write_all(&MAGIC_HEADER.to_ne_bytes())?;

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
            let output = encode_with_packer(&bitpacker4x, decompressed, initial, compressed);
            writer.write_all(output)?;
            initial = decompressed.iter().last().copied();
        }

        // ...And so on...
        let decompressed = chunks.remainder();
        let mut chunks = decompressed.chunks_exact(BitPacker1x::BLOCK_LEN);
        for decompressed in chunks.by_ref() {
            let output = encode_with_packer(&bitpacker1x, decompressed, initial, compressed);
            writer.write_all(output)?;
            initial = decompressed.iter().last().copied();
        }

        // ...Until we don't have any small enough bitpacker. We put them raw
        // at the end of out buffer with a header indicating the matter.
        let decompressed = chunks.remainder();
        if !decompressed.is_empty() {
            let header = encode_chunk_header(BitPackerLevel::None, u32::BITS as u8);
            // Note: Not convinced about the performance of writing a single
            //       byte followed by a larger write. However, we will use this
            //       codec with a BufWriter or directly with a Vec of bytes.
            writer.write_all(&[header])?;
            writer.write_all(bytemuck::cast_slice(decompressed))?;
        }

        Ok(())
    }

    /// Same as [Self::deserialize_from] but accepts a buffer to avoid allocating one.
    ///
    /// The `filter_block` function is used to filter out blocks. It takes the first
    /// and last u32 values of a block and returns `true` if the block must be kept.
    pub fn deserialize_from_with_tmp_buffer<F>(
        input: &[u8],
        filter_block: F,
        tmp_buffer: &mut Vec<u32>,
    ) -> io::Result<RoaringBitmap>
    where
        F: Fn(u32, u32) -> bool,
    {
        let Some((header, mut compressed)) = input.split_at_checked(size_of_val(&MAGIC_HEADER))
        else {
            return Err(io::Error::new(ErrorKind::UnexpectedEof, "expecting a two-bytes header"));
        };

        // Safety: This unwrap cannot happen as the header buffer is the right size
        let header = u16::from_ne_bytes(header.try_into().unwrap());

        if header != MAGIC_HEADER {
            return Err(io::Error::other("invalid header value"));
        }

        let bitpacker8x = BitPacker8x::new();
        let bitpacker4x = BitPacker4x::new();
        let bitpacker1x = BitPacker1x::new();

        let mut bitmap = RoaringBitmap::new();
        tmp_buffer.resize(BitPacker8x::BLOCK_LEN, 0u32);
        let decompressed = &mut tmp_buffer[..];
        let mut initial = None;

        while let Some((&chunk_header, encoded)) = compressed.split_first() {
            let (level, num_bits) = decode_chunk_header(chunk_header);
            let (bytes_read, decompressed) = match level {
                BitPackerLevel::None => {
                    if num_bits != u32::BITS as u8 {
                        return Err(io::Error::new(
                            ErrorKind::InvalidData,
                            "invalid number of bits to encode non-compressed u32s",
                        ));
                    }

                    let chunks = encoded.chunks_exact(size_of::<u32>());
                    if !chunks.remainder().is_empty() {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "expecting last chunk to be a multiple of the size of an u32",
                        ));
                    }

                    let integers = chunks
                        // safety: This unwrap cannot happen as
                        //         the size of u32 is set correctly.
                        .map(|b| b.try_into().unwrap())
                        .map(u32::from_ne_bytes);

                    if let Some((first, last)) =
                        integers.clone().next().zip(integers.clone().next_back())
                    {
                        if !(filter_block)(first, last) {
                            bitmap
                                .append(integers)
                                .map_err(|e| io::Error::new(ErrorKind::InvalidData, e))?;
                        }
                    }

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
            if let Some((first, last)) = decompressed.first().copied().zip(initial) {
                if !(filter_block)(first, last) {
                    // TODO investigate perf
                    // Safety: Bitpackers cannot output unsorter integers when
                    //         used with the compress_strictly_sorted function.
                    bitmap.append(decompressed.iter().copied()).unwrap();
                }
            }
            // What the delta-decoding read plus the chunk header size
            compressed = &compressed[bytes_read + 1..];
        }

        Ok(bitmap)
    }

    /// Returns the length of the serialized DeRoaringBitmap.
    pub fn deserialize_length_from(input: &[u8]) -> io::Result<u64> {
        let Some((header, mut compressed)) = input.split_at_checked(size_of_val(&MAGIC_HEADER))
        else {
            return Err(io::Error::new(ErrorKind::UnexpectedEof, "expecting a two-bytes header"));
        };

        // Safety: This unwrap cannot happen as the header buffer is the right size
        let header = u16::from_ne_bytes(header.try_into().unwrap());

        if header != MAGIC_HEADER {
            return Err(io::Error::other("invalid header value"));
        }

        let mut length = 0;
        while let Some((&chunk_header, encoded)) = compressed.split_first() {
            let (level, num_bits) = decode_chunk_header(chunk_header);
            let bytes_read = match level {
                BitPackerLevel::None => {
                    if num_bits != u32::BITS as u8 {
                        return Err(io::Error::new(
                            ErrorKind::InvalidData,
                            "invalid number of bits to encode non-compressed u32s",
                        ));
                    }

                    let chunks = encoded.chunks_exact(size_of::<u32>());
                    if !chunks.remainder().is_empty() {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "expecting last chunk to be a multiple of the size of an u32",
                        ));
                    }

                    // This call is optimized for performance
                    // and will not iterate over the chunks.
                    length += chunks.count() as u64;

                    // This is basically always the last chunk that exists in
                    // this delta-encoded format as the raw u32s are appended
                    // when there is not enough of them to fit in a bitpacker.
                    break;
                }
                BitPackerLevel::BitPacker1x => {
                    length += BitPacker1x::BLOCK_LEN as u64;
                    BitPacker1x::compressed_block_size(num_bits)
                }
                BitPackerLevel::BitPacker4x => {
                    length += BitPacker4x::BLOCK_LEN as u64;
                    BitPacker4x::compressed_block_size(num_bits)
                }
                BitPackerLevel::BitPacker8x => {
                    length += BitPacker8x::BLOCK_LEN as u64;
                    BitPacker8x::compressed_block_size(num_bits)
                }
            };

            // What the delta-decoding read plus the chunk header size
            compressed = &compressed[bytes_read + 1..];
        }

        Ok(length)
    }
}

/// A utility function to take all blocks.
pub fn take_all_blocks(_first: u32, _last: u32) -> bool {
    false
}

/// Takes a strickly sorted list of u32s and outputs delta-encoded
/// bytes with a chunk header. We expect the output buffer to be
/// at least BLOCK_LEN + 1.
fn encode_with_packer<'c, B: BitPackerExt>(
    bitpacker: &B,
    decompressed: &[u32],
    initial: Option<u32>,
    output: &'c mut [u8],
) -> &'c [u8] {
    let num_bits = bitpacker.num_bits_strictly_sorted(initial, decompressed);
    let compressed_len = B::compressed_block_size(num_bits);
    let chunk_header = encode_chunk_header(B::level(), num_bits);
    let buffer = &mut output[..compressed_len + 1];
    // Safety: The buffer is at least one byte
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

/// An identifier for the bitpacker to be able
/// to correctly decode the compressed integers.
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

/// Returns the chunk header based on the bitpacker level
/// and the number of bits to encode the list of integers.
fn encode_chunk_header(level: BitPackerLevel, num_bits: u8) -> u8 {
    debug_assert!(num_bits as u32 <= 2_u32.pow(6));
    let level = level as u8;
    debug_assert!(level <= 3);
    num_bits | (level << 6)
}

/// Decodes the chunk header and output the bitpacker level
/// and the number of bits to decode the following bytes.
fn decode_chunk_header(data: u8) -> (BitPackerLevel, u8) {
    let num_bits = data & 0b00111111;
    let level = match data >> 6 {
        0 => BitPackerLevel::None,
        1 => BitPackerLevel::BitPacker1x,
        2 => BitPackerLevel::BitPacker4x,
        3 => BitPackerLevel::BitPacker8x,
        invalid => panic!("Invalid bitpacker level: {invalid}"),
    };
    debug_assert!(num_bits as u32 <= 2_u32.pow(6));
    (level, num_bits)
}

/// A simple helper trait to get the BitPackerLevel
/// and correctly generate the chunk header.
trait BitPackerExt: BitPacker {
    /// Returns the level of the bitpacker: an identifier to be
    /// able to decode the numbers with the right bitpacker.
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

    use super::{take_all_blocks, DeRoaringBitmapCodec};

    quickcheck! {
        fn qc_random(xs: Vec<u32>) -> bool {
            let bitmap = RoaringBitmap::from_iter(xs);
            let mut compressed = Vec::new();
            let mut tmp_buffer = Vec::new();
            DeRoaringBitmapCodec::serialize_into_with_tmp_buffer(&bitmap, &mut compressed, &mut tmp_buffer).unwrap();
            let length = DeRoaringBitmapCodec::deserialize_length_from(&compressed[..]).unwrap();
            let decompressed = DeRoaringBitmapCodec::deserialize_from_with_tmp_buffer(&compressed[..], take_all_blocks, &mut tmp_buffer).unwrap();
            length == bitmap.len() && decompressed == bitmap
        }
    }

    quickcheck! {
        fn qc_random_check_serialized_size(xs: Vec<u32>) -> bool {
            let bitmap = RoaringBitmap::from_iter(xs);
            let mut compressed = Vec::new();
            let mut tmp_buffer = Vec::new();
            DeRoaringBitmapCodec::serialize_into_with_tmp_buffer(&bitmap, &mut compressed, &mut tmp_buffer).unwrap();
            let length = DeRoaringBitmapCodec::deserialize_length_from(&compressed).unwrap();
            let expected_len = DeRoaringBitmapCodec::serialized_size_with_tmp_buffer(&bitmap, &mut tmp_buffer);
            length == bitmap.len() && compressed.len() == expected_len
        }
    }

    quickcheck! {
        fn qc_random_intersection_with_serialized(lhs: Vec<u32>, rhs: Vec<u32>) -> bool {
            let mut compressed = Vec::new();
            let mut tmp_buffer = Vec::new();

            let lhs = RoaringBitmap::from_iter(lhs);
            let rhs = RoaringBitmap::from_iter(rhs);
            DeRoaringBitmapCodec::serialize_into_with_tmp_buffer(&lhs, &mut compressed, &mut tmp_buffer).unwrap();

            let sub_lhs = DeRoaringBitmapCodec::deserialize_from_with_tmp_buffer(&compressed, |first, last| {
                rhs.range_cardinality(first..=last) == 0
            }, &mut tmp_buffer).unwrap();

            let intersection = sub_lhs & rhs.clone();
            let expected_intersection = lhs & rhs;

            intersection == expected_intersection
        }
    }
}
