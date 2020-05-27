use std::borrow::Cow;

use bitpacking::{BitPacker, BitPacker4x};
use byteorder::{ReadBytesExt, NativeEndian};
use heed::{BytesEncode, BytesDecode};
use zerocopy::AsBytes;

pub struct BitPackerUnsorted;

impl BytesEncode<'_> for BitPackerUnsorted {
    type EItem = [u32];

    fn bytes_encode(item: &Self::EItem) -> Option<Cow<[u8]>> {
        // This is a hotfix to the SIGSEGV
        // https://github.com/tantivy-search/bitpacking/issues/23
        if item.is_empty() {
            return Some(Cow::default())
        }

        let bitpacker = BitPacker4x::new();
        let mut compressed = Vec::new();

        // The number of remaining numbers that don't fit in the block size.
        compressed.push((item.len() % BitPacker4x::BLOCK_LEN) as u8);

        // we cannot use a mut slice here because of #68630, TooGeneric error.
        // we can probably avoid this new allocation by directly using the compressed final Vec.
        let mut buffer = vec![0u8; 4 * BitPacker4x::BLOCK_LEN];

        for chunk in item.chunks(BitPacker4x::BLOCK_LEN) {
            if chunk.len() == BitPacker4x::BLOCK_LEN {
                // compute the number of bits necessary to encode this block
                let num_bits = bitpacker.num_bits(chunk);
                // Encode the block numbers into the buffer using the num_bits
                let compressed_len = bitpacker.compress(chunk, &mut buffer, num_bits);
                // Write the num_bits that will be read to decode this block
                compressed.push(num_bits);
                // Wrtie the bytes of the compressed block numbers
                compressed.extend_from_slice(&buffer[..compressed_len]);
            } else {
                // Save the remaining numbers which don't fit inside of a BLOCK_LEN
                compressed.extend_from_slice(chunk.as_bytes());
            }
        }

        Some(Cow::Owned(compressed))
    }
}

impl<'a> BytesDecode<'a> for BitPackerUnsorted {
    type DItem = Vec<u32>;

    fn bytes_decode(bytes: &[u8]) -> Option<Self::DItem> {
        if bytes.is_empty() {
            return Some(Vec::new())
        }

        let bitpacker = BitPacker4x::new();
        let (remaining, bytes) = bytes.split_first().unwrap();
        let remaining = *remaining as usize;

        let (mut bytes, mut remaining_bytes) = bytes.split_at(bytes.len() - remaining * 4);
        let mut decompressed = Vec::new();

        while let Some(num_bits) = bytes.get(0) {
            if *num_bits == 0 {
                decompressed.resize(decompressed.len() + BitPacker4x::BLOCK_LEN, 0);
                bytes = &bytes[1..];
                continue;
            }

            let block_size = BitPacker4x::compressed_block_size(*num_bits);

            let new_len = decompressed.len() + BitPacker4x::BLOCK_LEN;
            decompressed.resize(new_len, 0);

            // Create a view into the decompressed buffer and decomress into it
            let to_decompress = &mut decompressed[new_len - BitPacker4x::BLOCK_LEN..new_len];
            bitpacker.decompress(&bytes[1..block_size + 1], to_decompress, *num_bits);

            // Advance the bytes offset to read the next block (+ num_bits)
            bytes = &bytes[block_size + 1..];
        }

        // We add the remaining uncompressed numbers.
        let new_len = decompressed.len() + remaining;
        decompressed.resize(new_len, 0);
        let to_decompress = &mut decompressed[new_len - remaining..new_len];
        remaining_bytes.read_u32_into::<NativeEndian>(to_decompress).ok()?;

        Some(decompressed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    quickcheck! {
        fn qc_many_different_numbers(xs: Vec<u32>) -> bool {
            let original: Vec<_> = xs.iter().cloned().cycle().take(1300).collect();

            let encoded = BitPackerUnsorted::bytes_encode(&original).unwrap();
            let decoded = BitPackerUnsorted::bytes_decode(&encoded).unwrap();

            original == &decoded[..]
        }
    }

    #[test]
    fn empty() {
        let original = &[];

        let encoded = BitPackerUnsorted::bytes_encode(original).unwrap();
        let decoded = BitPackerUnsorted::bytes_decode(&encoded).unwrap();

        assert_eq!(original, &decoded[..]);
    }

    #[test]
    fn one_zero() {
        let original = &[0];

        let encoded = BitPackerUnsorted::bytes_encode(original).unwrap();
        let decoded = BitPackerUnsorted::bytes_decode(&encoded).unwrap();

        assert_eq!(original, &decoded[..]);
    }

    #[test]
    fn many_zeros() {
        let original: Vec<_> = std::iter::repeat(0).take(1300).collect();

        let encoded = BitPackerUnsorted::bytes_encode(&original).unwrap();
        let decoded = BitPackerUnsorted::bytes_decode(&encoded).unwrap();

        assert_eq!(original, &decoded[..]);
    }

    #[test]
    fn many_ones() {
        let original: Vec<_> = std::iter::repeat(1).take(1300).collect();

        let encoded = BitPackerUnsorted::bytes_encode(&original).unwrap();
        let decoded = BitPackerUnsorted::bytes_decode(&encoded).unwrap();

        assert_eq!(original, &decoded[..]);
    }
}
