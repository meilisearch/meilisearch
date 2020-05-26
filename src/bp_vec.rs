use byteorder::{ByteOrder, NativeEndian};
use bitpacking::{BitPacker, BitPacker4x};

/// An append only bitpacked u32 vector that ignore order of insertion.
#[derive(Default)]
pub struct BpVec {
    compressed: Vec<u8>,
    uncompressed: Vec<u32>,
}

impl BpVec {
    pub fn new() -> BpVec {
        BpVec::default()
    }

    pub fn push(&mut self, elem: u32) {
        self.uncompressed.push(elem);
        if self.uncompressed.len() == BitPacker4x::BLOCK_LEN {
            encode(&mut self.uncompressed[..], &mut self.compressed);
            self.uncompressed.clear();
        }
    }

    pub fn extend_from_slice(&mut self, elems: &[u32]) {
        self.uncompressed.extend_from_slice(elems);
        let remaining = self.uncompressed.len() % BitPacker4x::BLOCK_LEN;
        for chunk in self.uncompressed[remaining..].chunks_exact_mut(BitPacker4x::BLOCK_LEN) {
            encode(chunk, &mut self.compressed);
        }
        self.uncompressed.truncate(remaining);
        self.uncompressed.shrink_to_fit();
    }

    pub fn to_vec(self) -> Vec<u32> {
        let BpVec { compressed, mut uncompressed } = self;
        decode(&compressed, &mut uncompressed);
        uncompressed
    }

    pub fn compressed_capacity(&self) -> usize {
        self.compressed.capacity()
    }

    pub fn uncompressed_capacity(&self) -> usize {
        self.uncompressed.capacity()
    }
}

fn encode(items: &mut [u32], encoded: &mut Vec<u8>) {
    assert_eq!(items.len(), BitPacker4x::BLOCK_LEN);

    let bitpacker = BitPacker4x::new();

    // We reserve enough space in the output buffer, filled with zeroes.
    let len = encoded.len();
    // initial_value + num_bits + encoded numbers
    let max_possible_length = 4 + 1 + 4 * BitPacker4x::BLOCK_LEN;
    encoded.resize(len + max_possible_length, 0);

    // We sort the items to be able to efficiently bitpack them.
    items.sort_unstable();
    // We save the initial value to us for this block, the lowest one.
    let initial_value = items[0];
    // We compute the number of bits necessary to encode this block
    let num_bits = bitpacker.num_bits_sorted(initial_value, items);

    // We write the initial value for this block.
    let buffer = &mut encoded[len..];
    NativeEndian::write_u32(buffer, initial_value);
    // We write the num_bits that will be read to decode this block
    let buffer = &mut buffer[4..];
    buffer[0] = num_bits;
    // We encode the block numbers into the buffer using the num_bits
    let buffer = &mut buffer[1..];
    let compressed_len = bitpacker.compress_sorted(initial_value, items, buffer, num_bits);

    // We truncate the buffer to the avoid leaking padding zeroes
    encoded.truncate(len + 4 + 1 + compressed_len);
}

fn decode(mut encoded: &[u8], decoded: &mut Vec<u32>) {
    let bitpacker = BitPacker4x::new();

    // initial_value + num_bits
    while let Some(header) = encoded.get(0..4 + 1) {
        // We extract the header informations
        let initial_value = NativeEndian::read_u32(header);
        let num_bits = header[4];
        let bytes = &encoded[4 + 1..];

        // If the num_bits is equal to zero it means that all encoded numbers were zeroes
        if num_bits == 0 {
            decoded.resize(decoded.len() + BitPacker4x::BLOCK_LEN, initial_value);
            encoded = bytes;
            continue;
        }

        // We guess the block size based on the num_bits used for this block
        let block_size = BitPacker4x::compressed_block_size(num_bits);

        // We pad the decoded vector with zeroes
        let new_len = decoded.len() + BitPacker4x::BLOCK_LEN;
        decoded.resize(new_len, 0);

        // Create a view into the decoded buffer and decode into it
        let to_decompress = &mut decoded[new_len - BitPacker4x::BLOCK_LEN..new_len];
        bitpacker.decompress_sorted(initial_value, &bytes[..block_size], to_decompress, num_bits);

        // Advance the bytes offset to read the next block (+ num_bits)
        encoded = &bytes[block_size..];
    }
}

impl sdset::Collection<u32> for BpVec {
    fn push(&mut self, elem: u32) {
        BpVec::push(self, elem);
    }

    fn extend_from_slice(&mut self, elems: &[u32]) {
        BpVec::extend_from_slice(self, elems);
    }

    fn extend<I>(&mut self, elems: I) where I: IntoIterator<Item=u32> {
        elems.into_iter().for_each(|x| BpVec::push(self, x));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    quickcheck! {
        fn qc_push(xs: Vec<u32>) -> bool {
            let mut xs: Vec<_> = xs.iter().cloned().cycle().take(1300).collect();

            let mut bpvec = BpVec::new();
            xs.iter().for_each(|x| bpvec.push(*x));
            let mut result = bpvec.to_vec();

            result.sort_unstable();
            xs.sort_unstable();

            xs == result
        }
    }

    quickcheck! {
        fn qc_extend_from_slice(xs: Vec<u32>) -> bool {
            let mut xs: Vec<_> = xs.iter().cloned().cycle().take(1300).collect();

            let mut bpvec = BpVec::new();
            bpvec.extend_from_slice(&xs);
            let mut result = bpvec.to_vec();

            result.sort_unstable();
            xs.sort_unstable();

            xs == result
        }
    }

    #[test]
    fn empty() {
        let mut bpvec = BpVec::new();
        bpvec.extend_from_slice(&[]);
        let result = bpvec.to_vec();

        assert!(result.is_empty());
    }

    #[test]
    fn one_zero() {
        let mut bpvec = BpVec::new();
        bpvec.extend_from_slice(&[0]);
        let result = bpvec.to_vec();

        assert_eq!(&[0], &*result);
    }

    #[test]
    fn many_zeros() {
        let xs: Vec<_> = std::iter::repeat(0).take(1300).collect();

        let mut bpvec = BpVec::new();
        bpvec.extend_from_slice(&xs);
        let result = bpvec.to_vec();

        assert_eq!(xs, result);
    }

    #[test]
    fn many_ones() {
        let xs: Vec<_> = std::iter::repeat(1).take(1300).collect();

        let mut bpvec = BpVec::new();
        bpvec.extend_from_slice(&xs);
        let result = bpvec.to_vec();

        assert_eq!(xs, result);
    }
}
