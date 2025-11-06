use std::borrow::Cow;
use std::io::{self, Cursor};

use heed::BoxedError;
use roaring::RoaringBitmap;

use crate::heed_codec::BytesDecodeOwned;
use crate::update::del_add::{DelAdd, KvReaderDelAdd};

/// A conditionnal codec that either use the RoaringBitmap
/// or a lighter ByteOrder en/decoding method.
pub struct CboRoaringBitmapCodec;

impl CboRoaringBitmapCodec {
    pub fn serialized_size(roaring: &RoaringBitmap) -> usize {
        roaring.serialized_size()
    }

    pub fn serialize_into_vec(roaring: &RoaringBitmap, vec: &mut Vec<u8>) {
        Self::serialize_into_writer(roaring, vec).unwrap()
    }

    pub fn serialize_into_writer<W: io::Write>(
        roaring: &RoaringBitmap,
        writer: W,
    ) -> io::Result<()> {
        // Otherwise, we use the classic RoaringBitmapCodec that writes a header.
        roaring.serialize_into(writer)
    }

    pub fn deserialize_from(mut bytes: &[u8]) -> io::Result<RoaringBitmap> {
        match RoaringBitmap::deserialize_unchecked_from(bytes) {
            Ok(bitmap) => Ok(bitmap),
            Err(_) => {
                // FIX: this is a bandaid because in the codebase
                // there is still code that writes non-roaring bitmap values in lmmd
                // This does not work if the first bytes match a special cookie value from `roaring`
                // and is not roaring bitmap
                // For example function
                //  `milli::update::index_documents::extract::extract_word_docids::words_into_sorter`
                // writes raw document id in the memory
                let mut bitmap = RoaringBitmap::new();
                while let Ok(integer) =
                    byteorder::ReadBytesExt::read_u32::<byteorder::NativeEndian>(&mut bytes)
                {
                    bitmap.insert(integer);
                }
                Ok(bitmap)
            }
        }
    }

    pub fn intersection_with_serialized(
        mut bytes: &[u8],
        other: &RoaringBitmap,
    ) -> io::Result<RoaringBitmap> {
        match other.intersection_with_serialized_unchecked(Cursor::new(bytes)) {
            Ok(bitmap) => Ok(bitmap),
            Err(_) => {
                // FIX: this is a bandaid because in the codebase
                // there is still code that writes non-roaring bitmap values in lmmd
                // This does not work if the first bytes match a special cookie value from `roaring`
                // and is not roaring bitmap
                // For example function
                //  `milli::update::index_documents::extract::extract_word_docids::words_into_sorter`
                // writes raw document id in the memory
                let mut bitmap = RoaringBitmap::new();
                while let Ok(integer) =
                    byteorder::ReadBytesExt::read_u32::<byteorder::NativeEndian>(&mut bytes)
                {
                    if other.contains(integer) {
                        bitmap.insert(integer);
                    }
                }
                Ok(bitmap)
            }
        }
    }

    /// Merge serialized CboRoaringBitmaps in a buffer.
    ///
    /// if the merged values length is under the threshold, values are directly
    /// serialized in the buffer else a RoaringBitmap is created from the
    /// values and is serialized in the buffer.
    pub fn merge_into<I, A>(slices: I, buffer: &mut Vec<u8>) -> io::Result<()>
    where
        I: IntoIterator<Item = A>,
        A: AsRef<[u8]>,
    {
        let mut roaring = RoaringBitmap::new();
        let mut vec = Vec::new();

        for bytes in slices {
            roaring |= CboRoaringBitmapCodec::deserialize_from(bytes.as_ref())?;
        }

        if roaring.is_empty() {
            vec.sort_unstable();
            vec.dedup();

            // We can unwrap safely because the vector is sorted upper.
            let roaring = RoaringBitmap::from_sorted_iter(vec).unwrap();
            roaring.serialize_into(buffer)?;
        } else {
            roaring.extend(vec);
            roaring.serialize_into(buffer)?;
        }

        Ok(())
    }

    /// Merges a DelAdd delta into a CboRoaringBitmap.
    pub fn merge_deladd_into<'a>(
        deladd: &KvReaderDelAdd,
        previous: &[u8],
        buffer: &'a mut Vec<u8>,
    ) -> io::Result<Option<&'a [u8]>> {
        // Deserialize the bitmap that is already there
        let mut previous = Self::deserialize_from(previous)?;

        // Remove integers we no more want in the previous bitmap
        if let Some(value) = deladd.get(DelAdd::Deletion) {
            previous -= Self::deserialize_from(value)?;
        }

        // Insert the new integers we want in the previous bitmap
        if let Some(value) = deladd.get(DelAdd::Addition) {
            previous |= Self::deserialize_from(value)?;
        }

        if previous.is_empty() {
            return Ok(None);
        }

        Self::serialize_into_vec(&previous, buffer);
        Ok(Some(&buffer[..]))
    }
}

impl heed::BytesDecode<'_> for CboRoaringBitmapCodec {
    type DItem = RoaringBitmap;

    fn bytes_decode(bytes: &[u8]) -> Result<Self::DItem, BoxedError> {
        Self::deserialize_from(bytes).map_err(Into::into)
    }
}

impl BytesDecodeOwned for CboRoaringBitmapCodec {
    type DItem = RoaringBitmap;

    fn bytes_decode_owned(bytes: &[u8]) -> Result<Self::DItem, BoxedError> {
        Self::deserialize_from(bytes).map_err(Into::into)
    }
}

impl heed::BytesEncode<'_> for CboRoaringBitmapCodec {
    type EItem = RoaringBitmap;

    fn bytes_encode(item: &Self::EItem) -> Result<Cow<'_, [u8]>, BoxedError> {
        let mut item = item.clone();
        item.optimize();
        let mut vec = Vec::with_capacity(Self::serialized_size(&item));
        Self::serialize_into_vec(&item, &mut vec);
        Ok(Cow::Owned(vec))
    }
}

#[cfg(test)]
mod tests {
    use heed::BytesEncode;

    use super::*;

    #[test]
    fn merge_cbo_roaring_bitmaps() {
        let mut buffer = Vec::new();

        let small_data = [
            RoaringBitmap::from_sorted_iter(1..4).unwrap(),
            RoaringBitmap::from_sorted_iter(2..5).unwrap(),
            RoaringBitmap::from_sorted_iter(4..6).unwrap(),
            RoaringBitmap::from_sorted_iter(1..3).unwrap(),
        ];

        let small_data: Vec<_> =
            small_data.iter().map(|b| CboRoaringBitmapCodec::bytes_encode(b).unwrap()).collect();
        CboRoaringBitmapCodec::merge_into(small_data.as_slice(), &mut buffer).unwrap();
        let bitmap = CboRoaringBitmapCodec::deserialize_from(&buffer).unwrap();
        let expected = RoaringBitmap::from_sorted_iter(1..6).unwrap();
        assert_eq!(bitmap, expected);

        let medium_data = [
            RoaringBitmap::from_sorted_iter(1..4).unwrap(),
            RoaringBitmap::from_sorted_iter(2..5).unwrap(),
            RoaringBitmap::from_sorted_iter(4..8).unwrap(),
            RoaringBitmap::from_sorted_iter(0..3).unwrap(),
            RoaringBitmap::from_sorted_iter(7..23).unwrap(),
        ];

        let medium_data: Vec<_> =
            medium_data.iter().map(|b| CboRoaringBitmapCodec::bytes_encode(b).unwrap()).collect();
        buffer.clear();
        CboRoaringBitmapCodec::merge_into(medium_data.as_slice(), &mut buffer).unwrap();

        let bitmap = CboRoaringBitmapCodec::deserialize_from(&buffer).unwrap();
        let expected = RoaringBitmap::from_sorted_iter(0..23).unwrap();
        assert_eq!(bitmap, expected);
    }
}
