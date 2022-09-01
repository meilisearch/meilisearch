use heed::types::ByteSlice;
use heed::{BytesDecode, RoTxn};

pub use self::facet_distribution::{FacetDistribution, DEFAULT_VALUES_PER_FACET};
pub use self::filter::Filter;
use crate::heed_codec::facet::new::{FacetGroupValueCodec, FacetKeyCodec, MyByteSlice};

mod facet_distribution;
mod facet_distribution_iter;
mod facet_range_search;
pub mod facet_sort_ascending;
pub mod facet_sort_descending;
mod filter;

pub(crate) fn get_first_facet_value<'t, BoundCodec>(
    txn: &'t RoTxn,
    db: heed::Database<FacetKeyCodec<MyByteSlice>, FacetGroupValueCodec>,
    field_id: u16,
) -> heed::Result<Option<BoundCodec::DItem>>
where
    BoundCodec: BytesDecode<'t>,
{
    let mut level0prefix = vec![];
    level0prefix.extend_from_slice(&field_id.to_be_bytes());
    level0prefix.push(0);
    let mut level0_iter_forward =
        db.as_polymorph().prefix_iter::<_, ByteSlice, ByteSlice>(txn, level0prefix.as_slice())?;
    if let Some(first) = level0_iter_forward.next() {
        let (first_key, _) = first?;
        let first_key =
            FacetKeyCodec::<BoundCodec>::bytes_decode(first_key).ok_or(heed::Error::Encoding)?;
        Ok(Some(first_key.left_bound))
    } else {
        Ok(None)
    }
}
pub(crate) fn get_last_facet_value<'t, BoundCodec>(
    txn: &'t RoTxn,
    db: heed::Database<FacetKeyCodec<MyByteSlice>, FacetGroupValueCodec>,
    field_id: u16,
) -> heed::Result<Option<BoundCodec::DItem>>
where
    BoundCodec: BytesDecode<'t>,
{
    let mut level0prefix = vec![];
    level0prefix.extend_from_slice(&field_id.to_be_bytes());
    level0prefix.push(0);
    let mut level0_iter_backward = db
        .as_polymorph()
        .rev_prefix_iter::<_, ByteSlice, ByteSlice>(txn, level0prefix.as_slice())?;
    if let Some(last) = level0_iter_backward.next() {
        let (last_key, _) = last?;
        let last_key =
            FacetKeyCodec::<BoundCodec>::bytes_decode(last_key).ok_or(heed::Error::Encoding)?;
        Ok(Some(last_key.left_bound))
    } else {
        Ok(None)
    }
}
pub(crate) fn get_highest_level<'t>(
    txn: &'t RoTxn<'t>,
    db: heed::Database<FacetKeyCodec<MyByteSlice>, FacetGroupValueCodec>,
    field_id: u16,
) -> heed::Result<u8> {
    let field_id_prefix = &field_id.to_be_bytes();
    Ok(db
        .as_polymorph()
        .rev_prefix_iter::<_, ByteSlice, ByteSlice>(&txn, field_id_prefix)?
        .next()
        .map(|el| {
            let (key, _) = el.unwrap();
            let key = FacetKeyCodec::<MyByteSlice>::bytes_decode(key).unwrap();
            key.level
        })
        .unwrap_or(0))
}

#[cfg(test)]
pub mod test {
    use std::fmt::Display;
    use std::marker::PhantomData;
    use std::rc::Rc;

    use heed::{BytesDecode, BytesEncode, Env, RwTxn};
    use roaring::RoaringBitmap;

    use crate::heed_codec::facet::new::{
        FacetGroupValue, FacetGroupValueCodec, FacetKey, FacetKeyCodec, MyByteSlice,
    };
    use crate::snapshot_tests::display_bitmap;
    use crate::update::FacetsUpdateIncremental;

    pub struct FacetIndex<BoundCodec>
    where
        for<'a> BoundCodec:
            BytesEncode<'a> + BytesDecode<'a, DItem = <BoundCodec as BytesEncode<'a>>::EItem>,
    {
        pub env: Env,
        pub db: Database,
        _phantom: PhantomData<BoundCodec>,
    }

    pub struct Database {
        pub content: heed::Database<FacetKeyCodec<MyByteSlice>, FacetGroupValueCodec>,
        pub group_size: usize,
        pub max_group_size: usize,
        _tempdir: Rc<tempfile::TempDir>,
    }

    impl<BoundCodec> FacetIndex<BoundCodec>
    where
        for<'a> BoundCodec:
            BytesEncode<'a> + BytesDecode<'a, DItem = <BoundCodec as BytesEncode<'a>>::EItem>,
    {
        #[cfg(all(test, fuzzing))]
        pub fn open_from_tempdir(
            tempdir: Rc<tempfile::TempDir>,
            group_size: u8,
            max_group_size: u8,
        ) -> FacetIndex<BoundCodec> {
            let group_size = std::cmp::min(127, std::cmp::max(group_size, 2)) as usize;
            let max_group_size = std::cmp::max(group_size * 2, max_group_size as usize);
            let mut options = heed::EnvOpenOptions::new();
            let options = options.map_size(4096 * 4 * 10 * 100);
            unsafe {
                options.flag(heed::flags::Flags::MdbAlwaysFreePages);
            }
            let env = options.open(tempdir.path()).unwrap();
            let content = env.open_database(None).unwrap().unwrap();

            FacetIndex {
                db: Database { content, group_size, max_group_size, _tempdir: tempdir },
                env,
                _phantom: PhantomData,
            }
        }
        pub fn new(group_size: u8, max_group_size: u8) -> FacetIndex<BoundCodec> {
            let group_size = std::cmp::min(127, std::cmp::max(group_size, 2)) as usize;
            let max_group_size = std::cmp::max(group_size * 2, max_group_size as usize);
            let mut options = heed::EnvOpenOptions::new();
            let options = options.map_size(4096 * 4 * 100);
            let tempdir = tempfile::TempDir::new().unwrap();
            let env = options.open(tempdir.path()).unwrap();
            let content = env.create_database(None).unwrap();

            FacetIndex {
                db: Database { content, group_size, max_group_size, _tempdir: Rc::new(tempdir) },
                env,
                _phantom: PhantomData,
            }
        }
        pub fn insert<'a>(
            &self,
            rwtxn: &'a mut RwTxn,
            field_id: u16,
            key: &'a <BoundCodec as BytesEncode<'a>>::EItem,
            docids: &RoaringBitmap,
        ) {
            let update = FacetsUpdateIncremental::new(self.db.content);
            let key_bytes = BoundCodec::bytes_encode(&key).unwrap();
            update.insert(rwtxn, field_id, &key_bytes, docids).unwrap();
        }
        pub fn delete<'a>(
            &self,
            rwtxn: &'a mut RwTxn,
            field_id: u16,
            key: &'a <BoundCodec as BytesEncode<'a>>::EItem,
            value: u32,
        ) {
            let update = FacetsUpdateIncremental::new(self.db.content);
            let key_bytes = BoundCodec::bytes_encode(&key).unwrap();
            update.delete(rwtxn, field_id, &key_bytes, value).unwrap();
        }
    }

    impl<BoundCodec> Display for FacetIndex<BoundCodec>
    where
        for<'a> <BoundCodec as BytesEncode<'a>>::EItem: Sized + Display,
        for<'a> BoundCodec:
            BytesEncode<'a> + BytesDecode<'a, DItem = <BoundCodec as BytesEncode<'a>>::EItem>,
    {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            let txn = self.env.read_txn().unwrap();
            let mut iter = self.db.content.iter(&txn).unwrap();
            while let Some(el) = iter.next() {
                let (key, value) = el.unwrap();
                let FacetKey { field_id, level, left_bound: bound } = key;
                let bound = BoundCodec::bytes_decode(bound).unwrap();
                let FacetGroupValue { size, bitmap } = value;
                writeln!(
                    f,
                    "{field_id:<2} {level:<2} k{bound:<8} {size:<4} {values:?}",
                    values = display_bitmap(&bitmap)
                )?;
            }
            Ok(())
        }
    }
}
