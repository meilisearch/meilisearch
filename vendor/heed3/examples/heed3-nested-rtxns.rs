use heed3::types::*;
use heed3::{Database, EnvOpenOptions};
use rand::prelude::*;
use rayon::prelude::*;
use roaring::RoaringBitmap;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let path = tempfile::tempdir()?;

    let env = unsafe {
        EnvOpenOptions::new()
            .read_txn_without_tls()
            .map_size(2 * 1024 * 1024 * 1024) // 2 GiB
            .max_dbs(300)
            .open(path)?
    };

    // opening a write transaction
    let mut wtxn = env.write_txn()?;
    // we will open the default unnamed database
    let db: Database<U32<byteorder::BigEndian>, Bytes> = env.create_database(&mut wtxn, None)?;

    let mut buffer = Vec::new();
    for i in 0..100 {
        let mut rng = StdRng::seed_from_u64(i as u64);
        let max = rng.random_range(1000..=10000);
        let roaring = RoaringBitmap::from_sorted_iter(0..max)?;
        buffer.clear();
        roaring.serialize_into(&mut buffer)?;
        db.put(&mut wtxn, &i, &buffer)?;
    }

    // opening multiple read-only transactions
    // to check if those values are now available
    // without committing beforehand
    let rtxns = (0..100).map(|_| env.nested_read_txn(&wtxn)).collect::<heed3::Result<Vec<_>>>()?;

    rtxns.into_par_iter().enumerate().for_each(|(i, rtxn)| {
        let mut rng = StdRng::seed_from_u64(i as u64);
        let max = rng.random_range(1000..=10000);
        let roaring = RoaringBitmap::from_sorted_iter(0..max).unwrap();

        let mut buffer = Vec::new();
        roaring.serialize_into(&mut buffer).unwrap();

        let i = i as u32;
        let ret = db.get(&rtxn, &i).unwrap();
        assert_eq!(ret, Some(&buffer[..]));
    });

    for i in 100..1000 {
        let mut rng = StdRng::seed_from_u64(i as u64);
        let max = rng.random_range(1000..=10000);
        let roaring = RoaringBitmap::from_sorted_iter(0..max)?;
        buffer.clear();
        roaring.serialize_into(&mut buffer)?;
        db.put(&mut wtxn, &i, &buffer)?;
    }

    // opening multiple read-only transactions
    // to check if those values are now available
    // without committing beforehand
    let rtxns =
        (100..1000).map(|_| env.nested_read_txn(&wtxn)).collect::<heed3::Result<Vec<_>>>()?;

    rtxns.into_par_iter().enumerate().for_each(|(i, rtxn)| {
        let mut rng = StdRng::seed_from_u64(i as u64);
        let max = rng.random_range(1000..=10000);
        let roaring = RoaringBitmap::from_sorted_iter(0..max).unwrap();

        let mut buffer = Vec::new();
        roaring.serialize_into(&mut buffer).unwrap();

        let i = i as u32;
        let ret = db.get(&rtxn, &i).unwrap();
        assert_eq!(ret, Some(&buffer[..]));
    });

    eprintln!("Successful test!");

    Ok(())
}
