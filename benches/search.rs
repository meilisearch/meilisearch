#![feature(test)]
extern crate test;

use heed::EnvOpenOptions;
use mega_mini_indexer::Index;

#[bench]
fn search_minogue_kylie_live(b: &mut test::Bencher) {
    let database = "books-4cpu.mmdb";
    let query = "minogue kylie live";

    std::fs::create_dir_all(database).unwrap();
    let env = EnvOpenOptions::new()
        .map_size(100 * 1024 * 1024 * 1024) // 100 GB
        .max_readers(10)
        .max_dbs(5)
        .open(database).unwrap();

    let index = Index::new(&env).unwrap();

    b.iter(|| {
        let rtxn = env.read_txn().unwrap();
        let _documents_ids = index.search(&rtxn, query).unwrap();
    })
}
