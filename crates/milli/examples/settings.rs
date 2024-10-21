// use big_s::S;
use heed::EnvOpenOptions;
// use maplit::hashset;
use milli::{
    update::{IndexerConfig, Settings},
    Criterion, Index,
};

fn main() {
    let mut options = EnvOpenOptions::new();
    options.map_size(100 * 1024 * 1024 * 1024); // 100 GB

    let index = Index::new(options, "data_movies.ms").unwrap();
    let mut wtxn = index.write_txn().unwrap();

    let config = IndexerConfig::default();
    let mut builder = Settings::new(&mut wtxn, &index, &config);

    // builder.set_min_word_len_one_typo(5);
    // builder.set_min_word_len_two_typos(7);
    // builder.set_sortable_fields(hashset! { S("release_date") });
    builder.set_criteria(vec![
        Criterion::Words,
        Criterion::Typo,
        Criterion::Proximity,
        Criterion::Attribute,
        Criterion::Sort,
        Criterion::Exactness,
    ]);

    builder.execute(|_| (), || false).unwrap();
    wtxn.commit().unwrap();
}
