use rkv::{Manager, Rkv, SingleStore, Value, StoreOptions};
use std::{fs, path::Path};
use meilidb_core::{Database, QueryBuilder};

fn main() {
    let path = Path::new("test.rkv");
    fs::create_dir_all(path).unwrap();

    let database = Database::open_or_create(path).unwrap();
    println!("{:?}", database.indexes_names());

    let hello = database.open_index("hello").unwrap();
    let hello1 = database.open_index("hello1").unwrap();
    let hello2 = database.open_index("hello2").unwrap();

    // {
    //     let mut writer = env.write().unwrap();
    //     let mut raw_indexer = RawIndexer::new();

    //     let docid = DocumentId(0);
    //     let attr = SchemaAttr(0);
    //     let text = "Zut, l’aspirateur, j’ai oublié de l’éteindre !";
    //     raw_indexer.index_text(docid, attr, text);

    //     let Indexed { words_doc_indexes, .. } = raw_indexer.build();

    //     let mut fst_builder = fst::SetBuilder::memory();
    //     fst_builder.extend_iter(words_doc_indexes.keys()).unwrap();
    //     let bytes = fst_builder.into_inner().unwrap();
    //     let fst = fst::raw::Fst::from_bytes(bytes).unwrap();
    //     let fst = fst::Set::from(fst);

    //     words.put_words_fst(&mut writer, &fst).unwrap();

    //     for (word, indexes) in words_doc_indexes {
    //         words.put_words_indexes(&mut writer, &word, &indexes).unwrap();
    //     }

    //     writer.commit().unwrap();
    // }

    // let reader = env.read().unwrap();
    // let builder = QueryBuilder::new(index.main, index.postings_lists, index.synonyms);
    // let documents = builder.query(&reader, "oubli", 0..20).unwrap();

    // println!("{:?}", documents);
}
