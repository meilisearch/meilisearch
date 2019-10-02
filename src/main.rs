use rkv::{Manager, Rkv, SingleStore, Value, StoreOptions};
use std::{fs, path::Path};

use meilidb_schema::SchemaAttr;
use new_meilidb::{store, QueryBuilder, DocumentId};
use new_meilidb::raw_indexer::{RawIndexer, Indexed};

fn main() {
    let path = Path::new("test.rkv");
    fs::create_dir_all(path).unwrap();

    // The Manager enforces that each process opens the same environment
    // at most once by caching a handle to each environment that it opens.
    // Use it to retrieve the handle to an opened environment—or create one
    // if it hasn't already been opened:
    let created_arc = Manager::singleton().write().unwrap().get_or_create(path, Rkv::new).unwrap();
    let env = created_arc.read().unwrap();

    let (words, synonyms) = store::create(&env, "test").unwrap();

    {
        let mut writer = env.write().unwrap();
        let mut raw_indexer = RawIndexer::new();

        let docid = DocumentId(0);
        let attr = SchemaAttr(0);
        let text = "Zut, l’aspirateur, j’ai oublié de l’éteindre !";
        raw_indexer.index_text(docid, attr, text);

        let Indexed { words_doc_indexes, .. } = raw_indexer.build();

        let mut fst_builder = fst::SetBuilder::memory();
        fst_builder.extend_iter(words_doc_indexes.keys());
        let bytes = fst_builder.into_inner().unwrap();
        let fst = fst::raw::Fst::from_bytes(bytes).unwrap();
        let fst = fst::Set::from(fst);

        words.put_words_fst(&mut writer, &fst).unwrap();

        for (word, indexes) in words_doc_indexes {
            words.put_words_indexes(&mut writer, &word, &indexes).unwrap();
        }

        writer.commit().unwrap();
    }

    let reader = env.read().unwrap();
    let builder = QueryBuilder::new(words, synonyms);
    let documents = builder.query(&reader, "oubli", 0..20).unwrap();

    println!("{:?}", documents);
}
