use std::{fs, path::Path};

use serde_json::json;
use rkv::{Manager, Rkv, SingleStore, Value, StoreOptions};

use meilidb_core::{Database, MResult};
use meilidb_schema::{SchemaBuilder, DISPLAYED, INDEXED};

fn main() -> MResult<()> {
    env_logger::init();

    let path = Path::new("test.rkv");
    let database = Database::open_or_create(path)?;
    let rkv = database.rkv.read().unwrap();
    println!("{:?}", database.indexes_names());

    let hello = database.open_index("hello")?;
    let hello1 = database.open_index("hello1")?;
    let hello2 = database.open_index("hello2")?;

    let mut builder = SchemaBuilder::with_identifier("id");
    builder.new_attribute("alpha", DISPLAYED);
    builder.new_attribute("beta", DISPLAYED | INDEXED);
    builder.new_attribute("gamma", INDEXED);
    let schema = builder.build();

    let writer = rkv.write()?;

    hello.schema_update(writer, schema)?;

    let object = json!({
        "id": 23,
        "beta": "hello",
    });

    let mut additions = hello.documents_addition();
    additions.extend(vec![object]);

    let writer = rkv.write()?;

    additions.finalize(writer)?;

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

    let builder = hello.query_builder();
    let reader = rkv.read().unwrap();
    let documents = builder.query(&reader, "hello", 0..20).unwrap();

    println!("{:?}", documents);

    std::thread::sleep(std::time::Duration::from_secs(2));

    Ok(())
}
