use arbitrary::{Arbitrary, Unstructured};
use milli::heed::EnvOpenOptions;
use milli::update::{IndexDocuments, IndexDocumentsConfig, IndexerConfig};
use milli::Index;
use serde_json::{json, Value};
use tempfile::TempDir;

#[derive(Debug, Arbitrary)]
enum Document {
    One,
    Two,
    Three,
    Four,
    Five,
    Six,
}

impl Document {
    pub fn to_d(&self) -> Value {
        match self {
            Document::One => json!({ "id": 0, "doggo": "bernese" }),
            Document::Two => json!({ "id": 0, "doggo": "golden" }),
            Document::Three => json!({ "id": 0, "catto": "jorts" }),
            Document::Four => json!({ "id": 1, "doggo": "bernese" }),
            Document::Five => json!({ "id": 1, "doggo": "golden" }),
            Document::Six => json!({ "id": 1, "catto": "jorts" }),
        }
    }
}

#[derive(Debug, Arbitrary)]
enum DocId {
    Zero,
    One,
}

impl DocId {
    pub fn to_s(&self) -> String {
        match self {
            DocId::Zero => "0".to_string(),
            DocId::One => "1".to_string(),
        }
    }
}

#[derive(Debug, Arbitrary)]
enum Operation {
    AddDoc(Document),
    DeleteDoc(DocId),
}

#[derive(Debug, Arbitrary)]
struct Batch([Operation; 5]);

#[test]
#[ignore]
fn fuzz() {
    let mut options = EnvOpenOptions::new();
    options.map_size(1024 * 1024 * 1024 * 1024);
    let _tempdir = TempDir::new().unwrap();
    let index = Index::new(options, _tempdir.path()).unwrap();
    let indexer_config = IndexerConfig::default();
    let index_documents_config = IndexDocumentsConfig::default();

    loop {
        let v: Vec<u8> = std::iter::repeat_with(|| fastrand::u8(..)).take(1000).collect();

        let mut data = Unstructured::new(&v);
        let batches = <[Batch; 5]>::arbitrary(&mut data).unwrap();

        dbg!(&batches);

        let mut wtxn = index.write_txn().unwrap();

        for batch in batches {
            let mut builder = IndexDocuments::new(
                &mut wtxn,
                &index,
                &indexer_config,
                index_documents_config.clone(),
                |_| (),
                || false,
            )
            .unwrap();

            for op in batch.0 {
                match op {
                    Operation::AddDoc(doc) => {
                        let documents = milli::documents::objects_from_json_value(doc.to_d());
                        let documents =
                            milli::documents::documents_batch_reader_from_objects(documents);
                        let (b, _added) = builder.add_documents(documents).unwrap();
                        builder = b;
                    }
                    Operation::DeleteDoc(id) => {
                        let (b, _removed) = builder.remove_documents(vec![id.to_s()]).unwrap();
                        builder = b;
                    }
                }
            }
            builder.execute().unwrap();
            // wtxn.commit().unwrap();

            // after executing a batch we check if the database is corrupted
            // let rtxn = index.read_txn().unwrap();
            let res = index.search(&wtxn).execute().unwrap();
            index.documents(&wtxn, res.documents_ids).unwrap();
        }
        wtxn.abort().unwrap();
    }
}
