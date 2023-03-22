use std::{
    error::Error,
    fs::File,
    io::{BufRead, BufReader, Cursor, Seek},
    time::Duration,
};

use heed::EnvOpenOptions;
use milli::{
    documents::{DocumentsBatchBuilder, DocumentsBatchReader},
    update::{IndexDocuments, IndexDocumentsConfig, IndexerConfig, Settings},
    Criterion, Index, Object,
};

fn main() -> Result<(), Box<dyn Error>> {
    let mut options = EnvOpenOptions::new();
    options.map_size(100 * 1024 * 1024 * 1024); // 100 GB

    let index = Index::new(options, "data_organizations").unwrap();
    let mut wtxn = index.write_txn().unwrap();

    let primary_key = "uuid";
    //  let searchable_fields = vec!["body", "title", "url"];
    // let searchable_fields = vec!["title", "overview"];
    let searchable_fields =
        vec!["name", "primary_role", "city", "region", "country_code", "short_description"];
    // let filterable_fields = vec!["release_date", "genres"];

    let config = IndexerConfig::default();
    let mut builder = Settings::new(&mut wtxn, &index, &config);
    builder.set_primary_key(primary_key.to_owned());
    let searchable_fields = searchable_fields.iter().map(|s| s.to_string()).collect();
    builder.set_searchable_fields(searchable_fields);
    // let filterable_fields = filterable_fields.iter().map(|s| s.to_string()).collect();
    // builder.set_filterable_fields(filterable_fields);

    // builder.set_min_word_len_one_typo(5);
    // builder.set_min_word_len_two_typos(100);
    builder.set_criteria(vec![Criterion::Words, Criterion::Typo, Criterion::Proximity]);
    builder.execute(|_| (), || false).unwrap();

    let config = IndexerConfig::default();
    let indexing_config = IndexDocumentsConfig::default();
    let builder =
        IndexDocuments::new(&mut wtxn, &index, &config, indexing_config, |_| (), || false).unwrap();

    let documents = documents_from(
        // "/Users/meilisearch/Documents/milli2/benchmarks/datasets/movies.json",
        "/Users/meilisearch/Documents/datasets/organizations.csv",
        // "json"
        "csv",
    );
    let (builder, user_error) = builder.add_documents(documents).unwrap();
    user_error.unwrap();
    builder.execute().unwrap();
    wtxn.commit().unwrap();

    // let rtxn = index.read_txn().unwrap();

    // let mut wtxn = index.write_txn().unwrap();
    // let config = IndexerConfig::default();
    // let indexing_config = IndexDocumentsConfig::default();
    // let builder =
    //     IndexDocuments::new(&mut wtxn, &index, &config, indexing_config, |_| (), || false).unwrap();

    // let documents = documents_from("test_doc.json", "json");
    // let (builder, user_error) = builder.add_documents(documents).unwrap();
    // user_error.unwrap();
    // builder.execute().unwrap();
    // wtxn.commit().unwrap();

    // let _ = index.all_documents(&rtxn)?;

    // println!("done!");
    // std::thread::sleep(Duration::from_secs(100));

    index.prepare_for_closing().wait();
    Ok(())
}
fn documents_from(filename: &str, filetype: &str) -> DocumentsBatchReader<impl BufRead + Seek> {
    let reader = File::open(filename)
        .unwrap_or_else(|_| panic!("could not find the dataset in: {}", filename));
    let reader = BufReader::new(reader);
    let documents = match filetype {
        "csv" => documents_from_csv(reader).unwrap(),
        "json" => documents_from_json(reader).unwrap(),
        "jsonl" => documents_from_jsonl(reader).unwrap(),
        otherwise => panic!("invalid update format {:?}", otherwise),
    };
    DocumentsBatchReader::from_reader(Cursor::new(documents)).unwrap()
}

fn documents_from_jsonl(reader: impl BufRead) -> milli::Result<Vec<u8>> {
    let mut documents = DocumentsBatchBuilder::new(Vec::new());

    for result in serde_json::Deserializer::from_reader(reader).into_iter::<Object>() {
        let object = result.unwrap();
        documents.append_json_object(&object)?;
    }

    documents.into_inner().map_err(Into::into)
}

fn documents_from_json(reader: impl BufRead) -> milli::Result<Vec<u8>> {
    let mut documents = DocumentsBatchBuilder::new(Vec::new());

    documents.append_json_array(reader)?;

    documents.into_inner().map_err(Into::into)
}

fn documents_from_csv(reader: impl BufRead) -> milli::Result<Vec<u8>> {
    let csv = csv::Reader::from_reader(reader);

    let mut documents = DocumentsBatchBuilder::new(Vec::new());
    documents.append_csv(csv)?;

    documents.into_inner().map_err(Into::into)
}
