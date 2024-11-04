use std::error::Error;
use std::fs::File;
use std::io::{BufRead, BufReader, Cursor, Seek};
use std::path::Path;

use heed::EnvOpenOptions;
use milli::documents::{DocumentsBatchBuilder, DocumentsBatchReader};
use milli::update::{IndexDocuments, IndexDocumentsConfig, IndexerConfig, Settings};
use milli::{Index, Object};

fn usage(error: &str, program_name: &str) -> String {
    format!(
        "{}. Usage: {} <PATH-TO-INDEX> <PATH-TO-DATASET> [searchable_fields] [filterable_fields]",
        error, program_name
    )
}

fn main() -> Result<(), Box<dyn Error>> {
    let mut args = std::env::args();
    let program_name = args.next().expect("No program name");
    let index_path =
        args.next().unwrap_or_else(|| panic!("{}", usage("Missing path to index.", &program_name)));
    let dataset_path = args
        .next()
        .unwrap_or_else(|| panic!("{}", usage("Missing path to source dataset.", &program_name)));
    // let primary_key = args.next().unwrap_or_else(|| "id".into());
    // "title overview"
    let searchable_fields: Vec<String> = args
        .next()
        .map(|arg| arg.split_whitespace().map(ToString::to_string).collect())
        .unwrap_or_default();

    println!("{searchable_fields:?}");
    // "release_date genres"
    let filterable_fields: Vec<String> = args
        .next()
        .map(|arg| arg.split_whitespace().map(ToString::to_string).collect())
        .unwrap_or_default();

    let mut options = EnvOpenOptions::new();
    options.map_size(100 * 1024 * 1024 * 1024); // 100 GB

    std::fs::create_dir_all(&index_path).unwrap();
    let index = Index::new(options, index_path).unwrap();
    let mut wtxn = index.write_txn().unwrap();

    let config = IndexerConfig::default();
    let mut builder = Settings::new(&mut wtxn, &index, &config);
    // builder.set_primary_key(primary_key);
    let searchable_fields = searchable_fields.iter().map(|s| s.to_string()).collect();
    builder.set_searchable_fields(searchable_fields);
    let filterable_fields = filterable_fields.iter().map(|s| s.to_string()).collect();
    builder.set_filterable_fields(filterable_fields);

    builder.execute(|_| (), || false).unwrap();

    let config = IndexerConfig::default();
    let indexing_config = IndexDocumentsConfig::default();

    let builder =
        IndexDocuments::new(&mut wtxn, &index, &config, indexing_config, |_| (), || false).unwrap();

    let documents = documents_from(
        &dataset_path,
        Path::new(&dataset_path).extension().unwrap_or_default().to_str().unwrap_or_default(),
    );
    let (builder, user_error) = builder.add_documents(documents).unwrap();
    user_error.unwrap();
    builder.execute().unwrap();
    wtxn.commit().unwrap();

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
