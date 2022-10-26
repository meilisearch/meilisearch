#![allow(dead_code)]

use std::fs::{create_dir_all, remove_dir_all, File};
use std::io::{self, BufRead, BufReader, Cursor, Read, Seek};
use std::num::ParseFloatError;
use std::path::Path;

use criterion::BenchmarkId;
use milli::documents::{DocumentsBatchBuilder, DocumentsBatchReader};
use milli::heed::EnvOpenOptions;
use milli::update::{
    IndexDocuments, IndexDocumentsConfig, IndexDocumentsMethod, IndexerConfig, Settings,
};
use milli::{Filter, Index, Object, TermsMatchingStrategy};
use serde_json::Value;

pub struct Conf<'a> {
    /// where we are going to create our database.mmdb directory
    /// each benchmark will first try to delete it and then recreate it
    pub database_name: &'a str,
    /// the dataset to be used, it must be an uncompressed csv
    pub dataset: &'a str,
    /// The format of the dataset
    pub dataset_format: &'a str,
    pub group_name: &'a str,
    pub queries: &'a [&'a str],
    /// here you can change which criterion are used and in which order.
    /// - if you specify something all the base configuration will be thrown out
    /// - if you don't specify anything (None) the default configuration will be kept
    pub criterion: Option<&'a [&'a str]>,
    /// the last chance to configure your database as you want
    pub configure: fn(&mut Settings),
    pub filter: Option<&'a str>,
    pub sort: Option<Vec<&'a str>>,
    /// enable or disable the optional words on the query
    pub optional_words: bool,
    /// primary key, if there is None we'll auto-generate docids for every documents
    pub primary_key: Option<&'a str>,
}

impl Conf<'_> {
    pub const BASE: Self = Conf {
        database_name: "benches.mmdb",
        dataset_format: "csv",
        dataset: "",
        group_name: "",
        queries: &[],
        criterion: None,
        configure: |_| (),
        filter: None,
        sort: None,
        optional_words: true,
        primary_key: None,
    };
}

pub fn base_setup(conf: &Conf) -> Index {
    match remove_dir_all(&conf.database_name) {
        Ok(_) => (),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => (),
        Err(e) => panic!("{}", e),
    }
    create_dir_all(&conf.database_name).unwrap();

    let mut options = EnvOpenOptions::new();
    options.map_size(100 * 1024 * 1024 * 1024); // 100 GB
    options.max_readers(10);
    let index = Index::new(options, conf.database_name).unwrap();

    let config = IndexerConfig::default();
    let mut wtxn = index.write_txn().unwrap();
    let mut builder = Settings::new(&mut wtxn, &index, &config);

    if let Some(primary_key) = conf.primary_key {
        builder.set_primary_key(primary_key.to_string());
    }

    if let Some(criterion) = conf.criterion {
        builder.reset_filterable_fields();
        builder.reset_criteria();
        builder.reset_stop_words();

        let criterion = criterion.iter().map(|s| s.to_string()).collect();
        builder.set_criteria(criterion);
    }

    (conf.configure)(&mut builder);

    builder.execute(|_| (), || false).unwrap();
    wtxn.commit().unwrap();

    let config = IndexerConfig::default();
    let mut wtxn = index.write_txn().unwrap();
    let indexing_config = IndexDocumentsConfig {
        autogenerate_docids: conf.primary_key.is_none(),
        update_method: IndexDocumentsMethod::ReplaceDocuments,
        ..Default::default()
    };
    let builder =
        IndexDocuments::new(&mut wtxn, &index, &config, indexing_config, |_| (), || false).unwrap();
    let documents = documents_from(conf.dataset, conf.dataset_format);
    let (builder, user_error) = builder.add_documents(documents).unwrap();
    user_error.unwrap();
    builder.execute().unwrap();
    wtxn.commit().unwrap();

    index
}

pub fn run_benches(c: &mut criterion::Criterion, confs: &[Conf]) {
    for conf in confs {
        let index = base_setup(conf);

        let file_name = Path::new(conf.dataset).file_name().and_then(|f| f.to_str()).unwrap();
        let name = format!("{}: {}", file_name, conf.group_name);
        let mut group = c.benchmark_group(&name);

        for &query in conf.queries {
            group.bench_with_input(BenchmarkId::from_parameter(query), &query, |b, &query| {
                b.iter(|| {
                    let rtxn = index.read_txn().unwrap();
                    let mut search = index.search(&rtxn);
                    search.query(query).terms_matching_strategy(TermsMatchingStrategy::default());
                    if let Some(filter) = conf.filter {
                        let filter = Filter::from_str(filter).unwrap().unwrap();
                        search.filter(filter);
                    }
                    if let Some(sort) = &conf.sort {
                        let sort = sort.iter().map(|sort| sort.parse().unwrap()).collect();
                        search.sort_criteria(sort);
                    }
                    let _ids = search.execute().unwrap();
                });
            });
        }
        group.finish();

        index.prepare_for_closing().wait();
    }
}

pub fn documents_from(filename: &str, filetype: &str) -> DocumentsBatchReader<impl BufRead + Seek> {
    let reader =
        File::open(filename).expect(&format!("could not find the dataset in: {}", filename));
    let reader = BufReader::new(reader);
    let documents = match filetype {
        "csv" => documents_from_csv(reader).unwrap(),
        "json" => documents_from_json(reader).unwrap(),
        "jsonl" => documents_from_jsonl(reader).unwrap(),
        otherwise => panic!("invalid update format {:?}", otherwise),
    };
    DocumentsBatchReader::from_reader(Cursor::new(documents)).unwrap()
}

fn documents_from_jsonl(reader: impl BufRead) -> anyhow::Result<Vec<u8>> {
    let mut documents = DocumentsBatchBuilder::new(Vec::new());

    for result in serde_json::Deserializer::from_reader(reader).into_iter::<Object>() {
        let object = result?;
        documents.append_json_object(&object)?;
    }

    documents.into_inner().map_err(Into::into)
}

fn documents_from_json(reader: impl BufRead) -> anyhow::Result<Vec<u8>> {
    let mut documents = DocumentsBatchBuilder::new(Vec::new());

    documents.append_json_array(reader)?;

    documents.into_inner().map_err(Into::into)
}

fn documents_from_csv(reader: impl BufRead) -> anyhow::Result<Vec<u8>> {
    let csv = csv::Reader::from_reader(reader);

    let mut documents = DocumentsBatchBuilder::new(Vec::new());
    documents.append_csv(csv)?;

    documents.into_inner().map_err(Into::into)
}

enum AllowedType {
    String,
    Number,
}

fn parse_csv_header(header: &str) -> (String, AllowedType) {
    // if there are several separators we only split on the last one.
    match header.rsplit_once(':') {
        Some((field_name, field_type)) => match field_type {
            "string" => (field_name.to_string(), AllowedType::String),
            "number" => (field_name.to_string(), AllowedType::Number),
            // we may return an error in this case.
            _otherwise => (header.to_string(), AllowedType::String),
        },
        None => (header.to_string(), AllowedType::String),
    }
}

struct CSVDocumentDeserializer<R>
where
    R: Read,
{
    documents: csv::StringRecordsIntoIter<R>,
    headers: Vec<(String, AllowedType)>,
}

impl<R: Read> CSVDocumentDeserializer<R> {
    fn from_reader(reader: R) -> io::Result<Self> {
        let mut records = csv::Reader::from_reader(reader);

        let headers = records.headers()?.into_iter().map(parse_csv_header).collect();

        Ok(Self { documents: records.into_records(), headers })
    }
}

impl<R: Read> Iterator for CSVDocumentDeserializer<R> {
    type Item = anyhow::Result<Object>;

    fn next(&mut self) -> Option<Self::Item> {
        let csv_document = self.documents.next()?;

        match csv_document {
            Ok(csv_document) => {
                let mut document = Object::new();

                for ((field_name, field_type), value) in
                    self.headers.iter().zip(csv_document.into_iter())
                {
                    let parsed_value: Result<Value, ParseFloatError> = match field_type {
                        AllowedType::Number => {
                            value.parse::<f64>().map(Value::from).map_err(Into::into)
                        }
                        AllowedType::String => Ok(Value::String(value.to_string())),
                    };

                    match parsed_value {
                        Ok(value) => drop(document.insert(field_name.to_string(), value)),
                        Err(_e) => {
                            return Some(Err(anyhow::anyhow!(
                                "Value '{}' is not a valid number",
                                value
                            )))
                        }
                    }
                }

                Some(Ok(document))
            }
            Err(e) => Some(Err(anyhow::anyhow!("Error parsing csv document: {}", e))),
        }
    }
}
