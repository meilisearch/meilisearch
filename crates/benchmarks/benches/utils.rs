#![allow(dead_code)]

use std::fs::{create_dir_all, remove_dir_all, File};
use std::io::{self, BufReader, BufWriter, Read};
use std::path::Path;
use std::str::FromStr as _;

use anyhow::Context;
use bumpalo::Bump;
use criterion::BenchmarkId;
use memmap2::Mmap;
use milli::heed::EnvOpenOptions;
use milli::progress::Progress;
use milli::update::new::indexer;
use milli::update::{IndexerConfig, Settings};
use milli::vector::EmbeddingConfigs;
use milli::{Criterion, Filter, Index, Object, TermsMatchingStrategy};
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
    match remove_dir_all(conf.database_name) {
        Ok(_) => (),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => (),
        Err(e) => panic!("{}", e),
    }
    create_dir_all(conf.database_name).unwrap();

    let mut options = EnvOpenOptions::new();
    options.map_size(100 * 1024 * 1024 * 1024); // 100 GB
    options.max_readers(100);
    let index = Index::new(options, conf.database_name, true).unwrap();

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

        let criterion = criterion.iter().map(|s| Criterion::from_str(s).unwrap()).collect();
        builder.set_criteria(criterion);
    }

    (conf.configure)(&mut builder);

    builder.execute(|_| (), || false).unwrap();
    wtxn.commit().unwrap();

    let config = IndexerConfig::default();
    let mut wtxn = index.write_txn().unwrap();
    let rtxn = index.read_txn().unwrap();
    let db_fields_ids_map = index.fields_ids_map(&rtxn).unwrap();
    let mut new_fields_ids_map = db_fields_ids_map.clone();

    let documents = documents_from(conf.dataset, conf.dataset_format);
    let mut indexer = indexer::DocumentOperation::new();
    indexer.replace_documents(&documents).unwrap();

    let indexer_alloc = Bump::new();
    let (document_changes, _operation_stats, primary_key) = indexer
        .into_changes(
            &indexer_alloc,
            &index,
            &rtxn,
            None,
            &mut new_fields_ids_map,
            &|| false,
            Progress::default(),
        )
        .unwrap();

    indexer::index(
        &mut wtxn,
        &index,
        &milli::ThreadPoolNoAbortBuilder::new().build().unwrap(),
        config.grenad_parameters(),
        &db_fields_ids_map,
        new_fields_ids_map,
        primary_key,
        &document_changes,
        EmbeddingConfigs::default(),
        &|| false,
        &Progress::default(),
    )
    .unwrap();

    wtxn.commit().unwrap();
    drop(rtxn);

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

pub fn documents_from(filename: &str, filetype: &str) -> Mmap {
    let file = File::open(filename)
        .unwrap_or_else(|_| panic!("could not find the dataset in: {filename}"));
    match filetype {
        "csv" => documents_from_csv(file).unwrap(),
        "json" => documents_from_json(file).unwrap(),
        "jsonl" => documents_from_jsonl(file).unwrap(),
        otherwise => panic!("invalid update format {otherwise:?}"),
    }
}

fn documents_from_jsonl(file: File) -> anyhow::Result<Mmap> {
    unsafe { Mmap::map(&file).map_err(Into::into) }
}

fn documents_from_json(file: File) -> anyhow::Result<Mmap> {
    let reader = BufReader::new(file);
    let documents: Vec<milli::Object> = serde_json::from_reader(reader)?;
    let mut output = tempfile::tempfile().map(BufWriter::new)?;

    for document in documents {
        serde_json::to_writer(&mut output, &document)?;
    }

    let file = output.into_inner()?;
    unsafe { Mmap::map(&file).map_err(Into::into) }
}

fn documents_from_csv(file: File) -> anyhow::Result<Mmap> {
    let output = tempfile::tempfile()?;
    let mut output = BufWriter::new(output);
    let mut reader = csv::ReaderBuilder::new().from_reader(file);

    let headers = reader.headers().context("while retrieving headers")?.clone();
    let typed_fields: Vec<_> = headers.iter().map(parse_csv_header).collect();
    let mut object: serde_json::Map<_, _> =
        typed_fields.iter().map(|(k, _)| (k.to_string(), Value::Null)).collect();

    let mut line = 0;
    let mut record = csv::StringRecord::new();
    while reader.read_record(&mut record).context("while reading a record")? {
        // We increment here and not at the end of the loop
        // to take the header offset into account.
        line += 1;

        // Reset the document values
        object.iter_mut().for_each(|(_, v)| *v = Value::Null);

        for (i, (name, atype)) in typed_fields.iter().enumerate() {
            let value = &record[i];
            let trimmed_value = value.trim();
            let value = match atype {
                AllowedType::Number if trimmed_value.is_empty() => Value::Null,
                AllowedType::Number => {
                    match trimmed_value.parse::<i64>() {
                        Ok(integer) => Value::from(integer),
                        Err(_) => match trimmed_value.parse::<f64>() {
                            Ok(float) => Value::from(float),
                            Err(error) => {
                                anyhow::bail!("document format error on line {line}: {error}. For value: {value}")
                            }
                        },
                    }
                }
                AllowedType::Boolean if trimmed_value.is_empty() => Value::Null,
                AllowedType::Boolean => match trimmed_value.parse::<bool>() {
                    Ok(bool) => Value::from(bool),
                    Err(error) => {
                        anyhow::bail!(
                            "document format error on line {line}: {error}. For value: {value}"
                        )
                    }
                },
                AllowedType::String if value.is_empty() => Value::Null,
                AllowedType::String => Value::from(value),
            };

            *object.get_mut(name).expect("encountered an unknown field") = value;
        }

        serde_json::to_writer(&mut output, &object).context("while writing to disk")?;
    }

    let output = output.into_inner()?;
    unsafe { Mmap::map(&output).map_err(Into::into) }
}

enum AllowedType {
    String,
    Boolean,
    Number,
}

fn parse_csv_header(header: &str) -> (String, AllowedType) {
    // if there are several separators we only split on the last one.
    match header.rsplit_once(':') {
        Some((field_name, field_type)) => match field_type {
            "string" => (field_name.to_string(), AllowedType::String),
            "boolean" => (field_name.to_string(), AllowedType::Boolean),
            "number" => (field_name.to_string(), AllowedType::Number),
            // if the pattern isn't recognized, we keep the whole field.
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
                    let parsed_value: anyhow::Result<Value> = match field_type {
                        AllowedType::Number => {
                            value.parse::<f64>().map(Value::from).map_err(Into::into)
                        }
                        AllowedType::Boolean => {
                            value.parse::<bool>().map(Value::from).map_err(Into::into)
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
