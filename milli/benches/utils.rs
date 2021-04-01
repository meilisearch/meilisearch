use std::{fs::{File, create_dir_all}};

use heed::EnvOpenOptions;
use milli::{Index, update::{IndexDocumentsMethod, UpdateBuilder, UpdateFormat}};

pub fn base_setup(criteria: Option<Vec<String>>) -> Index {
    let database = "songs.mmdb";
    create_dir_all(&database).unwrap();

    let mut options = EnvOpenOptions::new();
    options.map_size(100 * 1024 * 1024 * 1024); // 100 GB
    options.max_readers(10);
    let index = Index::new(options, database).unwrap();

    let update_builder = UpdateBuilder::new(0);
    let mut wtxn = index.write_txn().unwrap();
    let mut builder = update_builder.settings(&mut wtxn, &index);

    if let Some(criteria) = criteria {
        builder.reset_faceted_fields();
        builder.reset_criteria();
        builder.reset_stop_words();

        builder.set_criteria(criteria);
    }

    builder.execute(|_, _| ()).unwrap();
    wtxn.commit().unwrap();

    let update_builder = UpdateBuilder::new(0);
    let mut wtxn = index.write_txn().unwrap();
    let mut builder = update_builder.index_documents(&mut wtxn, &index);
    builder.update_format(UpdateFormat::Csv);
    builder.index_documents_method(IndexDocumentsMethod::ReplaceDocuments);
    // we called from cargo the current directory is supposed to be milli/milli
    let reader = File::open("benches/smol_songs.csv").unwrap();
    builder.execute(reader, |_, _| ()).unwrap();
    wtxn.commit().unwrap();

    index
}
