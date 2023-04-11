use std::io::stdin;
use std::time::Instant;
use std::{error::Error, path::Path};

use heed::EnvOpenOptions;
use milli::{
    execute_search, DefaultSearchLogger, Index, SearchContext, SearchLogger, TermsMatchingStrategy,
};

#[global_allocator]
static ALLOC: mimalloc::MiMalloc = mimalloc::MiMalloc;

fn main() -> Result<(), Box<dyn Error>> {
    let mut args = std::env::args();
    let program_name = args.next().expect("No program name");
    let dataset = args.next().unwrap_or_else(|| {
        panic!(
            "Missing path to index. Usage: {} <PATH-TO-INDEX> [<logger-dir>] [print-documents]",
            program_name
        )
    });
    let detailed_logger_dir = args.next();
    let print_documents: bool =
        if let Some(arg) = args.next() { arg == "print-documents" } else { false };

    let mut options = EnvOpenOptions::new();
    options.map_size(100 * 1024 * 1024 * 1024); // 100 GB

    let index = Index::new(options, dataset)?;
    let txn = index.read_txn()?;
    let mut query = String::new();
    while stdin().read_line(&mut query)? > 0 {
        for _ in 0..2 {
            let mut default_logger = DefaultSearchLogger;
            // FIXME: consider resetting the state of the logger between search executions as otherwise panics are possible.
            // Workaround'd here by recreating the logger on each iteration of the loop
            let mut detailed_logger = detailed_logger_dir
                .as_ref()
                .map(|logger_dir| (milli::VisualSearchLogger::default(), logger_dir));
            let logger: &mut dyn SearchLogger<_> =
                if let Some((detailed_logger, _)) = detailed_logger.as_mut() {
                    detailed_logger
                } else {
                    &mut default_logger
                };

            let start = Instant::now();

            let mut ctx = SearchContext::new(&index, &txn);
            let docs = execute_search(
                &mut ctx,
                &(!query.trim().is_empty()).then(|| query.trim().to_owned()),
                // what a the from which when there is
                TermsMatchingStrategy::Last,
                false,
                &None,
                &None,
                0,
                20,
                None,
                &mut DefaultSearchLogger,
                logger,
            )?;
            if let Some((logger, dir)) = detailed_logger {
                logger.finish(&mut ctx, Path::new(dir))?;
            }
            let elapsed = start.elapsed();
            println!("new: {}us, docids: {:?}", elapsed.as_micros(), docs.documents_ids);
            if print_documents {
                let documents = index
                    .documents(&txn, docs.documents_ids.iter().copied())
                    .unwrap()
                    .into_iter()
                    .map(|(id, obkv)| {
                        let mut object = serde_json::Map::default();
                        for (fid, fid_name) in index.fields_ids_map(&txn).unwrap().iter() {
                            let value = obkv.get(fid).unwrap();
                            let value: serde_json::Value = serde_json::from_slice(value).unwrap();
                            object.insert(fid_name.to_owned(), value);
                        }
                        (id, serde_json::to_string_pretty(&object).unwrap())
                    })
                    .collect::<Vec<_>>();

                for (id, document) in documents {
                    println!("{id}:");
                    println!("{document}");
                }

                let documents = index
                    .documents(&txn, docs.documents_ids.iter().copied())
                    .unwrap()
                    .into_iter()
                    .map(|(id, obkv)| {
                        let mut object = serde_json::Map::default();
                        for (fid, fid_name) in index.fields_ids_map(&txn).unwrap().iter() {
                            let value = obkv.get(fid).unwrap();
                            let value: serde_json::Value = serde_json::from_slice(value).unwrap();
                            object.insert(fid_name.to_owned(), value);
                        }
                        (id, serde_json::to_string_pretty(&object).unwrap())
                    })
                    .collect::<Vec<_>>();
                println!("{}us: {:?}", elapsed.as_micros(), docs.documents_ids);
                for (id, document) in documents {
                    println!("{id}:");
                    println!("{document}");
                }
            }
        }
        query.clear();
    }

    Ok(())
}
