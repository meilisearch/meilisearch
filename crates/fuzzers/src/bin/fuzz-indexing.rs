use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use arbitrary::{Arbitrary, Unstructured};
use bumpalo::Bump;
use clap::Parser;
use either::Either;
use fuzzers::Operation;
use http_client::policy::IpPolicy;
use milli::documents::mmap_from_objects;
use milli::heed::EnvOpenOptions;
use milli::progress::Progress;
use milli::update::new::indexer;
use milli::update::{IndexerConfig, MissingDocumentPolicy};
use milli::vector::RuntimeEmbedders;
use milli::{CreateOrOpen, Index};
use serde_json::Value;
use tempfile::TempDir;

#[derive(Debug, Arbitrary)]
struct Batch([Operation; 5]);

#[derive(Debug, Clone, Parser)]
struct Opt {
    /// The number of fuzzer to run in parallel.
    #[clap(long)]
    par: Option<NonZeroUsize>,
    // We need to put a lot of newlines in the following documentation or else everything gets collapsed on one line
    /// The path in which the databases will be created.
    /// Using a ramdisk is recommended.
    ///
    /// Linux:
    ///
    /// sudo mount -t tmpfs -o size=2g tmpfs ramdisk # to create it
    ///
    /// sudo umount ramdisk # to remove it
    ///
    /// MacOS:
    ///
    /// diskutil erasevolume HFS+ 'RAM Disk' `hdiutil attach -nobrowse -nomount ram://4194304 # create it
    ///
    /// hdiutil detach /dev/:the_disk
    #[clap(long)]
    path: Option<PathBuf>,
}

fn main() {
    let opt = Opt::parse();
    let progression: &'static AtomicUsize = Box::leak(Box::new(AtomicUsize::new(0)));
    let stop: &'static AtomicBool = Box::leak(Box::new(AtomicBool::new(false)));

    let par = opt.par.unwrap_or_else(|| std::thread::available_parallelism().expect("available parallelism should be accessible")).get();
    let mut handles = Vec::with_capacity(par);

    for _ in 0..par {
        let opt = opt.clone();

        let handle = std::thread::spawn(move || {
            let options = EnvOpenOptions::new();
            let mut options = options.read_txn_without_tls();
            options.map_size(1024 * 1024 * 1024 * 1024);
            let tempdir = match opt.path {
                Some(path) => TempDir::new_in(path).expect("temp directory should be created in specified path"),
                None => TempDir::new().expect("temp directory should be created"),
            };
            let index =
                Index::new(options, tempdir.path(), CreateOrOpen::create_without_shards()).expect("index should be created successfully");
            let indexer_config = IndexerConfig::default();

            std::thread::scope(|s| {
                loop {
                    if stop.load(Ordering::Relaxed) {
                        return;
                    }
                    let v: Vec<u8> =
                        std::iter::repeat_with(|| fastrand::u8(..)).take(1000).collect();

                    let mut data = Unstructured::new(&v);
                    let batches = <[Batch; 5]>::arbitrary(&mut data).expect("batch data should be parsed successfully");
                    // will be used to display the error once a thread crashes
                    let dbg_input = format!("{:#?}", batches);

                    let handle = s.spawn(|| {
                        let mut wtxn = index.write_txn().expect("write transaction should be created");
                        let rtxn = index.read_txn().expect("read transaction should be created");

                        for batch in batches {
                            let db_fields_ids_map = index.fields_ids_map(&rtxn).expect("fields IDs map should be accessible");
                            let mut new_fields_ids_map = db_fields_ids_map.clone();

                            let indexer_alloc = Bump::new();
                            let embedders = RuntimeEmbedders::default();
                            let mut indexer = indexer::IndexOperations::new();

                            let mut operations = Vec::new();
                            for op in batch.0 {
                                match op {
                                    Operation::AddDoc(doc) => {
                                        let object = match doc.to_d() {
                                            Value::Object(object) => object,
                                            _ => unreachable!(),
                                        };
                                        let documents = mmap_from_objects(vec![object]);
                                        operations.push(Either::Left(documents));
                                    }
                                    Operation::DeleteDoc(id) => {
                                        let id = indexer_alloc.alloc_str(&id.to_s());
                                        let ids = indexer_alloc.alloc_slice_copy(&[&*id]);
                                        operations.push(Either::Right(ids));
                                    }
                                }
                            }

                            for op in &operations {
                                match op {
                                    Either::Left(documents) => indexer
                                        .replace_documents(
                                            documents,
                                            MissingDocumentPolicy::default(),
                                        )
                                        .expect("document operation should succeed"),
                                    Either::Right(ids) => indexer.delete_documents(ids),
                                }
                            }

                            let (document_changes, _operation_stats, primary_key) = indexer
                                .into_changes(
                                    &indexer_alloc,
                                    &index,
                                    &rtxn,
                                    None,
                                    &mut new_fields_ids_map,
                                    &|| false,
                                    Progress::default(),
                                    None,
                                )
                                .expect("document changes should be processed successfully");

                            indexer::index(
                                &mut wtxn,
                                &index,
                                &milli::ThreadPoolNoAbortBuilder::new().build().expect("thread pool should be created successfully"),
                                indexer_config.grenad_parameters(),
                                &db_fields_ids_map,
                                new_fields_ids_map,
                                primary_key,
                                &document_changes,
                                embedders,
                                &|| false,
                                &Progress::default(),
                                &IpPolicy::deny_all_local_ips(),
                                &Default::default(),
                            )
                            .expect("indexing operation should succeed");

                            // after executing a batch we check if the database is corrupted
                            let progress = Progress::default();
                            let res = index.search(&wtxn, &progress).execute().expect("search operation should succeed");
                            index.documents(&wtxn, res.documents_ids).expect("document retrieval should succeed");
                            progression.fetch_add(1, Ordering::Relaxed);
                        }
                        wtxn.abort();
                    });
                    if let err @ Err(_) = handle.join() {
                        stop.store(true, Ordering::Relaxed);
                        err.expect(&dbg_input);
                    }
                }
            });
        });
        handles.push(handle);
    }

    std::thread::spawn(|| {
        let mut last_value = 0;
        let start = std::time::Instant::now();
        loop {
            let total = progression.load(Ordering::Relaxed);
            let elapsed = start.elapsed().as_secs();
            if elapsed > 3600 {
                // after 1 hour, stop the fuzzer, success
                std::process::exit(0);
            }
            println!(
                "Has been running for {:?} seconds. Tested {} new values for a total of {}.",
                elapsed,
                total - last_value,
                total
            );
            last_value = total;
            std::thread::sleep(Duration::from_secs(1));
        }
    });

    for handle in handles {
        handle.join().unwrap();
    }
}
