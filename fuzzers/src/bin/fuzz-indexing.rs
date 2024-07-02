use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use arbitrary::{Arbitrary, Unstructured};
use clap::Parser;
use fuzzers::Operation;
use milli::heed::EnvOpenOptions;
use milli::update::{IndexDocuments, IndexDocumentsConfig, IndexerConfig};
use milli::Index;
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

    let par = opt.par.unwrap_or_else(|| std::thread::available_parallelism().unwrap()).get();
    let mut handles = Vec::with_capacity(par);

    for _ in 0..par {
        let opt = opt.clone();

        let handle = std::thread::spawn(move || {
            let mut options = EnvOpenOptions::new();
            options.map_size(1024 * 1024 * 1024 * 1024);
            let tempdir = match opt.path {
                Some(path) => TempDir::new_in(path).unwrap(),
                None => TempDir::new().unwrap(),
            };
            let index = Index::new(options, tempdir.path()).unwrap();
            let indexer_config = IndexerConfig::default();
            let index_documents_config = IndexDocumentsConfig::default();

            std::thread::scope(|s| {
                loop {
                    if stop.load(Ordering::Relaxed) {
                        return;
                    }
                    let v: Vec<u8> =
                        std::iter::repeat_with(|| fastrand::u8(..)).take(1000).collect();

                    let mut data = Unstructured::new(&v);
                    let batches = <[Batch; 5]>::arbitrary(&mut data).unwrap();
                    // will be used to display the error once a thread crashes
                    let dbg_input = format!("{:#?}", batches);

                    let handle = s.spawn(|| {
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
                                        let documents =
                                            milli::documents::objects_from_json_value(doc.to_d());
                                        let documents =
                                            milli::documents::documents_batch_reader_from_objects(
                                                documents,
                                            );
                                        let (b, _added) = builder.add_documents(documents).unwrap();
                                        builder = b;
                                    }
                                    Operation::DeleteDoc(id) => {
                                        let (b, _removed) =
                                            builder.remove_documents(vec![id.to_s()]).unwrap();
                                        builder = b;
                                    }
                                }
                            }
                            builder.execute().unwrap();

                            // after executing a batch we check if the database is corrupted
                            let res = index.search(&wtxn).execute().unwrap();
                            index.compressed_documents(&wtxn, res.documents_ids).unwrap();
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
