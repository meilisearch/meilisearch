use std::collections::{HashMap, BTreeSet};
use std::convert::TryFrom;
use std::fs::File;
use std::hash::BuildHasherDefault;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;

use anyhow::{ensure, Context};
use roaring::RoaringBitmap;
use crossbeam_channel::{select, Sender, Receiver};
use fst::IntoStreamer;
use fxhash::FxHasher32;
use heed::{EnvOpenOptions, Database};
use heed::types::*;
use rayon::prelude::*;
use slice_group_by::StrGroupBy;
use structopt::StructOpt;

pub type FastMap4<K, V> = HashMap<K, V, BuildHasherDefault<FxHasher32>>;
pub type SmallString32 = smallstr::SmallString<[u8; 32]>;
pub type BEU32 = heed::zerocopy::U32<heed::byteorder::BE>;
pub type DocumentId = u32;

#[cfg(target_os = "linux")]
#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

static ID_GENERATOR: AtomicUsize = AtomicUsize::new(0); // AtomicU32 ?

#[derive(Debug, StructOpt)]
#[structopt(name = "mm-indexer", about = "The server side of the daugt project.")]
struct Opt {
    /// The database path where the database is located.
    /// It is created if it doesn't already exist.
    #[structopt(long = "db", parse(from_os_str))]
    database: PathBuf,

    /// Files to index in parallel.
    files_to_index: Vec<PathBuf>,
}

fn union_postings_ids(_key: &[u8], old_value: Option<&[u8]>, new_value: RoaringBitmap) -> Option<Vec<u8>> {
    let result = match old_value {
        Some(bytes) => {
            let mut old_value = RoaringBitmap::deserialize_from(bytes).unwrap();
            old_value.union_with(&new_value);
            old_value
        },
        None => new_value,
    };

    let mut vec = Vec::new();
    result.serialize_into(&mut vec).unwrap();
    Some(vec)
}

fn union_words_fst(key: &[u8], old_value: Option<&[u8]>, new_value: &fst::Set<Vec<u8>>) -> Option<Vec<u8>> {
    if key != b"words-fst" { unimplemented!() }

    // Do an union of the old and the new set of words.
    let mut builder = fst::set::OpBuilder::new();

    let old_words = old_value.map(|v| fst::Set::new(v).unwrap());
    let old_words = old_words.as_ref().map(|v| v.into_stream());
    if let Some(old_words) = old_words {
        builder.push(old_words);
    }

    builder.push(new_value);

    let op = builder.r#union();
    let mut build = fst::SetBuilder::memory();
    build.extend_stream(op.into_stream()).unwrap();

    Some(build.into_inner().unwrap())
}

fn alphanumeric_tokens(string: &str) -> impl Iterator<Item = &str> {
    let is_alphanumeric = |s: &&str| s.chars().next().map_or(false, char::is_alphanumeric);
    string.linear_group_by_key(|c| c.is_alphanumeric()).filter(is_alphanumeric)
}

enum MainKey {
    WordsFst(fst::Set<Vec<u8>>),
    Headers(Vec<u8>),
}

#[derive(Clone)]
struct DbSender {
    main: Sender<MainKey>,
    postings_ids: Sender<(SmallString32, RoaringBitmap)>,
    documents: Sender<(DocumentId, Vec<u8>)>,
}

struct DbReceiver {
    main: Receiver<MainKey>,
    postings_ids: Receiver<(SmallString32, RoaringBitmap)>,
    documents: Receiver<(DocumentId, Vec<u8>)>,
}

fn thread_channel() -> (DbSender, DbReceiver) {
    let (sd_main, rc_main) = crossbeam_channel::bounded(4);
    let (sd_postings, rc_postings) = crossbeam_channel::bounded(10);
    let (sd_documents, rc_documents) = crossbeam_channel::bounded(10);

    let sender = DbSender { main: sd_main, postings_ids: sd_postings, documents: sd_documents };
    let receiver = DbReceiver { main: rc_main, postings_ids: rc_postings, documents: rc_documents };

    (sender, receiver)
}

fn writer_thread(env: heed::Env, receiver: DbReceiver) -> anyhow::Result<()> {
    let main = env.create_poly_database(None)?;
    let postings_ids: Database<Str, ByteSlice> = env.create_database(Some("postings-ids"))?;
    let documents: Database<OwnedType<BEU32>, ByteSlice> = env.create_database(Some("documents"))?;

    let mut wtxn = env.write_txn()?;

    loop {
        select! {
            recv(receiver.main) -> msg => {
                let msg = match msg {
                    Err(_) => break,
                    Ok(msg) => msg,
                };

                match msg {
                    MainKey::WordsFst(new_fst) => {
                        let old_value = main.get::<_, Str, ByteSlice>(&wtxn, "words-fst")?;
                        let new_value = union_words_fst(b"words-fst", old_value, &new_fst)
                            .context("error while do a words-fst union")?;
                        main.put::<_, Str, ByteSlice>(&mut wtxn, "words-fst", &new_value)?;
                    },
                    MainKey::Headers(headers) => {
                        if let Some(old_headers) = main.get::<_, Str, ByteSlice>(&wtxn, "headers")? {
                            ensure!(old_headers == &*headers, "headers differs from the previous ones");
                        }
                        main.put::<_, Str, ByteSlice>(&mut wtxn, "headers", &headers)?;
                    },
                }
            },
            recv(receiver.postings_ids) -> msg => {
                let (word, postings) = match msg {
                    Err(_) => break,
                    Ok(msg) => msg,
                };

                let old_value = postings_ids.get(&wtxn, &word)?;
                let new_value = union_postings_ids(word.as_bytes(), old_value, postings)
                    .context("error while do a words-fst union")?;
                postings_ids.put(&mut wtxn, &word, &new_value)?;
            },
            recv(receiver.documents) -> msg => {
                let (id, content) = match msg {
                    Err(_) => break,
                    Ok(msg) => msg,
                };
                documents.put(&mut wtxn, &BEU32::new(id), &content)?;
            },
        }
    }

    wtxn.commit()?;
    Ok(())
}

fn index_csv(tid: usize, db_sender: DbSender, mut rdr: csv::Reader<File>) -> anyhow::Result<usize> {
    const MAX_POSITION: usize = 1000;
    const MAX_ATTRIBUTES: usize = u32::max_value() as usize / MAX_POSITION;

    let mut document = csv::StringRecord::new();
    let mut new_postings_ids = FastMap4::default();
    let mut new_words = BTreeSet::default();
    let mut number_of_documents = 0;

    // Write the headers into a Vec of bytes.
    let headers = rdr.headers()?;
    let mut writer = csv::WriterBuilder::new().has_headers(false).from_writer(Vec::new());
    writer.write_byte_record(headers.as_byte_record())?;
    let headers = writer.into_inner()?;
    db_sender.main.send(MainKey::Headers(headers))?;

    while rdr.read_record(&mut document)? {
        let document_id = ID_GENERATOR.fetch_add(1, Ordering::SeqCst);
        let document_id = DocumentId::try_from(document_id).context("Generated id is too big")?;

        for (_attr, content) in document.iter().enumerate().take(MAX_ATTRIBUTES) {
            for (_pos, word) in alphanumeric_tokens(&content).enumerate().take(MAX_POSITION) {
                if !word.is_empty() && word.len() < 500 { // LMDB limits
                    new_postings_ids.entry(SmallString32::from(word))
                        .or_insert_with(RoaringBitmap::new)
                        .insert(document_id);
                }
            }
        }

        // We write the document in the database.
        let mut writer = csv::WriterBuilder::new().has_headers(false).from_writer(Vec::new());
        writer.write_byte_record(document.as_byte_record())?;
        let document = writer.into_inner()?;
        db_sender.documents.send((document_id, document))?;

        number_of_documents += 1;
        if number_of_documents % 100000 == 0 {
            eprintln!("{}, documents seen {}", tid, number_of_documents);
        }
    }

    eprintln!("Start collecting the postings lists and words");

    // We compute and store the postings list into the DB.
    for (word, new_ids) in new_postings_ids {
        db_sender.postings_ids.send((word.clone(), new_ids))?;
        new_words.insert(word);
    }

    eprintln!("Finished collecting the postings lists and words");

    eprintln!("Start merging the words-fst");

    let new_words_fst = fst::Set::from_iter(new_words.iter().map(|s| s.as_str()))?;
    drop(new_words);
    db_sender.main.send(MainKey::WordsFst(new_words_fst))?;

    eprintln!("Finished merging the words-fst");
    eprintln!("Total number of documents seen is {}", ID_GENERATOR.load(Ordering::Relaxed));

    Ok(number_of_documents)
}

fn main() -> anyhow::Result<()> {
    let opt = Opt::from_args();

    std::fs::create_dir_all(&opt.database)?;
    let env = EnvOpenOptions::new()
        .map_size(100 * 1024 * 1024 * 1024) // 100 GB
        .max_readers(10)
        .max_dbs(5)
        .open(opt.database)?;

    let (sender, receiver) = thread_channel();
    let writing_child = thread::spawn(move || writer_thread(env, receiver));

    let res = opt.files_to_index
        .into_par_iter()
        .enumerate()
        .map(|(tid, path)| {
            let rdr = csv::Reader::from_path(path)?;
            index_csv(tid, sender.clone(), rdr)
        })
        .try_reduce(|| 0, |a, b| Ok(a + b));


    eprintln!("witing the writing thread...");
    writing_child.join().unwrap().unwrap();

    println!("indexed {:?} documents", res);

    Ok(())
}
