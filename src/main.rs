use std::collections::hash_map::Entry;
use std::collections::{HashMap, BTreeSet};
use std::convert::TryFrom;
use std::fs::File;
use std::hash::BuildHasherDefault;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};

use anyhow::{ensure, Context};
use roaring::RoaringBitmap;
use fst::IntoStreamer;
use fxhash::FxHasher32;
use heed::{EnvOpenOptions, PolyDatabase, Database};
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

#[derive(Default)]
struct Indexed {
    fst: fst::Set<Vec<u8>>,
    postings_ids: FastMap4<SmallString32, RoaringBitmap>,
    headers: Vec<u8>,
    documents: Vec<(DocumentId, Vec<u8>)>,
}

impl Indexed {
    fn merge_with(mut self, mut other: Indexed) -> Indexed {

        // Union of the two FSTs
        let op = fst::set::OpBuilder::new()
            .add(self.fst.into_stream())
            .add(other.fst.into_stream())
            .r#union();

        let mut build = fst::SetBuilder::memory();
        build.extend_stream(op.into_stream()).unwrap();
        let fst = build.into_set();

        // Merge the postings by unions
        for (word, mut postings) in other.postings_ids {
            match self.postings_ids.entry(word) {
                Entry::Occupied(mut entry) => {
                    let old = entry.get();
                    postings.union_with(&old);
                    entry.insert(postings);
                },
                Entry::Vacant(entry) => {
                    entry.insert(postings);
                },
            }
        }

        // assert headers are valid
        if !self.headers.is_empty() {
            assert_eq!(self.headers, other.headers);
        }

        // extend the documents
        self.documents.append(&mut other.documents);

        Indexed {
            fst,
            postings_ids: self.postings_ids,
            headers: self.headers,
            documents: self.documents,
        }
    }
}

fn index_csv(mut rdr: csv::Reader<File>) -> anyhow::Result<Indexed> {
    const MAX_POSITION: usize = 1000;
    const MAX_ATTRIBUTES: usize = u32::max_value() as usize / MAX_POSITION;

    let mut document = csv::StringRecord::new();
    let mut postings_ids = FastMap4::default();
    let mut documents = Vec::new();

    // Write the headers into a Vec of bytes.
    let headers = rdr.headers()?;
    let mut writer = csv::WriterBuilder::new().has_headers(false).from_writer(Vec::new());
    writer.write_byte_record(headers.as_byte_record())?;
    let headers = writer.into_inner()?;

    while rdr.read_record(&mut document)? {
        let document_id = ID_GENERATOR.fetch_add(1, Ordering::SeqCst);
        let document_id = DocumentId::try_from(document_id).context("Generated id is too big")?;

        for (_attr, content) in document.iter().enumerate().take(MAX_ATTRIBUTES) {
            for (_pos, word) in alphanumeric_tokens(&content).enumerate().take(MAX_POSITION) {
                if !word.is_empty() && word.len() < 500 { // LMDB limits
                    postings_ids.entry(SmallString32::from(word))
                        .or_insert_with(RoaringBitmap::new)
                        .insert(document_id);
                }
            }
        }

        // We write the document in the database.
        let mut writer = csv::WriterBuilder::new().has_headers(false).from_writer(Vec::new());
        writer.write_byte_record(document.as_byte_record())?;
        let document = writer.into_inner()?;
        documents.push((document_id, document));
    }

    // We compute and store the postings list into the DB.
    let mut new_words = BTreeSet::default();
    for (word, _new_ids) in &postings_ids {
        new_words.insert(word.clone());
    }

    let new_words_fst = fst::Set::from_iter(new_words.iter().map(SmallString32::as_str))?;

    Ok(Indexed { fst: new_words_fst, headers, postings_ids, documents })
}

fn writer(
    wtxn: &mut heed::RwTxn,
    main: PolyDatabase,
    postings_ids: Database<Str, ByteSlice>,
    documents: Database<OwnedType<BEU32>, ByteSlice>,
    indexed: Indexed,
) -> anyhow::Result<usize>
{
    // Write and merge the words fst
    let old_value = main.get::<_, Str, ByteSlice>(wtxn, "words-fst")?;
    let new_value = union_words_fst(b"words-fst", old_value, &indexed.fst)
        .context("error while do a words-fst union")?;
    main.put::<_, Str, ByteSlice>(wtxn, "words-fst", &new_value)?;

    // Write and merge the headers
    if let Some(old_headers) = main.get::<_, Str, ByteSlice>(wtxn, "headers")? {
        ensure!(old_headers == &*indexed.headers, "headers differs from the previous ones");
    }
    main.put::<_, Str, ByteSlice>(wtxn, "headers", &indexed.headers)?;

    // Write and merge the postings lists
    for (word, postings) in indexed.postings_ids {
        let old_value = postings_ids.get(wtxn, word.as_str())?;
        let new_value = union_postings_ids(word.as_bytes(), old_value, postings)
            .context("error while do a words-fst union")?;
        postings_ids.put(wtxn, &word, &new_value)?;
    }

    let count = indexed.documents.len();

    // Write the documents
    for (id, content) in indexed.documents {
        documents.put(wtxn, &BEU32::new(id), &content)?;
    }

    Ok(count)
}

fn main() -> anyhow::Result<()> {
    let opt = Opt::from_args();

    std::fs::create_dir_all(&opt.database)?;
    let env = EnvOpenOptions::new()
        .map_size(100 * 1024 * 1024 * 1024) // 100 GB
        .max_readers(10)
        .max_dbs(5)
        .open(opt.database)?;

    let main = env.create_poly_database(None)?;
    let postings_ids: Database<Str, ByteSlice> = env.create_database(Some("postings-ids"))?;
    let documents: Database<OwnedType<BEU32>, ByteSlice> = env.create_database(Some("documents"))?;

    let res = opt.files_to_index
        .into_par_iter()
        .try_fold(|| Indexed::default(), |acc, path| {
            let rdr = csv::Reader::from_path(path)?;
            let indexed = index_csv(rdr)?;
            Ok(acc.merge_with(indexed)) as anyhow::Result<Indexed>
        })
        .map(|indexed| match indexed {
            Ok(indexed) => {
                let tid = rayon::current_thread_index();
                eprintln!("{:?}: A new step to write into LMDB", tid);
                let mut wtxn = env.write_txn()?;
                let count = writer(&mut wtxn, main, postings_ids, documents, indexed)?;
                wtxn.commit()?;
                eprintln!("{:?}: Wrote {} documents into LMDB", tid, count);
                Ok(count)
            },
            Err(e) => Err(e),
        })
        .inspect(|_| {
            eprintln!("Total number of documents seen so far is {}", ID_GENERATOR.load(Ordering::Relaxed))
        })
        .try_reduce(|| 0, |a, b| Ok(a + b));

    println!("indexed {:?} documents", res);

    Ok(())
}
