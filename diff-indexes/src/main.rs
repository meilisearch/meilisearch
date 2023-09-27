use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use clap::Parser;
use heed::flags::Flags;
use heed::types::{OwnedType, SerdeJson, Str, Unit};
use heed::{Database, EnvOpenOptions, PolyDatabase};
use milli::heed_codec::facet::{
    FacetGroupKeyCodec, FacetGroupValueCodec, FieldDocIdFacetF64Codec, FieldDocIdFacetStringCodec,
    FieldIdCodec, OrderedF64Codec,
};
use milli::heed_codec::{FstSetCodec, ScriptLanguageCodec, StrBEU16Codec, StrRefCodec};
use milli::index::db_name::*;
use milli::{
    BEU16StrCodec, CboRoaringBitmapCodec, FieldIdWordCountCodec, ObkvCodec, RoaringBitmapCodec,
    U8StrStrCodec, BEU16, BEU32,
};

mod compare;

/// Simple program to greet a person
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct Args {
    /// The left hand side database.
    #[arg(long, long = "lhs")]
    lhs_database: PathBuf,

    /// The right hand side database.
    #[arg(long, long = "rhs")]
    rhs_database: PathBuf,
}

fn main() -> anyhow::Result<()> {
    let Args { lhs_database, rhs_database } = Args::parse();

    let lhs_database = Index::open(EnvOpenOptions::new(), lhs_database)?;
    let rhs_database = Index::open(EnvOpenOptions::new(), rhs_database)?;
    compare::compare(lhs_database, rhs_database)?;

    Ok(())
}

pub struct Index {
    pub env: heed::Env,
    pub main: PolyDatabase,
    pub word_docids: Database<Str, RoaringBitmapCodec>,
    pub exact_word_docids: Database<Str, RoaringBitmapCodec>,
    pub word_prefix_docids: Database<Str, RoaringBitmapCodec>,
    pub exact_word_prefix_docids: Database<Str, RoaringBitmapCodec>,
    pub word_pair_proximity_docids: Database<U8StrStrCodec, CboRoaringBitmapCodec>,
    pub word_prefix_pair_proximity_docids: Database<U8StrStrCodec, CboRoaringBitmapCodec>,
    pub prefix_word_pair_proximity_docids: Database<U8StrStrCodec, CboRoaringBitmapCodec>,
    pub word_position_docids: Database<StrBEU16Codec, CboRoaringBitmapCodec>,
    pub word_fid_docids: Database<StrBEU16Codec, CboRoaringBitmapCodec>,
    pub field_id_word_count_docids: Database<FieldIdWordCountCodec, CboRoaringBitmapCodec>,
    pub word_prefix_position_docids: Database<StrBEU16Codec, CboRoaringBitmapCodec>,
    pub word_prefix_fid_docids: Database<StrBEU16Codec, CboRoaringBitmapCodec>,
    pub script_language_docids: Database<ScriptLanguageCodec, RoaringBitmapCodec>,
    pub facet_id_exists_docids: Database<FieldIdCodec, CboRoaringBitmapCodec>,
    pub facet_id_is_null_docids: Database<FieldIdCodec, CboRoaringBitmapCodec>,
    pub facet_id_is_empty_docids: Database<FieldIdCodec, CboRoaringBitmapCodec>,
    pub facet_id_f64_docids: Database<FacetGroupKeyCodec<OrderedF64Codec>, FacetGroupValueCodec>,
    pub facet_id_string_docids: Database<FacetGroupKeyCodec<StrRefCodec>, FacetGroupValueCodec>,
    pub facet_id_normalized_string_strings: Database<BEU16StrCodec, SerdeJson<BTreeSet<String>>>,
    pub facet_id_string_fst: Database<OwnedType<BEU16>, FstSetCodec>,
    pub field_id_docid_facet_f64s: Database<FieldDocIdFacetF64Codec, Unit>,
    pub field_id_docid_facet_strings: Database<FieldDocIdFacetStringCodec, Str>,
    pub vector_id_docid: Database<OwnedType<BEU32>, OwnedType<BEU32>>,
    pub documents: Database<OwnedType<BEU32>, ObkvCodec>,
}

impl Index {
    pub fn open<P: AsRef<Path>>(mut options: EnvOpenOptions, path: P) -> anyhow::Result<Index> {
        options.max_dbs(25);
        unsafe { options.flag(Flags::MdbAlwaysFreePages) };

        let env = options.open(path)?;
        let rtxn = env.read_txn()?;
        let main = env.open_poly_database(&rtxn, Some(MAIN))?.unwrap();
        let word_docids = env.open_database(&rtxn, Some(WORD_DOCIDS))?.unwrap();
        let exact_word_docids = env.open_database(&rtxn, Some(EXACT_WORD_DOCIDS))?.unwrap();
        let word_prefix_docids = env.open_database(&rtxn, Some(WORD_PREFIX_DOCIDS))?.unwrap();
        let exact_word_prefix_docids =
            env.open_database(&rtxn, Some(EXACT_WORD_PREFIX_DOCIDS))?.unwrap();
        let word_pair_proximity_docids =
            env.open_database(&rtxn, Some(WORD_PAIR_PROXIMITY_DOCIDS))?.unwrap();
        let script_language_docids =
            env.open_database(&rtxn, Some(SCRIPT_LANGUAGE_DOCIDS))?.unwrap();
        let word_prefix_pair_proximity_docids =
            env.open_database(&rtxn, Some(WORD_PREFIX_PAIR_PROXIMITY_DOCIDS))?.unwrap();
        let prefix_word_pair_proximity_docids =
            env.open_database(&rtxn, Some(PREFIX_WORD_PAIR_PROXIMITY_DOCIDS))?.unwrap();
        let word_position_docids = env.open_database(&rtxn, Some(WORD_POSITION_DOCIDS))?.unwrap();
        let word_fid_docids = env.open_database(&rtxn, Some(WORD_FIELD_ID_DOCIDS))?.unwrap();
        let field_id_word_count_docids =
            env.open_database(&rtxn, Some(FIELD_ID_WORD_COUNT_DOCIDS))?.unwrap();
        let word_prefix_position_docids =
            env.open_database(&rtxn, Some(WORD_PREFIX_POSITION_DOCIDS))?.unwrap();
        let word_prefix_fid_docids =
            env.open_database(&rtxn, Some(WORD_PREFIX_FIELD_ID_DOCIDS))?.unwrap();
        let facet_id_f64_docids = env.open_database(&rtxn, Some(FACET_ID_F64_DOCIDS))?.unwrap();
        let facet_id_string_docids =
            env.open_database(&rtxn, Some(FACET_ID_STRING_DOCIDS))?.unwrap();
        let facet_id_normalized_string_strings =
            env.open_database(&rtxn, Some(FACET_ID_NORMALIZED_STRING_STRINGS))?.unwrap();
        let facet_id_string_fst = env.open_database(&rtxn, Some(FACET_ID_STRING_FST))?.unwrap();
        let facet_id_exists_docids =
            env.open_database(&rtxn, Some(FACET_ID_EXISTS_DOCIDS))?.unwrap();
        let facet_id_is_null_docids =
            env.open_database(&rtxn, Some(FACET_ID_IS_NULL_DOCIDS))?.unwrap();
        let facet_id_is_empty_docids =
            env.open_database(&rtxn, Some(FACET_ID_IS_EMPTY_DOCIDS))?.unwrap();
        let field_id_docid_facet_f64s =
            env.open_database(&rtxn, Some(FIELD_ID_DOCID_FACET_F64S))?.unwrap();
        let field_id_docid_facet_strings =
            env.open_database(&rtxn, Some(FIELD_ID_DOCID_FACET_STRINGS))?.unwrap();
        let vector_id_docid = env.open_database(&rtxn, Some(VECTOR_ID_DOCID))?.unwrap();
        let documents = env.open_database(&rtxn, Some(DOCUMENTS))?.unwrap();

        drop(rtxn);

        Ok(Index {
            env,
            main,
            word_docids,
            exact_word_docids,
            word_prefix_docids,
            exact_word_prefix_docids,
            word_pair_proximity_docids,
            script_language_docids,
            word_prefix_pair_proximity_docids,
            prefix_word_pair_proximity_docids,
            word_position_docids,
            word_fid_docids,
            word_prefix_position_docids,
            word_prefix_fid_docids,
            field_id_word_count_docids,
            facet_id_f64_docids,
            facet_id_string_docids,
            facet_id_normalized_string_strings,
            facet_id_string_fst,
            facet_id_exists_docids,
            facet_id_is_null_docids,
            facet_id_is_empty_docids,
            field_id_docid_facet_f64s,
            field_id_docid_facet_strings,
            vector_id_docid,
            documents,
        })
    }
}
