use std::cmp::Reverse;
use std::path::PathBuf;

use clap::Parser;
use milli::heed::{types::ByteSlice, EnvOpenOptions, PolyDatabase, RoTxn};
use milli::index::db_name::*;
use milli::index::Index;
use piechart::{Chart, Color, Data};

/// Simple program to greet a person
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// The path to the LMDB Meilisearch index database.
    path: PathBuf,

    /// The radius of the graphs
    #[clap(long, default_value_t = 10)]
    graph_radius: u16,

    /// The radius of the graphs
    #[clap(long, default_value_t = 6)]
    graph_aspect_ratio: u16,
}

fn main() -> anyhow::Result<()> {
    let Args { path, graph_radius, graph_aspect_ratio } = Args::parse();
    let env = EnvOpenOptions::new().max_dbs(24).open(path)?;

    // TODO not sure to keep that...
    //      if removed put the pub(crate) back in the Index struct
    matches!(
        Option::<Index>::None,
        Some(Index {
            env: _,
            main: _,
            word_docids: _,
            exact_word_docids: _,
            word_prefix_docids: _,
            exact_word_prefix_docids: _,
            word_pair_proximity_docids: _,
            word_prefix_pair_proximity_docids: _,
            prefix_word_pair_proximity_docids: _,
            word_position_docids: _,
            word_fid_docids: _,
            field_id_word_count_docids: _,
            word_prefix_position_docids: _,
            word_prefix_fid_docids: _,
            script_language_docids: _,
            facet_id_exists_docids: _,
            facet_id_is_null_docids: _,
            facet_id_is_empty_docids: _,
            facet_id_f64_docids: _,
            facet_id_string_docids: _,
            field_id_docid_facet_f64s: _,
            field_id_docid_facet_strings: _,
            documents: _,
        })
    );

    let mut wtxn = env.write_txn()?;
    let main = env.create_poly_database(&mut wtxn, Some(MAIN))?;
    let word_docids = env.create_poly_database(&mut wtxn, Some(WORD_DOCIDS))?;
    let exact_word_docids = env.create_poly_database(&mut wtxn, Some(EXACT_WORD_DOCIDS))?;
    let word_prefix_docids = env.create_poly_database(&mut wtxn, Some(WORD_PREFIX_DOCIDS))?;
    let exact_word_prefix_docids =
        env.create_poly_database(&mut wtxn, Some(EXACT_WORD_PREFIX_DOCIDS))?;
    let word_pair_proximity_docids =
        env.create_poly_database(&mut wtxn, Some(WORD_PAIR_PROXIMITY_DOCIDS))?;
    let script_language_docids =
        env.create_poly_database(&mut wtxn, Some(SCRIPT_LANGUAGE_DOCIDS))?;
    let word_prefix_pair_proximity_docids =
        env.create_poly_database(&mut wtxn, Some(WORD_PREFIX_PAIR_PROXIMITY_DOCIDS))?;
    let prefix_word_pair_proximity_docids =
        env.create_poly_database(&mut wtxn, Some(PREFIX_WORD_PAIR_PROXIMITY_DOCIDS))?;
    let word_position_docids = env.create_poly_database(&mut wtxn, Some(WORD_POSITION_DOCIDS))?;
    let word_fid_docids = env.create_poly_database(&mut wtxn, Some(WORD_FIELD_ID_DOCIDS))?;
    let field_id_word_count_docids =
        env.create_poly_database(&mut wtxn, Some(FIELD_ID_WORD_COUNT_DOCIDS))?;
    let word_prefix_position_docids =
        env.create_poly_database(&mut wtxn, Some(WORD_PREFIX_POSITION_DOCIDS))?;
    let word_prefix_fid_docids =
        env.create_poly_database(&mut wtxn, Some(WORD_PREFIX_FIELD_ID_DOCIDS))?;
    let facet_id_f64_docids = env.create_poly_database(&mut wtxn, Some(FACET_ID_F64_DOCIDS))?;
    let facet_id_string_docids =
        env.create_poly_database(&mut wtxn, Some(FACET_ID_STRING_DOCIDS))?;
    let facet_id_exists_docids =
        env.create_poly_database(&mut wtxn, Some(FACET_ID_EXISTS_DOCIDS))?;
    let facet_id_is_null_docids =
        env.create_poly_database(&mut wtxn, Some(FACET_ID_IS_NULL_DOCIDS))?;
    let facet_id_is_empty_docids =
        env.create_poly_database(&mut wtxn, Some(FACET_ID_IS_EMPTY_DOCIDS))?;
    let field_id_docid_facet_f64s =
        env.create_poly_database(&mut wtxn, Some(FIELD_ID_DOCID_FACET_F64S))?;
    let field_id_docid_facet_strings =
        env.create_poly_database(&mut wtxn, Some(FIELD_ID_DOCID_FACET_STRINGS))?;
    let documents = env.create_poly_database(&mut wtxn, Some(DOCUMENTS))?;
    wtxn.commit()?;

    let list = [
        (main, MAIN),
        (word_docids, WORD_DOCIDS),
        (exact_word_docids, EXACT_WORD_DOCIDS),
        (word_prefix_docids, WORD_PREFIX_DOCIDS),
        (exact_word_prefix_docids, EXACT_WORD_PREFIX_DOCIDS),
        (word_pair_proximity_docids, WORD_PAIR_PROXIMITY_DOCIDS),
        (script_language_docids, SCRIPT_LANGUAGE_DOCIDS),
        (word_prefix_pair_proximity_docids, WORD_PREFIX_PAIR_PROXIMITY_DOCIDS),
        (prefix_word_pair_proximity_docids, PREFIX_WORD_PAIR_PROXIMITY_DOCIDS),
        (word_position_docids, WORD_POSITION_DOCIDS),
        (word_fid_docids, WORD_FIELD_ID_DOCIDS),
        (field_id_word_count_docids, FIELD_ID_WORD_COUNT_DOCIDS),
        (word_prefix_position_docids, WORD_PREFIX_POSITION_DOCIDS),
        (word_prefix_fid_docids, WORD_PREFIX_FIELD_ID_DOCIDS),
        (facet_id_f64_docids, FACET_ID_F64_DOCIDS),
        (facet_id_string_docids, FACET_ID_STRING_DOCIDS),
        (facet_id_exists_docids, FACET_ID_EXISTS_DOCIDS),
        (facet_id_is_null_docids, FACET_ID_IS_NULL_DOCIDS),
        (facet_id_is_empty_docids, FACET_ID_IS_EMPTY_DOCIDS),
        (field_id_docid_facet_f64s, FIELD_ID_DOCID_FACET_F64S),
        (field_id_docid_facet_strings, FIELD_ID_DOCID_FACET_STRINGS),
        (documents, DOCUMENTS),
    ];

    let rtxn = env.read_txn()?;
    let result: Result<Vec<_>, _> =
        list.into_iter().map(|(db, name)| compute_stats(&rtxn, db).map(|s| (s, name))).collect();
    let mut stats = result?;

    println!("{:1$} Number of Entries", "", graph_radius as usize * 2);
    stats.sort_by_key(|(s, _)| Reverse(s.number_of_entries));
    let data = compute_graph_data(stats.iter().map(|(s, n)| (s.number_of_entries as f32, *n)));
    Chart::new().radius(graph_radius).aspect_ratio(graph_aspect_ratio).draw(&data);
    display_legend(&data);
    print!("\r\n");

    println!("{:1$} Size of Entries", "", graph_radius as usize * 2);
    stats.sort_by_key(|(s, _)| Reverse(s.size_of_entries));
    let data = compute_graph_data(stats.iter().map(|(s, n)| (s.size_of_entries as f32, *n)));
    Chart::new().radius(graph_radius).aspect_ratio(graph_aspect_ratio).draw(&data);
    display_legend(&data);
    print!("\r\n");

    println!("{:1$} Size of Data", "", graph_radius as usize * 2);
    stats.sort_by_key(|(s, _)| Reverse(s.size_of_data));
    let data = compute_graph_data(stats.iter().map(|(s, n)| (s.size_of_data as f32, *n)));
    Chart::new().radius(graph_radius).aspect_ratio(graph_aspect_ratio).draw(&data);
    display_legend(&data);
    print!("\r\n");

    println!("{:1$} Size of Keys", "", graph_radius as usize * 2);
    stats.sort_by_key(|(s, _)| Reverse(s.size_of_keys));
    let data = compute_graph_data(stats.iter().map(|(s, n)| (s.size_of_keys as f32, *n)));
    Chart::new().radius(graph_radius).aspect_ratio(graph_aspect_ratio).draw(&data);
    display_legend(&data);

    Ok(())
}

fn display_legend(data: &[Data]) {
    let total: f32 = data.iter().map(|d| d.value).sum();
    for Data { label, value, color, fill } in data {
        println!(
            "{} {} {:.02}%",
            color.unwrap().paint(fill.to_string()),
            label,
            value / total * 100.0
        );
    }
}

fn compute_graph_data<'a>(stats: impl IntoIterator<Item = (f32, &'a str)>) -> Vec<Data> {
    let mut colors = [
        Color::Red,
        Color::Green,
        Color::Yellow,
        Color::Blue,
        Color::Purple,
        Color::Cyan,
        Color::White,
    ]
    .into_iter()
    .cycle();

    let mut characters = ['▴', '▵', '▾', '▿', '▪', '▫', '•', '◦'].into_iter().cycle();

    stats
        .into_iter()
        .map(|(value, name)| Data {
            label: (*name).into(),
            value,
            color: Some(colors.next().unwrap().into()),
            fill: characters.next().unwrap(),
        })
        .collect()
}

#[derive(Debug)]
pub struct Stats {
    pub number_of_entries: u64,
    pub size_of_keys: u64,
    pub size_of_data: u64,
    pub size_of_entries: u64,
}

fn compute_stats(rtxn: &RoTxn, db: PolyDatabase) -> anyhow::Result<Stats> {
    let mut number_of_entries = 0;
    let mut size_of_keys = 0;
    let mut size_of_data = 0;

    for result in db.iter::<_, ByteSlice, ByteSlice>(rtxn)? {
        let (key, data) = result?;
        number_of_entries += 1;
        size_of_keys += key.len() as u64;
        size_of_data += data.len() as u64;
    }

    Ok(Stats {
        number_of_entries,
        size_of_keys,
        size_of_data,
        size_of_entries: size_of_keys + size_of_data,
    })
}
