#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

use std::collections::btree_map::{BTreeMap, Entry};
use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::io::{self, Write};
use std::iter::FromIterator;
use std::path::PathBuf;
use std::time::{Instant, Duration};

use termcolor::{Color, ColorChoice, ColorSpec, StandardStream, WriteColor};
use structopt::StructOpt;
use rustyline::{Editor, Config};

use meilidb_core::Highlight;
use meilidb_data::Database;
use meilidb_schema::SchemaAttr;

#[derive(Debug, StructOpt)]
pub struct Opt {
    /// The destination where the database must be created
    #[structopt(parse(from_os_str))]
    pub database_path: PathBuf,

    /// Fields that must be displayed.
    pub displayed_fields: Vec<String>,

    /// The number of returned results
    #[structopt(short = "n", long = "number-results", default_value = "10")]
    pub number_results: usize,

    /// The number of characters before and after the first match
    #[structopt(short = "C", long = "context", default_value = "35")]
    pub char_context: usize,
}

type Document = HashMap<String, String>;

fn display_highlights(text: &str, ranges: &[usize]) -> io::Result<()> {
    let mut stdout = StandardStream::stdout(ColorChoice::Always);
    let mut highlighted = false;

    for range in ranges.windows(2) {
        let [start, end] = match range { [start, end] => [*start, *end], _ => unreachable!() };
        if highlighted {
            stdout.set_color(ColorSpec::new().set_fg(Some(Color::Yellow)))?;
        }
        write!(&mut stdout, "{}", &text[start..end])?;
        stdout.reset()?;
        highlighted = !highlighted;
    }

    Ok(())
}

fn char_to_byte_range(index: usize, length: usize, text: &str) -> (usize, usize) {
    let mut byte_index = 0;
    let mut byte_length = 0;

    for (n, (i, c)) in text.char_indices().enumerate() {
        if n == index {
            byte_index = i;
        }

        if n + 1 == index + length {
            byte_length = i - byte_index + c.len_utf8();
            break;
        }
    }

    (byte_index, byte_length)
}

fn create_highlight_areas(text: &str, highlights: &[Highlight]) -> Vec<usize> {
    let mut byte_indexes = BTreeMap::new();

    for highlight in highlights {
        let char_index = highlight.char_index as usize;
        let char_length = highlight.char_length as usize;
        let (byte_index, byte_length) = char_to_byte_range(char_index, char_length, text);

        match byte_indexes.entry(byte_index) {
            Entry::Vacant(entry) => { entry.insert(byte_length); },
            Entry::Occupied(mut entry) => {
                if *entry.get() < byte_length {
                    entry.insert(byte_length);
                }
            },
        }
    }

    let mut title_areas = Vec::new();
    title_areas.push(0);
    for (byte_index, length) in byte_indexes {
        title_areas.push(byte_index);
        title_areas.push(byte_index + length);
    }
    title_areas.push(text.len());
    title_areas.sort_unstable();
    title_areas
}

/// note: matches must have been sorted by `char_index` and `char_length` before being passed.
///
/// ```no_run
/// matches.sort_unstable_by_key(|m| (m.char_index, m.char_length));
///
/// let matches = matches.matches.iter().filter(|m| SchemaAttr::new(m.attribute) == attr).cloned();
///
/// let (text, matches) = crop_text(&text, matches, 35);
/// ```
fn crop_text(
    text: &str,
    highlights: impl IntoIterator<Item=Highlight>,
    context: usize,
) -> (String, Vec<Highlight>)
{
    let mut highlights = highlights.into_iter().peekable();

    let char_index = highlights.peek().map(|m| m.char_index as usize).unwrap_or(0);
    let start = char_index.saturating_sub(context);
    let text = text.chars().skip(start).take(context * 2).collect();

    let highlights = highlights
        .take_while(|m| {
            (m.char_index as usize) + (m.char_length as usize) <= start + (context * 2)
        })
        .map(|highlight| {
            Highlight { char_index: highlight.char_index - start as u16, ..highlight }
        })
        .collect();

    (text, highlights)
}

fn main() -> Result<(), Box<dyn Error>> {
    let _ = env_logger::init();
    let opt = Opt::from_args();

    let start = Instant::now();
    let database = Database::start_default(&opt.database_path)?;

    let index = database.open_index("test")?.unwrap();
    let schema = index.schema();

    println!("database prepared for you in {:.2?}", start.elapsed());

    let fields = opt.displayed_fields.iter().map(String::as_str);
    let fields = HashSet::from_iter(fields);

    let config = Config::builder().auto_add_history(true).build();
    let mut readline = Editor::<()>::with_config(config);
    let _ = readline.load_history("query-history.txt");

    for result in readline.iter("Searching for: ") {
        match result {
            Ok(query) => {
                let start_total = Instant::now();

                let builder = index.query_builder();
                let documents = builder.query(&query, 0..opt.number_results)?;

                let mut retrieve_duration = Duration::default();

                let number_of_documents = documents.len();
                for mut doc in documents {

                    doc.highlights.sort_unstable_by_key(|m| (m.char_index, m.char_length));

                    let start_retrieve = Instant::now();
                    let result = index.document::<Document>(Some(&fields), doc.id);
                    retrieve_duration += start_retrieve.elapsed();

                    match result {
                        Ok(Some(document)) => {
                            for (name, text) in document {
                                print!("{}: ", name);

                                let attr = schema.attribute(&name).unwrap();
                                let highlights = doc.highlights.iter()
                                                .filter(|m| SchemaAttr::new(m.attribute) == attr)
                                                .cloned();
                                let (text, highlights) = crop_text(&text, highlights, opt.char_context);
                                let areas = create_highlight_areas(&text, &highlights);
                                display_highlights(&text, &areas)?;
                                println!();
                            }
                        },
                        Ok(None) => eprintln!("missing document"),
                        Err(e) => eprintln!("{}", e),
                    }

                    let mut matching_attributes = HashSet::new();
                    for highlight in doc.highlights {
                        let attr = SchemaAttr::new(highlight.attribute);
                        let name = schema.attribute_name(attr);
                        matching_attributes.insert(name);
                    }

                    let matching_attributes = Vec::from_iter(matching_attributes);
                    println!("matching in: {:?}", matching_attributes);

                    println!();
                }

                eprintln!("document field retrieve took {:.2?}", retrieve_duration);
                eprintln!("===== Found {} results in {:.2?} =====", number_of_documents, start_total.elapsed());
            },
            Err(err) => {
                println!("Error: {:?}", err);
                break
            }
        }
    }

    readline.save_history("query-history.txt").unwrap();
    Ok(())
}
