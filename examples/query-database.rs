#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

use std::collections::btree_map::{BTreeMap, Entry};
use std::iter::FromIterator;
use std::io::{self, Write};
use std::time::Instant;
use std::path::PathBuf;
use std::error::Error;

use hashbrown::{HashMap, HashSet};
use termcolor::{Color, ColorChoice, ColorSpec, StandardStream, WriteColor};
use structopt::StructOpt;

use meilidb::database::schema::SchemaAttr;
use meilidb::database::Database;
use meilidb::Match;

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

fn create_highlight_areas(text: &str, matches: &[Match], attribute: SchemaAttr) -> Vec<usize> {
    let mut byte_indexes = BTreeMap::new();

    for match_ in matches {
        let match_attribute = match_.attribute;
        if SchemaAttr::new(match_attribute) == attribute {
            let char_index = match_.char_index as usize;
            let char_length = match_.char_length as usize;
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

fn main() -> Result<(), Box<Error>> {
    let _ = env_logger::init();
    let opt = Opt::from_args();

    let start = Instant::now();
    let database = Database::open(&opt.database_path)?;
    println!("database prepared for you in {:.2?}", start.elapsed());

    let mut buffer = String::new();
    let input = io::stdin();

    loop {
        print!("Searching for: ");
        io::stdout().flush()?;

        if input.read_line(&mut buffer)? == 0 { break }
        let query = buffer.trim_end_matches('\n');

        let view = database.view("default")?;
        let schema = view.schema();

        let start = Instant::now();

        let builder = view.query_builder().unwrap();
        let documents = builder.query(query, 0..opt.number_results);

        let number_of_documents = documents.len();
        for doc in documents {
            match view.document_by_id::<Document>(doc.id) {
                Ok(document) => {
                    for name in &opt.displayed_fields {
                        let attr = match schema.attribute(name) {
                            Some(attr) => attr,
                            None => continue,
                        };
                        let text = match document.get(name) {
                            Some(text) => text,
                            None => continue,
                        };

                        print!("{}: ", name);
                        let areas = create_highlight_areas(&text, &doc.matches, attr);
                        display_highlights(&text, &areas)?;
                        println!();
                    }
                },
                Err(e) => eprintln!("{}", e),
            }

            let mut matching_attributes = HashSet::new();
            for _match in doc.matches {
                let attr = SchemaAttr::new(_match.attribute);
                let name = schema.attribute_name(attr);
                matching_attributes.insert(name);
            }

            let matching_attributes = Vec::from_iter(matching_attributes);
            println!("matching in: {:?}", matching_attributes);

            println!();
        }

        eprintln!("===== Found {} results in {:.2?} =====", number_of_documents, start.elapsed());
        buffer.clear();
    }

    Ok(())
}
