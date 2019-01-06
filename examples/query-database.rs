#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

use std::collections::btree_map::{BTreeMap, Entry};
use std::iter::FromIterator;
use std::io::{self, Write};
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

fn create_highlight_areas(text: &str, matches: &[Match], attribute: SchemaAttr) -> Vec<usize> {
    let mut byte_indexes = BTreeMap::new();

    for match_ in matches {
        let match_attribute = match_.attribute.attribute();
        if SchemaAttr::new(match_attribute) == attribute {
            let word_area = match_.word_area;
            let byte_index = word_area.byte_index() as usize;
            let length = word_area.length() as usize;
            match byte_indexes.entry(byte_index) {
                Entry::Vacant(entry) => { entry.insert(length); },
                Entry::Occupied(mut entry) => if *entry.get() < length { entry.insert(length); },
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

    let (elapsed, result) = elapsed::measure_time(|| Database::open(&opt.database_path));
    let database = result?;
    println!("database prepared for you in {}", elapsed);

    let mut buffer = String::new();
    let input = io::stdin();

    loop {
        print!("Searching for: ");
        io::stdout().flush()?;

        if input.read_line(&mut buffer)? == 0 { break }
        let query = buffer.trim_end_matches('\n');

        let view = database.view();
        let schema = view.schema();

        let (elapsed, documents) = elapsed::measure_time(|| {
            let builder = view.query_builder().unwrap();
            builder.query(query, 0..opt.number_results)
        });

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
                        let areas = create_highlight_areas(&text, doc.matches.as_matches(), attr);
                        display_highlights(&text, &areas)?;
                        println!();
                    }
                },
                Err(e) => eprintln!("{}", e),
            }

            let mut matching_attributes = HashSet::new();
            for _match in doc.matches.as_matches() {
                let attr = SchemaAttr::new(_match.attribute.attribute());
                let name = schema.attribute_name(attr);
                matching_attributes.insert(name);
            }

            let matching_attributes = Vec::from_iter(matching_attributes);
            println!("matching in: {:?}", matching_attributes);

            println!();
        }

        eprintln!("===== Found {} results in {} =====", number_of_documents, elapsed);
        buffer.clear();
    }

    Ok(())
}
