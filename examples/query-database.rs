use std::io::{self, Write};
use std::path::PathBuf;
use std::error::Error;

use termcolor::{Color, ColorChoice, ColorSpec, StandardStream, WriteColor};
use serde_derive::{Serialize, Deserialize};
use structopt::StructOpt;

use meilidb::database::Database;
use meilidb::Match;

#[derive(Debug, StructOpt)]
pub struct Opt {
    /// The destination where the database must be created
    #[structopt(parse(from_os_str))]
    pub database_path: PathBuf,

    /// The number of returned results
    #[structopt(short = "n", long = "number-results", default_value = "10")]
    pub number_results: usize,
}

#[derive(Debug, Serialize, Deserialize)]
struct Document {
    id: String,
    title: String,
    description: String,
    image: String,
}

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

fn create_highlight_areas(text: &str, matches: &[Match], attribute: u16) -> Vec<usize> {
    let mut title_areas = Vec::new();

    title_areas.push(0);
    for match_ in matches {
        if match_.attribute.attribute() == attribute {
            let word_area = match_.word_area;
            let byte_index = word_area.byte_index() as usize;
            let length = word_area.length() as usize;
            title_areas.push(byte_index);
            title_areas.push(byte_index + length);
        }
    }
    title_areas.push(text.len());
    title_areas
}

fn main() -> Result<(), Box<Error>> {
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

        let (elapsed, documents) = elapsed::measure_time(|| {
            let builder = view.query_builder().unwrap();
            builder.query(query, 0..opt.number_results)
        });

        let number_of_documents = documents.len();
        for doc in documents {
            match view.retrieve_document::<Document>(doc.id) {
                Ok(document) => {

                    print!("title: ");
                    let title_areas = create_highlight_areas(&document.title, &doc.matches, 1);
                    display_highlights(&document.title, &title_areas)?;
                    println!();

                    print!("description: ");
                    let description_areas = create_highlight_areas(&document.description, &doc.matches, 2);
                    display_highlights(&document.description, &description_areas)?;
                    println!();
                },
                Err(e) => eprintln!("{}", e),
            }
        }

        println!("Found {} results in {}", number_of_documents, elapsed);
        buffer.clear();
    }

    Ok(())
}
