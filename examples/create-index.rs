use std::fs;
use std::path::Path;
use std::error::Error;
use std::path::PathBuf;

use elapsed::measure_time;
use moby_name_gen::random_name;
use structopt::StructOpt;

use pentium::index::schema::{Schema, SchemaBuilder, STORED, INDEXED};
use pentium::index::update::{Update, PositiveUpdateBuilder};
use pentium::tokenizer::DefaultBuilder;
use pentium::index::Index;

#[derive(Debug, StructOpt)]
pub struct Cmd {
    /// csv file to index
    #[structopt(parse(from_os_str))]
    pub csv_file: PathBuf,
}

fn generate_update_from_csv(path: &Path) -> Result<(Schema, Update), Box<Error>> {
    let mut csv = csv::Reader::from_path(path)?;

    let mut attributes = Vec::new();
    let (schema, id_attr_index) = {
        let mut id_attr_index = None;
        let mut builder = SchemaBuilder::new();

        for (i, header_name) in csv.headers()?.iter().enumerate() {
            // FIXME this does not disallow multiple "id" fields
            if header_name == "id" { id_attr_index = Some(i) };

            let field = builder.new_attribute(header_name, STORED | INDEXED);
            attributes.push(field);
        }

        let id = match id_attr_index {
            Some(index) => index,
            None => return Err(String::from("No \"id\" field found which is mandatory").into()),
        };

        (builder.build(), id)
    };

    let update_path = PathBuf::from("./positive-update-xxx.sst");
    let tokenizer_builder = DefaultBuilder::new();
    let mut builder = PositiveUpdateBuilder::new(&update_path, schema.clone(), tokenizer_builder);

    for record in csv.records() {
        let record = match record {
            Ok(x) => x,
            Err(e) => { eprintln!("{:?}", e); continue }
        };

        let id = record.into_iter().nth(id_attr_index).unwrap().parse()?;
        for (value, attr) in record.into_iter().zip(&attributes) {
            builder.update_field(id, *attr, value.to_string());
        }
    }

    builder.build().map(|update| (schema, update))
}

fn main() -> Result<(), Box<Error>> {
    let command = Cmd::from_args();

    let path = random_name();

    println!("generating the update...");
    let (schema, update) = generate_update_from_csv(&command.csv_file)?;

    println!("creating the index");
    let index = Index::create(&path, schema)?;

    println!("ingesting the changes in the index");
    index.ingest_update(update)?;

    // FIXME this is really ugly !!!!
    // the index does not support moving update files
    // so we must remove it by hand
    fs::remove_file("./positive-update-xxx.sst")?;

    println!("the index {:?} has been created!", path);

    Ok(())
}
