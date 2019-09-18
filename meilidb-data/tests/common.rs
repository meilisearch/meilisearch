
use meilidb_data::{Database};
use meilidb_data::Index;
use meilidb_schema::{SchemaBuilder, DISPLAYED, INDEXED};

pub fn simple_index() -> Index {
    let tmp_dir = tempfile::tempdir().unwrap();
    let database = Database::open(&tmp_dir).unwrap();

    let mut builder = SchemaBuilder::with_identifier("objectId");
    builder.new_attribute("objectId", DISPLAYED | INDEXED);
    builder.new_attribute("title", DISPLAYED | INDEXED);
    let schema = builder.build();

    database.create_index("hello", schema).unwrap()
}
