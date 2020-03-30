use crate::parser::Operation;
use meilisearch_core::{DocumentId, Schema, MainT };
use heed::RoTxn;


pub struct Filter<'r> {
    reader: &'r RoTxn<MainT>,
    operation: Box<Operation>,
}

impl<'r> Filter<'r> {
    pub fn new<T: AsRef<str>>(expr: T, schema: &Schema, reader: &'r RoTxn<MainT>) -> Result<Self, Box<dyn std::error::Error>> {
        let operation = Box::new(Operation::parse_with_schema(expr, schema)?);
        Ok( Self {
                reader,
                operation,
            })
    }

    pub fn test(&self, _document_id: &DocumentId) -> Result<bool, Box<dyn std::error::Error>> {
        unimplemented!()
    }
}
