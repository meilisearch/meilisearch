use meilidb_schema::Schema;
use crate::{store, error::UnsupportedOperation, MResult};

pub fn apply_schema_update(
    writer: &mut rkv::Writer,
    main_store: store::Main,
    new_schema: &Schema,
) -> MResult<()>
{
    if let Some(_) = main_store.schema(writer)? {
        return Err(UnsupportedOperation::SchemaAlreadyExists.into())
    }

    main_store.put_schema(writer, new_schema)
}
