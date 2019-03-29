use hashbrown::HashMap;
use meilidb_core::DocumentId;
use crate::{SchemaAttr, Number};

pub type RankedMap = HashMap<(DocumentId, SchemaAttr), Number>;
