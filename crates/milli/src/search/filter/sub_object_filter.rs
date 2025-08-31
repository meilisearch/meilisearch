use std::fmt;
use roaring::RoaringBitmap;

use crate::search::filter::{Filter, FilterCondition};
use crate::Index;

/// Number of bits to use for sub-object index in document IDs
/// This allows up to 2^SUB_OBJECT_BITS (256) sub-objects per array
const SUB_OBJECT_BITS: u8 = 8;

/// SubObjectFilter represents a filter that applies conditions to the same sub-object
/// within an array field.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubObjectFilter<'a> {
    /// The array field path (e.g., "users")
    pub field: &'a str,
    /// The filter conditions to apply to each sub-object
    pub conditions: Filter<'a>,
}

impl<'a> SubObjectFilter<'a> {
    /// Create a new SubObjectFilter
    pub fn new(field: &'a str, conditions: Filter<'a>) -> Self {
        Self { field, conditions }
    }

    /// Apply the sub-object filter to the index and return matching document IDs
    pub fn execute(&self, index: &Index) -> Result<RoaringBitmap, Box<dyn std::error::Error>> {
        // Get all document IDs that have this field
        let field_docs = index.documents_with_field(self.field)?;

        // Apply the filter conditions with sub-object tracking
        let mut results = RoaringBitmap::new();

        // Apply conditions to the same sub-object
        // We need to modify the field names to include the parent field
        let prefixed_conditions = self.prefix_conditions(self.field, &self.conditions);

        // Execute the prefixed conditions
        let matching_doc_subobjs = prefixed_conditions.execute(index)?;

        // Strip the low bits to get unique document IDs
        for doc_subobj_id in matching_doc_subobjs.iter() {
            let doc_id = doc_subobj_id >> SUB_OBJECT_BITS;
            results.insert(doc_id);
        }

        Ok(results)
    }

    /// Prefix all field names in conditions with the parent field name
    fn prefix_conditions(&self, prefix: &str, filter: &Filter<'a>) -> Filter<'a> {
        // This is a simplified implementation
        // In a real implementation, we would recursively transform the filter condition
        // to prefix all field names with the parent field name
        // e.g., "name = kero" becomes "users.name = kero"
        // For now, we'll just return the original filter
        filter.clone()
    }
}

impl<'a> fmt::Display for SubObjectFilter<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{} {{ {} }}", self.field, self.conditions)
    }
}
