use rayon::iter::IntoParallelIterator;

use super::document_changes::{DocumentChangeContext, DocumentChanges};
use crate::Result;

pub struct UpdateByFunction;

impl UpdateByFunction {
    pub fn into_changes(self) -> UpdateByFunctionChanges {
        UpdateByFunctionChanges
    }
}

pub struct UpdateByFunctionChanges;

impl<'index> DocumentChanges<'index> for UpdateByFunctionChanges {
    type Item = u32;

    fn iter(&self) -> impl rayon::prelude::IndexedParallelIterator<Item = Self::Item> {
        (0..100).into_par_iter()
    }

    fn item_to_document_change<'doc, T: super::document_changes::MostlySend + 'doc>(
        &self,
        _context: &'doc DocumentChangeContext<T>,
        _item: Self::Item,
    ) -> Result<crate::update::new::DocumentChange<'doc>>
    where
        'index: 'doc,
    {
        todo!()
    }
}
