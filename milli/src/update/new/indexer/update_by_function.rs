use std::sync::Arc;

use rayon::iter::{IndexedParallelIterator, IntoParallelIterator, ParallelIterator};

use super::DocumentChanges;
use crate::update::new::DocumentChange;
use crate::{Error, FieldsIdsMap, Result};

pub struct UpdateByFunction;

impl<'p> DocumentChanges<'p> for UpdateByFunction {
    type Parameter = ();

    fn document_changes(
        self,
        _fields_ids_map: &mut FieldsIdsMap,
        _param: Self::Parameter,
    ) -> Result<
        impl IndexedParallelIterator<Item = std::result::Result<DocumentChange, Arc<Error>>>
            + Clone
            + 'p,
    > {
        Ok((0..100).into_par_iter().map(|_| todo!()))
    }
}
