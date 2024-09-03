use rayon::iter::{IntoParallelIterator, ParallelIterator};

use super::DocumentChanges;
use crate::update::new::DocumentChange;
use crate::{FieldsIdsMap, Result};

pub struct UpdateByFunction;

impl<'p> DocumentChanges<'p> for UpdateByFunction {
    type Parameter = ();

    fn document_changes(
        self,
        _fields_ids_map: &mut FieldsIdsMap,
        _param: Self::Parameter,
    ) -> Result<impl ParallelIterator<Item = Result<DocumentChange>> + Clone + 'p> {
        Ok((0..100).into_par_iter().map(|_| todo!()))
    }
}
