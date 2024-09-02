use rayon::iter::{IntoParallelIterator, ParallelIterator};

use super::DocumentChanges;
use crate::update::new::DocumentChange;
use crate::Result;

pub struct UpdateByFunction;

impl<'p> DocumentChanges<'p> for UpdateByFunction {
    type Parameter = ();

    fn document_changes(
        self,
        _param: Self::Parameter,
    ) -> Result<impl ParallelIterator<Item = Result<DocumentChange>> + Clone + 'p> {
        Ok((0..100).into_par_iter().map(|_| todo!()))
    }
}
