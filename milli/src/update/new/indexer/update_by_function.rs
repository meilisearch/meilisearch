use rayon::iter::{IntoParallelIterator, ParallelIterator};

use super::Indexer;
use crate::update::new::DocumentChange;
use crate::Result;

pub struct UpdateByFunctionIndexer;

impl<'p> Indexer<'p> for UpdateByFunctionIndexer {
    type Parameter = ();

    fn document_changes(
        self,
        _param: Self::Parameter,
    ) -> Result<impl ParallelIterator<Item = Result<Option<DocumentChange>>> + 'p> {
        Ok(vec![].into_par_iter())
    }
}
