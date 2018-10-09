use fst::{Streamer, Automaton};
use crate::metadata::ops::{self, IndexedDocIndexes};
use crate::metadata::{stream_ops, Metadata};

fn union_with_automatons<'a, A>(metas: &'a [Metadata], autos: Vec<A>) -> ops::Union
where A: 'a + Automaton + Clone,
{
    let mut op = ops::OpBuilder::with_automatons(autos);
    for metadata in metas {
        op.push(metadata);
    }
    op.union()
}

pub struct Difference<'f> {
    inner: stream_ops::Difference<'f>,
}

impl<'f> Difference<'f> {
    pub fn new<A>(positives: &'f [Metadata], negatives: &'f [Metadata], automatons: Vec<A>) -> Self
    where A: 'f + Automaton + Clone
    {
        let positives = union_with_automatons(positives, automatons.clone());
        let negatives = union_with_automatons(negatives, automatons);

        let mut builder = stream_ops::OpBuilder::new();
        builder.push(positives);
        builder.push(negatives);

        Difference { inner: builder.difference() }
    }
}

impl<'a, 'f> Streamer<'a> for Difference<'f> {
    type Item = (&'a [u8], &'a [IndexedDocIndexes]);

    fn next(&'a mut self) -> Option<Self::Item> {
        self.inner.next()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fst::automaton::AlwaysMatch;
    use crate::metadata::{Metadata, MetadataBuilder};
    use crate::vec_read_only::VecReadOnly;
    use crate::DocIndex;

    fn construct_metadata(documents: Vec<(String, DocIndex)>) -> Metadata {
        let mapw = Vec::new();
        let indexesw = Vec::new();

        let mut builder = MetadataBuilder::new(mapw, indexesw);

        for (string, doc_index) in documents {
            builder.insert(string, doc_index);
        }

        let (map, indexes) = builder.into_inner().unwrap();
        Metadata::from_bytes(map, indexes).unwrap()
    }

    #[test]
    fn empty() {
        let positive_metas = construct_metadata(vec![
            ("chameau".into(), DocIndex{ document: 12, attribute: 1, attribute_index: 22 }),
            ("chameau".into(), DocIndex{ document: 31, attribute: 0, attribute_index: 1 }),
        ]);

        let negative_metas = construct_metadata(vec![
            ("chameau".into(), DocIndex{ document: 12, attribute: 1, attribute_index: 22 }),
            ("chameau".into(), DocIndex{ document: 31, attribute: 0, attribute_index: 1 }),
        ]);

        let positives = &[positive_metas];
        let negatives = &[negative_metas];
        let mut diff = Difference::new(positives, negatives, vec![AlwaysMatch]);

        assert_eq!(diff.next(), None);
    }

    #[test]
    fn one_positive() {
        let di1 = DocIndex{ document: 12, attribute: 1, attribute_index: 22 };
        let di2 = DocIndex{ document: 31, attribute: 0, attribute_index: 1 };

        let positive_metas = construct_metadata(vec![
            ("chameau".into(), di1),
            ("chameau".into(), di2),
        ]);

        let negative_metas = construct_metadata(vec![
            ("chameau".into(), di1),
        ]);

        let positives = &[positive_metas];
        let negatives = &[negative_metas];
        let mut diff = Difference::new(positives, negatives, vec![AlwaysMatch]);

        let idi = IndexedDocIndexes{ index: 0, doc_indexes: VecReadOnly::new(vec![di2]) };
        assert_eq!(diff.next(), Some(("chameau".as_bytes(), &[idi][..])));
        assert_eq!(diff.next(), None);
    }

    #[test]
    fn more_negative_than_positive() {
        let di1 = DocIndex{ document: 12, attribute: 1, attribute_index: 22 };
        let di2 = DocIndex{ document: 31, attribute: 0, attribute_index: 1 };

        let positive_metas = construct_metadata(vec![
            ("chameau".into(), di1),
        ]);

        let negative_metas = construct_metadata(vec![
            ("chameau".into(), di1),
            ("chameau".into(), di2),
        ]);

        let positives = &[positive_metas];
        let negatives = &[negative_metas];
        let mut diff = Difference::new(positives, negatives, vec![AlwaysMatch]);

        assert_eq!(diff.next(), None);
    }
}
