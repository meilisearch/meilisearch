use sdset::multi::OpBuilder as SdOpBuilder;
use sdset::{SetOperation, Set};

use crate::database::blob::PositiveBlob;
use crate::data::DocIndexes;
use crate::DocIndex;

pub struct OpBuilder<'m> {
    // the operation on the maps is always an union.
    map_op: fst::map::OpBuilder<'m>,
    indexes: Vec<&'m DocIndexes>,
}

/// Do a set operation on multiple positive blobs.
impl<'m> OpBuilder<'m> {
    pub fn new() -> Self {
        Self {
            map_op: fst::map::OpBuilder::new(),
            indexes: Vec::new(),
        }
    }

    pub fn with_capacity(cap: usize) -> Self {
        Self {
            map_op: fst::map::OpBuilder::new(), // TODO patch fst to add with_capacity
            indexes: Vec::with_capacity(cap),
        }
    }

    pub fn add(mut self, blob: &'m PositiveBlob) -> Self {
        self.push(blob);
        self
    }

    pub fn push(&mut self, blob: &'m PositiveBlob) {
        self.map_op.push(blob.as_map());
        self.indexes.push(blob.as_indexes());
    }

    pub fn union(self) -> Union<'m> {
        Union::new(self.map_op.union(), self.indexes)
    }

    pub fn intersection(self) -> Intersection<'m> {
        Intersection::new(self.map_op.union(), self.indexes)
    }

    pub fn difference(self) -> Difference<'m> {
        Difference::new(self.map_op.union(), self.indexes)
    }

    pub fn symmetric_difference(self) -> SymmetricDifference<'m> {
        SymmetricDifference::new(self.map_op.union(), self.indexes)
    }
}

macro_rules! logical_operation {
    (struct $name:ident, $operation:ident) => {

pub struct $name<'m> {
    stream: fst::map::Union<'m>,
    indexes: Vec<&'m DocIndexes>,
    outs: Vec<DocIndex>,
}

impl<'m> $name<'m> {
    fn new(stream: fst::map::Union<'m>, indexes: Vec<&'m DocIndexes>) -> Self {
        $name {
            stream: stream,
            indexes: indexes,
            outs: Vec::new(),
        }
    }
}

impl<'m, 'a> fst::Streamer<'a> for $name<'m> {
    type Item = (&'a [u8], &'a [DocIndex]);

    fn next(&'a mut self) -> Option<Self::Item> {
        // loop {
        //     let (input, ivalues) = match self.stream.next() {
        //         Some(value) => value,
        //         None => return None,
        //     };

        //     self.outs.clear();

        //     let mut builder = SdOpBuilder::with_capacity(ivalues.len());
        //     for ivalue in ivalues {
        //         let indexes = self.indexes[ivalue.index];
        //         let indexes = indexes.get(ivalue.value).expect("BUG: could not find document indexes");
        //         let set = Set::new_unchecked(indexes);
        //         builder.push(set);
        //     }

        //     builder.$operation().extend_vec(&mut self.outs);

        //     if self.outs.is_empty() { continue }
        //     return Some((input, &self.outs))
        // }

        // FIXME make the above code compile
        match self.stream.next() {
            Some((input, ivalues)) => {
                self.outs.clear();

                let mut builder = SdOpBuilder::with_capacity(ivalues.len());
                for ivalue in ivalues {
                    let doc_indexes = &self.indexes[ivalue.index][ivalue.value as usize];
                    let set = Set::new_unchecked(doc_indexes);
                    builder.push(set);
                }

                builder.$operation().extend_vec(&mut self.outs);

                if self.outs.is_empty() { return None }
                return Some((input, &self.outs))
            },
            None => None
        }
    }
}
}}

logical_operation!(struct Union, union);
logical_operation!(struct Intersection, intersection);
logical_operation!(struct Difference, difference);
logical_operation!(struct SymmetricDifference, symmetric_difference);
