use std::error::Error;

use fst::{IntoStreamer, Streamer};
use group_by::GroupBy;
use sdset::duo::DifferenceByKey;
use sdset::{Set, SetOperation};

use crate::database::blob::{Blob, Sign, PositiveBlob, PositiveBlobBuilder, NegativeBlob};
use crate::database::blob::{positive, negative};

fn blob_same_sign(a: &Blob, b: &Blob) -> bool {
    a.sign() == b.sign()
}

fn unwrap_positive(blob: &Blob) -> &PositiveBlob {
    match blob {
        Blob::Positive(blob) => blob,
        Blob::Negative(_) => panic!("called `unwrap_positive()` on a `Negative` value"),
    }
}

fn unwrap_negative(blob: &Blob) -> &NegativeBlob {
    match blob {
        Blob::Negative(blob) => blob,
        Blob::Positive(_) => panic!("called `unwrap_negative()` on a `Positive` value"),
    }
}

pub struct OpBuilder {
    blobs: Vec<Blob>,
}

impl OpBuilder {
    pub fn new() -> OpBuilder {
        OpBuilder { blobs: Vec::new() }
    }

    pub fn with_capacity(cap: usize) -> OpBuilder {
        OpBuilder { blobs: Vec::with_capacity(cap) }
    }

    pub fn push(&mut self, blob: Blob) {
        if self.blobs.is_empty() && blob.is_negative() { return }
        self.blobs.push(blob);
    }

    pub fn merge(self) -> Result<PositiveBlob, Box<Error>> {
        let groups = GroupBy::new(&self.blobs, blob_same_sign);
        let mut aggregated = Vec::new();

        for blobs in groups {
            match blobs[0].sign() {
                Sign::Positive => {
                    let mut op_builder = positive::OpBuilder::with_capacity(blobs.len());
                    for blob in blobs {
                        op_builder.push(unwrap_positive(blob));
                    }

                    let mut stream = op_builder.union().into_stream();
                    let mut builder = PositiveBlobBuilder::memory();
                    while let Some((input, doc_indexes)) = stream.next() {
                        // FIXME empty doc_indexes must be handled by OpBuilder
                        if !doc_indexes.is_empty() {
                            builder.insert(input, doc_indexes).unwrap();
                        }
                    }
                    let (map, doc_indexes) = builder.into_inner().unwrap();
                    let blob = PositiveBlob::from_bytes(map, doc_indexes).unwrap();
                    aggregated.push(Blob::Positive(blob));
                },
                Sign::Negative => {
                    let mut op_builder = negative::OpBuilder::with_capacity(blobs.len());
                    for blob in blobs {
                        op_builder.push(unwrap_negative(blob));
                    }
                    let blob = op_builder.union().into_negative_blob();
                    aggregated.push(Blob::Negative(blob));
                },
            }
        }

        let mut buffer = Vec::new();
        aggregated.chunks(2).try_fold(PositiveBlob::default(), |base, slice| {
            let negative = NegativeBlob::default();
            let (positive, negative) = match slice {
                [a, b] => (unwrap_positive(a), unwrap_negative(b)),
                [a] => (unwrap_positive(a), &negative),
                _ => unreachable!(),
            };

            let mut builder = PositiveBlobBuilder::memory();
            let doc_ids = Set::new_unchecked(negative.as_ref());

            let op_builder = positive::OpBuilder::new().add(&base).add(&positive);
            let mut stream = op_builder.union().into_stream();
            while let Some((input, doc_indexes)) = stream.next() {
                let doc_indexes = Set::new_unchecked(doc_indexes);
                let op = DifferenceByKey::new(doc_indexes, doc_ids, |x| x.document_id, |x| *x);

                buffer.clear();
                op.extend_vec(&mut buffer);
                if !buffer.is_empty() {
                    builder.insert(input, &buffer)?;
                }
            }

            let (map, doc_indexes) = builder.into_inner()?;
            PositiveBlob::from_bytes(map, doc_indexes)
        })
    }
}
