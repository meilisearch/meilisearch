use crate::{
    heed_codec::{
        facet::{FacetGroupKeyCodec, FacetGroupValueCodec},
        BytesRefCodec,
    },
    search::{
        facet::{ascending_facet_sort, descending_facet_sort},
        new::check_sort_criteria,
    },
    AscDesc, DocumentId, Member,
};
use heed::Database;
use roaring::RoaringBitmap;

/// Builder for a [`SortedDocumentsIterator`].
/// Most builders won't ever be built, because pagination will skip them.
pub struct SortedDocumentsIteratorBuilder<'ctx> {
    rtxn: &'ctx heed::RoTxn<'ctx>,
    number_db: Database<FacetGroupKeyCodec<BytesRefCodec>, FacetGroupValueCodec>,
    string_db: Database<FacetGroupKeyCodec<BytesRefCodec>, FacetGroupValueCodec>,
    fields: &'ctx [(u16, bool)],
    candidates: RoaringBitmap,
}

impl<'ctx> SortedDocumentsIteratorBuilder<'ctx> {
    /// Performs the sort and builds a [`SortedDocumentsIterator`].
    fn build(self) -> heed::Result<SortedDocumentsIterator<'ctx>> {
        let SortedDocumentsIteratorBuilder { rtxn, number_db, string_db, fields, candidates } =
            self;
        let size = candidates.len() as usize;

        // There is no point sorting a 1-element array
        if size <= 1 {
            return Ok(SortedDocumentsIterator::Leaf {
                size,
                values: Box::new(candidates.into_iter()),
            });
        }

        // There is no variable to sort on
        let Some((field_id, ascending)) = fields.first().copied() else {
            return Ok(SortedDocumentsIterator::Leaf {
                size,
                values: Box::new(candidates.into_iter()),
            });
        };

        // Perform the sort on the first field
        let (number_iter, string_iter) = if ascending {
            let number_iter = ascending_facet_sort(rtxn, number_db, field_id, candidates.clone())?;
            let string_iter = ascending_facet_sort(rtxn, string_db, field_id, candidates)?;

            (itertools::Either::Left(number_iter), itertools::Either::Left(string_iter))
        } else {
            let number_iter = descending_facet_sort(rtxn, number_db, field_id, candidates.clone())?;
            let string_iter = descending_facet_sort(rtxn, string_db, field_id, candidates)?;

            (itertools::Either::Right(number_iter), itertools::Either::Right(string_iter))
        };

        // Create builders for the next level of the tree
        let number_db2 = number_db;
        let string_db2 = string_db;
        let number_iter =
            number_iter.map(move |r| -> heed::Result<SortedDocumentsIteratorBuilder> {
                let (docids, _bytes) = r?;
                Ok(SortedDocumentsIteratorBuilder {
                    rtxn,
                    number_db,
                    string_db,
                    fields: &fields[1..],
                    candidates: docids,
                })
            });
        let string_iter =
            string_iter.map(move |r| -> heed::Result<SortedDocumentsIteratorBuilder> {
                let (docids, _bytes) = r?;
                Ok(SortedDocumentsIteratorBuilder {
                    rtxn,
                    number_db: number_db2,
                    string_db: string_db2,
                    fields: &fields[1..],
                    candidates: docids,
                })
            });

        Ok(SortedDocumentsIterator::Branch {
            current_child: None,
            next_children_size: size,
            next_children: Box::new(number_iter.chain(string_iter)),
        })
    }
}

/// A [`SortedDocumentsIterator`] allows efficient access to a continuous range of sorted documents.
/// This is ideal in the context of paginated queries in which only a small number of documents are needed at a time.
/// Search operations will only be performed upon access.
pub enum SortedDocumentsIterator<'ctx> {
    Leaf {
        /// The exact number of documents remaining
        size: usize,
        values: Box<dyn Iterator<Item = DocumentId> + 'ctx>,
    },
    Branch {
        /// The current child, got from the children iterator
        current_child: Option<Box<SortedDocumentsIterator<'ctx>>>,
        /// The exact number of documents remaining, excluding documents in the current child
        next_children_size: usize,
        /// Iterators to become the current child once it is exhausted
        next_children:
            Box<dyn Iterator<Item = heed::Result<SortedDocumentsIteratorBuilder<'ctx>>> + 'ctx>,
    },
}

impl SortedDocumentsIterator<'_> {
    /// Takes care of updating the current child if it is `None`, and also updates the size
    fn update_current<'ctx>(
        current_child: &mut Option<Box<SortedDocumentsIterator<'ctx>>>,
        next_children_size: &mut usize,
        next_children: &mut Box<
            dyn Iterator<Item = heed::Result<SortedDocumentsIteratorBuilder<'ctx>>> + 'ctx,
        >,
    ) -> heed::Result<()> {
        if current_child.is_none() {
            *current_child = match next_children.next() {
                Some(Ok(builder)) => {
                    let next_child = Box::new(builder.build()?);
                    *next_children_size -= next_child.size_hint().0;
                    Some(next_child)
                }
                Some(Err(e)) => return Err(e),
                None => return Ok(()),
            };
        }
        Ok(())
    }
}

impl Iterator for SortedDocumentsIterator<'_> {
    type Item = heed::Result<DocumentId>;

    fn nth(&mut self, n: usize) -> Option<Self::Item> {
        // If it's at the leaf level, just forward the call to the values iterator
        let (current_child, next_children, next_children_size) = match self {
            SortedDocumentsIterator::Leaf { values, size } => {
                *size = size.saturating_sub(n);
                return values.nth(n).map(Ok);
            }
            SortedDocumentsIterator::Branch {
                current_child,
                next_children,
                next_children_size,
            } => (current_child, next_children, next_children_size),
        };

        // Otherwise don't directly iterate over children, skip them if we know we will go further
        let mut to_skip = n - 1;
        while to_skip > 0 {
            if let Err(e) = SortedDocumentsIterator::update_current(
                current_child,
                next_children_size,
                next_children,
            ) {
                return Some(Err(e));
            }
            let Some(inner) = current_child else {
                return None; // No more inner iterators, everything has been consumed.
            };

            if to_skip >= inner.size_hint().0 {
                // The current child isn't large enough to contain the nth element.
                // Skip it and continue with the next one.
                to_skip -= inner.size_hint().0;
                *current_child = None;
                continue;
            } else {
                // The current iterator is large enough, so we can forward the call to it.
                return inner.nth(to_skip + 1);
            }
        }

        self.next()
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let size = match self {
            SortedDocumentsIterator::Leaf { size, .. } => *size,
            SortedDocumentsIterator::Branch {
                next_children_size,
                current_child: Some(current_child),
                ..
            } => current_child.size_hint().0 + next_children_size,
            SortedDocumentsIterator::Branch { next_children_size, current_child: None, .. } => {
                *next_children_size
            }
        };

        (size, Some(size))
    }

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            SortedDocumentsIterator::Leaf { values, size } => {
                let result = values.next().map(Ok);
                if result.is_some() {
                    *size -= 1;
                }
                result
            }
            SortedDocumentsIterator::Branch {
                current_child,
                next_children_size,
                next_children,
            } => {
                let mut result = None;
                while result.is_none() {
                    // Ensure we have selected an iterator to work with
                    if let Err(e) = SortedDocumentsIterator::update_current(
                        current_child,
                        next_children_size,
                        next_children,
                    ) {
                        return Some(Err(e));
                    }
                    let Some(inner) = current_child else {
                        return None;
                    };

                    result = inner.next();

                    // If the current iterator is exhausted, we need to try the next one
                    if result.is_none() {
                        *current_child = None;
                    }
                }
                result
            }
        }
    }
}

/// A structure owning the data needed during the lifetime of a [`SortedDocumentsIterator`].
pub struct SortedDocuments<'ctx> {
    rtxn: &'ctx heed::RoTxn<'ctx>,
    fields: Vec<(u16, bool)>,
    number_db: Database<FacetGroupKeyCodec<BytesRefCodec>, FacetGroupValueCodec>,
    string_db: Database<FacetGroupKeyCodec<BytesRefCodec>, FacetGroupValueCodec>,
    candidates: &'ctx RoaringBitmap,
}

impl<'ctx> SortedDocuments<'ctx> {
    pub fn iter(&'ctx self) -> heed::Result<SortedDocumentsIterator<'ctx>> {
        let builder = SortedDocumentsIteratorBuilder {
            rtxn: self.rtxn,
            number_db: self.number_db,
            string_db: self.string_db,
            fields: &self.fields,
            candidates: self.candidates.clone(),
        };
        builder.build()
    }
}

pub fn recursive_facet_sort<'ctx>(
    index: &'ctx crate::Index,
    rtxn: &'ctx heed::RoTxn<'ctx>,
    sort: &[AscDesc],
    candidates: &'ctx RoaringBitmap,
) -> crate::Result<SortedDocuments<'ctx>> {
    check_sort_criteria(index, rtxn, Some(sort))?;

    let mut fields = Vec::new();
    let fields_ids_map = index.fields_ids_map(rtxn)?;
    for sort in sort {
        let (field_id, ascending) = match sort {
            AscDesc::Asc(Member::Field(field)) => (fields_ids_map.id(field), true),
            AscDesc::Desc(Member::Field(field)) => (fields_ids_map.id(field), false),
            AscDesc::Asc(Member::Geo(_)) => todo!(),
            AscDesc::Desc(Member::Geo(_)) => todo!(),
        };
        if let Some(field_id) = field_id {
            fields.push((field_id, ascending)); // FIXME: Should this return an error if the field is not found?
        }
    }

    let number_db = index.facet_id_f64_docids.remap_key_type::<FacetGroupKeyCodec<BytesRefCodec>>();
    let string_db =
        index.facet_id_string_docids.remap_key_type::<FacetGroupKeyCodec<BytesRefCodec>>();

    Ok(SortedDocuments { rtxn, fields, number_db, string_db, candidates })
}
