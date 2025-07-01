use std::collections::VecDeque;

use crate::{
    documents::{geo_sort::next_bucket, GeoSortParameter},
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

#[derive(Debug, Clone, Copy)]
enum AscDescId {
    Facet { field_id: u16, ascending: bool },
    Geo { field_ids: [u16; 2], target_point: [f64; 2], ascending: bool },
}

/// Builder for a [`SortedDocumentsIterator`].
/// Most builders won't ever be built, because pagination will skip them.
pub struct SortedDocumentsIteratorBuilder<'ctx> {
    index: &'ctx crate::Index,
    rtxn: &'ctx heed::RoTxn<'ctx>,
    number_db: Database<FacetGroupKeyCodec<BytesRefCodec>, FacetGroupValueCodec>,
    string_db: Database<FacetGroupKeyCodec<BytesRefCodec>, FacetGroupValueCodec>,
    fields: &'ctx [AscDescId],
    candidates: RoaringBitmap,
    geo_candidates: &'ctx RoaringBitmap,
}

impl<'ctx> SortedDocumentsIteratorBuilder<'ctx> {
    /// Performs the sort and builds a [`SortedDocumentsIterator`].
    fn build(self) -> crate::Result<SortedDocumentsIterator<'ctx>> {
        let size = self.candidates.len() as usize;

        // There is no point sorting a 1-element array
        if size <= 1 {
            return Ok(SortedDocumentsIterator::Leaf {
                size,
                values: Box::new(self.candidates.into_iter()),
            });
        }

        match self.fields.first().copied() {
            Some(AscDescId::Facet { field_id, ascending }) => self.build_facet(field_id, ascending),
            Some(AscDescId::Geo { field_ids, target_point, ascending }) => {
                self.build_geo(field_ids, target_point, ascending)
            }
            None => Ok(SortedDocumentsIterator::Leaf {
                size,
                values: Box::new(self.candidates.into_iter()),
            }),
        }
    }

    fn build_facet(
        self,
        field_id: u16,
        ascending: bool,
    ) -> crate::Result<SortedDocumentsIterator<'ctx>> {
        let SortedDocumentsIteratorBuilder {
            index,
            rtxn,
            number_db,
            string_db,
            fields,
            candidates,
            geo_candidates,
        } = self;
        let size = candidates.len() as usize;

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
        let number_iter = number_iter.map(|r| r.map(|(d, _)| d));
        let string_iter = string_iter.map(|r| r.map(|(d, _)| d));
        let next_children = number_iter.chain(string_iter).map(move |r| {
            Ok(SortedDocumentsIteratorBuilder {
                index,
                rtxn,
                number_db,
                string_db,
                fields: &fields[1..],
                candidates: r?,
                geo_candidates,
            })
        });

        Ok(SortedDocumentsIterator::Branch {
            current_child: None,
            next_children_size: size,
            next_children: Box::new(next_children),
        })
    }

    fn build_geo(
        self,
        field_ids: [u16; 2],
        target_point: [f64; 2],
        ascending: bool,
    ) -> crate::Result<SortedDocumentsIterator<'ctx>> {
        let SortedDocumentsIteratorBuilder {
            index,
            rtxn,
            number_db,
            string_db,
            fields,
            candidates,
            geo_candidates,
        } = self;

        let mut cache = VecDeque::new();
        let mut rtree = None;
        let size = candidates.len() as usize;
        let not_geo_candidates = candidates.clone() - geo_candidates;
        let mut geo_remaining = size - not_geo_candidates.len() as usize;
        let mut not_geo_candidates = Some(not_geo_candidates);

        let next_children = std::iter::from_fn(move || {
            // Find the next bucket of geo-sorted documents.
            // next_bucket loops and will go back to the beginning so we use a variable to track how many are left.
            if geo_remaining > 0 {
                if let Ok(Some((docids, _point))) = next_bucket(
                    index,
                    rtxn,
                    &candidates,
                    ascending,
                    target_point,
                    &Some(field_ids),
                    &mut rtree,
                    &mut cache,
                    geo_candidates,
                    GeoSortParameter::default(),
                ) {
                    geo_remaining -= docids.len() as usize;
                    return Some(Ok(SortedDocumentsIteratorBuilder {
                        index,
                        rtxn,
                        number_db,
                        string_db,
                        fields: &fields[1..],
                        candidates: docids,
                        geo_candidates,
                    }));
                }
            }
            
            // Once all geo candidates have been processed, we can return the others
            if let Some(not_geo_candidates) = not_geo_candidates.take() {
                if !not_geo_candidates.is_empty() {
                    return Some(Ok(SortedDocumentsIteratorBuilder {
                        index,
                        rtxn,
                        number_db,
                        string_db,
                        fields: &fields[1..],
                        candidates: not_geo_candidates,
                        geo_candidates,
                    }));
                }
            }

            None
        });

        Ok(SortedDocumentsIterator::Branch {
            current_child: None,
            next_children_size: size,
            next_children: Box::new(next_children),
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
            Box<dyn Iterator<Item = crate::Result<SortedDocumentsIteratorBuilder<'ctx>>> + 'ctx>,
    },
}

impl SortedDocumentsIterator<'_> {
    /// Takes care of updating the current child if it is `None`, and also updates the size
    fn update_current<'ctx>(
        current_child: &mut Option<Box<SortedDocumentsIterator<'ctx>>>,
        next_children_size: &mut usize,
        next_children: &mut Box<
            dyn Iterator<Item = crate::Result<SortedDocumentsIteratorBuilder<'ctx>>> + 'ctx,
        >,
    ) -> crate::Result<()> {
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
    type Item = crate::Result<DocumentId>;

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
    index: &'ctx crate::Index,
    rtxn: &'ctx heed::RoTxn<'ctx>,
    fields: Vec<AscDescId>,
    number_db: Database<FacetGroupKeyCodec<BytesRefCodec>, FacetGroupValueCodec>,
    string_db: Database<FacetGroupKeyCodec<BytesRefCodec>, FacetGroupValueCodec>,
    candidates: &'ctx RoaringBitmap,
    geo_candidates: RoaringBitmap,
}

impl<'ctx> SortedDocuments<'ctx> {
    pub fn iter(&'ctx self) -> crate::Result<SortedDocumentsIterator<'ctx>> {
        let builder = SortedDocumentsIteratorBuilder {
            index: self.index,
            rtxn: self.rtxn,
            number_db: self.number_db,
            string_db: self.string_db,
            fields: &self.fields,
            candidates: self.candidates.clone(),
            geo_candidates: &self.geo_candidates,
        };
        builder.build()
    }
}

pub fn recursive_facet_sort<'ctx>(
    index: &'ctx crate::Index,
    rtxn: &'ctx heed::RoTxn<'ctx>,
    sort: Vec<AscDesc>,
    candidates: &'ctx RoaringBitmap,
) -> crate::Result<SortedDocuments<'ctx>> {
    check_sort_criteria(index, rtxn, Some(&sort))?;

    let mut fields = Vec::new();
    let fields_ids_map = index.fields_ids_map(rtxn)?;
    let geo_candidates = index.geo_faceted_documents_ids(rtxn)?; // TODO: skip when no geo sort
    for sort in sort {
        match sort {
            AscDesc::Asc(Member::Field(field)) => {
                if let Some(field_id) = fields_ids_map.id(&field) {
                    fields.push(AscDescId::Facet { field_id, ascending: true });
                }
            }
            AscDesc::Desc(Member::Field(field)) => {
                if let Some(field_id) = fields_ids_map.id(&field) {
                    fields.push(AscDescId::Facet { field_id, ascending: false });
                }
            }
            AscDesc::Asc(Member::Geo(target_point)) => {
                if let (Some(lat), Some(lng)) =
                    (fields_ids_map.id("_geo.lat"), fields_ids_map.id("_geo.lng"))
                {
                    fields.push(AscDescId::Geo {
                        field_ids: [lat, lng],
                        target_point,
                        ascending: true,
                    });
                }
            }
            AscDesc::Desc(Member::Geo(target_point)) => {
                if let (Some(lat), Some(lng)) =
                    (fields_ids_map.id("_geo.lat"), fields_ids_map.id("_geo.lng"))
                {
                    fields.push(AscDescId::Geo {
                        field_ids: [lat, lng],
                        target_point,
                        ascending: false,
                    });
                }
            }
        };
        // FIXME: Should this return an error if the field is not found?
    }

    let number_db = index.facet_id_f64_docids.remap_key_type::<FacetGroupKeyCodec<BytesRefCodec>>();
    let string_db =
        index.facet_id_string_docids.remap_key_type::<FacetGroupKeyCodec<BytesRefCodec>>();

    Ok(SortedDocuments { index, rtxn, fields, number_db, string_db, candidates, geo_candidates })
}
