use std::collections::{BTreeSet, VecDeque};

use heed::Database;
use roaring::RoaringBitmap;

use crate::constants::RESERVED_GEO_FIELD_NAME;
use crate::documents::geo_sort::next_bucket;
use crate::documents::GeoSortParameter;
use crate::heed_codec::facet::{FacetGroupKeyCodec, FacetGroupValueCodec};
use crate::heed_codec::BytesRefCodec;
use crate::search::facet::{ascending_facet_sort, descending_facet_sort};
use crate::{is_faceted, AscDesc, DocumentId, Member, UserError};

#[derive(Debug, Clone, Copy)]
enum AscDescId {
    Facet { field_id: u16, ascending: bool },
    Geo { field_ids: [u16; 2], target_point: [f64; 2], ascending: bool },
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

    /// Implementing the `nth` method allows for efficient access to the nth document in the sorted order.
    /// It's used by `skip` internally.
    /// The default implementation of `nth` would iterate over all children, which is inefficient for large datasets.
    /// This implementation will jump over whole chunks of children until it gets close.
    fn nth(&mut self, n: usize) -> Option<Self::Item> {
        if n == 0 {
            return self.next();
        }

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
        let mut to_skip = n;
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
                return inner.nth(to_skip);
            }
        }

        self.next()
    }

    /// Iterators need to keep track of their size so that they can be skipped efficiently by the `nth` method.
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

        match self.fields {
            [] => Ok(SortedDocumentsIterator::Leaf {
                size,
                values: Box::new(self.candidates.into_iter()),
            }),
            [AscDescId::Facet { field_id, ascending }, next_fields @ ..] => {
                SortedDocumentsIteratorBuilder::build_facet(
                    self.index,
                    self.rtxn,
                    self.number_db,
                    self.string_db,
                    next_fields,
                    self.candidates,
                    self.geo_candidates,
                    *field_id,
                    *ascending,
                )
            }
            [AscDescId::Geo { field_ids, target_point, ascending }, next_fields @ ..] => {
                SortedDocumentsIteratorBuilder::build_geo(
                    self.index,
                    self.rtxn,
                    self.number_db,
                    self.string_db,
                    next_fields,
                    self.candidates,
                    self.geo_candidates,
                    *field_ids,
                    *target_point,
                    *ascending,
                )
            }
        }
    }

    /// Builds a [`SortedDocumentsIterator`] based on the results of a facet sort.
    #[allow(clippy::too_many_arguments)]
    fn build_facet(
        index: &'ctx crate::Index,
        rtxn: &'ctx heed::RoTxn<'ctx>,
        number_db: Database<FacetGroupKeyCodec<BytesRefCodec>, FacetGroupValueCodec>,
        string_db: Database<FacetGroupKeyCodec<BytesRefCodec>, FacetGroupValueCodec>,
        next_fields: &'ctx [AscDescId],
        candidates: RoaringBitmap,
        geo_candidates: &'ctx RoaringBitmap,
        field_id: u16,
        ascending: bool,
    ) -> crate::Result<SortedDocumentsIterator<'ctx>> {
        let size = candidates.len() as usize;

        // Get documents that have this facet field
        let faceted_candidates = index.exists_faceted_documents_ids(rtxn, field_id)?;
        // Documents that don't have this facet field should be returned at the end
        let not_faceted_candidates = &candidates - &faceted_candidates;
        // Only sort candidates that have the facet field
        let faceted_candidates = candidates & faceted_candidates;
        let mut not_faceted_candidates = Some(not_faceted_candidates);

        // Perform the sort on the first field
        let (number_iter, string_iter) = if ascending {
            let number_iter =
                ascending_facet_sort(rtxn, number_db, field_id, faceted_candidates.clone())?;
            let string_iter = ascending_facet_sort(rtxn, string_db, field_id, faceted_candidates)?;

            (itertools::Either::Left(number_iter), itertools::Either::Left(string_iter))
        } else {
            let number_iter =
                descending_facet_sort(rtxn, number_db, field_id, faceted_candidates.clone())?;
            let string_iter = descending_facet_sort(rtxn, string_db, field_id, faceted_candidates)?;

            (itertools::Either::Right(number_iter), itertools::Either::Right(string_iter))
        };

        // Create builders for the next level of the tree
        let number_iter = number_iter.map(|r| r.map(|(d, _)| d));
        let string_iter = string_iter.map(|r| r.map(|(d, _)| d));
        // Chain faceted documents with non-faceted documents at the end
        let next_children = number_iter
            .chain(string_iter)
            .map(move |r| {
                Ok(SortedDocumentsIteratorBuilder {
                    index,
                    rtxn,
                    number_db,
                    string_db,
                    fields: next_fields,
                    candidates: r?,
                    geo_candidates,
                })
            })
            .chain(std::iter::from_fn(move || {
                // Once all faceted candidates have been processed, return the non-faceted ones
                if let Some(not_faceted) = not_faceted_candidates.take() {
                    if !not_faceted.is_empty() {
                        return Some(Ok(SortedDocumentsIteratorBuilder {
                            index,
                            rtxn,
                            number_db,
                            string_db,
                            fields: next_fields,
                            candidates: not_faceted,
                            geo_candidates,
                        }));
                    }
                }
                None
            }));

        Ok(SortedDocumentsIterator::Branch {
            current_child: None,
            next_children_size: size,
            next_children: Box::new(next_children),
        })
    }

    /// Builds a [`SortedDocumentsIterator`] based on the (lazy) results of a geo sort.
    #[allow(clippy::too_many_arguments)]
    fn build_geo(
        index: &'ctx crate::Index,
        rtxn: &'ctx heed::RoTxn<'ctx>,
        number_db: Database<FacetGroupKeyCodec<BytesRefCodec>, FacetGroupValueCodec>,
        string_db: Database<FacetGroupKeyCodec<BytesRefCodec>, FacetGroupValueCodec>,
        next_fields: &'ctx [AscDescId],
        candidates: RoaringBitmap,
        geo_candidates: &'ctx RoaringBitmap,
        field_ids: [u16; 2],
        target_point: [f64; 2],
        ascending: bool,
    ) -> crate::Result<SortedDocumentsIterator<'ctx>> {
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
                        fields: next_fields,
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
                        fields: next_fields,
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

pub fn recursive_sort<'ctx>(
    index: &'ctx crate::Index,
    rtxn: &'ctx heed::RoTxn<'ctx>,
    sort: Vec<AscDesc>,
    candidates: &'ctx RoaringBitmap,
) -> crate::Result<SortedDocuments<'ctx>> {
    let sortable_fields: BTreeSet<_> = index.sortable_fields(rtxn)?.into_iter().collect();
    let fields_ids_map = index.fields_ids_map(rtxn)?;

    // Retrieve the field ids that are used for sorting
    let mut fields = Vec::new();
    let mut need_geo_candidates = false;
    for asc_desc in sort {
        let (field, geofield) = match asc_desc {
            AscDesc::Asc(Member::Field(field)) => (Some((field, true)), None),
            AscDesc::Desc(Member::Field(field)) => (Some((field, false)), None),
            AscDesc::Asc(Member::Geo(target_point)) => (None, Some((target_point, true))),
            AscDesc::Desc(Member::Geo(target_point)) => (None, Some((target_point, false))),
        };
        if let Some((field, ascending)) = field {
            if is_faceted(&field, &sortable_fields) {
                // The field may be in sortable_fields but not in fields_ids_map if no document
                // has ever contained this field. In that case, we just skip this sort criterion
                // since there are no values to sort by. Documents will be returned in their
                // default order for this field.
                if let Some(field_id) = fields_ids_map.id(&field) {
                    fields.push(AscDescId::Facet { field_id, ascending });
                }
                continue;
            }
            return Err(UserError::InvalidDocumentSortableAttribute {
                field: field.to_string(),
                sortable_fields: sortable_fields.clone(),
            }
            .into());
        }
        if let Some((target_point, ascending)) = geofield {
            if sortable_fields.contains(RESERVED_GEO_FIELD_NAME) {
                if let (Some(lat), Some(lng)) =
                    (fields_ids_map.id("_geo.lat"), fields_ids_map.id("_geo.lng"))
                {
                    need_geo_candidates = true;
                    fields.push(AscDescId::Geo { field_ids: [lat, lng], target_point, ascending });
                    continue;
                }
            }
            return Err(UserError::InvalidDocumentSortableAttribute {
                field: RESERVED_GEO_FIELD_NAME.to_string(),
                sortable_fields: sortable_fields.clone(),
            }
            .into());
        }
    }

    let geo_candidates = if need_geo_candidates {
        index.geo_faceted_documents_ids(rtxn)?
    } else {
        RoaringBitmap::new()
    };

    let number_db = index.facet_id_f64_docids.remap_key_type::<FacetGroupKeyCodec<BytesRefCodec>>();
    let string_db =
        index.facet_id_string_docids.remap_key_type::<FacetGroupKeyCodec<BytesRefCodec>>();

    Ok(SortedDocuments { index, rtxn, fields, number_db, string_db, candidates, geo_candidates })
}
