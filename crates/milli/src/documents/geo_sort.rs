use std::collections::VecDeque;

use heed::types::{Bytes, Unit};
use heed::{RoPrefix, RoTxn};
use roaring::RoaringBitmap;
use rstar::RTree;

use crate::heed_codec::facet::{FieldDocIdFacetCodec, OrderedF64Codec};
use crate::search::new::{facet_string_values, facet_values_prefix_key};
use crate::{distance_between_two_points, lat_lng_to_xyz, GeoPoint, Index};

#[derive(Debug, Clone, Copy)]
pub struct GeoSortParameter {
    // Define the strategy used by the geo sort
    pub strategy: GeoSortStrategy,
    // Limit the number of docs in a single bucket to avoid unexpectedly large overhead
    pub max_bucket_size: u64,
    // Considering the errors of GPS and geographical calculations, distances less than distance_error_margin will be treated as equal
    pub distance_error_margin: f64,
}

impl Default for GeoSortParameter {
    fn default() -> Self {
        Self {
            strategy: GeoSortStrategy::default(),
            max_bucket_size: 1000,
            distance_error_margin: 1.0,
        }
    }
}
/// Define the strategy used by the geo sort.
/// The parameter represents the cache size, and, in the case of the Dynamic strategy,
/// the point where we move from using the iterative strategy to the rtree.
#[derive(Debug, Clone, Copy)]
pub enum GeoSortStrategy {
    AlwaysIterative(usize),
    AlwaysRtree(usize),
    Dynamic(usize),
}

impl Default for GeoSortStrategy {
    fn default() -> Self {
        GeoSortStrategy::Dynamic(1000)
    }
}

impl GeoSortStrategy {
    pub fn use_rtree(&self, candidates: usize) -> bool {
        match self {
            GeoSortStrategy::AlwaysIterative(_) => false,
            GeoSortStrategy::AlwaysRtree(_) => true,
            GeoSortStrategy::Dynamic(i) => candidates >= *i,
        }
    }

    pub fn cache_size(&self) -> usize {
        match self {
            GeoSortStrategy::AlwaysIterative(i)
            | GeoSortStrategy::AlwaysRtree(i)
            | GeoSortStrategy::Dynamic(i) => *i,
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub fn fill_cache(
    index: &Index,
    txn: &RoTxn<heed::AnyTls>,
    strategy: GeoSortStrategy,
    ascending: bool,
    target_point: [f64; 2],
    field_ids: &Option<[u16; 2]>,
    rtree: &mut Option<RTree<GeoPoint>>,
    geo_candidates: &RoaringBitmap,
    cached_sorted_docids: &mut VecDeque<(u32, [f64; 2])>,
) -> crate::Result<()> {
    debug_assert!(cached_sorted_docids.is_empty());

    // lazily initialize the rtree if needed by the strategy, and cache it in `self.rtree`
    let rtree = if strategy.use_rtree(geo_candidates.len() as usize) {
        if let Some(rtree) = rtree.as_ref() {
            // get rtree from cache
            Some(rtree)
        } else {
            let rtree2 = index.geo_rtree(txn)?.expect("geo candidates but no rtree");
            // insert rtree in cache and returns it.
            // Can't use `get_or_insert_with` because getting the rtree from the DB is a fallible operation.
            Some(&*rtree.insert(rtree2))
        }
    } else {
        None
    };

    let cache_size = strategy.cache_size();
    if let Some(rtree) = rtree {
        if ascending {
            let point = lat_lng_to_xyz(&target_point);
            for point in rtree.nearest_neighbor_iter(&point) {
                if geo_candidates.contains(point.data.0) {
                    cached_sorted_docids.push_back(point.data);
                    if cached_sorted_docids.len() >= cache_size {
                        break;
                    }
                }
            }
        } else {
            // in the case of the desc geo sort we look for the closest point to the opposite of the queried point
            // and we insert the points in reverse order they get reversed when emptying the cache later on
            let point = lat_lng_to_xyz(&opposite_of(target_point));
            for point in rtree.nearest_neighbor_iter(&point) {
                if geo_candidates.contains(point.data.0) {
                    cached_sorted_docids.push_front(point.data);
                    if cached_sorted_docids.len() >= cache_size {
                        break;
                    }
                }
            }
        }
    } else {
        // the iterative version
        let [lat, lng] = field_ids.expect("fill_buffer can't be called without the lat&lng");

        let mut documents = geo_candidates
            .iter()
            .map(|id| -> crate::Result<_> { Ok((id, geo_value(id, lat, lng, index, txn)?)) })
            .collect::<crate::Result<Vec<(u32, [f64; 2])>>>()?;
        // computing the distance between two points is expensive thus we cache the result
        documents
            .sort_by_cached_key(|(_, p)| distance_between_two_points(&target_point, p) as usize);
        cached_sorted_docids.extend(documents);
    };

    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub fn next_bucket(
    index: &Index,
    txn: &RoTxn<heed::AnyTls>,
    universe: &RoaringBitmap,
    ascending: bool,
    target_point: [f64; 2],
    field_ids: &Option<[u16; 2]>,
    rtree: &mut Option<RTree<GeoPoint>>,
    cached_sorted_docids: &mut VecDeque<(u32, [f64; 2])>,
    geo_candidates: &RoaringBitmap,
    parameter: GeoSortParameter,
) -> crate::Result<Option<(RoaringBitmap, Option<[f64; 2]>)>> {
    let mut geo_candidates = geo_candidates & universe;

    if geo_candidates.is_empty() {
        return Ok(Some((universe.clone(), None)));
    }

    let next = |cache: &mut VecDeque<_>| {
        if ascending {
            cache.pop_front()
        } else {
            cache.pop_back()
        }
    };
    let put_back = |cache: &mut VecDeque<_>, x: _| {
        if ascending {
            cache.push_front(x)
        } else {
            cache.push_back(x)
        }
    };

    let mut current_bucket = RoaringBitmap::new();
    // current_distance stores the first point and distance in current bucket
    let mut current_distance: Option<([f64; 2], f64)> = None;
    loop {
        // The loop will only exit when we have found all points with equal distance or have exhausted the candidates.
        if let Some((id, point)) = next(cached_sorted_docids) {
            if geo_candidates.contains(id) {
                let distance = distance_between_two_points(&target_point, &point);
                if let Some((point0, bucket_distance)) = current_distance.as_ref() {
                    if (bucket_distance - distance).abs() > parameter.distance_error_margin {
                        // different distance, point belongs to next bucket
                        put_back(cached_sorted_docids, (id, point));
                        return Ok(Some((current_bucket, Some(point0.to_owned()))));
                    } else {
                        // same distance, point belongs to current bucket
                        current_bucket.insert(id);
                        // remove from candidates to prevent it from being added to the cache again
                        geo_candidates.remove(id);
                        // current bucket size reaches limit, force return
                        if current_bucket.len() == parameter.max_bucket_size {
                            return Ok(Some((current_bucket, Some(point0.to_owned()))));
                        }
                    }
                } else {
                    // first doc in current bucket
                    current_distance = Some((point, distance));
                    current_bucket.insert(id);
                    geo_candidates.remove(id);
                    // current bucket size reaches limit, force return
                    if current_bucket.len() == parameter.max_bucket_size {
                        return Ok(Some((current_bucket, Some(point.to_owned()))));
                    }
                }
            }
        } else {
            // cache exhausted, we need to refill it
            fill_cache(
                index,
                txn,
                parameter.strategy,
                ascending,
                target_point,
                field_ids,
                rtree,
                &geo_candidates,
                cached_sorted_docids,
            )?;

            if cached_sorted_docids.is_empty() {
                // candidates exhausted, exit
                if let Some((point0, _)) = current_distance.as_ref() {
                    return Ok(Some((current_bucket, Some(point0.to_owned()))));
                } else {
                    return Ok(Some((universe.clone(), None)));
                }
            }
        }
    }
}

/// Return an iterator over each number value in the given field of the given document.
fn facet_number_values<'a>(
    docid: u32,
    field_id: u16,
    index: &Index,
    txn: &'a RoTxn<'a>,
) -> crate::Result<RoPrefix<'a, FieldDocIdFacetCodec<OrderedF64Codec>, Unit>> {
    let key = facet_values_prefix_key(field_id, docid);

    let iter = index
        .field_id_docid_facet_f64s
        .remap_key_type::<Bytes>()
        .prefix_iter(txn, &key)?
        .remap_key_type();

    Ok(iter)
}

/// Extracts the lat and long values from a single document.
///
/// If it is not able to find it in the facet number index it will extract it
/// from the facet string index and parse it as f64 (as the geo extraction behaves).
pub(crate) fn geo_value(
    docid: u32,
    field_lat: u16,
    field_lng: u16,
    index: &Index,
    rtxn: &RoTxn<'_>,
) -> crate::Result<[f64; 2]> {
    let extract_geo = |geo_field: u16| -> crate::Result<f64> {
        match facet_number_values(docid, geo_field, index, rtxn)?.next() {
            Some(Ok(((_, _, geo), ()))) => Ok(geo),
            Some(Err(e)) => Err(e.into()),
            None => match facet_string_values(docid, geo_field, index, rtxn)?.next() {
                Some(Ok((_, geo))) => {
                    Ok(geo.parse::<f64>().expect("cannot parse geo field as f64"))
                }
                Some(Err(e)) => Err(e.into()),
                None => panic!("A geo faceted document doesn't contain any lat or lng"),
            },
        }
    };

    let lat = extract_geo(field_lat)?;
    let lng = extract_geo(field_lng)?;

    Ok([lat, lng])
}

/// Compute the antipodal coordinate of `coord`
pub(crate) fn opposite_of(mut coord: [f64; 2]) -> [f64; 2] {
    coord[0] *= -1.;
    // in the case of x,0 we want to return x,180
    if coord[1] > 0. {
        coord[1] -= 180.;
    } else {
        coord[1] += 180.;
    }

    coord
}
