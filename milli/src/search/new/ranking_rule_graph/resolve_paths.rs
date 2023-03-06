#![allow(clippy::too_many_arguments)]

use super::edge_docids_cache::EdgeDocidsCache;
use super::empty_paths_cache::EmptyPathsCache;
use super::{RankingRuleGraph, RankingRuleGraphTrait};
use crate::new::{BitmapOrAllRef, SearchContext};
use crate::Result;
use roaring::{MultiOps, RoaringBitmap};

impl<G: RankingRuleGraphTrait> RankingRuleGraph<G> {
    // TODO: reduce the universe after computing each path
    // TODO: deserialize roaring bitmap within a universe
    pub fn resolve_paths<'search>(
        &mut self,
        ctx: &mut SearchContext<'search>,
        edge_docids_cache: &mut EdgeDocidsCache<G>,
        empty_paths_cache: &mut EmptyPathsCache,
        universe: &RoaringBitmap,
        mut paths: Vec<Vec<u32>>,
    ) -> Result<RoaringBitmap> {
        paths.sort_unstable();
        // let mut needs_filtering_empty_edges = false;
        // let mut needs_filtering_empty_prefix = false;
        // let mut needs_filtering_empty_couple_edges = false;
        let mut needs_filtering = false;
        let mut path_bitmaps = vec![];
        'path_loop: loop {
            // TODO: distinguish between empty_edges, empty_prefix, and empty_couple_edges filtering
            if needs_filtering {
                for path in paths.iter_mut() {
                    if empty_paths_cache.path_is_empty(path) {
                        path.clear();
                    }
                }
                needs_filtering = false;
            }
            let Some(edge_indexes) = paths.pop() else {
                break;
            };

            if edge_indexes.is_empty() {
                continue;
            }

            let mut path_bitmap = universe.clone();
            let mut visited_edges = vec![];
            let mut cached_edge_docids = vec![];
            'edge_loop: for edge_index in edge_indexes {
                visited_edges.push(edge_index);
                let edge_docids =
                    edge_docids_cache.get_edge_docids(ctx, edge_index, self, universe)?;
                match edge_docids {
                    BitmapOrAllRef::Bitmap(edge_docids) => {
                        cached_edge_docids.push((edge_index, edge_docids.clone()));
                        let (_, edge_docids) = cached_edge_docids.last().unwrap();
                        if edge_docids.is_disjoint(universe) {
                            // 1. Store in the cache that this edge is empty for this universe
                            empty_paths_cache.forbid_edge(edge_index);
                            // 2. remove this edge from the proximity graph
                            self.remove_edge(edge_index);
                            edge_docids_cache.cache.remove(&edge_index);
                            needs_filtering = true;
                            // needs_filtering_empty_edges = true;
                            // 3. continue executing this function again on the remaining paths
                            continue 'path_loop;
                        } else {
                            path_bitmap &= edge_docids;
                            if path_bitmap.is_disjoint(universe) {
                                // needs_filtering_empty_prefix = true;
                                needs_filtering = true;
                                empty_paths_cache.forbid_prefix(&visited_edges);
                                // if the intersection between this edge and any
                                // previous one is disjoint with the universe,
                                // then we add these two edges to the empty_path_cache
                                for (edge_index2, edge_docids2) in
                                    cached_edge_docids[..cached_edge_docids.len() - 1].iter()
                                {
                                    let intersection = edge_docids & edge_docids2;
                                    if intersection.is_disjoint(universe) {
                                        // needs_filtering_empty_couple_edges = true;
                                        empty_paths_cache
                                            .forbid_couple_edges(*edge_index2, edge_index);
                                    }
                                }
                                continue 'path_loop;
                            }
                        }
                    }
                    BitmapOrAllRef::All => continue 'edge_loop,
                }
            }
            path_bitmaps.push(path_bitmap);
        }

        Ok(MultiOps::union(path_bitmaps))
    }
}
