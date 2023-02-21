#![allow(clippy::too_many_arguments)]

use heed::RoTxn;
use roaring::{MultiOps, RoaringBitmap};

use super::edge_docids_cache::EdgeDocidsCache;
use super::empty_paths_cache::EmptyPathsCache;
use super::paths_map::PathsMap;
use super::{RankingRuleGraph, RankingRuleGraphTrait};
use crate::new::db_cache::DatabaseCache;
use crate::new::ranking_rule_graph::Edge;
use crate::new::BitmapOrAllRef;
use crate::{Index, Result};

impl<G: RankingRuleGraphTrait> RankingRuleGraph<G> {
    pub fn resolve_paths<'transaction>(
        &mut self,
        index: &Index,
        txn: &'transaction RoTxn,
        db_cache: &mut DatabaseCache<'transaction>,
        edge_docids_cache: &mut EdgeDocidsCache<G>,
        empty_paths_cache: &mut EmptyPathsCache,
        universe: &RoaringBitmap,
        mut paths: PathsMap<u64>,
    ) -> Result<RoaringBitmap> {
        let mut path_bitmaps = vec![];

        paths.remove_edges(&empty_paths_cache.empty_edges);
        paths.remove_prefixes(&empty_paths_cache.empty_prefixes);

        'path_loop: while let Some((edge_indexes, _)) = paths.remove_first() {
            // if path is excluded, continue...
            let mut processed_edges = vec![];
            let mut path_bitmap = universe.clone();
            'edge_loop: for edge_index in edge_indexes {
                processed_edges.push(edge_index);
                let edge_docids =
                    edge_docids_cache.get_edge_docids(index, txn, db_cache, edge_index, self)?;
                match edge_docids {
                    BitmapOrAllRef::Bitmap(edge_docids) => {
                        if edge_docids.is_disjoint(universe) {
                            // 1. Store in the cache that this edge is empty for this universe
                            empty_paths_cache.empty_edges.insert(edge_index);
                            // 2. remove all the paths that contain this edge for this universe
                            paths.remove_edge(&edge_index);
                            // 3. remove this edge from the proximity graph

                            self.remove_edge(edge_index);

                            // 4. continue executing this function again on the remaining paths
                            continue 'path_loop;
                        } else {
                            path_bitmap &= edge_docids;
                            if path_bitmap.is_disjoint(universe) {
                                // 1. Store in the cache that this prefix is empty for this universe
                                empty_paths_cache
                                    .empty_prefixes
                                    .insert(processed_edges.iter().copied(), ());
                                // 2. remove all the paths beginning with this prefix
                                paths.remove_prefix(&processed_edges);
                                // 3. continue executing this function again on the remaining paths?
                                continue 'path_loop;
                            }
                        }
                    }
                    BitmapOrAllRef::All => continue 'edge_loop,
                }
            }
            path_bitmaps.push(path_bitmap);
        }
        let docids = MultiOps::union(path_bitmaps);
        Ok(docids)
        // for each path, translate it to an intersection of cached roaring bitmaps
        // then do a union for all paths

        // get the docids of the given paths in the proximity graph
        // in the fastest possible way
        // 1. roaring MultiOps (before we can do the Frozen+AST thing)
        // 2. minimize number of operations
    }
}
