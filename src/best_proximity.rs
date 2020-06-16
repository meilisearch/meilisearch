use std::cmp;
use std::time::Instant;

use crate::iter_shortest_paths::astar_bag;

const ONE_ATTRIBUTE: u32 = 1000;
const MAX_DISTANCE: u32 = 8;

fn index_proximity(lhs: u32, rhs: u32) -> u32 {
    if lhs <= rhs {
        cmp::min(rhs - lhs, MAX_DISTANCE)
    } else {
        cmp::min((lhs - rhs) + 1, MAX_DISTANCE)
    }
}

pub fn positions_proximity(lhs: u32, rhs: u32) -> u32 {
    let (lhs_attr, lhs_index) = extract_position(lhs);
    let (rhs_attr, rhs_index) = extract_position(rhs);
    if lhs_attr != rhs_attr { MAX_DISTANCE }
    else { index_proximity(lhs_index, rhs_index) }
}

// Returns the attribute and index parts.
fn extract_position(position: u32) -> (u32, u32) {
    (position / ONE_ATTRIBUTE, position % ONE_ATTRIBUTE)
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
enum Node {
    // Is this node is the first node.
    Uninit,
    Init {
        // The layer where this node located.
        layer: usize,
        // The position where this node is located.
        position: u32,
        // The total accumulated proximity until this node, used for skipping nodes.
        acc_proximity: u32,
        // The parent position from the above layer.
        parent_position: u32,
    },
}

impl Node {
    // TODO we must skip the successors that have already been seen
    // TODO we must skip the successors that doesn't return any documents
    //      this way we are able to skip entire paths
    fn successors(&self, positions: &[Vec<u32>], best_proximity: u32) -> Vec<(Node, u32)> {
        match self {
            Node::Uninit => {
                positions[0].iter().map(|p| {
                    (Node::Init { layer: 0, position: *p, acc_proximity: 0, parent_position: 0 }, 0)
                }).collect()
            },
            // We reached the highest layer
            n @ Node::Init { .. } if n.is_complete(positions) => vec![],
            Node::Init { layer, position, acc_proximity, .. } => {
                positions[layer + 1].iter().filter_map(|p| {
                    let proximity = positions_proximity(*position, *p);
                    let node = Node::Init {
                        layer: layer + 1,
                        position: *p,
                        acc_proximity: acc_proximity + proximity,
                        parent_position: *position,
                    };
                    // We do not produce the nodes we have already seen in previous iterations loops.
                    if node.is_complete(positions) && acc_proximity + proximity < best_proximity {
                        None
                    } else {
                        Some((node, proximity))
                    }
                }).collect()
            }
        }
    }

    fn is_complete(&self, positions: &[Vec<u32>]) -> bool {
        match self {
            Node::Uninit => false,
            Node::Init { layer, .. } => *layer == positions.len() - 1,
        }
    }

    fn position(&self) -> Option<u32> {
        match self {
            Node::Uninit => None,
            Node::Init { position, .. } => Some(*position),
        }
    }

    fn proximity(&self) -> u32 {
        match self {
            Node::Uninit => 0,
            Node::Init { layer, position, acc_proximity, parent_position } => {
                if layer.checked_sub(1).is_some() {
                    acc_proximity + positions_proximity(*position, *parent_position)
                } else {
                    0
                }
            },
        }
    }

    fn is_reachable<F>(&self, contains_documents: &mut F) -> bool
    where F: FnMut((usize, u32), (usize, u32)) -> bool,
    {
        match self {
            Node::Uninit => true,
            Node::Init { layer, position, parent_position, .. } => {
                match layer.checked_sub(1) {
                    Some(parent_layer) => {
                        (contains_documents)((parent_layer, *parent_position), (*layer, *position))
                    },
                    None => true,
                }
            },
        }
    }
}

pub struct BestProximity<F> {
    positions: Vec<Vec<u32>>,
    best_proximity: u32,
    contains_documents: F,
}

impl<F> BestProximity<F> {
    pub fn new(positions: Vec<Vec<u32>>, contains_documents: F) -> BestProximity<F> {
        let best_proximity = (positions.len() as u32).saturating_sub(1);
        BestProximity { positions, best_proximity, contains_documents }
    }
}

impl<F> Iterator for BestProximity<F>
where F: FnMut((usize, u32), (usize, u32)) -> bool,
{
    type Item = (u32, Vec<Vec<u32>>);

    fn next(&mut self) -> Option<Self::Item> {
        let before = Instant::now();

        if self.best_proximity == self.positions.len() as u32 * MAX_DISTANCE {
            return None;
        }

        let BestProximity { positions, best_proximity, contains_documents } = self;

        let result = astar_bag(
            &Node::Uninit, // start
            |n| n.successors(&positions, *best_proximity),
            |_| 0, // heuristic
            |n| { // success
                let c = n.is_complete(&positions) && n.proximity() >= *best_proximity;
                if n.is_reachable(contains_documents) { Some(c) } else { None }
            },
        );

        eprintln!("BestProximity::next() took {:.02?}", before.elapsed());

        match result {
            Some((paths, proximity)) => {
                self.best_proximity = proximity + 1;
                // We retrieve the last path that we convert into a Vec
                let paths: Vec<_> = paths.map(|p| p.iter().filter_map(Node::position).collect()).collect();
                eprintln!("result: {} {:?}", proximity, paths);
                Some((proximity, paths))
            },
            None => {
                eprintln!("result: {:?}", None as Option<()>);
                self.best_proximity += 1;
                None
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn same_attribute() {
        let positions = vec![
            vec![0,    2, 3, 4   ],
            vec![   1,           ],
            vec![         3,    6],
        ];
        let mut iter = BestProximity::new(positions, |_, _| true);

        assert_eq!(iter.next(), Some((1+2, vec![vec![0, 1, 3]]))); // 3
        assert_eq!(iter.next(), Some((2+2, vec![vec![2, 1, 3]]))); // 4
        assert_eq!(iter.next(), Some((3+2, vec![vec![3, 1, 3]]))); // 5
        assert_eq!(iter.next(), Some((1+5, vec![vec![0, 1, 6], vec![4, 1, 3]]))); // 6
        assert_eq!(iter.next(), Some((2+5, vec![vec![2, 1, 6]]))); // 7
        assert_eq!(iter.next(), Some((3+5, vec![vec![3, 1, 6]]))); // 8
        assert_eq!(iter.next(), Some((4+5, vec![vec![4, 1, 6]]))); // 9
        assert_eq!(iter.next(), None);
    }

    #[test]
    fn different_attributes() {
        let positions = vec![
            vec![0,    2,       1000, 1001, 2000      ],
            vec![   1,          1000,       2001      ],
            vec![         3, 6,             2002, 3000],
        ];
        let mut iter = BestProximity::new(positions, |_, _| true);

        assert_eq!(iter.next(), Some((1+1, vec![vec![2000, 2001, 2002]]))); // 2
        assert_eq!(iter.next(), Some((1+2, vec![vec![0, 1, 3]]))); // 3
        assert_eq!(iter.next(), Some((2+2, vec![vec![2, 1, 3]]))); // 4
        assert_eq!(iter.next(), Some((1+5, vec![vec![0, 1, 6]]))); // 6
        // We ignore others here...
    }

    #[test]
    fn easy_proximities() {
        fn slice_proximity(positions: &[u32]) -> u32 {
            positions.windows(2).map(|ps| positions_proximity(ps[0], ps[1])).sum::<u32>()
        }

        assert_eq!(slice_proximity(&[1000, 1000, 2002]), 8);
    }
}
