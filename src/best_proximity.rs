use std::cmp;
use std::time::Instant;

use pathfinding::directed::astar::astar_bag;

use crate::SmallVec16;

const ONE_ATTRIBUTE: u32 = 1000;
const MAX_DISTANCE: u32 = 8;

fn index_proximity(lhs: u32, rhs: u32) -> u32 {
    if lhs <= rhs {
        cmp::min(rhs - lhs, MAX_DISTANCE)
    } else {
        cmp::min(lhs - rhs, MAX_DISTANCE) + 1
    }
}

fn positions_proximity(lhs: u32, rhs: u32) -> u32 {
    let (lhs_attr, lhs_index) = extract_position(lhs);
    let (rhs_attr, rhs_index) = extract_position(rhs);
    if lhs_attr != rhs_attr { MAX_DISTANCE }
    else { index_proximity(lhs_index, rhs_index) }
}

// Returns the attribute and index parts.
fn extract_position(position: u32) -> (u32, u32) {
    (position / ONE_ATTRIBUTE, position % ONE_ATTRIBUTE)
}

#[derive(Debug, Default, Clone, PartialOrd, Ord, PartialEq, Eq, Hash)]
struct Path(SmallVec16<u32>);

impl Path {
    // TODO we must skip the successors that have already been sent
    // TODO we must skip the successors that doesn't return any documents
    //      this way we are able to skip entire paths
    fn successors(&self, positions: &[Vec<u32>], best_proximity: u32) -> Vec<(Path, u32)> {
        let next_positions = match positions.get(self.0.len()) {
            Some(positions) => positions,
            None => return vec![],
        };

        next_positions.iter()
            .filter_map(|p| {
                let mut path = self.clone();
                path.0.push(*p);
                let proximity = path.proximity();
                if path.is_complete(positions) && proximity < best_proximity {
                    None
                } else {
                    Some((path, proximity))
                }
            })
            .inspect(|p| eprintln!("{:?}", p))
            .collect()
    }

    fn proximity(&self) -> u32 {
        self.0.windows(2).map(|ps| positions_proximity(ps[0], ps[1])).sum::<u32>()
    }

    fn heuristic(&self, positions: &[Vec<u32>]) -> u32 {
        let remaining = (positions.len() - self.0.len()) as u32;
        self.proximity() + remaining * MAX_DISTANCE
    }

    fn is_complete(&self, positions: &[Vec<u32>]) -> bool {
        let res = positions.len() == self.0.len();
        eprintln!("is_complete: {:?} {}", self, res);
        res
    }
}

pub struct BestProximity {
    positions: Vec<Vec<u32>>,
    best_proximity: u32,
}

impl BestProximity {
    pub fn new(positions: Vec<Vec<u32>>) -> BestProximity {
        BestProximity { positions, best_proximity: 0 }
    }
}

impl Iterator for BestProximity {
    type Item = (u32, Vec<Vec<u32>>);

    fn next(&mut self) -> Option<Self::Item> {
        let before = Instant::now();

        if self.best_proximity == self.positions.len() as u32 * MAX_DISTANCE {
            return None;
        }

        // We start with nothing
        let start = Path::default();
        let result = astar_bag(
            &start,
            |p| p.successors(&self.positions, self.best_proximity),
            |p| p.heuristic(&self.positions),
            |p| p.is_complete(&self.positions), // success
        );

        eprintln!("BestProximity::next() took {:.02?}", before.elapsed());

        match result {
            Some((paths, proximity)) => {
                self.best_proximity = proximity + 1;
                // We retrieve the last path that we convert into a Vec
                let paths: Vec<_> = paths.map(|p| {
                    p.last().unwrap().0.to_vec()
                }).collect();
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
        let mut iter = BestProximity::new(positions);

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
        let mut iter = BestProximity::new(positions);

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
