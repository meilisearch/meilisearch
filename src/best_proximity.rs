use std::cmp;
use pathfinding::directed::dijkstra::dijkstra;

const ONE_ATTRIBUTE: u32 = 1000;
const MAX_INDEX: u32 = ONE_ATTRIBUTE - 1;
const MAX_DISTANCE: u32 = 8;

fn index_proximity(lhs: u32, rhs: u32) -> u32 {
    if lhs < rhs {
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

// Returns a position from the two parts of it.
fn construct_position(attr: u32, index: u32) -> u32 {
    attr * ONE_ATTRIBUTE + index
}

#[derive(Debug, Clone, PartialOrd, Ord, PartialEq, Eq, Hash)]
struct Path(Vec<u32>);

impl Path {
    fn new(positions: &[Vec<u32>]) -> Option<Path> {
        let position = positions.first()?.first()?;
        Some(Path(vec![*position]))
    }

    fn successors(&self, _positions: &[Vec<u32>]) -> Vec<(Path, u32)> {
        vec![]
    }

    fn proximity(&self) -> u32 {
        self.0.windows(2).map(|ps| positions_proximity(ps[0], ps[1])).sum::<u32>()
    }

    fn is_complete(&self, positions: &[Vec<u32>]) -> bool {
        positions.len() == self.0.len()
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

    fn is_path_successful(&self, path: &Path) -> bool {
        path.is_complete(&self.positions) && path.proximity() >= self.best_proximity
    }
}

impl Iterator for BestProximity {
    type Item = (u32, Vec<Vec<u32>>);

    fn next(&mut self) -> Option<Self::Item> {
        let mut output: Option<(u32, Vec<Vec<u32>>)> = None;

        unimplemented!("we must use and update self.best_proximity");

        loop {
            let start = Path::new(&self.positions)?;
            let result = dijkstra(
                &start,
                |p| p.successors(&self.positions),
                |p| self.is_path_successful(p) && output.as_ref().map_or(true, |paths| !paths.1.contains(&p.0)),
            );

            match result {
                Some((mut paths, proximity)) => {
                    let positions = paths.pop().unwrap();

                    // If the current output is
                    match &mut output {
                        Some((best_proximity, paths)) => {
                            // If the shortest path we found is bigger than the one requested
                            // it means that we found all the paths with the same proximity and can
                            // return those to the user.
                            if proximity > *best_proximity {
                                break;
                            }

                            // We add the new path to the output list as this path is known
                            // to be the requested distance.
                            paths.push(positions.0);
                        },
                        None => output = Some((positions.proximity(), vec![positions.0])),
                    }
                },
                None => break,
            }
        }

        output
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

        assert_eq!(iter.next(), Some((1+2, vec![vec![0], vec![1], vec![3]]))); // 3
        eprintln!("------------------");
        assert_eq!(iter.next(), Some((2+2, vec![vec![2], vec![1], vec![3]]))); // 4
        // assert_eq!(iter.next(), Some((3+2, vec![3, 1, 3]))); // 5
        // assert_eq!(iter.next(), Some((1+5, vec![0, 1, 6]))); // 6
        // assert_eq!(iter.next(), Some((4+2, vec![4, 1, 3]))); // 6
        // assert_eq!(iter.next(), Some((2+5, vec![2, 1, 6]))); // 7
        // assert_eq!(iter.next(), Some((3+5, vec![3, 1, 6]))); // 8
        // assert_eq!(iter.next(), Some((4+5, vec![4, 1, 6]))); // 9
        // assert_eq!(iter.next(), None);
    }
}
