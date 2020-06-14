use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashSet};
use std::hash::Hash;
use std::usize;

use indexmap::map::Entry::{Occupied, Vacant};
use indexmap::IndexMap;

pub fn astar_bag<N, FN, IN, FH, FS>(
    start: &N,
    mut successors: FN,
    mut heuristic: FH,
    mut success: FS,
) -> Option<(AstarSolution<N>, u32)>
where
    N: Eq + Hash + Clone,
    FN: FnMut(&N) -> IN,
    IN: IntoIterator<Item = (N, u32)>,
    FH: FnMut(&N) -> u32,
    FS: FnMut(&N) -> Option<bool>,
{
    let mut to_see = BinaryHeap::new();
    let mut min_cost = None;
    let mut sinks = HashSet::new();
    to_see.push(SmallestCostHolder {
        estimated_cost: heuristic(start),
        cost: 0,
        index: 0,
    });
    let mut parents: IndexMap<N, (HashSet<usize>, u32)> = IndexMap::new();
    parents.insert(start.clone(), (HashSet::new(), 0));
    while let Some(SmallestCostHolder { cost, index, estimated_cost, .. }) = to_see.pop() {
        if let Some(min_cost) = min_cost {
            if estimated_cost > min_cost {
                break;
            }
        }
        let successors = {
            let (node, &(_, c)) = parents.get_index(index).unwrap();
            // We check that the node is even reachable and if so if it is an answer.
            // If this node is unreachable we skip it.
            match success(node) {
                Some(success) => if success {
                    min_cost = Some(cost);
                    sinks.insert(index);
                },
                None => continue,
            }

            // We may have inserted a node several time into the binary heap if we found
            // a better way to access it. Ensure that we are currently dealing with the
            // best path and discard the others.
            if cost > c {
                continue;
            }
            successors(node)
        };
        for (successor, move_cost) in successors {
            let new_cost = cost + move_cost;
            let h; // heuristic(&successor)
            let n; // index for successor
            match parents.entry(successor) {
                Vacant(e) => {
                    h = heuristic(e.key());
                    n = e.index();
                    let mut p = HashSet::new();
                    p.insert(index);
                    e.insert((p, new_cost));
                }
                Occupied(mut e) => {
                    if e.get().1 > new_cost {
                        h = heuristic(e.key());
                        n = e.index();
                        let s = e.get_mut();
                        s.0.clear();
                        s.0.insert(index);
                        s.1 = new_cost;
                    } else {
                        if e.get().1 == new_cost {
                            // New parent with an identical cost, this is not
                            // considered as an insertion.
                            e.get_mut().0.insert(index);
                        }
                        continue;
                    }
                }
            }

            to_see.push(SmallestCostHolder {
                estimated_cost: new_cost + h,
                cost: new_cost,
                index: n,
            });
        }
    }

    min_cost.map(|cost| {
        let parents = parents
            .into_iter()
            .map(|(k, (ps, _))| (k, ps.into_iter().collect()))
            .collect();
        (
            AstarSolution {
                sinks: sinks.into_iter().collect(),
                parents,
                current: vec![],
                terminated: false,
            },
            cost,
        )
    })
}

struct SmallestCostHolder<K> {
    estimated_cost: K,
    cost: K,
    index: usize,
}

impl<K: PartialEq> PartialEq for SmallestCostHolder<K> {
    fn eq(&self, other: &Self) -> bool {
        self.estimated_cost.eq(&other.estimated_cost) && self.cost.eq(&other.cost)
    }
}

impl<K: PartialEq> Eq for SmallestCostHolder<K> {}

impl<K: Ord> PartialOrd for SmallestCostHolder<K> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<K: Ord> Ord for SmallestCostHolder<K> {
    fn cmp(&self, other: &Self) -> Ordering {
        match other.estimated_cost.cmp(&self.estimated_cost) {
            Ordering::Equal => self.cost.cmp(&other.cost),
            s => s,
        }
    }
}

/// Iterator structure created by the `astar_bag` function.
#[derive(Clone)]
pub struct AstarSolution<N> {
    sinks: Vec<usize>,
    parents: Vec<(N, Vec<usize>)>,
    current: Vec<Vec<usize>>,
    terminated: bool,
}

impl<N: Clone + Eq + Hash> AstarSolution<N> {
    fn complete(&mut self) {
        loop {
            let ps = match self.current.last() {
                None => self.sinks.clone(),
                Some(last) => {
                    let &top = last.last().unwrap();
                    self.parents(top).clone()
                }
            };
            if ps.is_empty() {
                break;
            }
            self.current.push(ps);
        }
    }

    fn next_vec(&mut self) {
        while self.current.last().map(Vec::len) == Some(1) {
            self.current.pop();
        }
        self.current.last_mut().map(Vec::pop);
    }

    fn node(&self, i: usize) -> &N {
        &self.parents[i].0
    }

    fn parents(&self, i: usize) -> &Vec<usize> {
        &self.parents[i].1
    }
}

impl<N: Clone + Eq + Hash> Iterator for AstarSolution<N> {
    type Item = Vec<N>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.terminated {
            return None;
        }
        self.complete();
        let path = self
            .current
            .iter()
            .rev()
            .map(|v| v.last().cloned().unwrap())
            .map(|i| self.node(i).clone())
            .collect::<Vec<_>>();
        self.next_vec();
        self.terminated = self.current.is_empty();
        Some(path)
    }
}
