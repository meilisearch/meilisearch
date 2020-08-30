use std::cmp;
use roaring::RoaringBitmap;

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
pub fn extract_position(position: u32) -> (u32, u32) {
    (position / ONE_ATTRIBUTE, position % ONE_ATTRIBUTE)
}

// Returns the group of four positions in which this position reside (i.e. 0, 4, 12).
pub fn group_of_four(position: u32) -> u32 {
    position - position % 4
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Node {
    // Is this node is the first node.
    Uninit,
    Init {
        // The layer where this node located.
        layer: usize,
        // The position where this node is located.
        position: u32,
        // The parent position from the above layer.
        parent_position: u32,
    },
}

impl Node {
    // TODO we must skip the successors that have already been seen
    // TODO we must skip the successors that doesn't return any documents
    //      this way we are able to skip entire paths
    pub fn successors<F>(&self, positions: &[RoaringBitmap], contains_documents: &mut F) -> Vec<(Node, u32)>
    where F: FnMut((usize, u32), (usize, u32)) -> bool,
    {
        match self {
            Node::Uninit => {
                positions[0].iter().map(|position| {
                    (Node::Init { layer: 0, position, parent_position: 0 }, 0)
                }).collect()
            },
            // We reached the highest layer
            n @ Node::Init { .. } if n.is_complete(positions) => vec![],
            Node::Init { layer, position, .. } => {
                positions[layer + 1].iter().filter_map(|p| {
                    let proximity = positions_proximity(*position, p);
                    let node = Node::Init {
                        layer: layer + 1,
                        position: p,
                        parent_position: *position,
                    };
                    // We do not produce the nodes we have already seen in previous iterations loops.
                    if node.is_reachable(contains_documents) {
                        Some((node, proximity))
                    } else {
                        None
                    }
                }).collect()
            }
        }
    }

    pub fn is_complete(&self, positions: &[RoaringBitmap]) -> bool {
        match self {
            Node::Uninit => false,
            Node::Init { layer, .. } => *layer == positions.len() - 1,
        }
    }

    pub fn position(&self) -> Option<u32> {
        match self {
            Node::Uninit => None,
            Node::Init { position, .. } => Some(*position),
        }
    }

    pub fn is_reachable<F>(&self, contains_documents: &mut F) -> bool
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
