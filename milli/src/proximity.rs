use std::cmp;

use crate::{relative_from_absolute_position, Position};

pub const MAX_DISTANCE: u32 = 8;

pub fn index_proximity(lhs: u32, rhs: u32) -> u32 {
    if lhs <= rhs {
        cmp::min(rhs - lhs, MAX_DISTANCE)
    } else {
        cmp::min((lhs - rhs) + 1, MAX_DISTANCE)
    }
}

pub fn positions_proximity(lhs: Position, rhs: Position) -> u32 {
    let (lhs_attr, lhs_index) = relative_from_absolute_position(lhs);
    let (rhs_attr, rhs_index) = relative_from_absolute_position(rhs);
    if lhs_attr != rhs_attr {
        MAX_DISTANCE
    } else {
        index_proximity(lhs_index as u32, rhs_index as u32)
    }
}

pub fn path_proximity(path: &[Position]) -> u32 {
    path.windows(2).map(|w| positions_proximity(w[0], w[1])).sum::<u32>()
}
