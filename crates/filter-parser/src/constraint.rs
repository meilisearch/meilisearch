use std::collections::BTreeMap;
use std::num::Saturating;
use std::ops::ControlFlow;

use itertools::Itertools as _;

use crate::{Condition, IndexFilterCondition, Token, VectorFilter};

pub type FilterConstraintSet = Vec<BTreeMap<ConstraintTarget, Vec<ConstraintCondition>>>;

#[derive(Debug, Default)]
pub struct FilterConstraints {
    pub constraints: FilterConstraintSet,
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone)]
pub enum ConstraintTarget {
    Fid(Token),
    Vector { fid: Token, embedder: Option<Token> },
    Geo,
}

#[derive(Debug, Clone)]
pub struct ConstraintCondition {
    pub kind: ConstraintConditionKind,
    pub polarity: bool,
}

impl FilterConstraints {
    pub fn new(filter: &IndexFilterCondition, fuel: &mut FilterConstraintFuel) -> Self {
        let mut constraints = Default::default();

        Self::evaluate_filter(&mut constraints, filter, true, fuel);

        Self { constraints }
    }

    fn evaluate_filter(
        constraints: &mut FilterConstraintSet,
        filter: &IndexFilterCondition,
        polarity: bool,
        fuel: &mut FilterConstraintFuel,
    ) {
        if fuel.consume_depth_fuel().is_break() {
            return;
        }
        match filter {
            IndexFilterCondition::Not(index_filter_condition) => {
                Self::evaluate_filter(constraints, index_filter_condition, !polarity, fuel)
            }
            IndexFilterCondition::Condition { fid, op } => {
                let constraint = ConstraintCondition {
                    kind: ConstraintConditionKind::Condition { condition: op.clone() },
                    polarity,
                };
                let mut these_constraints = BTreeMap::new();
                these_constraints.insert(ConstraintTarget::Fid(fid.clone()), vec![constraint]);
                constraints.push(these_constraints);
            }
            IndexFilterCondition::In { fid, els } => {
                // same as an OR of eq conditions
                let filter = IndexFilterCondition::Or(
                    els.iter()
                        .map(|el| IndexFilterCondition::Condition {
                            fid: fid.clone(),
                            op: Condition::Equal(el.clone().into()),
                        })
                        .collect(),
                );
                Self::evaluate_filter(constraints, &filter, polarity, fuel);
            }
            IndexFilterCondition::Or(index_filter_conditions) => {
                if polarity {
                    // OR means a new list of constraints
                    for cond in index_filter_conditions {
                        if fuel.consume_or_fuel().is_break() {
                            break;
                        }
                        Self::evaluate_filter(constraints, cond, true, fuel);
                    }
                } else {
                    let mut conjunction = Self::evaluate_and(index_filter_conditions, false, fuel);
                    constraints.append(&mut conjunction);
                }
            }
            IndexFilterCondition::And(index_filter_conditions) => {
                if polarity {
                    let mut conjunction = Self::evaluate_and(index_filter_conditions, true, fuel);
                    constraints.append(&mut conjunction);
                } else {
                    // OR means a new list of constraints
                    for cond in index_filter_conditions {
                        if fuel.consume_or_fuel().is_break() {
                            break;
                        }
                        Self::evaluate_filter(constraints, cond, false, fuel);
                    }
                }
            }
            IndexFilterCondition::VectorExists { fid, embedder, filter } => {
                let constraint = ConstraintCondition {
                    kind: ConstraintConditionKind::VectorExists { filter: filter.clone() },
                    polarity,
                };
                let mut these_constraints = BTreeMap::new();
                these_constraints.insert(
                    ConstraintTarget::Vector { fid: fid.clone(), embedder: embedder.clone() },
                    vec![constraint],
                );
                constraints.push(these_constraints);
            }
            IndexFilterCondition::GeoLowerThan { point, radius, resolution } => {
                let constraint = ConstraintCondition {
                    kind: ConstraintConditionKind::GeoLowerThan {
                        point: point.clone(),
                        radius: radius.clone(),
                        resolution: resolution.clone(),
                    },
                    polarity,
                };
                let mut these_constraints = BTreeMap::new();
                these_constraints.insert(ConstraintTarget::Geo, vec![constraint]);
                constraints.push(these_constraints);
            }
            IndexFilterCondition::GeoBoundingBox { top_right_point, bottom_left_point } => {
                let constraint = ConstraintCondition {
                    kind: ConstraintConditionKind::GeoBoundingBox {
                        top_right_point: top_right_point.clone(),
                        bottom_left_point: bottom_left_point.clone(),
                    },
                    polarity,
                };
                let mut these_constraints = BTreeMap::new();
                these_constraints.insert(ConstraintTarget::Geo, vec![constraint]);
                constraints.push(these_constraints);
            }
            IndexFilterCondition::GeoPolygon { points } => {
                let constraint = ConstraintCondition {
                    kind: ConstraintConditionKind::GeoPolygon { points: points.clone() },
                    polarity,
                };
                let mut these_constraints = BTreeMap::new();
                these_constraints.insert(ConstraintTarget::Geo, vec![constraint]);
                constraints.push(these_constraints);
            }
        }
        fuel.restore_depth_fuel();
    }

    fn evaluate_and(
        filter_conditions: &Vec<IndexFilterCondition>,
        polarity: bool,
        fuel: &mut FilterConstraintFuel,
    ) -> FilterConstraintSet {
        // AND means we fuse all lists of constraints
        let mut conjunction: FilterConstraintSet = Default::default();
        let mut local_constraints: FilterConstraintSet = Default::default();
        for cond in filter_conditions {
            Self::evaluate_filter(&mut local_constraints, cond, polarity, fuel);
            if conjunction.is_empty() {
                conjunction.append(&mut local_constraints);
                continue;
            }
            conjunction = conjunction
                .drain(..)
                .cartesian_product(std::mem::take(&mut local_constraints))
                .take_while(|_| fuel.consume_and_fuel().is_continue())
                .map(|(left, right)| {
                    left.into_iter()
                        .merge_join_by(right, |(left, _), (right, _)| left.cmp(right))
                        .map(|eob| match eob {
                            itertools::EitherOrBoth::Both((target, mut left), (_, mut right)) => {
                                left.append(&mut right);
                                (target, left)
                            }
                            itertools::EitherOrBoth::Left((target, constraint))
                            | itertools::EitherOrBoth::Right((target, constraint)) => {
                                (target, constraint)
                            }
                        })
                        .collect()
                })
                .collect();
        }
        conjunction
    }

    pub fn max_number_of_constraints(&self) -> usize {
        self.constraints.iter().map(|constraints| constraints.len()).max().unwrap_or_default()
    }
}

#[derive(Debug, Clone)]
pub enum ConstraintConditionKind {
    Condition { condition: Condition },
    VectorExists { filter: VectorFilter },
    GeoLowerThan { point: [Token; 2], radius: Token, resolution: Option<Token> },
    GeoBoundingBox { top_right_point: [Token; 2], bottom_left_point: [Token; 2] },
    GeoPolygon { points: Vec<[Token; 2]> },
}

#[derive(Debug, Clone, Copy)]
pub struct FilterConstraintFuel {
    remaining_or: Saturating<u16>,
    remaining_and: Saturating<u16>,
    remaining_depth: Saturating<u8>,
}

impl FilterConstraintFuel {
    pub fn new(or_fuel: u16, and_fuel: u16, depth_fuel: u8) -> Self {
        Self {
            remaining_or: Saturating(or_fuel),
            remaining_and: Saturating(and_fuel),
            remaining_depth: Saturating(depth_fuel),
        }
    }

    fn consume_or_fuel(&mut self) -> ControlFlow<(), ()> {
        self.remaining_or -= 1;
        if self.remaining_or.0 == 0 {
            ControlFlow::Break(())
        } else {
            ControlFlow::Continue(())
        }
    }

    fn consume_and_fuel(&mut self) -> ControlFlow<(), ()> {
        self.remaining_and -= 1;
        if self.remaining_and.0 == 0 {
            ControlFlow::Break(())
        } else {
            ControlFlow::Continue(())
        }
    }

    fn consume_depth_fuel(&mut self) -> ControlFlow<(), ()> {
        self.remaining_depth -= 1;
        if self.remaining_depth.0 == 0 {
            ControlFlow::Break(())
        } else {
            ControlFlow::Continue(())
        }
    }

    fn restore_depth_fuel(&mut self) {
        if !self.is_exhausted() {
            self.remaining_depth += 1;
        }
    }

    pub fn is_exhausted(&self) -> bool {
        self.remaining_or.0 == 0 || self.remaining_and.0 == 0 || self.remaining_depth.0 == 0
    }
}
