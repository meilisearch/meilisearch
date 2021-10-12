
use std::fmt::Debug;
use std::ops::Bound::{self, Excluded, Included};

use either::Either;
use heed::types::DecodeIgnore;
use log::debug;
use nom::error::{convert_error, VerboseError};
use roaring::RoaringBitmap;

use self::FilterCondition::*;

use super::filter_parser::{Operator, ParseContext};
use super::FacetNumberRange;
use crate::error::{Error, UserError};
use crate::heed_codec::facet::{
    FacetLevelValueF64Codec, FacetStringLevelZeroCodec, FacetStringLevelZeroValueCodec,
};
use crate::{
    distance_between_two_points, CboRoaringBitmapCodec, FieldId, Index, Result,
};

#[derive(Debug, Clone, PartialEq)]
pub enum FilterCondition {
    Operator(FieldId, Operator),
    Or(Box<Self>, Box<Self>),
    And(Box<Self>, Box<Self>),
    Empty,
}

// impl From<std::>

//for nom
impl FilterCondition {
    pub fn from_array<I, J, A, B>(
        rtxn: &heed::RoTxn,
        index: &Index,
        array: I,
    ) -> Result<Option<FilterCondition>>
    where
        I: IntoIterator<Item = Either<J, B>>,
        J: IntoIterator<Item = A>,
        A: AsRef<str>,
        B: AsRef<str>,
    {
        let mut ands: Option<FilterCondition> = None;

        for either in array {
            match either {
                Either::Left(array) => {
                    let mut ors = None;
                    for rule in array {
                        let condition = FilterCondition::from_str(rtxn, index, rule.as_ref())?;
                        ors = match ors.take() {
                            Some(ors) => Some(Or(Box::new(ors), Box::new(condition))),
                            None => Some(condition),
                        };
                    }

                    if let Some(rule) = ors {
                        ands = match ands.take() {
                            Some(ands) => Some(And(Box::new(ands), Box::new(rule))),
                            None => Some(rule),
                        };
                    }
                }
                Either::Right(rule) => {
                    let condition = FilterCondition::from_str(rtxn, index, rule.as_ref())?;
                    ands = match ands.take() {
                        Some(ands) => Some(And(Box::new(ands), Box::new(condition))),
                        None => Some(condition),
                    };
                }
            }
        }

        Ok(ands)
    }
    pub fn from_str(
        rtxn: &heed::RoTxn,
        index: &Index,
        expression: &str,
    ) -> Result<FilterCondition> {
        let fields_ids_map = index.fields_ids_map(rtxn)?;
        let filterable_fields = index.filterable_fields(rtxn)?;
        let ctx =
            ParseContext { fields_ids_map: &fields_ids_map, filterable_fields: &filterable_fields };
        match ctx.parse_expression::<VerboseError<&str>>(expression) {
            Ok((_, fc)) => Ok(fc),
            Err(e) => {
                let ve = match e {
                    nom::Err::Error(x) => x,
                    nom::Err::Failure(x) => x,
                    _ => unreachable!(),
                };
                Err(Error::UserError(UserError::InvalidFilterNom {
                    input: convert_error(expression, ve).to_string(),
                }))
            }
        }
    }
    pub fn negate(self) -> FilterCondition {
        match self {
            Operator(fid, op) => match op.negate() {
                (op, None) => Operator(fid, op),
                (a, Some(b)) => Or(Box::new(Operator(fid, a)), Box::new(Operator(fid, b))),
            },
            Or(a, b) => And(Box::new(a.negate()), Box::new(b.negate())),
            And(a, b) => Or(Box::new(a.negate()), Box::new(b.negate())),
            Empty => Empty,
        }
    }
}

impl FilterCondition {
    /// Aggregates the documents ids that are part of the specified range automatically
    /// going deeper through the levels.
    fn explore_facet_number_levels(
        rtxn: &heed::RoTxn,
        db: heed::Database<FacetLevelValueF64Codec, CboRoaringBitmapCodec>,
        field_id: FieldId,
        level: u8,
        left: Bound<f64>,
        right: Bound<f64>,
        output: &mut RoaringBitmap,
    ) -> Result<()> {
        match (left, right) {
            // If the request is an exact value we must go directly to the deepest level.
            (Included(l), Included(r)) if l == r && level > 0 => {
                return Self::explore_facet_number_levels(
                    rtxn, db, field_id, 0, left, right, output,
                );
            }
            // lower TO upper when lower > upper must return no result
            (Included(l), Included(r)) if l > r => return Ok(()),
            (Included(l), Excluded(r)) if l >= r => return Ok(()),
            (Excluded(l), Excluded(r)) if l >= r => return Ok(()),
            (Excluded(l), Included(r)) if l >= r => return Ok(()),
            (_, _) => (),
        }

        let mut left_found = None;
        let mut right_found = None;

        // We must create a custom iterator to be able to iterate over the
        // requested range as the range iterator cannot express some conditions.
        let iter = FacetNumberRange::new(rtxn, db, field_id, level, left, right)?;

        debug!("Iterating between {:?} and {:?} (level {})", left, right, level);

        for (i, result) in iter.enumerate() {
            let ((_fid, level, l, r), docids) = result?;
            debug!("{:?} to {:?} (level {}) found {} documents", l, r, level, docids.len());
            *output |= docids;
            // We save the leftest and rightest bounds we actually found at this level.
            if i == 0 {
                left_found = Some(l);
            }
            right_found = Some(r);
        }

        // Can we go deeper?
        let deeper_level = match level.checked_sub(1) {
            Some(level) => level,
            None => return Ok(()),
        };

        // We must refine the left and right bounds of this range by retrieving the
        // missing part in a deeper level.
        match left_found.zip(right_found) {
            Some((left_found, right_found)) => {
                // If the bound is satisfied we avoid calling this function again.
                if !matches!(left, Included(l) if l == left_found) {
                    let sub_right = Excluded(left_found);
                    debug!(
                        "calling left with {:?} to {:?} (level {})",
                        left, sub_right, deeper_level
                    );
                    Self::explore_facet_number_levels(
                        rtxn,
                        db,
                        field_id,
                        deeper_level,
                        left,
                        sub_right,
                        output,
                    )?;
                }
                if !matches!(right, Included(r) if r == right_found) {
                    let sub_left = Excluded(right_found);
                    debug!(
                        "calling right with {:?} to {:?} (level {})",
                        sub_left, right, deeper_level
                    );
                    Self::explore_facet_number_levels(
                        rtxn,
                        db,
                        field_id,
                        deeper_level,
                        sub_left,
                        right,
                        output,
                    )?;
                }
            }
            None => {
                // If we found nothing at this level it means that we must find
                // the same bounds but at a deeper, more precise level.
                Self::explore_facet_number_levels(
                    rtxn,
                    db,
                    field_id,
                    deeper_level,
                    left,
                    right,
                    output,
                )?;
            }
        }

        Ok(())
    }

    fn evaluate_operator(
        rtxn: &heed::RoTxn,
        index: &Index,
        numbers_db: heed::Database<FacetLevelValueF64Codec, CboRoaringBitmapCodec>,
        strings_db: heed::Database<FacetStringLevelZeroCodec, FacetStringLevelZeroValueCodec>,
        field_id: FieldId,
        operator: &Operator,
    ) -> Result<RoaringBitmap> {
        // Make sure we always bound the ranges with the field id and the level,
        // as the facets values are all in the same database and prefixed by the
        // field id and the level.
        let (left, right) = match operator {
            Operator::GreaterThan(val) => (Excluded(*val), Included(f64::MAX)),
            Operator::GreaterThanOrEqual(val) => (Included(*val), Included(f64::MAX)),
            Operator::Equal(number, string) => {
                let (_original_value, string_docids) =
                    strings_db.get(rtxn, &(field_id, &string))?.unwrap_or_default();
                let number_docids = match number {
                    Some(n) => {
                        let n = Included(*n);
                        let mut output = RoaringBitmap::new();
                        Self::explore_facet_number_levels(
                            rtxn,
                            numbers_db,
                            field_id,
                            0,
                            n,
                            n,
                            &mut output,
                        )?;
                        output
                    }
                    None => RoaringBitmap::new(),
                };
                return Ok(string_docids | number_docids);
            }
            Operator::NotEqual(number, string) => {
                let all_numbers_ids = if number.is_some() {
                    index.number_faceted_documents_ids(rtxn, field_id)?
                } else {
                    RoaringBitmap::new()
                };
                let all_strings_ids = index.string_faceted_documents_ids(rtxn, field_id)?;
                let operator = Operator::Equal(*number, string.clone());
                let docids = Self::evaluate_operator(
                    rtxn, index, numbers_db, strings_db, field_id, &operator,
                )?;
                return Ok((all_numbers_ids | all_strings_ids) - docids);
            }
            Operator::LowerThan(val) => (Included(f64::MIN), Excluded(*val)),
            Operator::LowerThanOrEqual(val) => (Included(f64::MIN), Included(*val)),
            Operator::Between(left, right) => (Included(*left), Included(*right)),
            Operator::GeoLowerThan(base_point, distance) => {
                let rtree = match index.geo_rtree(rtxn)? {
                    Some(rtree) => rtree,
                    None => return Ok(RoaringBitmap::new()),
                };

                let result = rtree
                    .nearest_neighbor_iter(base_point)
                    .take_while(|point| {
                        distance_between_two_points(base_point, point.geom()) < *distance
                    })
                    .map(|point| point.data)
                    .collect();

                return Ok(result);
            }
            Operator::GeoGreaterThan(point, distance) => {
                let result = Self::evaluate_operator(
                    rtxn,
                    index,
                    numbers_db,
                    strings_db,
                    field_id,
                    &Operator::GeoLowerThan(point.clone(), *distance),
                )?;
                let geo_faceted_doc_ids = index.geo_faceted_documents_ids(rtxn)?;
                return Ok(geo_faceted_doc_ids - result);
            }
        };

        // Ask for the biggest value that can exist for this specific field, if it exists
        // that's fine if it don't, the value just before will be returned instead.
        let biggest_level = numbers_db
            .remap_data_type::<DecodeIgnore>()
            .get_lower_than_or_equal_to(rtxn, &(field_id, u8::MAX, f64::MAX, f64::MAX))?
            .and_then(|((id, level, _, _), _)| if id == field_id { Some(level) } else { None });

        match biggest_level {
            Some(level) => {
                let mut output = RoaringBitmap::new();
                Self::explore_facet_number_levels(
                    rtxn,
                    numbers_db,
                    field_id,
                    level,
                    left,
                    right,
                    &mut output,
                )?;
                Ok(output)
            }
            None => Ok(RoaringBitmap::new()),
        }
    }

    pub fn evaluate(&self, rtxn: &heed::RoTxn, index: &Index) -> Result<RoaringBitmap> {
        let numbers_db = index.facet_id_f64_docids;
        let strings_db = index.facet_id_string_docids;

        match self {
            Operator(fid, op) => {
                Self::evaluate_operator(rtxn, index, numbers_db, strings_db, *fid, op)
            }
            Or(lhs, rhs) => {
                let lhs = lhs.evaluate(rtxn, index)?;
                let rhs = rhs.evaluate(rtxn, index)?;
                Ok(lhs | rhs)
            }
            And(lhs, rhs) => {
                let lhs = lhs.evaluate(rtxn, index)?;
                let rhs = rhs.evaluate(rtxn, index)?;
                Ok(lhs & rhs)
            }
            Empty => Ok(RoaringBitmap::new()),
        }
    }
}
