use std::error::Error as StdError;
use std::fmt::Debug;
use std::ops::Bound::{self, Unbounded, Included, Excluded};
use std::str::FromStr;

use anyhow::{bail, ensure, Context};
use heed::types::{ByteSlice, DecodeIgnore};
use log::debug;
use num_traits::Bounded;
use roaring::RoaringBitmap;

use crate::facet::FacetType;
use crate::heed_codec::facet::{FacetLevelValueI64Codec, FacetLevelValueF64Codec};
use crate::{Index, CboRoaringBitmapCodec};

use self::FacetCondition::*;
use self::FacetOperator::*;

#[derive(Debug, Copy, Clone, PartialEq)]
pub enum FacetOperator<T> {
    GreaterThan(T),
    GreaterThanOrEqual(T),
    LowerThan(T),
    LowerThanOrEqual(T),
    Equal(T),
    Between(T, T),
}

// TODO also support ANDs, ORs, NOTs.
#[derive(Debug, Copy, Clone, PartialEq)]
pub enum FacetCondition {
    OperatorI64(u8, FacetOperator<i64>),
    OperatorF64(u8, FacetOperator<f64>),
}

impl FacetCondition {
    pub fn from_str(
        rtxn: &heed::RoTxn,
        index: &Index,
        string: &str,
    ) -> anyhow::Result<Option<FacetCondition>>
    {
        let fields_ids_map = index.fields_ids_map(rtxn)?;
        let faceted_fields = index.faceted_fields(rtxn)?;

        // TODO use a better parsing technic
        let mut iter = string.split_whitespace();

        let field_name = match iter.next() {
            Some(field_name) => field_name,
            None => return Ok(None),
        };

        let field_id = fields_ids_map.id(&field_name).with_context(|| format!("field {} not found", field_name))?;
        let field_type = faceted_fields.get(&field_id).with_context(|| format!("field {} is not faceted", field_name))?;

        match field_type {
            FacetType::Integer => Self::parse_condition(iter).map(|op| Some(OperatorI64(field_id, op))),
            FacetType::Float => Self::parse_condition(iter).map(|op| Some(OperatorF64(field_id, op))),
            FacetType::String => bail!("invalid facet type"),
        }
    }

    fn parse_condition<'a, T: FromStr>(
        mut iter: impl Iterator<Item=&'a str>,
    ) -> anyhow::Result<FacetOperator<T>>
    where T::Err: Send + Sync + StdError + 'static,
    {
        match iter.next() {
            Some(">") => {
                let param = iter.next().context("missing parameter")?;
                let value = param.parse().with_context(|| format!("invalid parameter ({:?})", param))?;
                Ok(GreaterThan(value))
            },
            Some(">=") => {
                let param = iter.next().context("missing parameter")?;
                let value = param.parse().with_context(|| format!("invalid parameter ({:?})", param))?;
                Ok(GreaterThanOrEqual(value))
            },
            Some("<") => {
                let param = iter.next().context("missing parameter")?;
                let value = param.parse().with_context(|| format!("invalid parameter ({:?})", param))?;
                Ok(LowerThan(value))
            },
            Some("<=") => {
                let param = iter.next().context("missing parameter")?;
                let value = param.parse().with_context(|| format!("invalid parameter ({:?})", param))?;
                Ok(LowerThanOrEqual(value))
            },
            Some("=") => {
                let param = iter.next().context("missing parameter")?;
                let value = param.parse().with_context(|| format!("invalid parameter ({:?})", param))?;
                Ok(Equal(value))
            },
            Some(otherwise) => {
                // BETWEEN or X TO Y (both inclusive)
                let left_param = otherwise.parse().with_context(|| format!("invalid first TO parameter ({:?})", otherwise))?;
                ensure!(iter.next().map_or(false, |s| s.eq_ignore_ascii_case("to")), "TO keyword missing or invalid");
                let next = iter.next().context("missing second TO parameter")?;
                let right_param = next.parse().with_context(|| format!("invalid second TO parameter ({:?})", next))?;
                Ok(Between(left_param, right_param))
            },
            None => bail!("missing facet filter first parameter"),
        }
    }

    /// Aggregates the documents ids that are part of the specified range automatically
    /// going deeper through the levels.
    fn explore_facet_levels<'t, T: 't, KC>(
        rtxn: &'t heed::RoTxn,
        db: heed::Database<ByteSlice, CboRoaringBitmapCodec>,
        field_id: u8,
        level: u8,
        left: Bound<T>,
        right: Bound<T>,
        output: &mut RoaringBitmap,
    ) -> anyhow::Result<()>
    where
        T: Copy + PartialEq + PartialOrd + Bounded + Debug,
        KC: heed::BytesDecode<'t, DItem = (u8, u8, T, T)>,
        KC: for<'x> heed::BytesEncode<'x, EItem = (u8, u8, T, T)>,
    {
        match (left, right) {
            // If the request is an exact value we must go directly to the deepest level.
            (Included(l), Included(r)) if l == r && level > 0 => {
                return Self::explore_facet_levels::<T, KC>(rtxn, db, field_id, 0, left, right, output);
            },
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
        let left_bound = match left {
            Included(left) => Included((field_id, level, left, T::min_value())),
            Excluded(left) => Excluded((field_id, level, left, T::min_value())),
            Unbounded => Unbounded,
        };
        let right_bound = Included((field_id, level, T::max_value(), T::max_value()));
        // We also make sure that we don't decode the data before we are sure we must return it.
        let iter = db
            .remap_key_type::<KC>()
            .lazily_decode_data()
            .range(rtxn, &(left_bound, right_bound))?
            .take_while(|r| r.as_ref().map_or(true, |((.., r), _)| {
                match right {
                    Included(right) => *r <= right,
                    Excluded(right) => *r < right,
                    Unbounded => true,
                }
            }))
            .map(|r| r.and_then(|(key, lazy)| lazy.decode().map(|data| (key, data))));

        debug!("Iterating between {:?} and {:?} (level {})", left, right, level);

        for (i, result) in iter.enumerate() {
            let ((_fid, level, l, r), docids) = result?;
            debug!("{:?} to {:?} (level {}) found {} documents", l, r, level, docids.len());
            output.union_with(&docids);
            // We save the leftest and rightest bounds we actually found at this level.
            if i == 0 { left_found = Some(l); }
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
                    debug!("calling left with {:?} to {:?} (level {})",  left, sub_right, deeper_level);
                    Self::explore_facet_levels::<T, KC>(rtxn, db, field_id, deeper_level, left, sub_right, output)?;
                }
                if !matches!(right, Included(r) if r == right_found) {
                    let sub_left = Excluded(right_found);
                    debug!("calling right with {:?} to {:?} (level {})", sub_left, right, deeper_level);
                    Self::explore_facet_levels::<T, KC>(rtxn, db, field_id, deeper_level, sub_left, right, output)?;
                }
            },
            None => {
                // If we found nothing at this level it means that we must find
                // the same bounds but at a deeper, more precise level.
                Self::explore_facet_levels::<T, KC>(rtxn, db, field_id, deeper_level, left, right, output)?;
            },
        }

        Ok(())
    }

    fn evaluate_operator<'t, T: 't, KC>(
        rtxn: &'t heed::RoTxn,
        db: heed::Database<ByteSlice, CboRoaringBitmapCodec>,
        field_id: u8,
        operator: FacetOperator<T>,
    ) -> anyhow::Result<RoaringBitmap>
    where
        T: Copy + PartialEq + PartialOrd + Bounded + Debug,
        KC: heed::BytesDecode<'t, DItem = (u8, u8, T, T)>,
        KC: for<'x> heed::BytesEncode<'x, EItem = (u8, u8, T, T)>,
    {
        // Make sure we always bound the ranges with the field id and the level,
        // as the facets values are all in the same database and prefixed by the
        // field id and the level.
        let (left, right) = match operator {
            GreaterThan(val)        => (Excluded(val),            Included(T::max_value())),
            GreaterThanOrEqual(val) => (Included(val),            Included(T::max_value())),
            LowerThan(val)          => (Included(T::min_value()), Excluded(val)),
            LowerThanOrEqual(val)   => (Included(T::min_value()), Included(val)),
            Equal(val)              => (Included(val),            Included(val)),
            Between(left, right)    => (Included(left),           Included(right)),
        };

        // Ask for the biggest value that can exist for this specific field, if it exists
        // that's fine if it don't, the value just before will be returned instead.
        let biggest_level = db
            .remap_types::<KC, DecodeIgnore>()
            .get_lower_than_or_equal_to(rtxn, &(field_id, u8::MAX, T::max_value(), T::max_value()))?
            .and_then(|((id, level, _, _), _)| if id == field_id { Some(level) } else { None });

        match biggest_level {
            Some(level) => {
                let mut output = RoaringBitmap::new();
                Self::explore_facet_levels::<T, KC>(rtxn, db, field_id, level, left, right, &mut output)?;
                Ok(output)
            },
            None => Ok(RoaringBitmap::new()),
        }
    }

    pub fn evaluate(
        &self,
        rtxn: &heed::RoTxn,
        db: heed::Database<ByteSlice, CboRoaringBitmapCodec>,
    ) -> anyhow::Result<RoaringBitmap>
    {
        match *self {
            FacetCondition::OperatorI64(fid, operator) => {
                Self::evaluate_operator::<i64, FacetLevelValueI64Codec>(rtxn, db, fid, operator)
            },
            FacetCondition::OperatorF64(fid, operator) => {
                Self::evaluate_operator::<f64, FacetLevelValueF64Codec>(rtxn, db, fid, operator)
            }
        }
    }
}
