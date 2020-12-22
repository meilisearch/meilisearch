use std::collections::HashMap;
use std::fmt::Debug;
use std::ops::Bound::{self, Included, Excluded};
use std::str::FromStr;

use heed::types::{ByteSlice, DecodeIgnore};
use log::debug;
use num_traits::Bounded;
use pest::error::{Error as PestError, ErrorVariant};
use pest::iterators::{Pair, Pairs};
use pest::Parser;
use roaring::RoaringBitmap;

use crate::facet::FacetType;
use crate::heed_codec::facet::FacetValueStringCodec;
use crate::heed_codec::facet::{FacetLevelValueI64Codec, FacetLevelValueF64Codec};
use crate::{Index, FieldId, FieldsIdsMap, CboRoaringBitmapCodec};

use super::FacetRange;
use super::parser::Rule;
use super::parser::{PREC_CLIMBER, FilterParser};

use self::FacetCondition::*;
use self::FacetNumberOperator::*;

#[derive(Debug, Copy, Clone, PartialEq)]
pub enum FacetNumberOperator<T> {
    GreaterThan(T),
    GreaterThanOrEqual(T),
    Equal(T),
    NotEqual(T),
    LowerThan(T),
    LowerThanOrEqual(T),
    Between(T, T),
}

impl<T> FacetNumberOperator<T> {
    /// This method can return two operations in case it must express
    /// an OR operation for the between case (i.e. `TO`).
    fn negate(self) -> (Self, Option<Self>) {
        match self {
            GreaterThan(x)        => (LowerThanOrEqual(x), None),
            GreaterThanOrEqual(x) => (LowerThan(x), None),
            Equal(x)              => (NotEqual(x), None),
            NotEqual(x)           => (Equal(x), None),
            LowerThan(x)          => (GreaterThanOrEqual(x), None),
            LowerThanOrEqual(x)   => (GreaterThan(x), None),
            Between(x, y)         => (LowerThan(x), Some(GreaterThan(y))),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum FacetStringOperator {
    Equal(String),
    NotEqual(String),
}

impl FacetStringOperator {
    fn equal(s: &str) -> Self {
        FacetStringOperator::Equal(s.to_lowercase())
    }

    #[allow(dead_code)]
    fn not_equal(s: &str) -> Self {
        FacetStringOperator::equal(s).negate()
    }

    fn negate(self) -> Self {
        match self {
            FacetStringOperator::Equal(x)    => FacetStringOperator::NotEqual(x),
            FacetStringOperator::NotEqual(x) => FacetStringOperator::Equal(x),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum FacetCondition {
    OperatorI64(FieldId, FacetNumberOperator<i64>),
    OperatorF64(FieldId, FacetNumberOperator<f64>),
    OperatorString(FieldId, FacetStringOperator),
    Or(Box<Self>, Box<Self>),
    And(Box<Self>, Box<Self>),
}

fn get_field_id_facet_type<'a>(
    fields_ids_map: &FieldsIdsMap,
    faceted_fields: &HashMap<FieldId, FacetType>,
    items: &mut Pairs<'a, Rule>,
) -> Result<(FieldId, FacetType), PestError<Rule>>
{
    // lexing ensures that we at least have a key
    let key = items.next().unwrap();
    let field_id = fields_ids_map
        .id(key.as_str())
        .ok_or_else(|| {
            PestError::new_from_span(
                ErrorVariant::CustomError {
                    message: format!(
                        "attribute `{}` not found, available attributes are: {}",
                        key.as_str(),
                        fields_ids_map.iter().map(|(_, n)| n).collect::<Vec<_>>().join(", ")
                    ),
                },
                key.as_span(),
            )
        })?;

    let facet_type = faceted_fields
        .get(&field_id)
        .copied()
        .ok_or_else(|| {
            PestError::new_from_span(
                ErrorVariant::CustomError {
                    message: format!(
                        "attribute `{}` is not faceted, available faceted attributes are: {}",
                        key.as_str(),
                        faceted_fields.keys().flat_map(|id| fields_ids_map.name(*id)).collect::<Vec<_>>().join(", ")
                    ),
                },
                key.as_span(),
            )
        })?;

    Ok((field_id, facet_type))
}

fn pest_parse<T>(pair: Pair<Rule>) -> Result<T, pest::error::Error<Rule>>
where T: FromStr,
      T::Err: ToString,
{
    match pair.as_str().parse() {
        Ok(value) => Ok(value),
        Err(e) => {
            Err(PestError::<Rule>::new_from_span(
                ErrorVariant::CustomError { message: e.to_string() },
                pair.as_span(),
            ))
        }
    }
}

impl FacetCondition {
    pub fn from_str(
        rtxn: &heed::RoTxn,
        index: &Index,
        expression: &str,
    ) -> anyhow::Result<FacetCondition>
    {
        let fields_ids_map = index.fields_ids_map(rtxn)?;
        let faceted_fields = index.faceted_fields(rtxn)?;
        let lexed = FilterParser::parse(Rule::prgm, expression)?;
        FacetCondition::from_pairs(&fields_ids_map, &faceted_fields, lexed)
    }

    fn from_pairs(
        fim: &FieldsIdsMap,
        ff: &HashMap<FieldId, FacetType>,
        expression: Pairs<Rule>,
    ) -> anyhow::Result<Self>
    {
        PREC_CLIMBER.climb(
            expression,
            |pair: Pair<Rule>| match pair.as_rule() {
                Rule::greater => Ok(Self::greater_than(fim, ff, pair)?),
                Rule::geq => Ok(Self::greater_than_or_equal(fim, ff, pair)?),
                Rule::eq => Ok(Self::equal(fim, ff, pair)?),
                Rule::neq => Ok(Self::equal(fim, ff, pair)?.negate()),
                Rule::leq => Ok(Self::lower_than_or_equal(fim, ff, pair)?),
                Rule::less => Ok(Self::lower_than(fim, ff, pair)?),
                Rule::between => Ok(Self::between(fim, ff, pair)?),
                Rule::not => Ok(Self::from_pairs(fim, ff, pair.into_inner())?.negate()),
                Rule::prgm => Self::from_pairs(fim, ff, pair.into_inner()),
                Rule::term => Self::from_pairs(fim, ff, pair.into_inner()),
                _ => unreachable!(),
            },
            |lhs: anyhow::Result<Self>, op: Pair<Rule>, rhs: anyhow::Result<Self>| {
                match op.as_rule() {
                    Rule::or => Ok(Or(Box::new(lhs?), Box::new(rhs?))),
                    Rule::and => Ok(And(Box::new(lhs?), Box::new(rhs?))),
                    _ => unreachable!(),
                }
            },
        )
    }

    fn negate(self) -> FacetCondition {
        match self {
            OperatorI64(fid, op) => match op.negate() {
                (op, None) => OperatorI64(fid, op),
                (a, Some(b)) => Or(Box::new(OperatorI64(fid, a)), Box::new(OperatorI64(fid, b))),
            },
            OperatorF64(fid, op) => match op.negate() {
                (op, None) => OperatorF64(fid, op),
                (a, Some(b)) => Or(Box::new(OperatorF64(fid, a)), Box::new(OperatorF64(fid, b))),
            },
            OperatorString(fid, op) => OperatorString(fid, op.negate()),
            Or(a, b) => And(Box::new(a.negate()), Box::new(b.negate())),
            And(a, b) => Or(Box::new(a.negate()), Box::new(b.negate())),
        }
    }

    fn between(
        fields_ids_map: &FieldsIdsMap,
        faceted_fields: &HashMap<FieldId, FacetType>,
        item: Pair<Rule>,
    ) -> anyhow::Result<FacetCondition>
    {
        let item_span = item.as_span();
        let mut items = item.into_inner();
        let (fid, ftype) = get_field_id_facet_type(fields_ids_map, faceted_fields, &mut items)?;
        let lvalue = items.next().unwrap();
        let rvalue = items.next().unwrap();
        match ftype {
            FacetType::Integer => {
                let lvalue = pest_parse(lvalue)?;
                let rvalue = pest_parse(rvalue)?;
                Ok(OperatorI64(fid, Between(lvalue, rvalue)))
            },
            FacetType::Float => {
                let lvalue = pest_parse(lvalue)?;
                let rvalue = pest_parse(rvalue)?;
                Ok(OperatorF64(fid, Between(lvalue, rvalue)))
            },
            FacetType::String => {
                Err(PestError::<Rule>::new_from_span(
                    ErrorVariant::CustomError {
                        message: "invalid operator on a faceted string".to_string(),
                    },
                    item_span,
                ).into())
            },
        }
    }

    fn equal(
        fields_ids_map: &FieldsIdsMap,
        faceted_fields: &HashMap<FieldId, FacetType>,
        item: Pair<Rule>,
    ) -> anyhow::Result<FacetCondition>
    {
        let mut items = item.into_inner();
        let (fid, ftype) = get_field_id_facet_type(fields_ids_map, faceted_fields, &mut items)?;
        let value = items.next().unwrap();
        match ftype {
            FacetType::Integer => Ok(OperatorI64(fid, Equal(pest_parse(value)?))),
            FacetType::Float => Ok(OperatorF64(fid, Equal(pest_parse(value)?))),
            FacetType::String => Ok(OperatorString(fid, FacetStringOperator::equal(value.as_str()))),
        }
    }

    fn greater_than(
        fields_ids_map: &FieldsIdsMap,
        faceted_fields: &HashMap<FieldId, FacetType>,
        item: Pair<Rule>,
    ) -> anyhow::Result<FacetCondition>
    {
        let item_span = item.as_span();
        let mut items = item.into_inner();
        let (fid, ftype) = get_field_id_facet_type(fields_ids_map, faceted_fields, &mut items)?;
        let value = items.next().unwrap();
        match ftype {
            FacetType::Integer => Ok(OperatorI64(fid, GreaterThan(pest_parse(value)?))),
            FacetType::Float => Ok(OperatorF64(fid, GreaterThan(pest_parse(value)?))),
            FacetType::String => {
                Err(PestError::<Rule>::new_from_span(
                    ErrorVariant::CustomError {
                        message: "invalid operator on a faceted string".to_string(),
                    },
                    item_span,
                ).into())
            },
        }
    }

    fn greater_than_or_equal(
        fields_ids_map: &FieldsIdsMap,
        faceted_fields: &HashMap<FieldId, FacetType>,
        item: Pair<Rule>,
    ) -> anyhow::Result<FacetCondition>
    {
        let item_span = item.as_span();
        let mut items = item.into_inner();
        let (fid, ftype) = get_field_id_facet_type(fields_ids_map, faceted_fields, &mut items)?;
        let value = items.next().unwrap();
        match ftype {
            FacetType::Integer => Ok(OperatorI64(fid, GreaterThanOrEqual(pest_parse(value)?))),
            FacetType::Float => Ok(OperatorF64(fid, GreaterThanOrEqual(pest_parse(value)?))),
            FacetType::String => {
                Err(PestError::<Rule>::new_from_span(
                    ErrorVariant::CustomError {
                        message: "invalid operator on a faceted string".to_string(),
                    },
                    item_span,
                ).into())
            },
        }
    }

    fn lower_than(
        fields_ids_map: &FieldsIdsMap,
        faceted_fields: &HashMap<FieldId, FacetType>,
        item: Pair<Rule>,
    ) -> anyhow::Result<FacetCondition>
    {
        let item_span = item.as_span();
        let mut items = item.into_inner();
        let (fid, ftype) = get_field_id_facet_type(fields_ids_map, faceted_fields, &mut items)?;
        let value = items.next().unwrap();
        match ftype {
            FacetType::Integer => Ok(OperatorI64(fid, LowerThan(pest_parse(value)?))),
            FacetType::Float => Ok(OperatorF64(fid, LowerThan(pest_parse(value)?))),
            FacetType::String => {
                Err(PestError::<Rule>::new_from_span(
                    ErrorVariant::CustomError {
                        message: "invalid operator on a faceted string".to_string(),
                    },
                    item_span,
                ).into())
            },
        }
    }

    fn lower_than_or_equal(
        fields_ids_map: &FieldsIdsMap,
        faceted_fields: &HashMap<FieldId, FacetType>,
        item: Pair<Rule>,
    ) -> anyhow::Result<FacetCondition>
    {
        let item_span = item.as_span();
        let mut items = item.into_inner();
        let (fid, ftype) = get_field_id_facet_type(fields_ids_map, faceted_fields, &mut items)?;
        let value = items.next().unwrap();
        match ftype {
            FacetType::Integer => Ok(OperatorI64(fid, LowerThanOrEqual(pest_parse(value)?))),
            FacetType::Float => Ok(OperatorF64(fid, LowerThanOrEqual(pest_parse(value)?))),
            FacetType::String => {
                Err(PestError::<Rule>::new_from_span(
                    ErrorVariant::CustomError {
                        message: "invalid operator on a faceted string".to_string(),
                    },
                    item_span,
                ).into())
            },
        }
    }
}

impl FacetCondition {
    /// Aggregates the documents ids that are part of the specified range automatically
    /// going deeper through the levels.
    fn explore_facet_levels<'t, T: 't, KC>(
        rtxn: &'t heed::RoTxn,
        db: heed::Database<ByteSlice, CboRoaringBitmapCodec>,
        field_id: FieldId,
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
        let iter = FacetRange::new(rtxn, db.remap_key_type::<KC>(), field_id, level, left, right)?;

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

    fn evaluate_number_operator<'t, T: 't, KC>(
        rtxn: &'t heed::RoTxn,
        index: &Index,
        db: heed::Database<ByteSlice, CboRoaringBitmapCodec>,
        field_id: FieldId,
        operator: FacetNumberOperator<T>,
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
            Equal(val)              => (Included(val),            Included(val)),
            NotEqual(val)           => {
                let all_documents_ids = index.faceted_documents_ids(rtxn, field_id)?;
                let docids = Self::evaluate_number_operator::<T, KC>(rtxn, index, db, field_id, Equal(val))?;
                return Ok(all_documents_ids - docids);
            },
            LowerThan(val)          => (Included(T::min_value()), Excluded(val)),
            LowerThanOrEqual(val)   => (Included(T::min_value()), Included(val)),
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

    fn evaluate_string_operator(
        rtxn: &heed::RoTxn,
        index: &Index,
        db: heed::Database<FacetValueStringCodec, CboRoaringBitmapCodec>,
        field_id: FieldId,
        operator: &FacetStringOperator,
    ) -> anyhow::Result<RoaringBitmap>
    {
        match operator {
            FacetStringOperator::Equal(string) => {
                match db.get(rtxn, &(field_id, string))? {
                    Some(docids) => Ok(docids),
                    None => Ok(RoaringBitmap::new())
                }
            },
            FacetStringOperator::NotEqual(string) => {
                let all_documents_ids = index.faceted_documents_ids(rtxn, field_id)?;
                let op = FacetStringOperator::Equal(string.clone());
                let docids = Self::evaluate_string_operator(rtxn, index, db, field_id, &op)?;
                Ok(all_documents_ids - docids)
            },
        }
    }

    pub fn evaluate(
        &self,
        rtxn: &heed::RoTxn,
        index: &Index,
    ) -> anyhow::Result<RoaringBitmap>
    {
        let db = index.facet_field_id_value_docids;
        match self {
            OperatorI64(fid, op) => {
                Self::evaluate_number_operator::<i64, FacetLevelValueI64Codec>(rtxn, index, db, *fid, *op)
            },
            OperatorF64(fid, op) => {
                Self::evaluate_number_operator::<f64, FacetLevelValueF64Codec>(rtxn, index, db, *fid, *op)
            },
            OperatorString(fid, op) => {
                let db = db.remap_key_type::<FacetValueStringCodec>();
                Self::evaluate_string_operator(rtxn, index, db, *fid, op)
            },
            Or(lhs, rhs) => {
                let lhs = lhs.evaluate(rtxn, index)?;
                let rhs = rhs.evaluate(rtxn, index)?;
                Ok(lhs | rhs)
            },
            And(lhs, rhs) => {
                let lhs = lhs.evaluate(rtxn, index)?;
                let rhs = rhs.evaluate(rtxn, index)?;
                Ok(lhs & rhs)
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::update::Settings;
    use heed::EnvOpenOptions;
    use maplit::hashmap;

    #[test]
    fn string() {
        let path = tempfile::tempdir().unwrap();
        let mut options = EnvOpenOptions::new();
        options.map_size(10 * 1024 * 1024); // 10 MB
        let index = Index::new(options, &path).unwrap();

        // Set the faceted fields to be the channel.
        let mut wtxn = index.write_txn().unwrap();
        let mut builder = Settings::new(&mut wtxn, &index);
        builder.set_faceted_fields(hashmap!{ "channel".into() => "string".into() });
        builder.execute(|_| ()).unwrap();
        wtxn.commit().unwrap();

        // Test that the facet condition is correctly generated.
        let rtxn = index.read_txn().unwrap();
        let condition = FacetCondition::from_str(&rtxn, &index, "channel = ponce").unwrap();
        let expected = OperatorString(1, FacetStringOperator::equal("Ponce"));
        assert_eq!(condition, expected);

        let condition = FacetCondition::from_str(&rtxn, &index, "channel != ponce").unwrap();
        let expected = OperatorString(1, FacetStringOperator::not_equal("ponce"));
        assert_eq!(condition, expected);

        let condition = FacetCondition::from_str(&rtxn, &index, "NOT channel = ponce").unwrap();
        let expected = OperatorString(1, FacetStringOperator::not_equal("ponce"));
        assert_eq!(condition, expected);
    }

    #[test]
    fn i64() {
        let path = tempfile::tempdir().unwrap();
        let mut options = EnvOpenOptions::new();
        options.map_size(10 * 1024 * 1024); // 10 MB
        let index = Index::new(options, &path).unwrap();

        // Set the faceted fields to be the channel.
        let mut wtxn = index.write_txn().unwrap();
        let mut builder = Settings::new(&mut wtxn, &index);
        builder.set_faceted_fields(hashmap!{ "timestamp".into() => "integer".into() });
        builder.execute(|_| ()).unwrap();
        wtxn.commit().unwrap();

        // Test that the facet condition is correctly generated.
        let rtxn = index.read_txn().unwrap();
        let condition = FacetCondition::from_str(&rtxn, &index, "timestamp 22 TO 44").unwrap();
        let expected = OperatorI64(1, Between(22, 44));
        assert_eq!(condition, expected);

        let condition = FacetCondition::from_str(&rtxn, &index, "NOT timestamp 22 TO 44").unwrap();
        let expected = Or(
            Box::new(OperatorI64(1, LowerThan(22))),
            Box::new(OperatorI64(1, GreaterThan(44))),
        );
        assert_eq!(condition, expected);
    }

    #[test]
    fn parentheses() {
        let path = tempfile::tempdir().unwrap();
        let mut options = EnvOpenOptions::new();
        options.map_size(10 * 1024 * 1024); // 10 MB
        let index = Index::new(options, &path).unwrap();

        // Set the faceted fields to be the channel.
        let mut wtxn = index.write_txn().unwrap();
        let mut builder = Settings::new(&mut wtxn, &index);
        builder.set_searchable_fields(vec!["channel".into(), "timestamp".into()]); // to keep the fields order
        builder.set_faceted_fields(hashmap!{
            "channel".into() => "string".into(),
            "timestamp".into() => "integer".into(),
        });
        builder.execute(|_| ()).unwrap();
        wtxn.commit().unwrap();

        // Test that the facet condition is correctly generated.
        let rtxn = index.read_txn().unwrap();
        let condition = FacetCondition::from_str(
            &rtxn, &index,
            "channel = gotaga OR (timestamp 22 TO 44 AND channel != ponce)",
        ).unwrap();
        let expected = Or(
            Box::new(OperatorString(0, FacetStringOperator::equal("gotaga"))),
            Box::new(And(
                Box::new(OperatorI64(1, Between(22, 44))),
                Box::new(OperatorString(0, FacetStringOperator::not_equal("ponce"))),
            ))
        );
        assert_eq!(condition, expected);

        let condition = FacetCondition::from_str(
            &rtxn, &index,
            "channel = gotaga OR NOT (timestamp 22 TO 44 AND channel != ponce)",
        ).unwrap();
        let expected = Or(
            Box::new(OperatorString(0, FacetStringOperator::equal("gotaga"))),
            Box::new(Or(
                Box::new(Or(
                    Box::new(OperatorI64(1, LowerThan(22))),
                    Box::new(OperatorI64(1, GreaterThan(44))),
                )),
                Box::new(OperatorString(0, FacetStringOperator::equal("ponce"))),
            )),
        );
        assert_eq!(condition, expected);
    }
}
