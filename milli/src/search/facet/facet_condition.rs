use std::collections::HashMap;
use std::fmt::Debug;
use std::ops::Bound::{self, Included, Excluded};
use std::str::FromStr;

use anyhow::Context;
use either::Either;
use heed::types::DecodeIgnore;
use log::debug;
use pest::error::{Error as PestError, ErrorVariant};
use pest::iterators::{Pair, Pairs};
use pest::Parser;
use roaring::RoaringBitmap;

use crate::facet::FacetType;
use crate::heed_codec::facet::{FacetValueStringCodec, FacetLevelValueF64Codec};
use crate::{Index, FieldId, FieldsIdsMap, CboRoaringBitmapCodec};

use super::FacetRange;
use super::parser::Rule;
use super::parser::{PREC_CLIMBER, FilterParser};

use self::FacetCondition::*;
use self::FacetNumberOperator::*;

#[derive(Debug, Copy, Clone, PartialEq)]
pub enum FacetNumberOperator {
    GreaterThan(f64),
    GreaterThanOrEqual(f64),
    Equal(f64),
    NotEqual(f64),
    LowerThan(f64),
    LowerThanOrEqual(f64),
    Between(f64, f64),
}

impl FacetNumberOperator {
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
    OperatorString(FieldId, FacetStringOperator),
    OperatorNumber(FieldId, FacetNumberOperator),
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
    pub fn from_array<I, J, A, B>(
        rtxn: &heed::RoTxn,
        index: &Index,
        array: I,
    ) -> anyhow::Result<Option<FacetCondition>>
    where I: IntoIterator<Item=Either<J, B>>,
          J: IntoIterator<Item=A>,
          A: AsRef<str>,
          B: AsRef<str>,
    {
        fn facet_condition(
            fields_ids_map: &FieldsIdsMap,
            faceted_fields: &HashMap<String, FacetType>,
            key: &str,
            value: &str,
        ) -> anyhow::Result<FacetCondition>
        {
            let fid = fields_ids_map.id(key).with_context(|| {
                format!("{:?} isn't present in the fields ids map", key)
            })?;
            let ftype = faceted_fields.get(key).copied().with_context(|| {
                format!("{:?} isn't a faceted field", key)
            })?;
            let (neg, value) = match value.trim().strip_prefix('-') {
                Some(value) => (true, value.trim()),
                None => (false, value.trim()),
            };

            let operator = match ftype {
                FacetType::String => OperatorString(fid, FacetStringOperator::equal(value)),
                FacetType::Number => OperatorNumber(fid, FacetNumberOperator::Equal(value.parse()?)),
            };

            if neg { Ok(operator.negate()) } else { Ok(operator) }
        }

        let fields_ids_map = index.fields_ids_map(rtxn)?;
        let faceted_fields = index.faceted_fields(rtxn)?;
        let mut ands = None;

        for either in array {
            match either {
                Either::Left(array) => {
                    let mut ors = None;
                    for rule in array {
                        let mut iter = rule.as_ref().splitn(2, ':');
                        let key = iter.next().context("missing facet condition key")?;
                        let value = iter.next().context("missing facet condition value")?;
                        let condition = facet_condition(&fields_ids_map, &faceted_fields, key, value)?;
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
                },
                Either::Right(rule) => {
                    let mut iter = rule.as_ref().splitn(2, ':');
                    let key = iter.next().context("missing facet condition key")?;
                    let value = iter.next().context("missing facet condition value")?;
                    let condition = facet_condition(&fields_ids_map, &faceted_fields, key, value)?;
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
    ) -> anyhow::Result<FacetCondition>
    {
        let fields_ids_map = index.fields_ids_map(rtxn)?;
        let faceted_fields = index.faceted_fields_ids(rtxn)?;
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
            OperatorString(fid, op) => OperatorString(fid, op.negate()),
            OperatorNumber(fid, op) => match op.negate() {
                (op, None) => OperatorNumber(fid, op),
                (a, Some(b)) => Or(Box::new(OperatorNumber(fid, a)), Box::new(OperatorNumber(fid, b))),
            },
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
            FacetType::String => {
                Err(PestError::<Rule>::new_from_span(
                    ErrorVariant::CustomError {
                        message: "invalid operator on a faceted string".to_string(),
                    },
                    item_span,
                ).into())
            },
            FacetType::Number => {
                let lvalue = pest_parse(lvalue)?;
                let rvalue = pest_parse(rvalue)?;
                Ok(OperatorNumber(fid, Between(lvalue, rvalue)))
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
            FacetType::String => Ok(OperatorString(fid, FacetStringOperator::equal(value.as_str()))),
            FacetType::Number => Ok(OperatorNumber(fid, Equal(pest_parse(value)?))),
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
            FacetType::String => {
                Err(PestError::<Rule>::new_from_span(
                    ErrorVariant::CustomError {
                        message: "invalid operator on a faceted string".to_string(),
                    },
                    item_span,
                ).into())
            },
            FacetType::Number => Ok(OperatorNumber(fid, GreaterThan(pest_parse(value)?))),
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
            FacetType::String => {
                Err(PestError::<Rule>::new_from_span(
                    ErrorVariant::CustomError {
                        message: "invalid operator on a faceted string".to_string(),
                    },
                    item_span,
                ).into())
            },
            FacetType::Number => Ok(OperatorNumber(fid, GreaterThanOrEqual(pest_parse(value)?))),
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
            FacetType::String => {
                Err(PestError::<Rule>::new_from_span(
                    ErrorVariant::CustomError {
                        message: "invalid operator on a faceted string".to_string(),
                    },
                    item_span,
                ).into())
            },
            FacetType::Number => Ok(OperatorNumber(fid, LowerThan(pest_parse(value)?))),
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
            FacetType::String => {
                Err(PestError::<Rule>::new_from_span(
                    ErrorVariant::CustomError {
                        message: "invalid operator on a faceted string".to_string(),
                    },
                    item_span,
                ).into())
            },
            FacetType::Number => Ok(OperatorNumber(fid, LowerThanOrEqual(pest_parse(value)?))),
        }
    }
}

impl FacetCondition {
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
    ) -> anyhow::Result<()>
    {
        match (left, right) {
            // If the request is an exact value we must go directly to the deepest level.
            (Included(l), Included(r)) if l == r && level > 0 => {
                return Self::explore_facet_number_levels(rtxn, db, field_id, 0, left, right, output);
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
        let iter = FacetRange::new(rtxn, db, field_id, level, left, right)?;

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
                    Self::explore_facet_number_levels(rtxn, db, field_id, deeper_level, left, sub_right, output)?;
                }
                if !matches!(right, Included(r) if r == right_found) {
                    let sub_left = Excluded(right_found);
                    debug!("calling right with {:?} to {:?} (level {})", sub_left, right, deeper_level);
                    Self::explore_facet_number_levels(rtxn, db, field_id, deeper_level, sub_left, right, output)?;
                }
            },
            None => {
                // If we found nothing at this level it means that we must find
                // the same bounds but at a deeper, more precise level.
                Self::explore_facet_number_levels(rtxn, db, field_id, deeper_level, left, right, output)?;
            },
        }

        Ok(())
    }

    fn evaluate_number_operator<>(
        rtxn: &heed::RoTxn,
        index: &Index,
        db: heed::Database<FacetLevelValueF64Codec, CboRoaringBitmapCodec>,
        field_id: FieldId,
        operator: FacetNumberOperator,
    ) -> anyhow::Result<RoaringBitmap>
    {
        // Make sure we always bound the ranges with the field id and the level,
        // as the facets values are all in the same database and prefixed by the
        // field id and the level.
        let (left, right) = match operator {
            GreaterThan(val)        => (Excluded(val),      Included(f64::MAX)),
            GreaterThanOrEqual(val) => (Included(val),      Included(f64::MAX)),
            Equal(val)              => (Included(val),      Included(val)),
            NotEqual(val)           => {
                let all_documents_ids = index.faceted_documents_ids(rtxn, field_id)?;
                let docids = Self::evaluate_number_operator(rtxn, index, db, field_id, Equal(val))?;
                return Ok(all_documents_ids - docids);
            },
            LowerThan(val)          => (Included(f64::MIN), Excluded(val)),
            LowerThanOrEqual(val)   => (Included(f64::MIN), Included(val)),
            Between(left, right)    => (Included(left),     Included(right)),
        };

        // Ask for the biggest value that can exist for this specific field, if it exists
        // that's fine if it don't, the value just before will be returned instead.
        let biggest_level = db
            .remap_data_type::<DecodeIgnore>()
            .get_lower_than_or_equal_to(rtxn, &(field_id, u8::MAX, f64::MAX, f64::MAX))?
            .and_then(|((id, level, _, _), _)| if id == field_id { Some(level) } else { None });

        match biggest_level {
            Some(level) => {
                let mut output = RoaringBitmap::new();
                Self::explore_facet_number_levels(rtxn, db, field_id, level, left, right, &mut output)?;
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
            OperatorString(fid, op) => {
                let db = db.remap_key_type::<FacetValueStringCodec>();
                Self::evaluate_string_operator(rtxn, index, db, *fid, op)
            },
            OperatorNumber(fid, op) => {
                let db = db.remap_key_type::<FacetLevelValueF64Codec>();
                Self::evaluate_number_operator(rtxn, index, db, *fid, *op)
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
        let mut builder = Settings::new(&mut wtxn, &index, 0);
        builder.set_faceted_fields(hashmap!{ "channel".into() => "string".into() });
        builder.execute(|_, _| ()).unwrap();
        wtxn.commit().unwrap();

        // Test that the facet condition is correctly generated.
        let rtxn = index.read_txn().unwrap();
        let condition = FacetCondition::from_str(&rtxn, &index, "channel = ponce").unwrap();
        let expected = OperatorString(0, FacetStringOperator::equal("Ponce"));
        assert_eq!(condition, expected);

        let condition = FacetCondition::from_str(&rtxn, &index, "channel != ponce").unwrap();
        let expected = OperatorString(0, FacetStringOperator::not_equal("ponce"));
        assert_eq!(condition, expected);

        let condition = FacetCondition::from_str(&rtxn, &index, "NOT channel = ponce").unwrap();
        let expected = OperatorString(0, FacetStringOperator::not_equal("ponce"));
        assert_eq!(condition, expected);
    }

    #[test]
    fn number() {
        let path = tempfile::tempdir().unwrap();
        let mut options = EnvOpenOptions::new();
        options.map_size(10 * 1024 * 1024); // 10 MB
        let index = Index::new(options, &path).unwrap();

        // Set the faceted fields to be the channel.
        let mut wtxn = index.write_txn().unwrap();
        let mut builder = Settings::new(&mut wtxn, &index, 0);
        builder.set_faceted_fields(hashmap!{ "timestamp".into() => "number".into() });
        builder.execute(|_, _| ()).unwrap();
        wtxn.commit().unwrap();

        // Test that the facet condition is correctly generated.
        let rtxn = index.read_txn().unwrap();
        let condition = FacetCondition::from_str(&rtxn, &index, "timestamp 22 TO 44").unwrap();
        let expected = OperatorNumber(0, Between(22.0, 44.0));
        assert_eq!(condition, expected);

        let condition = FacetCondition::from_str(&rtxn, &index, "NOT timestamp 22 TO 44").unwrap();
        let expected = Or(
            Box::new(OperatorNumber(0, LowerThan(22.0))),
            Box::new(OperatorNumber(0, GreaterThan(44.0))),
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
        let mut builder = Settings::new(&mut wtxn, &index, 0);
        builder.set_searchable_fields(vec!["channel".into(), "timestamp".into()]); // to keep the fields order
        builder.set_faceted_fields(hashmap!{
            "channel".into() => "string".into(),
            "timestamp".into() => "number".into(),
        });
        builder.execute(|_, _| ()).unwrap();
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
                Box::new(OperatorNumber(1, Between(22.0, 44.0))),
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
                    Box::new(OperatorNumber(1, LowerThan(22.0))),
                    Box::new(OperatorNumber(1, GreaterThan(44.0))),
                )),
                Box::new(OperatorString(0, FacetStringOperator::equal("ponce"))),
            )),
        );
        assert_eq!(condition, expected);
    }

    #[test]
    fn from_array() {
        let path = tempfile::tempdir().unwrap();
        let mut options = EnvOpenOptions::new();
        options.map_size(10 * 1024 * 1024); // 10 MB
        let index = Index::new(options, &path).unwrap();

        // Set the faceted fields to be the channel.
        let mut wtxn = index.write_txn().unwrap();
        let mut builder = Settings::new(&mut wtxn, &index, 0);
        builder.set_searchable_fields(vec!["channel".into(), "timestamp".into()]); // to keep the fields order
        builder.set_faceted_fields(hashmap!{
            "channel".into() => "string".into(),
            "timestamp".into() => "number".into(),
        });
        builder.execute(|_, _| ()).unwrap();
        wtxn.commit().unwrap();

        // Test that the facet condition is correctly generated.
        let rtxn = index.read_txn().unwrap();
        let condition = FacetCondition::from_array(
            &rtxn, &index,
            vec![Either::Right("channel:gotaga"), Either::Left(vec!["timestamp:44", "channel:-ponce"])],
        ).unwrap().unwrap();
        let expected = FacetCondition::from_str(
            &rtxn, &index,
            "channel = gotaga AND (timestamp = 44 OR channel != ponce)",
        ).unwrap();
        assert_eq!(condition, expected);
    }
}
