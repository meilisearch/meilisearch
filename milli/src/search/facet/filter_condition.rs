use std::collections::HashSet;
use std::fmt::Debug;
use std::ops::Bound::{self, Excluded, Included};
use std::result::Result as StdResult;
use std::str::FromStr;

use either::Either;
use heed::types::DecodeIgnore;
use log::debug;
use pest::error::{Error as PestError, ErrorVariant};
use pest::iterators::{Pair, Pairs};
use pest::Parser;
use roaring::RoaringBitmap;

use self::FilterCondition::*;
use self::Operator::*;
use super::parser::{FilterParser, Rule, PREC_CLIMBER};
use super::FacetNumberRange;
use crate::error::UserError;
use crate::heed_codec::facet::{FacetLevelValueF64Codec, FacetValueStringCodec};
use crate::{CboRoaringBitmapCodec, FieldId, FieldsIdsMap, Index, Result};

#[derive(Debug, Clone, PartialEq)]
pub enum Operator {
    GreaterThan(f64),
    GreaterThanOrEqual(f64),
    Equal(Option<f64>, String),
    NotEqual(Option<f64>, String),
    LowerThan(f64),
    LowerThanOrEqual(f64),
    Between(f64, f64),
}

impl Operator {
    /// This method can return two operations in case it must express
    /// an OR operation for the between case (i.e. `TO`).
    fn negate(self) -> (Self, Option<Self>) {
        match self {
            GreaterThan(n) => (LowerThanOrEqual(n), None),
            GreaterThanOrEqual(n) => (LowerThan(n), None),
            Equal(n, s) => (NotEqual(n, s), None),
            NotEqual(n, s) => (Equal(n, s), None),
            LowerThan(n) => (GreaterThanOrEqual(n), None),
            LowerThanOrEqual(n) => (GreaterThan(n), None),
            Between(n, m) => (LowerThan(n), Some(GreaterThan(m))),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum FilterCondition {
    Operator(FieldId, Operator),
    Or(Box<Self>, Box<Self>),
    And(Box<Self>, Box<Self>),
}

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
        let mut ands = None;

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
        let filterable_fields = index.filterable_fields_ids(rtxn)?;
        let lexed =
            FilterParser::parse(Rule::prgm, expression).map_err(UserError::InvalidFilter)?;
        FilterCondition::from_pairs(&fields_ids_map, &filterable_fields, lexed)
    }

    fn from_pairs(
        fim: &FieldsIdsMap,
        ff: &HashSet<FieldId>,
        expression: Pairs<Rule>,
    ) -> Result<Self> {
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
            |lhs: Result<Self>, op: Pair<Rule>, rhs: Result<Self>| match op.as_rule() {
                Rule::or => Ok(Or(Box::new(lhs?), Box::new(rhs?))),
                Rule::and => Ok(And(Box::new(lhs?), Box::new(rhs?))),
                _ => unreachable!(),
            },
        )
    }

    fn negate(self) -> FilterCondition {
        match self {
            Operator(fid, op) => match op.negate() {
                (op, None) => Operator(fid, op),
                (a, Some(b)) => Or(Box::new(Operator(fid, a)), Box::new(Operator(fid, b))),
            },
            Or(a, b) => And(Box::new(a.negate()), Box::new(b.negate())),
            And(a, b) => Or(Box::new(a.negate()), Box::new(b.negate())),
        }
    }

    fn between(
        fields_ids_map: &FieldsIdsMap,
        filterable_fields: &HashSet<FieldId>,
        item: Pair<Rule>,
    ) -> Result<FilterCondition> {
        let mut items = item.into_inner();
        let fid = field_id(fields_ids_map, filterable_fields, &mut items)
            .map_err(UserError::InvalidFilterAttribute)?;

        let (lresult, _) = pest_parse(items.next().unwrap());
        let (rresult, _) = pest_parse(items.next().unwrap());

        let lvalue = lresult.map_err(UserError::InvalidFilter)?;
        let rvalue = rresult.map_err(UserError::InvalidFilter)?;

        Ok(Operator(fid, Between(lvalue, rvalue)))
    }

    fn equal(
        fields_ids_map: &FieldsIdsMap,
        filterable_fields: &HashSet<FieldId>,
        item: Pair<Rule>,
    ) -> Result<FilterCondition> {
        let mut items = item.into_inner();
        let fid = field_id(fields_ids_map, filterable_fields, &mut items)
            .map_err(UserError::InvalidFilterAttribute)?;

        let value = items.next().unwrap();
        let (result, svalue) = pest_parse(value);

        let svalue = svalue.to_lowercase();
        Ok(Operator(fid, Equal(result.ok(), svalue)))
    }

    fn greater_than(
        fields_ids_map: &FieldsIdsMap,
        filterable_fields: &HashSet<FieldId>,
        item: Pair<Rule>,
    ) -> Result<FilterCondition> {
        let mut items = item.into_inner();
        let fid = field_id(fields_ids_map, filterable_fields, &mut items)
            .map_err(UserError::InvalidFilterAttribute)?;

        let value = items.next().unwrap();
        let (result, _svalue) = pest_parse(value);
        let value = result.map_err(UserError::InvalidFilter)?;

        Ok(Operator(fid, GreaterThan(value)))
    }

    fn greater_than_or_equal(
        fields_ids_map: &FieldsIdsMap,
        filterable_fields: &HashSet<FieldId>,
        item: Pair<Rule>,
    ) -> Result<FilterCondition> {
        let mut items = item.into_inner();
        let fid = field_id(fields_ids_map, filterable_fields, &mut items)
            .map_err(UserError::InvalidFilterAttribute)?;

        let value = items.next().unwrap();
        let (result, _svalue) = pest_parse(value);
        let value = result.map_err(UserError::InvalidFilter)?;

        Ok(Operator(fid, GreaterThanOrEqual(value)))
    }

    fn lower_than(
        fields_ids_map: &FieldsIdsMap,
        filterable_fields: &HashSet<FieldId>,
        item: Pair<Rule>,
    ) -> Result<FilterCondition> {
        let mut items = item.into_inner();
        let fid = field_id(fields_ids_map, filterable_fields, &mut items)
            .map_err(UserError::InvalidFilterAttribute)?;

        let value = items.next().unwrap();
        let (result, _svalue) = pest_parse(value);
        let value = result.map_err(UserError::InvalidFilter)?;

        Ok(Operator(fid, LowerThan(value)))
    }

    fn lower_than_or_equal(
        fields_ids_map: &FieldsIdsMap,
        filterable_fields: &HashSet<FieldId>,
        item: Pair<Rule>,
    ) -> Result<FilterCondition> {
        let mut items = item.into_inner();
        let fid = field_id(fields_ids_map, filterable_fields, &mut items)
            .map_err(UserError::InvalidFilterAttribute)?;

        let value = items.next().unwrap();
        let (result, _svalue) = pest_parse(value);
        let value = result.map_err(UserError::InvalidFilter)?;

        Ok(Operator(fid, LowerThanOrEqual(value)))
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
        strings_db: heed::Database<FacetValueStringCodec, CboRoaringBitmapCodec>,
        field_id: FieldId,
        operator: &Operator,
    ) -> Result<RoaringBitmap> {
        // Make sure we always bound the ranges with the field id and the level,
        // as the facets values are all in the same database and prefixed by the
        // field id and the level.
        let (left, right) = match operator {
            GreaterThan(val) => (Excluded(*val), Included(f64::MAX)),
            GreaterThanOrEqual(val) => (Included(*val), Included(f64::MAX)),
            Equal(number, string) => {
                let string_docids = strings_db.get(rtxn, &(field_id, &string))?.unwrap_or_default();
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
            NotEqual(number, string) => {
                let all_numbers_ids = if number.is_some() {
                    index.number_faceted_documents_ids(rtxn, field_id)?
                } else {
                    RoaringBitmap::new()
                };
                let all_strings_ids = index.string_faceted_documents_ids(rtxn, field_id)?;
                let operator = Equal(*number, string.clone());
                let docids = Self::evaluate_operator(
                    rtxn, index, numbers_db, strings_db, field_id, &operator,
                )?;
                return Ok((all_numbers_ids | all_strings_ids) - docids);
            }
            LowerThan(val) => (Included(f64::MIN), Excluded(*val)),
            LowerThanOrEqual(val) => (Included(f64::MIN), Included(*val)),
            Between(left, right) => (Included(*left), Included(*right)),
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
        }
    }
}

/// Retrieve the field id base on the pest value, returns an error is
/// the field does not exist or is not filterable.
///
/// The pest pair is simply a string associated with a span, a location to highlight in
/// the error message.
fn field_id(
    fields_ids_map: &FieldsIdsMap,
    filterable_fields: &HashSet<FieldId>,
    items: &mut Pairs<Rule>,
) -> StdResult<FieldId, PestError<Rule>> {
    // lexing ensures that we at least have a key
    let key = items.next().unwrap();

    let field_id = match fields_ids_map.id(key.as_str()) {
        Some(field_id) => field_id,
        None => {
            return Err(PestError::new_from_span(
                ErrorVariant::CustomError {
                    message: format!(
                        "attribute `{}` not found, available attributes are: {}",
                        key.as_str(),
                        fields_ids_map.iter().map(|(_, n)| n).collect::<Vec<_>>().join(", "),
                    ),
                },
                key.as_span(),
            ))
        }
    };

    if !filterable_fields.contains(&field_id) {
        return Err(PestError::new_from_span(
            ErrorVariant::CustomError {
                message: format!(
                    "attribute `{}` is not filterable, available filterable attributes are: {}",
                    key.as_str(),
                    filterable_fields
                        .iter()
                        .flat_map(|id| { fields_ids_map.name(*id) })
                        .collect::<Vec<_>>()
                        .join(", "),
                ),
            },
            key.as_span(),
        ));
    }

    Ok(field_id)
}

/// Tries to parse the pest pair into the type `T` specified, always returns
/// the original string that we tried to parse.
///
/// Returns the parsing error associated with the span if the conversion fails.
fn pest_parse<T>(pair: Pair<Rule>) -> (StdResult<T, pest::error::Error<Rule>>, String)
where
    T: FromStr,
    T::Err: ToString,
{
    let result = match pair.as_str().parse::<T>() {
        Ok(value) => Ok(value),
        Err(e) => Err(PestError::<Rule>::new_from_span(
            ErrorVariant::CustomError { message: e.to_string() },
            pair.as_span(),
        )),
    };

    (result, pair.as_str().to_string())
}

#[cfg(test)]
mod tests {
    use big_s::S;
    use heed::EnvOpenOptions;
    use maplit::hashset;

    use super::*;
    use crate::update::Settings;

    #[test]
    fn string() {
        let path = tempfile::tempdir().unwrap();
        let mut options = EnvOpenOptions::new();
        options.map_size(10 * 1024 * 1024); // 10 MB
        let index = Index::new(options, &path).unwrap();

        // Set the filterable fields to be the channel.
        let mut wtxn = index.write_txn().unwrap();
        let mut builder = Settings::new(&mut wtxn, &index, 0);
        builder.set_filterable_fields(hashset! { S("channel") });
        builder.execute(|_, _| ()).unwrap();
        wtxn.commit().unwrap();

        // Test that the facet condition is correctly generated.
        let rtxn = index.read_txn().unwrap();
        let condition = FilterCondition::from_str(&rtxn, &index, "channel = Ponce").unwrap();
        let expected = Operator(0, Operator::Equal(None, S("ponce")));
        assert_eq!(condition, expected);

        let condition = FilterCondition::from_str(&rtxn, &index, "channel != ponce").unwrap();
        let expected = Operator(0, Operator::NotEqual(None, S("ponce")));
        assert_eq!(condition, expected);

        let condition = FilterCondition::from_str(&rtxn, &index, "NOT channel = ponce").unwrap();
        let expected = Operator(0, Operator::NotEqual(None, S("ponce")));
        assert_eq!(condition, expected);
    }

    #[test]
    fn number() {
        let path = tempfile::tempdir().unwrap();
        let mut options = EnvOpenOptions::new();
        options.map_size(10 * 1024 * 1024); // 10 MB
        let index = Index::new(options, &path).unwrap();

        // Set the filterable fields to be the channel.
        let mut wtxn = index.write_txn().unwrap();
        let mut builder = Settings::new(&mut wtxn, &index, 0);
        builder.set_filterable_fields(hashset! { "timestamp".into() });
        builder.execute(|_, _| ()).unwrap();
        wtxn.commit().unwrap();

        // Test that the facet condition is correctly generated.
        let rtxn = index.read_txn().unwrap();
        let condition = FilterCondition::from_str(&rtxn, &index, "timestamp 22 TO 44").unwrap();
        let expected = Operator(0, Between(22.0, 44.0));
        assert_eq!(condition, expected);

        let condition = FilterCondition::from_str(&rtxn, &index, "NOT timestamp 22 TO 44").unwrap();
        let expected =
            Or(Box::new(Operator(0, LowerThan(22.0))), Box::new(Operator(0, GreaterThan(44.0))));
        assert_eq!(condition, expected);
    }

    #[test]
    fn parentheses() {
        let path = tempfile::tempdir().unwrap();
        let mut options = EnvOpenOptions::new();
        options.map_size(10 * 1024 * 1024); // 10 MB
        let index = Index::new(options, &path).unwrap();

        // Set the filterable fields to be the channel.
        let mut wtxn = index.write_txn().unwrap();
        let mut builder = Settings::new(&mut wtxn, &index, 0);
        builder.set_searchable_fields(vec![S("channel"), S("timestamp")]); // to keep the fields order
        builder.set_filterable_fields(hashset! { S("channel"), S("timestamp") });
        builder.execute(|_, _| ()).unwrap();
        wtxn.commit().unwrap();

        // Test that the facet condition is correctly generated.
        let rtxn = index.read_txn().unwrap();
        let condition = FilterCondition::from_str(
            &rtxn,
            &index,
            "channel = gotaga OR (timestamp 22 TO 44 AND channel != ponce)",
        )
        .unwrap();
        let expected = Or(
            Box::new(Operator(0, Operator::Equal(None, S("gotaga")))),
            Box::new(And(
                Box::new(Operator(1, Between(22.0, 44.0))),
                Box::new(Operator(0, Operator::NotEqual(None, S("ponce")))),
            )),
        );
        assert_eq!(condition, expected);

        let condition = FilterCondition::from_str(
            &rtxn,
            &index,
            "channel = gotaga OR NOT (timestamp 22 TO 44 AND channel != ponce)",
        )
        .unwrap();
        let expected = Or(
            Box::new(Operator(0, Operator::Equal(None, S("gotaga")))),
            Box::new(Or(
                Box::new(Or(
                    Box::new(Operator(1, LowerThan(22.0))),
                    Box::new(Operator(1, GreaterThan(44.0))),
                )),
                Box::new(Operator(0, Operator::Equal(None, S("ponce")))),
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

        // Set the filterable fields to be the channel.
        let mut wtxn = index.write_txn().unwrap();
        let mut builder = Settings::new(&mut wtxn, &index, 0);
        builder.set_searchable_fields(vec![S("channel"), S("timestamp")]); // to keep the fields order
        builder.set_filterable_fields(hashset! { S("channel"), S("timestamp") });
        builder.execute(|_, _| ()).unwrap();
        wtxn.commit().unwrap();

        // Test that the facet condition is correctly generated.
        let rtxn = index.read_txn().unwrap();
        let condition = FilterCondition::from_array(
            &rtxn,
            &index,
            vec![
                Either::Right("channel = gotaga"),
                Either::Left(vec!["timestamp = 44", "channel != ponce"]),
            ],
        )
        .unwrap()
        .unwrap();
        let expected = FilterCondition::from_str(
            &rtxn,
            &index,
            "channel = gotaga AND (timestamp = 44 OR channel != ponce)",
        )
        .unwrap();
        assert_eq!(condition, expected);
    }
}
