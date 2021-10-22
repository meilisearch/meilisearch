use std::fmt::Debug;
use std::ops::Bound::{self, Excluded, Included};

use either::Either;
use filter_parser::{Condition, FilterCondition, Span, Token};
use heed::types::DecodeIgnore;
use log::debug;
use nom::error::{convert_error, VerboseError};
use roaring::RoaringBitmap;

use super::FacetNumberRange;
use crate::error::{Error, UserError};
use crate::heed_codec::facet::{
    FacetLevelValueF64Codec, FacetStringLevelZeroCodec, FacetStringLevelZeroValueCodec,
};
use crate::{distance_between_two_points, CboRoaringBitmapCodec, FieldId, Index, Result};

#[derive(Debug, Clone)]
pub struct Filter<'a> {
    condition: FilterCondition<'a>,
}

impl<'a> Filter<'a> {
    pub fn from_array<I, J>(
        rtxn: &heed::RoTxn,
        index: &Index,
        array: I,
    ) -> Result<Option<FilterCondition<'a>>>
    where
        I: IntoIterator<Item = Either<J, &'a str>>,
        J: IntoIterator<Item = &'a str>,
    {
        let mut ands: Option<FilterCondition> = None;

        for either in array {
            match either {
                Either::Left(array) => {
                    let mut ors = None;
                    for rule in array {
                        let condition =
                            FilterCondition::parse::<VerboseError<Span>>(rule.as_ref()).unwrap();
                        ors = match ors.take() {
                            Some(ors) => {
                                Some(FilterCondition::Or(Box::new(ors), Box::new(condition)))
                            }
                            None => Some(condition),
                        };
                    }

                    if let Some(rule) = ors {
                        ands = match ands.take() {
                            Some(ands) => {
                                Some(FilterCondition::And(Box::new(ands), Box::new(rule)))
                            }
                            None => Some(rule),
                        };
                    }
                }
                Either::Right(rule) => {
                    let condition =
                        FilterCondition::parse::<VerboseError<Span>>(rule.as_ref()).unwrap();
                    ands = match ands.take() {
                        Some(ands) => {
                            Some(FilterCondition::And(Box::new(ands), Box::new(condition)))
                        }
                        None => Some(condition),
                    };
                }
            }
        }

        Ok(ands)
    }

    pub fn from_str(rtxn: &heed::RoTxn, index: &Index, expression: &'a str) -> Result<Self> {
        let fields_ids_map = index.fields_ids_map(rtxn)?;
        let filterable_fields = index.filterable_fields(rtxn)?;
        // TODO TAMO
        let condition = FilterCondition::parse::<VerboseError<Span>>(expression).ok().unwrap();
        /*
        let condition = match FilterCondition::parse::<VerboseError<Span>>(expression) {
            Ok(fc) => Ok(fc),
            Err(e) => {
                let ve = match e {
                    nom::Err::Error(x) => x,
                    nom::Err::Failure(x) => x,
                    _ => unreachable!(),
                };
                Err(Error::UserError(UserError::InvalidFilter {
                    input: convert_error(Span::new(expression), ve).to_string(),
                }))
            }
        };
        */
        Ok(Self { condition })
    }
}

impl<'a> Filter<'a> {
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
        operator: &Condition<'a>,
    ) -> Result<RoaringBitmap> {
        // Make sure we always bound the ranges with the field id and the level,
        // as the facets values are all in the same database and prefixed by the
        // field id and the level.
        // TODO TAMO: return good error when we can't parse a span
        let (left, right) = match operator {
            Condition::GreaterThan(val) => {
                (Excluded(val.inner.parse::<f64>().unwrap()), Included(f64::MAX))
            }
            Condition::GreaterThanOrEqual(val) => {
                (Included(val.inner.parse::<f64>().unwrap()), Included(f64::MAX))
            }
            Condition::LowerThan(val) => (Included(f64::MIN), Excluded(val.inner.parse().unwrap())),
            Condition::LowerThanOrEqual(val) => {
                (Included(f64::MIN), Included(val.inner.parse().unwrap()))
            }
            Condition::Between { from, to } => {
                (Included(from.inner.parse::<f64>().unwrap()), Included(to.inner.parse().unwrap()))
            }
            Condition::Equal(val) => {
                let (_original_value, string_docids) =
                    strings_db.get(rtxn, &(field_id, val.inner))?.unwrap_or_default();
                let number = val.inner.parse::<f64>().ok();
                let number_docids = match number {
                    Some(n) => {
                        let n = Included(n);
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
            Condition::NotEqual(val) => {
                let number = val.inner.parse::<f64>().ok();
                let all_numbers_ids = if number.is_some() {
                    index.number_faceted_documents_ids(rtxn, field_id)?
                } else {
                    RoaringBitmap::new()
                };
                let all_strings_ids = index.string_faceted_documents_ids(rtxn, field_id)?;
                let operator = Condition::Equal(val.clone());
                let docids = Self::evaluate_operator(
                    rtxn, index, numbers_db, strings_db, field_id, &operator,
                )?;
                return Ok((all_numbers_ids | all_strings_ids) - docids);
            } /*
                          Condition::GeoLowerThan(base_point, distance) => {
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
                          Condition::GeoGreaterThan(point, distance) => {
                              let result = Self::evaluate_operator(
                                  rtxn,
                                  index,
                                  numbers_db,
                                  strings_db,
                                  field_id,
                                  &Condition::GeoLowerThan(point.clone(), *distance),
                              )?;
                              let geo_faceted_doc_ids = index.geo_faceted_documents_ids(rtxn)?;
                              return Ok(geo_faceted_doc_ids - result);
                          }
              */
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

        match &self.condition {
            FilterCondition::Condition { fid, op } => {
                // TODO: parse fid
                let _ = fid;
                let fid = 42;
                Self::evaluate_operator(rtxn, index, numbers_db, strings_db, fid, &op)
            }
            FilterCondition::Or(lhs, rhs) => {
                let lhs = Self::evaluate(&(lhs.as_ref().clone()).into(), rtxn, index)?;
                let rhs = Self::evaluate(&(rhs.as_ref().clone()).into(), rtxn, index)?;
                Ok(lhs | rhs)
            }
            FilterCondition::And(lhs, rhs) => {
                let lhs = Self::evaluate(&(lhs.as_ref().clone()).into(), rtxn, index)?;
                let rhs = Self::evaluate(&(rhs.as_ref().clone()).into(), rtxn, index)?;
                Ok(lhs & rhs)
            }
            Empty => Ok(RoaringBitmap::new()),
        }
    }
}

impl<'a> From<FilterCondition<'a>> for Filter<'a> {
    fn from(fc: FilterCondition<'a>) -> Self {
        Self { condition: fc }
    }
}

#[cfg(test)]
mod tests {
    use big_s::S;
    use either::Either;
    use heed::EnvOpenOptions;
    use maplit::hashset;

    use super::*;
    use crate::update::Settings;
    use crate::Index;

    #[test]
    fn number() {
        let path = tempfile::tempdir().unwrap();
        let mut options = EnvOpenOptions::new();
        options.map_size(10 * 1024 * 1024); // 10 MB
        let index = Index::new(options, &path).unwrap();

        // Set the filterable fields to be the channel.
        let mut wtxn = index.write_txn().unwrap();
        let mut map = index.fields_ids_map(&wtxn).unwrap();
        map.insert("timestamp");
        index.put_fields_ids_map(&mut wtxn, &map).unwrap();
        let mut builder = Settings::new(&mut wtxn, &index, 0);
        builder.set_filterable_fields(hashset! { "timestamp".into() });
        builder.execute(|_, _| ()).unwrap();
        wtxn.commit().unwrap();

        // Test that the facet condition is correctly generated.
        let rtxn = index.read_txn().unwrap();
        let condition = FilterCondition::from_str(&rtxn, &index, "timestamp 22 TO 44").unwrap();
        let expected = FilterCondition::Operator(0, Between(22.0, 44.0));
        assert_eq!(condition, expected);

        let condition = FilterCondition::from_str(&rtxn, &index, "NOT timestamp 22 TO 44").unwrap();
        let expected = FilterCondition::Or(
            Box::new(FilterCondition::Operator(0, LowerThan(22.0))),
            Box::new(FilterCondition::Operator(0, GreaterThan(44.0))),
        );
        assert_eq!(condition, expected);
    }

    #[test]
    fn compare() {
        let path = tempfile::tempdir().unwrap();
        let mut options = EnvOpenOptions::new();
        options.map_size(10 * 1024 * 1024); // 10 MB
        let index = Index::new(options, &path).unwrap();

        let mut wtxn = index.write_txn().unwrap();
        let mut builder = Settings::new(&mut wtxn, &index, 0);
        builder.set_searchable_fields(vec![S("channel"), S("timestamp"), S("id")]); // to keep the fields order
        builder.set_filterable_fields(hashset! { S("channel"), S("timestamp") ,S("id")});
        builder.execute(|_, _| ()).unwrap();
        wtxn.commit().unwrap();

        let rtxn = index.read_txn().unwrap();
        let condition = FilterCondition::from_str(&rtxn, &index, "channel < 20").unwrap();
        let expected = FilterCondition::Operator(0, LowerThan(20.0));
        assert_eq!(condition, expected);

        let rtxn = index.read_txn().unwrap();
        let condition = FilterCondition::from_str(&rtxn, &index, "id < 200").unwrap();
        let expected = FilterCondition::Operator(2, LowerThan(200.0));
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
        let expected = FilterCondition::Or(
            Box::new(FilterCondition::Operator(0, Operator::Equal(None, S("gotaga")))),
            Box::new(FilterCondition::And(
                Box::new(FilterCondition::Operator(1, Between(22.0, 44.0))),
                Box::new(FilterCondition::Operator(0, Operator::NotEqual(None, S("ponce")))),
            )),
        );
        assert_eq!(condition, expected);

        let condition = FilterCondition::from_str(
            &rtxn,
            &index,
            "channel = gotaga OR NOT (timestamp 22 TO 44 AND channel != ponce)",
        )
        .unwrap();
        let expected = FilterCondition::Or(
            Box::new(FilterCondition::Operator(0, Operator::Equal(None, S("gotaga")))),
            Box::new(FilterCondition::Or(
                Box::new(FilterCondition::Or(
                    Box::new(FilterCondition::Operator(1, LowerThan(22.0))),
                    Box::new(FilterCondition::Operator(1, GreaterThan(44.0))),
                )),
                Box::new(FilterCondition::Operator(0, Operator::Equal(None, S("ponce")))),
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

        // Simple array with Left
        let rtxn = index.read_txn().unwrap();
        let condition = FilterCondition::from_array::<_, _, _, &str>(
            &rtxn,
            &index,
            vec![Either::Left(["channel = mv"])],
        )
        .unwrap()
        .unwrap();
        let expected = FilterCondition::from_str(&rtxn, &index, "channel = mv").unwrap();
        assert_eq!(condition, expected);

        // Simple array with Right
        let rtxn = index.read_txn().unwrap();
        let condition = FilterCondition::from_array::<_, Option<&str>, _, _>(
            &rtxn,
            &index,
            vec![Either::Right("channel = mv")],
        )
        .unwrap()
        .unwrap();
        let expected = FilterCondition::from_str(&rtxn, &index, "channel = mv").unwrap();
        assert_eq!(condition, expected);

        // Array with Left and escaped quote
        let rtxn = index.read_txn().unwrap();
        let condition = FilterCondition::from_array::<_, _, _, &str>(
            &rtxn,
            &index,
            vec![Either::Left(["channel = \"Mister Mv\""])],
        )
        .unwrap()
        .unwrap();
        let expected = FilterCondition::from_str(&rtxn, &index, "channel = \"Mister Mv\"").unwrap();
        assert_eq!(condition, expected);

        // Array with Right and escaped quote
        let rtxn = index.read_txn().unwrap();
        let condition = FilterCondition::from_array::<_, Option<&str>, _, _>(
            &rtxn,
            &index,
            vec![Either::Right("channel = \"Mister Mv\"")],
        )
        .unwrap()
        .unwrap();
        let expected = FilterCondition::from_str(&rtxn, &index, "channel = \"Mister Mv\"").unwrap();
        assert_eq!(condition, expected);

        // Array with Left and escaped simple quote
        let rtxn = index.read_txn().unwrap();
        let condition = FilterCondition::from_array::<_, _, _, &str>(
            &rtxn,
            &index,
            vec![Either::Left(["channel = 'Mister Mv'"])],
        )
        .unwrap()
        .unwrap();
        let expected = FilterCondition::from_str(&rtxn, &index, "channel = 'Mister Mv'").unwrap();
        assert_eq!(condition, expected);

        // Array with Right and escaped simple quote
        let rtxn = index.read_txn().unwrap();
        let condition = FilterCondition::from_array::<_, Option<&str>, _, _>(
            &rtxn,
            &index,
            vec![Either::Right("channel = 'Mister Mv'")],
        )
        .unwrap()
        .unwrap();
        let expected = FilterCondition::from_str(&rtxn, &index, "channel = 'Mister Mv'").unwrap();
        assert_eq!(condition, expected);

        // Simple with parenthesis
        let rtxn = index.read_txn().unwrap();
        let condition = FilterCondition::from_array::<_, _, _, &str>(
            &rtxn,
            &index,
            vec![Either::Left(["(channel = mv)"])],
        )
        .unwrap()
        .unwrap();
        let expected = FilterCondition::from_str(&rtxn, &index, "(channel = mv)").unwrap();
        assert_eq!(condition, expected);

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

    #[test]
    fn geo_radius() {
        let path = tempfile::tempdir().unwrap();
        let mut options = EnvOpenOptions::new();
        options.map_size(10 * 1024 * 1024); // 10 MB
        let index = Index::new(options, &path).unwrap();

        // Set the filterable fields to be the channel.
        let mut wtxn = index.write_txn().unwrap();
        let mut builder = Settings::new(&mut wtxn, &index, 0);
        builder.set_searchable_fields(vec![S("_geo"), S("price")]); // to keep the fields order
        builder.set_filterable_fields(hashset! { S("_geo"), S("price") });
        builder.execute(|_, _| ()).unwrap();
        wtxn.commit().unwrap();

        let rtxn = index.read_txn().unwrap();
        // basic test
        let condition =
            FilterCondition::from_str(&rtxn, &index, "_geoRadius(12, 13.0005, 2000)").unwrap();
        let expected = FilterCondition::Operator(0, GeoLowerThan([12., 13.0005], 2000.));
        assert_eq!(condition, expected);

        // test the negation of the GeoLowerThan
        let condition =
            FilterCondition::from_str(&rtxn, &index, "NOT _geoRadius(50, 18, 2000.500)").unwrap();
        let expected = FilterCondition::Operator(0, GeoGreaterThan([50., 18.], 2000.500));
        assert_eq!(condition, expected);

        // composition of multiple operations
        let condition = FilterCondition::from_str(
            &rtxn,
            &index,
            "(NOT _geoRadius(1, 2, 300) AND _geoRadius(1.001, 2.002, 1000.300)) OR price <= 10",
        )
        .unwrap();
        let expected = FilterCondition::Or(
            Box::new(FilterCondition::And(
                Box::new(FilterCondition::Operator(0, GeoGreaterThan([1., 2.], 300.))),
                Box::new(FilterCondition::Operator(0, GeoLowerThan([1.001, 2.002], 1000.300))),
            )),
            Box::new(FilterCondition::Operator(1, LowerThanOrEqual(10.))),
        );
        assert_eq!(condition, expected);
    }

    #[test]
    fn geo_radius_error() {
        let path = tempfile::tempdir().unwrap();
        let mut options = EnvOpenOptions::new();
        options.map_size(10 * 1024 * 1024); // 10 MB
        let index = Index::new(options, &path).unwrap();

        // Set the filterable fields to be the channel.
        let mut wtxn = index.write_txn().unwrap();
        let mut builder = Settings::new(&mut wtxn, &index, 0);
        builder.set_searchable_fields(vec![S("_geo"), S("price")]); // to keep the fields order
        builder.set_filterable_fields(hashset! { S("_geo"), S("price") });
        builder.execute(|_, _| ()).unwrap();
        wtxn.commit().unwrap();

        let rtxn = index.read_txn().unwrap();

        // georadius don't have any parameters
        let result = FilterCondition::from_str(&rtxn, &index, "_geoRadius");
        assert!(result.is_err());
        let error = result.unwrap_err();
        assert!(error.to_string().contains("The `_geoRadius` filter expect three arguments: `_geoRadius(latitude, longitude, radius)`"));

        // georadius don't have any parameters
        let result = FilterCondition::from_str(&rtxn, &index, "_geoRadius()");
        assert!(result.is_err());
        let error = result.unwrap_err();
        assert!(error.to_string().contains("The `_geoRadius` filter expect three arguments: `_geoRadius(latitude, longitude, radius)`"));

        // georadius don't have enough parameters
        let result = FilterCondition::from_str(&rtxn, &index, "_geoRadius(1, 2)");
        assert!(result.is_err());
        let error = result.unwrap_err();
        assert!(error.to_string().contains("The `_geoRadius` filter expect three arguments: `_geoRadius(latitude, longitude, radius)`"));

        // georadius have too many parameters
        let result =
            FilterCondition::from_str(&rtxn, &index, "_geoRadius(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)");
        assert!(result.is_err());
        let error = result.unwrap_err();
        assert!(error.to_string().contains("The `_geoRadius` filter expect three arguments: `_geoRadius(latitude, longitude, radius)`"));

        let result = FilterCondition::from_str(&rtxn, &index, "_geoRadius(-100, 150, 10)");
        assert!(result.is_err());
        let error = result.unwrap_err();
        assert!(
            error.to_string().contains("Latitude must be contained between -90 and 90 degrees."),
            "{}",
            error.to_string()
        );

        // georadius have a bad latitude
        let result = FilterCondition::from_str(&rtxn, &index, "_geoRadius(-90.0000001, 150, 10)");
        assert!(result.is_err());
        let error = result.unwrap_err();
        assert!(error
            .to_string()
            .contains("Latitude must be contained between -90 and 90 degrees."));

        // georadius have a bad longitude
        let result = FilterCondition::from_str(&rtxn, &index, "_geoRadius(-10, 250, 10)");
        assert!(result.is_err());
        let error = result.unwrap_err();
        assert!(error
            .to_string()
            .contains("Longitude must be contained between -180 and 180 degrees."));

        // georadius have a bad longitude
        let result = FilterCondition::from_str(&rtxn, &index, "_geoRadius(-10, 180.000001, 10)");
        assert!(result.is_err());
        let error = result.unwrap_err();
        assert!(error
            .to_string()
            .contains("Longitude must be contained between -180 and 180 degrees."));
    }
}
