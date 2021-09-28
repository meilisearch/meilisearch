use std::collections::HashSet;
use std::fmt::Debug;
use std::result::Result as StdResult;

use super::FilterCondition;
use crate::{FieldId, FieldsIdsMap};
use nom::{
    branch::alt,
    bytes::complete::{tag, take_while1},
    character::complete::{char, multispace0},
    combinator::map,
    error::ErrorKind,
    error::ParseError,
    error::VerboseError,
    multi::many0,
    sequence::{delimited, preceded, tuple},
    IResult,
};

use self::Operator::*;
#[derive(Debug, Clone, PartialEq)]
pub enum Operator {
    GreaterThan(f64),
    GreaterThanOrEqual(f64),
    Equal(Option<f64>, String),
    NotEqual(Option<f64>, String),
    LowerThan(f64),
    LowerThanOrEqual(f64),
    Between(f64, f64),
    GeoLowerThan([f64; 2], f64),
    GeoGreaterThan([f64; 2], f64),
}

impl Operator {
    /// This method can return two operations in case it must express
    /// an OR operation for the between case (i.e. `TO`).
    pub fn negate(self) -> (Self, Option<Self>) {
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

pub struct ParseContext<'a> {
    pub fields_ids_map: &'a FieldsIdsMap,
    pub filterable_fields: &'a HashSet<String>,
}

impl<'a> ParseContext<'a> {
    fn parse_or_nom<E>(&'a self, input: &'a str) -> IResult<&'a str, FilterCondition, E>
    where
        E: ParseError<&'a str>,
    {
        let (input, lhs) = self.parse_and_nom(input)?;
        let (input, ors) = many0(preceded(tag("OR"), |c| Self::parse_or_nom(self, c)))(input)?;
        let expr = ors
            .into_iter()
            .fold(lhs, |acc, branch| FilterCondition::Or(Box::new(acc), Box::new(branch)));
        Ok((input, expr))
    }

    fn parse_and_nom<E>(&'a self, input: &'a str) -> IResult<&'a str, FilterCondition, E>
    where
        E: ParseError<&'a str>,
    {
        let (input, lhs) = self.parse_not_nom(input)?;
        let (input, ors) = many0(preceded(tag("AND"), |c| Self::parse_and_nom(self, c)))(input)?;
        let expr = ors
            .into_iter()
            .fold(lhs, |acc, branch| FilterCondition::And(Box::new(acc), Box::new(branch)));
        Ok((input, expr))
    }

    fn parse_not_nom<E>(&'a self, input: &'a str) -> IResult<&'a str, FilterCondition, E>
    where
        E: ParseError<&'a str>,
    {
        alt((
            map(
                preceded(alt((self.ws(tag("!")), self.ws(tag("NOT")))), |c| {
                    Self::parse_condition_expression(self, c)
                }),
                |e| e.negate(),
            ),
            |c| Self::parse_condition_expression(self, c),
        ))(input)
    }

    fn ws<F, O, E>(&'a self, inner: F) -> impl FnMut(&'a str) -> IResult<&'a str, O, E>
    where
        F: Fn(&'a str) -> IResult<&'a str, O, E>,
        E: ParseError<&'a str>,
    {
        delimited(multispace0, inner, multispace0)
    }

    fn parse_simple_condition<E>(&'a self, input: &'a str) -> IResult<&'a str, FilterCondition, E>
    where
        E: ParseError<&'a str>,
    {
        let operator = alt((tag(">"), tag(">="), tag("="), tag("<"), tag("!="), tag("<=")));
        let (input, (key, op, value)) =
            tuple((self.ws(|c| self.parse_key(c)), operator, self.ws(|c| self.parse_key(c))))(
                input,
            )?;
        let fid = self.parse_fid(input, key)?;
        let r: StdResult<f64, nom::Err<VerboseError<&str>>> = self.parse_numeric(value);
        let k = match op {
            "=" => FilterCondition::Operator(fid, Equal(r.ok(), value.to_string().to_lowercase())),
            "!=" => {
                FilterCondition::Operator(fid, NotEqual(r.ok(), value.to_string().to_lowercase()))
            }
            ">" | "<" | "<=" | ">=" => return self.parse_numeric_unary_condition(op, fid, value),
            _ => unreachable!(),
        };
        Ok((input, k))
    }

    fn parse_numeric<E, T>(&'a self, input: &'a str) -> StdResult<T, nom::Err<E>>
    where
        E: ParseError<&'a str>,
        T: std::str::FromStr,
    {
        match input.parse::<T>() {
            Ok(n) => Ok(n),
            Err(_) => {
                return match input.chars().nth(0) {
                    Some(ch) => Err(nom::Err::Failure(E::from_char(input, ch))),
                    None => Err(nom::Err::Failure(E::from_error_kind(input, ErrorKind::Eof))),
                };
            }
        }
    }

    fn parse_numeric_unary_condition<E>(
        &'a self,
        input: &'a str,
        fid: u16,
        value: &'a str,
    ) -> IResult<&'a str, FilterCondition, E>
    where
        E: ParseError<&'a str>,
    {
        let numeric: f64 = self.parse_numeric(value)?;
        let k = match input {
            ">" => FilterCondition::Operator(fid, GreaterThan(numeric)),
            "<" => FilterCondition::Operator(fid, LowerThan(numeric)),
            "<=" => FilterCondition::Operator(fid, LowerThanOrEqual(numeric)),
            ">=" => FilterCondition::Operator(fid, GreaterThanOrEqual(numeric)),
            _ => unreachable!(),
        };
        Ok((input, k))
    }

    fn parse_fid<E>(&'a self, input: &'a str, key: &'a str) -> StdResult<FieldId, nom::Err<E>>
    where
        E: ParseError<&'a str>,
    {
        let error = match input.chars().nth(0) {
            Some(ch) => Err(nom::Err::Failure(E::from_char(input, ch))),
            None => Err(nom::Err::Failure(E::from_error_kind(input, ErrorKind::Eof))),
        };
        if !self.filterable_fields.contains(key) {
            return error;
        }
        match self.fields_ids_map.id(key) {
            Some(fid) => Ok(fid),
            None => error,
        }
    }

    fn parse_range_condition<E>(&'a self, input: &'a str) -> IResult<&'a str, FilterCondition, E>
    where
        E: ParseError<&'a str>,
    {
        let (input, (key, from, _, to)) = tuple((
            self.ws(|c| self.parse_key(c)),
            self.ws(|c| self.parse_key(c)),
            tag("TO"),
            self.ws(|c| self.parse_key(c)),
        ))(input)?;

        let fid = self.parse_fid(input, key)?;
        let numeric_from: f64 = self.parse_numeric(from)?;
        let numeric_to: f64 = self.parse_numeric(to)?;
        let res = FilterCondition::Operator(fid, Between(numeric_from, numeric_to));
        Ok((input, res))
    }

    fn parse_condition<E>(&'a self, input: &'a str) -> IResult<&'a str, FilterCondition, E>
    where
        E: ParseError<&'a str>,
    {
        let l1 = |c| self.parse_simple_condition(c);
        let l2 = |c| self.parse_range_condition(c);
        let (input, condition) = alt((l1, l2))(input)?;
        Ok((input, condition))
    }

    fn parse_condition_expression<E>(&'a self, input: &'a str) -> IResult<&str, FilterCondition, E>
    where
        E: ParseError<&'a str>,
    {
        return alt((
            delimited(self.ws(char('(')), |c| Self::parse_expression(self, c), self.ws(char(')'))),
            |c| Self::parse_condition(self, c),
        ))(input);
    }

    fn parse_key<E>(&'a self, input: &'a str) -> IResult<&'a str, &'a str, E>
    where
        E: ParseError<&'a str>,
    {
        let key = |input| take_while1(Self::is_key_component)(input);
        alt((key, delimited(char('"'), key, char('"'))))(input)
    }
    fn is_key_component(c: char) -> bool {
        c.is_alphanumeric() || ['_', '-', '.'].contains(&c)
    }

    pub fn parse_expression<E>(&'a self, input: &'a str) -> IResult<&'a str, FilterCondition, E>
    where
        E: ParseError<&'a str>,
    {
        self.parse_or_nom(input)
    }
}

#[cfg(test)]
mod tests {
    use big_s::S;
    use either::Either;
    use heed::EnvOpenOptions;
    use maplit::hashset;

    use super::*;
    use crate::{update::Settings, Index};

    #[test]
    fn string() {
        let path = tempfile::tempdir().unwrap();
        let mut options = EnvOpenOptions::new();
        options.map_size(10 * 1024 * 1024); // 10 MB
        let index = Index::new(options, &path).unwrap();

        // Set the filterable fields to be the channel.
        let mut wtxn = index.write_txn().unwrap();
        let mut map = index.fields_ids_map(&wtxn).unwrap();
        map.insert("channel");
        index.put_fields_ids_map(&mut wtxn, &map).unwrap();
        let mut builder = Settings::new(&mut wtxn, &index, 0);
        builder.set_filterable_fields(hashset! { S("channel") });
        builder.execute(|_, _| ()).unwrap();
        wtxn.commit().unwrap();

        // Test that the facet condition is correctly generated.
        let rtxn = index.read_txn().unwrap();
        let condition = FilterCondition::from_str(&rtxn, &index, "channel = Ponce").unwrap();
        let expected = FilterCondition::Operator(0, Operator::Equal(None, S("ponce")));
        assert_eq!(condition, expected);

        let condition = FilterCondition::from_str(&rtxn, &index, "channel != ponce").unwrap();
        let expected = FilterCondition::Operator(0, Operator::NotEqual(None, S("ponce")));
        assert_eq!(condition, expected);

        let condition = FilterCondition::from_str(&rtxn, &index, "NOT channel = ponce").unwrap();
        let expected = FilterCondition::Operator(0, Operator::NotEqual(None, S("ponce")));
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
        builder.set_searchable_fields(vec![S("channel"), S("timestamp")]); // to keep the fields order
        builder.set_filterable_fields(hashset! { S("channel"), S("timestamp") });
        builder.execute(|_, _| ()).unwrap();
        wtxn.commit().unwrap();

        let rtxn = index.read_txn().unwrap();
        let condition = FilterCondition::from_str(&rtxn, &index, "channel < 20").unwrap();
        let expected = FilterCondition::Operator(0, LowerThan(20.0));

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
        let expected = Operator(0, GeoLowerThan([12., 13.0005], 2000.));
        assert_eq!(condition, expected);

        // test the negation of the GeoLowerThan
        let condition =
            FilterCondition::from_str(&rtxn, &index, "NOT _geoRadius(50, 18, 2000.500)").unwrap();
        let expected = Operator(0, GeoGreaterThan([50., 18.], 2000.500));
        assert_eq!(condition, expected);

        // composition of multiple operations
        let condition = FilterCondition::from_str(
            &rtxn,
            &index,
            "(NOT _geoRadius(1, 2, 300) AND _geoRadius(1.001, 2.002, 1000.300)) OR price <= 10",
        )
        .unwrap();
        let expected = Or(
            Box::new(And(
                Box::new(Operator(0, GeoGreaterThan([1., 2.], 300.))),
                Box::new(Operator(0, GeoLowerThan([1.001, 2.002], 1000.300))),
            )),
            Box::new(Operator(1, LowerThanOrEqual(10.))),
        );
        assert_eq!(condition, expected);

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

        // georadius have a bad latitude
        let result = FilterCondition::from_str(&rtxn, &index, "_geoRadius(-200, 150, 10)");
        assert!(result.is_err());
        let error = result.unwrap_err();
        assert!(error
            .to_string()
            .contains("Latitude and longitude must be contained between -180 to 180 degrees."));

        // georadius have a bad longitude
        let result = FilterCondition::from_str(&rtxn, &index, "_geoRadius(-10, 181, 10)");
        assert!(result.is_err());
        let error = result.unwrap_err();
        assert!(error
            .to_string()
            .contains("Latitude and longitude must be contained between -180 to 180 degrees."));
    }
}
