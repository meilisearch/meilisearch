use std::fmt::Write;
use std::iter::FromIterator;

use big_s::S;
use either::Either;
use meili_snap::snapshot;
use roaring::RoaringBitmap;

use crate::constants::RESERVED_GEO_FIELD_NAME;
use crate::index::tests::TempIndex;
use crate::{Filter, FilterableAttributesRule};

#[test]
fn empty_db() {
    let index = TempIndex::new();
    //Set the filterable fields to be the channel.
    index
        .update_settings(|settings| {
            settings
                .set_filterable_fields(vec![FilterableAttributesRule::Field("PrIcE".to_string())]);
        })
        .unwrap();

    let rtxn = index.read_txn().unwrap();

    let filter = Filter::from_str("PrIcE < 1000").unwrap().unwrap();
    let bitmap = filter.evaluate(&rtxn, &index).unwrap();
    assert!(bitmap.is_empty());

    let filter = Filter::from_str("NOT PrIcE >= 1000").unwrap().unwrap();
    let bitmap = filter.evaluate(&rtxn, &index).unwrap();
    assert!(bitmap.is_empty());
}

#[test]
fn from_array() {
    // Simple array with Left
    let condition = Filter::from_array(vec![Either::Left(["channel = mv"])]).unwrap().unwrap();
    let expected = Filter::from_str("channel = mv").unwrap().unwrap();
    assert_eq!(condition, expected);

    // Simple array with Right
    let condition = Filter::from_array::<_, Option<&str>>(vec![Either::Right("channel = mv")])
        .unwrap()
        .unwrap();
    let expected = Filter::from_str("channel = mv").unwrap().unwrap();
    assert_eq!(condition, expected);

    // Array with Left and escaped quote
    let condition =
        Filter::from_array(vec![Either::Left(["channel = \"Mister Mv\""])]).unwrap().unwrap();
    let expected = Filter::from_str("channel = \"Mister Mv\"").unwrap().unwrap();
    assert_eq!(condition, expected);

    // Array with Right and escaped quote
    let condition =
        Filter::from_array::<_, Option<&str>>(vec![Either::Right("channel = \"Mister Mv\"")])
            .unwrap()
            .unwrap();
    let expected = Filter::from_str("channel = \"Mister Mv\"").unwrap().unwrap();
    assert_eq!(condition, expected);

    // Array with Left and escaped simple quote
    let condition =
        Filter::from_array(vec![Either::Left(["channel = 'Mister Mv'"])]).unwrap().unwrap();
    let expected = Filter::from_str("channel = 'Mister Mv'").unwrap().unwrap();
    assert_eq!(condition, expected);

    // Array with Right and escaped simple quote
    let condition =
        Filter::from_array::<_, Option<&str>>(vec![Either::Right("channel = 'Mister Mv'")])
            .unwrap()
            .unwrap();
    let expected = Filter::from_str("channel = 'Mister Mv'").unwrap().unwrap();
    assert_eq!(condition, expected);

    // Simple with parenthesis
    let condition = Filter::from_array(vec![Either::Left(["(channel = mv)"])]).unwrap().unwrap();
    let expected = Filter::from_str("(channel = mv)").unwrap().unwrap();
    assert_eq!(condition, expected);

    // Test that the facet condition is correctly generated.
    let condition = Filter::from_array(vec![
        Either::Right("channel = gotaga"),
        Either::Left(vec!["timestamp = 44", "channel != ponce"]),
    ])
    .unwrap()
    .unwrap();
    let expected = Filter::from_str("channel = gotaga AND (timestamp = 44 OR channel != ponce)")
        .unwrap()
        .unwrap();
    assert_eq!(condition, expected);
}

#[test]
fn not_filterable() {
    let index = TempIndex::new();

    let rtxn = index.read_txn().unwrap();
    let filter = Filter::from_str("_geoRadius(42, 150, 10)").unwrap().unwrap();
    let error = filter.evaluate(&rtxn, &index).unwrap_err();
    snapshot!(error.to_string(), @r"
    Attribute `_geo/_geojson` is not filterable. This index does not have configured filterable attributes.
    12:14 _geoRadius(42, 150, 10)
    ");

    let filter = Filter::from_str("_geoBoundingBox([42, 150], [30, 10])").unwrap().unwrap();
    let error = filter.evaluate(&rtxn, &index).unwrap_err();
    snapshot!(error.to_string(), @r"
    Attribute `_geo/_geojson` is not filterable. This index does not have configured filterable attributes.
    18:20 _geoBoundingBox([42, 150], [30, 10])
    ");

    let filter = Filter::from_str("dog = \"bernese mountain\"").unwrap().unwrap();
    let error = filter.evaluate(&rtxn, &index).unwrap_err();
    snapshot!(error.to_string(), @r###"
    Attribute `dog` is not filterable. This index does not have configured filterable attributes.
    1:4 dog = "bernese mountain"
    "###);
    drop(rtxn);

    index
        .update_settings(|settings| {
            settings.set_searchable_fields(vec![S("title")]);
            settings
                .set_filterable_fields(vec![FilterableAttributesRule::Field("title".to_string())]);
        })
        .unwrap();

    let rtxn = index.read_txn().unwrap();

    let filter = Filter::from_str("_geoRadius(-90, 150, 10)").unwrap().unwrap();
    let error = filter.evaluate(&rtxn, &index).unwrap_err();
    snapshot!(error.to_string(), @r"
    Attribute `_geo/_geojson` is not filterable. Available filterable attribute patterns are: `title`.
    12:15 _geoRadius(-90, 150, 10)
    ");

    let filter = Filter::from_str("_geoBoundingBox([42, 150], [30, 10])").unwrap().unwrap();
    let error = filter.evaluate(&rtxn, &index).unwrap_err();
    snapshot!(error.to_string(), @r"
    Attribute `_geo/_geojson` is not filterable. Available filterable attribute patterns are: `title`.
    18:20 _geoBoundingBox([42, 150], [30, 10])
    ");

    let filter = Filter::from_str("name = 12").unwrap().unwrap();
    let error = filter.evaluate(&rtxn, &index).unwrap_err();
    snapshot!(error.to_string(), @r###"
    Attribute `name` is not filterable. Available filterable attribute patterns are: `title`.
    1:5 name = 12
    "###);

    let filter = Filter::from_str("title = \"test\" AND name = 12").unwrap().unwrap();
    let error = filter.evaluate(&rtxn, &index).unwrap_err();
    snapshot!(error.to_string(), @r###"
    Attribute `name` is not filterable. Available filterable attribute patterns are: `title`.
    20:24 title = "test" AND name = 12
    "###);

    let filter = Filter::from_str("title = \"test\" AND name IN [12]").unwrap().unwrap();
    let error = filter.evaluate(&rtxn, &index).unwrap_err();
    snapshot!(error.to_string(), @r###"
    Attribute `name` is not filterable. Available filterable attribute patterns are: `title`.
    20:24 title = "test" AND name IN [12]
    "###);

    let filter = Filter::from_str("title = \"test\" AND name != 12").unwrap().unwrap();
    let error = filter.evaluate(&rtxn, &index).unwrap_err();
    snapshot!(error.to_string(), @r###"
    Attribute `name` is not filterable. Available filterable attribute patterns are: `title`.
    20:24 title = "test" AND name != 12
    "###);
}

#[test]
fn escaped_quote_in_filter_value_2380() {
    let index = TempIndex::new();

    index
        .add_documents(documents!([
            {
                "id": "test_1",
                "monitor_diagonal": "27' to 30'"
            },
            {
                "id": "test_2",
                "monitor_diagonal": "27\" to 30\""
            },
            {
                "id": "test_3",
                "monitor_diagonal": "27\" to 30'"
            },
        ]))
        .unwrap();

    index
        .update_settings(|settings| {
            settings.set_filterable_fields(vec![FilterableAttributesRule::Field(
                "monitor_diagonal".to_string(),
            )]);
        })
        .unwrap();

    let rtxn = index.read_txn().unwrap();

    let mut search = index.search(&rtxn);
    // this filter is copy pasted from #2380 with the exact same espace sequence
    search.filter(Filter::from_str("monitor_diagonal = '27\" to 30\\''").unwrap().unwrap());
    let crate::SearchResult { documents_ids, .. } = search.execute().unwrap();
    assert_eq!(documents_ids, vec![2]);

    search.filter(Filter::from_str(r#"monitor_diagonal = "27' to 30'" "#).unwrap().unwrap());
    let crate::SearchResult { documents_ids, .. } = search.execute().unwrap();
    assert_eq!(documents_ids, vec![0]);

    search.filter(Filter::from_str(r#"monitor_diagonal = "27\" to 30\"" "#).unwrap().unwrap());
    let crate::SearchResult { documents_ids, .. } = search.execute().unwrap();
    assert_eq!(documents_ids, vec![1]);

    search.filter(Filter::from_str(r#"monitor_diagonal = "27\" to 30'" "#).unwrap().unwrap());
    let crate::SearchResult { documents_ids, .. } = search.execute().unwrap();
    assert_eq!(documents_ids, vec![2]);
}

#[test]
fn zero_radius() {
    let index = TempIndex::new();

    index
        .update_settings(|settings| {
            settings.set_filterable_fields(vec![FilterableAttributesRule::Field(S(
                RESERVED_GEO_FIELD_NAME,
            ))]);
        })
        .unwrap();

    index
        .add_documents(documents!([
          {
            "id": 1,
            "name": "Nàpiz' Milano",
            "address": "Viale Vittorio Veneto, 30, 20124, Milan, Italy",
            "type": "pizza",
            "rating": 9,
            RESERVED_GEO_FIELD_NAME: {
              "lat": 45.4777599,
              "lng": 9.1967508
            }
          },
          {
            "id": 2,
            "name": "Artico Gelateria Tradizionale",
            "address": "Via Dogana, 1, 20123 Milan, Italy",
            "type": "ice cream",
            "rating": 10,
            RESERVED_GEO_FIELD_NAME: {
              "lat": 45.4632046,
              "lng": 9.1719421
            }
          },
        ]))
        .unwrap();

    let rtxn = index.read_txn().unwrap();

    let mut search = index.search(&rtxn);

    search.filter(Filter::from_str("_geoRadius(45.4777599, 9.1967508, 0)").unwrap().unwrap());
    let crate::SearchResult { documents_ids, .. } = search.execute().unwrap();
    assert_eq!(documents_ids, vec![0]);
}

#[test]
fn geo_radius_error() {
    let index = TempIndex::new();

    index
        .update_settings(|settings| {
            settings.set_searchable_fields(vec![S(RESERVED_GEO_FIELD_NAME), S("price")]); // to keep the fields order
            settings.set_filterable_fields(vec![
                FilterableAttributesRule::Field(S(RESERVED_GEO_FIELD_NAME)),
                FilterableAttributesRule::Field("price".to_string()),
            ]);
        })
        .unwrap();

    let rtxn = index.read_txn().unwrap();

    // georadius have a bad latitude
    let filter = Filter::from_str("_geoRadius(-100, 150, 10)").unwrap().unwrap();
    let error = filter.evaluate(&rtxn, &index).unwrap_err();
    snapshot!(error.to_string(), @r"
    Bad latitude `-100`. Latitude must be contained between -90 and 90 degrees.
    12:16 _geoRadius(-100, 150, 10)
    ");

    // georadius have a bad latitude
    let filter = Filter::from_str("_geoRadius(-90.0000001, 150, 10)").unwrap().unwrap();
    let error = filter.evaluate(&rtxn, &index).unwrap_err();
    snapshot!(error.to_string(), @r"
    Bad latitude `-90.0000001`. Latitude must be contained between -90 and 90 degrees.
    12:23 _geoRadius(-90.0000001, 150, 10)
    ");

    // georadius have a bad longitude
    let filter = Filter::from_str("_geoRadius(-10, 250, 10)").unwrap().unwrap();
    let error = filter.evaluate(&rtxn, &index).unwrap_err();
    snapshot!(error.to_string(), @r"
    Bad longitude `250`. Longitude must be contained between -180 and 180 degrees. Hint: try using `-110` instead.
    17:20 _geoRadius(-10, 250, 10)
    ");

    // georadius have a bad longitude
    let filter = Filter::from_str("_geoRadius(-10, 180.000001, 10)").unwrap().unwrap();
    let error = filter.evaluate(&rtxn, &index).unwrap_err();
    snapshot!(error.to_string(), @r"
    Bad longitude `180.000001`. Longitude must be contained between -180 and 180 degrees. Hint: try using `-179.999999` instead.
    17:27 _geoRadius(-10, 180.000001, 10)
    ");
}

#[test]
fn geo_bounding_box_error() {
    let index = TempIndex::new();

    index
        .update_settings(|settings| {
            settings.set_searchable_fields(vec![S(RESERVED_GEO_FIELD_NAME), S("price")]); // to keep the fields order
            settings.set_filterable_fields(vec![
                FilterableAttributesRule::Field(S(RESERVED_GEO_FIELD_NAME)),
                FilterableAttributesRule::Field("price".to_string()),
            ]);
        })
        .unwrap();

    let rtxn = index.read_txn().unwrap();

    // geoboundingbox top left coord have a bad latitude
    let filter =
        Filter::from_str("_geoBoundingBox([-90.0000001, 150], [30, 10])").unwrap().unwrap();
    let error = filter.evaluate(&rtxn, &index).unwrap_err();
    snapshot!(error.to_string(), @r"
    Bad latitude `-90.0000001`. Latitude must be contained between -90 and 90 degrees.
    18:29 _geoBoundingBox([-90.0000001, 150], [30, 10])
    ");

    // geoboundingbox top left coord have a bad latitude
    let filter = Filter::from_str("_geoBoundingBox([90.0000001, 150], [30, 10])").unwrap().unwrap();
    let error = filter.evaluate(&rtxn, &index).unwrap_err();
    snapshot!(error.to_string(), @r"
    Bad latitude `90.0000001`. Latitude must be contained between -90 and 90 degrees.
    18:28 _geoBoundingBox([90.0000001, 150], [30, 10])
    ");

    // geoboundingbox bottom right coord have a bad latitude
    let filter =
        Filter::from_str("_geoBoundingBox([30, 10], [-90.0000001, 150])").unwrap().unwrap();
    let error = filter.evaluate(&rtxn, &index).unwrap_err();
    snapshot!(error.to_string(), @r"
    Bad latitude `-90.0000001`. Latitude must be contained between -90 and 90 degrees.
    28:39 _geoBoundingBox([30, 10], [-90.0000001, 150])
    ");

    // geoboundingbox bottom right coord have a bad latitude
    let filter = Filter::from_str("_geoBoundingBox([30, 10], [90.0000001, 150])").unwrap().unwrap();
    let error = filter.evaluate(&rtxn, &index).unwrap_err();
    snapshot!(error.to_string(), @r"
    Bad latitude `90.0000001`. Latitude must be contained between -90 and 90 degrees.
    28:38 _geoBoundingBox([30, 10], [90.0000001, 150])
    ");

    // geoboundingbox top left coord have a bad longitude
    let filter = Filter::from_str("_geoBoundingBox([-10, 180.000001], [30, 10])").unwrap().unwrap();
    let error = filter.evaluate(&rtxn, &index).unwrap_err();
    snapshot!(error.to_string(), @r"
    Bad longitude `180.000001`. Longitude must be contained between -180 and 180 degrees. Hint: try using `-179.999999` instead.
    23:33 _geoBoundingBox([-10, 180.000001], [30, 10])
    ");

    // geoboundingbox top left coord have a bad longitude
    let filter =
        Filter::from_str("_geoBoundingBox([-10, -180.000001], [30, 10])").unwrap().unwrap();
    let error = filter.evaluate(&rtxn, &index).unwrap_err();
    snapshot!(error.to_string(), @r"
    Bad longitude `-180.000001`. Longitude must be contained between -180 and 180 degrees. Hint: try using `179.999999` instead.
    23:34 _geoBoundingBox([-10, -180.000001], [30, 10])
    ");

    // geoboundingbox bottom right coord have a bad longitude
    let filter =
        Filter::from_str("_geoBoundingBox([30, 10], [-10, -180.000001])").unwrap().unwrap();
    let error = filter.evaluate(&rtxn, &index).unwrap_err();
    snapshot!(error.to_string(), @r"
    Bad longitude `-180.000001`. Longitude must be contained between -180 and 180 degrees. Hint: try using `179.999999` instead.
    33:44 _geoBoundingBox([30, 10], [-10, -180.000001])
    ");

    // geoboundingbox bottom right coord have a bad longitude
    let filter = Filter::from_str("_geoBoundingBox([30, 10], [-10, 180.000001])").unwrap().unwrap();
    let error = filter.evaluate(&rtxn, &index).unwrap_err();
    snapshot!(error.to_string(), @r"
    Bad longitude `180.000001`. Longitude must be contained between -180 and 180 degrees. Hint: try using `-179.999999` instead.
    33:43 _geoBoundingBox([30, 10], [-10, 180.000001])
    ");
}

#[test]
fn filter_depth() {
    // generates a big (2 MiB) filter with too much of ORs.
    let tipic_filter = "account_ids=14361 OR ";
    let mut filter_string = String::with_capacity(tipic_filter.len() * 14360);
    for i in 1..=14361 {
        let _ = write!(&mut filter_string, "account_ids={}", i);
        if i != 14361 {
            let _ = write!(&mut filter_string, " OR ");
        }
    }

    // Note: the filter used to be rejected for being too deep, but that is
    // no longer the case
    let filter = Filter::from_str(&filter_string).unwrap();
    assert!(filter.is_some());
}

#[test]
fn empty_filter() {
    let option = Filter::from_str("     ").unwrap();
    assert_eq!(option, None);
}

#[test]
fn non_finite_float() {
    let index = TempIndex::new();

    index
        .update_settings(|settings| {
            settings.set_searchable_fields(vec![S("price")]); // to keep the fields order
            settings
                .set_filterable_fields(vec![FilterableAttributesRule::Field("price".to_string())]);
        })
        .unwrap();
    index
        .add_documents(documents!([
            {
                "id": "test_1",
                "price": "inf"
            },
            {
                "id": "test_2",
                "price": "2000"
            },
            {
                "id": "test_3",
                "price": "infinity"
            },
        ]))
        .unwrap();

    let rtxn = index.read_txn().unwrap();
    let filter = Filter::from_str("price = inf").unwrap().unwrap();
    let result = filter.evaluate(&rtxn, &index).unwrap();
    assert!(result.contains(0));
    let filter = Filter::from_str("price < inf").unwrap().unwrap();
    let result = filter.evaluate(&rtxn, &index).unwrap();
    // this is allowed due to filters with strings
    assert!(result.contains(1));

    let filter = Filter::from_str("price = NaN").unwrap().unwrap();
    let result = filter.evaluate(&rtxn, &index).unwrap();
    assert!(result.is_empty());
    let filter = Filter::from_str("price < NaN").unwrap().unwrap();
    let result = filter.evaluate(&rtxn, &index).unwrap();
    assert!(result.contains(1));

    let filter = Filter::from_str("price = infinity").unwrap().unwrap();
    let result = filter.evaluate(&rtxn, &index).unwrap();
    assert!(result.contains(2));
    let filter = Filter::from_str("price < infinity").unwrap().unwrap();
    let result = filter.evaluate(&rtxn, &index).unwrap();
    assert!(result.contains(0));
    assert!(result.contains(1));
}

#[test]
fn filter_number() {
    let index = TempIndex::new();

    index
        .update_settings(|settings| {
            settings.set_primary_key("id".to_owned());
            settings.set_filterable_fields(vec![
                FilterableAttributesRule::Field("id".to_string()),
                FilterableAttributesRule::Field("one".to_string()),
                FilterableAttributesRule::Field("two".to_string()),
            ]);
        })
        .unwrap();

    let mut docs = vec![];
    for i in 0..100 {
        docs.push(serde_json::json!({ "id": i, "two": i % 10 }));
    }

    index.add_documents(documents!(docs)).unwrap();

    let rtxn = index.read_txn().unwrap();
    for i in 0..100 {
        let filter_str = format!("id = {i}");
        let filter = Filter::from_str(&filter_str).unwrap().unwrap();
        let result = filter.evaluate(&rtxn, &index).unwrap();
        assert_eq!(result, RoaringBitmap::from_iter([i]));
    }
    for i in 0..100 {
        let filter_str = format!("id > {i}");
        let filter = Filter::from_str(&filter_str).unwrap().unwrap();
        let result = filter.evaluate(&rtxn, &index).unwrap();
        assert_eq!(result, RoaringBitmap::from_iter((i + 1)..100));
    }
    for i in 0..100 {
        let filter_str = format!("id < {i}");
        let filter = Filter::from_str(&filter_str).unwrap().unwrap();
        let result = filter.evaluate(&rtxn, &index).unwrap();
        assert_eq!(result, RoaringBitmap::from_iter(0..i));
    }
    for i in 0..100 {
        let filter_str = format!("id <= {i}");
        let filter = Filter::from_str(&filter_str).unwrap().unwrap();
        let result = filter.evaluate(&rtxn, &index).unwrap();
        assert_eq!(result, RoaringBitmap::from_iter(0..=i));
    }
    for i in 0..100 {
        let filter_str = format!("id >= {i}");
        let filter = Filter::from_str(&filter_str).unwrap().unwrap();
        let result = filter.evaluate(&rtxn, &index).unwrap();
        assert_eq!(result, RoaringBitmap::from_iter(i..100));
    }
    for i in 0..100 {
        for j in i..100 {
            let filter_str = format!("id {i} TO {j}");
            let filter = Filter::from_str(&filter_str).unwrap().unwrap();
            let result = filter.evaluate(&rtxn, &index).unwrap();
            assert_eq!(result, RoaringBitmap::from_iter(i..=j));
        }
    }
    let filter = Filter::from_str("one >= 0 OR one <= 0").unwrap().unwrap();
    let result = filter.evaluate(&rtxn, &index).unwrap();
    assert_eq!(result, RoaringBitmap::default());

    let filter = Filter::from_str("one = 0").unwrap().unwrap();
    let result = filter.evaluate(&rtxn, &index).unwrap();
    assert_eq!(result, RoaringBitmap::default());

    for i in 0..10 {
        for j in i..10 {
            let filter_str = format!("two {i} TO {j}");
            let filter = Filter::from_str(&filter_str).unwrap().unwrap();
            let result = filter.evaluate(&rtxn, &index).unwrap();
            assert_eq!(
                result,
                RoaringBitmap::from_iter((0..100).filter(|x| (i..=j).contains(&(x % 10))))
            );
        }
    }
    let filter = Filter::from_str("two != 0").unwrap().unwrap();
    let result = filter.evaluate(&rtxn, &index).unwrap();
    assert_eq!(result, RoaringBitmap::from_iter((0..100).filter(|x| x % 10 != 0)));
}
