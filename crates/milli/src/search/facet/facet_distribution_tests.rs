use std::iter;

use big_s::S;

use crate::documents::mmap_from_objects;
use crate::index::tests::TempIndex;
use crate::{milli_snap, FacetDistribution, FilterableAttributesRule, OrderBy};

#[test]
fn few_candidates_few_facet_values() {
    // All the tests here avoid using the code in `facet_distribution_iter` because there aren't
    // enough candidates.

    let index = TempIndex::new();

    index
        .update_settings(|settings| {
            settings.set_filterable_fields(vec![FilterableAttributesRule::Field(S("colour"))])
        })
        .unwrap();

    let documents = documents!([
        { "id": 0, "colour": "Blue" },
        { "id": 1, "colour": "  blue" },
        { "id": 2, "colour": "RED" }
    ]);

    index.add_documents(documents).unwrap();

    let txn = index.read_txn().unwrap();

    let map = FacetDistribution::new(&txn, &index)
        .facets(iter::once(("colour", OrderBy::default())))
        .execute()
        .unwrap();

    milli_snap!(format!("{map:?}"), @r###"{"colour": {"Blue": 2, "RED": 1}}"###);

    let map = FacetDistribution::new(&txn, &index)
        .facets(iter::once(("colour", OrderBy::default())))
        .candidates([0, 1, 2].iter().copied().collect())
        .execute()
        .unwrap();

    milli_snap!(format!("{map:?}"), @r###"{"colour": {"Blue": 2, "RED": 1}}"###);

    let map = FacetDistribution::new(&txn, &index)
        .facets(iter::once(("colour", OrderBy::default())))
        .candidates([1, 2].iter().copied().collect())
        .execute()
        .unwrap();

    // I think it would be fine if "  blue" was "Blue" instead.
    // We just need to get any non-normalised string I think, even if it's not in
    // the candidates
    milli_snap!(format!("{map:?}"), @r###"{"colour": {"  blue": 1, "RED": 1}}"###);

    let map = FacetDistribution::new(&txn, &index)
        .facets(iter::once(("colour", OrderBy::default())))
        .candidates([2].iter().copied().collect())
        .execute()
        .unwrap();

    milli_snap!(format!("{map:?}"), @r###"{"colour": {"RED": 1}}"###);

    let map = FacetDistribution::new(&txn, &index)
        .facets(iter::once(("colour", OrderBy::default())))
        .candidates([0, 1, 2].iter().copied().collect())
        .max_values_per_facet(1)
        .execute()
        .unwrap();

    milli_snap!(format!("{map:?}"), @r###"{"colour": {"Blue": 2}}"###);

    let map = FacetDistribution::new(&txn, &index)
        .facets(iter::once(("colour", OrderBy::Count)))
        .candidates([0, 1, 2].iter().copied().collect())
        .max_values_per_facet(1)
        .execute()
        .unwrap();

    milli_snap!(format!("{map:?}"), @r###"{"colour": {"Blue": 2}}"###);
}

#[test]
fn many_candidates_few_facet_values() {
    let index = TempIndex::new_with_map_size(4096 * 10_000);

    index
        .update_settings(|settings| {
            settings.set_filterable_fields(vec![FilterableAttributesRule::Field(S("colour"))])
        })
        .unwrap();

    let facet_values = ["Red", "RED", " red ", "Blue", "BLUE"];

    let mut documents = vec![];
    for i in 0..10_000 {
        let document = serde_json::json!({
            "id": i,
            "colour": facet_values[i % 5],
        })
        .as_object()
        .unwrap()
        .clone();
        documents.push(document);
    }

    let documents = mmap_from_objects(documents);
    index.add_documents(documents).unwrap();

    let txn = index.read_txn().unwrap();

    let map = FacetDistribution::new(&txn, &index)
        .facets(iter::once(("colour", OrderBy::default())))
        .execute()
        .unwrap();

    milli_snap!(format!("{map:?}"), @r###"{"colour": {"Blue": 4000, "Red": 6000}}"###);

    let map = FacetDistribution::new(&txn, &index)
        .facets(iter::once(("colour", OrderBy::default())))
        .max_values_per_facet(1)
        .execute()
        .unwrap();

    milli_snap!(format!("{map:?}"), @r###"{"colour": {"Blue": 4000}}"###);

    let map = FacetDistribution::new(&txn, &index)
        .facets(iter::once(("colour", OrderBy::default())))
        .candidates((0..10_000).collect())
        .execute()
        .unwrap();

    milli_snap!(format!("{map:?}"), @r###"{"colour": {"Blue": 4000, "Red": 6000}}"###);

    let map = FacetDistribution::new(&txn, &index)
        .facets(iter::once(("colour", OrderBy::default())))
        .candidates((0..5_000).collect())
        .execute()
        .unwrap();

    milli_snap!(format!("{map:?}"), @r###"{"colour": {"Blue": 2000, "Red": 3000}}"###);

    let map = FacetDistribution::new(&txn, &index)
        .facets(iter::once(("colour", OrderBy::default())))
        .candidates((0..5_000).collect())
        .execute()
        .unwrap();

    milli_snap!(format!("{map:?}"), @r###"{"colour": {"Blue": 2000, "Red": 3000}}"###);

    let map = FacetDistribution::new(&txn, &index)
        .facets(iter::once(("colour", OrderBy::default())))
        .candidates((0..5_000).collect())
        .max_values_per_facet(1)
        .execute()
        .unwrap();

    milli_snap!(format!("{map:?}"), @r###"{"colour": {"Blue": 2000}}"###);

    let map = FacetDistribution::new(&txn, &index)
        .facets(iter::once(("colour", OrderBy::Count)))
        .candidates((0..5_000).collect())
        .max_values_per_facet(1)
        .execute()
        .unwrap();

    milli_snap!(format!("{map:?}"), @r###"{"colour": {"Red": 3000}}"###);
}

#[test]
fn many_candidates_many_facet_values() {
    let index = TempIndex::new_with_map_size(4096 * 10_000);

    index
        .update_settings(|settings| {
            settings.set_filterable_fields(vec![FilterableAttributesRule::Field(S("colour"))])
        })
        .unwrap();

    let facet_values = (0..1000).map(|x| format!("{x:x}")).collect::<Vec<_>>();

    let mut documents = vec![];
    for i in 0..10_000 {
        let document = serde_json::json!({
            "id": i,
            "colour": facet_values[i % 1000],
        })
        .as_object()
        .unwrap()
        .clone();
        documents.push(document);
    }

    let documents = mmap_from_objects(documents);
    index.add_documents(documents).unwrap();

    let txn = index.read_txn().unwrap();

    let map = FacetDistribution::new(&txn, &index)
        .facets(iter::once(("colour", OrderBy::default())))
        .execute()
        .unwrap();

    milli_snap!(format!("{map:?}"), "no_candidates", @"ac9229ed5964d893af96a7076e2f8af5");

    let map = FacetDistribution::new(&txn, &index)
        .facets(iter::once(("colour", OrderBy::default())))
        .max_values_per_facet(2)
        .execute()
        .unwrap();

    milli_snap!(format!("{map:?}"), "no_candidates_with_max_2", @r###"{"colour": {"0": 10, "1": 10}}"###);

    let map = FacetDistribution::new(&txn, &index)
        .facets(iter::once(("colour", OrderBy::default())))
        .candidates((0..10_000).collect())
        .execute()
        .unwrap();

    milli_snap!(format!("{map:?}"), "candidates_0_10_000", @"ac9229ed5964d893af96a7076e2f8af5");

    let map = FacetDistribution::new(&txn, &index)
        .facets(iter::once(("colour", OrderBy::default())))
        .candidates((0..5_000).collect())
        .execute()
        .unwrap();

    milli_snap!(format!("{map:?}"), "candidates_0_5_000", @"825f23a4090d05756f46176987b7d992");
}

#[test]
fn facet_stats() {
    let index = TempIndex::new_with_map_size(4096 * 10_000);

    index
        .update_settings(|settings| {
            settings.set_filterable_fields(vec![FilterableAttributesRule::Field(S("colour"))])
        })
        .unwrap();

    let facet_values = (0..1000).collect::<Vec<_>>();

    let mut documents = vec![];
    for i in 0..1000 {
        let document = serde_json::json!({
            "id": i,
            "colour": facet_values[i % 1000],
        })
        .as_object()
        .unwrap()
        .clone();
        documents.push(document);
    }

    let documents = mmap_from_objects(documents);
    index.add_documents(documents).unwrap();

    let txn = index.read_txn().unwrap();

    let map = FacetDistribution::new(&txn, &index)
        .facets(iter::once(("colour", OrderBy::default())))
        .compute_stats()
        .unwrap();

    milli_snap!(format!("{map:?}"), "no_candidates", @"{}");

    let map = FacetDistribution::new(&txn, &index)
        .facets(iter::once(("colour", OrderBy::default())))
        .candidates((0..1000).collect())
        .compute_stats()
        .unwrap();

    milli_snap!(format!("{map:?}"), "candidates_0_1000", @r###"{"colour": (0.0, 999.0)}"###);

    let map = FacetDistribution::new(&txn, &index)
        .facets(iter::once(("colour", OrderBy::default())))
        .candidates((217..777).collect())
        .compute_stats()
        .unwrap();

    milli_snap!(format!("{map:?}"), "candidates_217_777", @r###"{"colour": (217.0, 776.0)}"###);
}

#[test]
fn facet_stats_array() {
    let index = TempIndex::new_with_map_size(4096 * 10_000);

    index
        .update_settings(|settings| {
            settings.set_filterable_fields(vec![FilterableAttributesRule::Field(S("colour"))])
        })
        .unwrap();

    let facet_values = (0..1000).collect::<Vec<_>>();

    let mut documents = vec![];
    for i in 0..1000 {
        let document = serde_json::json!({
            "id": i,
            "colour": [facet_values[i % 1000], facet_values[i % 1000] + 1000],
        })
        .as_object()
        .unwrap()
        .clone();
        documents.push(document);
    }

    let documents = mmap_from_objects(documents);
    index.add_documents(documents).unwrap();

    let txn = index.read_txn().unwrap();

    let map = FacetDistribution::new(&txn, &index)
        .facets(iter::once(("colour", OrderBy::default())))
        .compute_stats()
        .unwrap();

    milli_snap!(format!("{map:?}"), "no_candidates", @"{}");

    let map = FacetDistribution::new(&txn, &index)
        .facets(iter::once(("colour", OrderBy::default())))
        .candidates((0..1000).collect())
        .compute_stats()
        .unwrap();

    milli_snap!(format!("{map:?}"), "candidates_0_1000", @r###"{"colour": (0.0, 1999.0)}"###);

    let map = FacetDistribution::new(&txn, &index)
        .facets(iter::once(("colour", OrderBy::default())))
        .candidates((217..777).collect())
        .compute_stats()
        .unwrap();

    milli_snap!(format!("{map:?}"), "candidates_217_777", @r###"{"colour": (217.0, 1776.0)}"###);
}

#[test]
fn facet_stats_mixed_array() {
    let index = TempIndex::new_with_map_size(4096 * 10_000);

    index
        .update_settings(|settings| {
            settings.set_filterable_fields(vec![FilterableAttributesRule::Field(S("colour"))])
        })
        .unwrap();

    let facet_values = (0..1000).collect::<Vec<_>>();

    let mut documents = vec![];
    for i in 0..1000 {
        let document = serde_json::json!({
            "id": i,
            "colour": [facet_values[i % 1000], format!("{}", facet_values[i % 1000] + 1000)],
        })
        .as_object()
        .unwrap()
        .clone();
        documents.push(document);
    }

    let documents = mmap_from_objects(documents);
    index.add_documents(documents).unwrap();

    let txn = index.read_txn().unwrap();

    let map = FacetDistribution::new(&txn, &index)
        .facets(iter::once(("colour", OrderBy::default())))
        .compute_stats()
        .unwrap();

    milli_snap!(format!("{map:?}"), "no_candidates", @"{}");

    let map = FacetDistribution::new(&txn, &index)
        .facets(iter::once(("colour", OrderBy::default())))
        .candidates((0..1000).collect())
        .compute_stats()
        .unwrap();

    milli_snap!(format!("{map:?}"), "candidates_0_1000", @r###"{"colour": (0.0, 999.0)}"###);

    let map = FacetDistribution::new(&txn, &index)
        .facets(iter::once(("colour", OrderBy::default())))
        .candidates((217..777).collect())
        .compute_stats()
        .unwrap();

    milli_snap!(format!("{map:?}"), "candidates_217_777", @r###"{"colour": (217.0, 776.0)}"###);
}

#[test]
fn facet_mixed_values() {
    let index = TempIndex::new_with_map_size(4096 * 10_000);

    index
        .update_settings(|settings| {
            settings.set_filterable_fields(vec![FilterableAttributesRule::Field(S("colour"))])
        })
        .unwrap();

    let facet_values = (0..1000).collect::<Vec<_>>();

    let mut documents = vec![];
    for i in 0..1000 {
        let document = if i % 2 == 0 {
            serde_json::json!({
                "id": i,
                "colour": [facet_values[i % 1000], facet_values[i % 1000] + 1000],
            })
        } else {
            serde_json::json!({
                "id": i,
                "colour": format!("{}", facet_values[i % 1000] + 10000),
            })
        };
        let document = document.as_object().unwrap().clone();
        documents.push(document);
    }

    let documents = mmap_from_objects(documents);
    index.add_documents(documents).unwrap();

    let txn = index.read_txn().unwrap();

    let map = FacetDistribution::new(&txn, &index)
        .facets(iter::once(("colour", OrderBy::default())))
        .compute_stats()
        .unwrap();

    milli_snap!(format!("{map:?}"), "no_candidates", @"{}");

    let map = FacetDistribution::new(&txn, &index)
        .facets(iter::once(("colour", OrderBy::default())))
        .candidates((0..1000).collect())
        .compute_stats()
        .unwrap();

    milli_snap!(format!("{map:?}"), "candidates_0_1000", @r###"{"colour": (0.0, 1998.0)}"###);

    let map = FacetDistribution::new(&txn, &index)
        .facets(iter::once(("colour", OrderBy::default())))
        .candidates((217..777).collect())
        .compute_stats()
        .unwrap();

    milli_snap!(format!("{map:?}"), "candidates_217_777", @r###"{"colour": (218.0, 1776.0)}"###);
}
