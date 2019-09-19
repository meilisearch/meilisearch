#[macro_use] extern crate maplit;

mod common;

use big_s::S;
use meilidb_data::RankingOrdering;

#[test]
fn stop_words() {
    let index = common::simple_index();
    let stop_words = hashset!{ S("le"), S("la"), S("les"), };
    index.custom_settings().set_stop_words(&stop_words).unwrap();
    let ret_stop_words = index.custom_settings().get_stop_words().unwrap().unwrap();
    assert_eq!(ret_stop_words, stop_words);
}

#[test]
fn ranking_order() {
    let index = common::simple_index();
    let ranking_order = vec![S("SumOfTypos"), S("NumberOfWords"), S("WordsProximity"), S("SumOfWordsAttribute"), S("SumOfWordsPosition"), S("Exact"), S("DocumentId")];
    index.custom_settings().set_ranking_order(&ranking_order).unwrap();
    let ret_ranking_orderer = index.custom_settings().get_ranking_order().unwrap().unwrap();
    assert_eq!(ret_ranking_orderer, ranking_order);
}

#[test]
fn distinct_field() {
    let index = common::simple_index();
    let distinct_field = S("title");
    index.custom_settings().set_distinct_field(&distinct_field).unwrap();
    let ret_distinct_field = index.custom_settings().get_distinct_field().unwrap().unwrap();
    assert_eq!(ret_distinct_field, distinct_field);
}

#[test]
fn ranking_rules() {
    let index = common::simple_index();
    let ranking_rules = hashmap!{ S("objectId") => RankingOrdering::Asc };
    index.custom_settings().set_ranking_rules(&ranking_rules).unwrap();
    let ret_ranking_rules = index.custom_settings().get_ranking_rules().unwrap().unwrap();
    assert_eq!(ret_ranking_rules, ranking_rules);
}

