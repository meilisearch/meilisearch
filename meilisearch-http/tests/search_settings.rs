use assert_json_diff::assert_json_eq;
use serde_json::json;
use std::convert::Into;

mod common;

#[actix_rt::test]
async fn search_with_settings_basic() {
    let mut server = common::Server::test_server().await;

    let config = json!({
      "rankingRules": [
        "typo",
        "words",
        "proximity",
        "attribute",
        "wordsPosition",
        "desc(age)",
        "exactness",
        "desc(balance)"
      ],
      "distinctAttribute": null,
      "searchableAttributes": [
        "name",
        "age",
        "color",
        "gender",
        "email",
        "address",
        "about"
      ],
      "displayedAttributes": [
        "name",
        "age",
        "gender",
        "color",
        "email",
        "phone",
        "address",
        "balance"
      ],
      "stopWords": null,
      "synonyms": null,
    });

    server.update_all_settings(config).await;

    let query = "q=ea%20exercitation&limit=3";

    let expect = json!([
      {
        "balance": "$2,467.47",
        "age": 34,
        "color": "blue",
        "name": "Patricia Goff",
        "gender": "female",
        "email": "patriciagoff@chorizon.com",
        "phone": "+1 (864) 463-2277",
        "address": "866 Hornell Loop, Cresaptown, Ohio, 1700"
      },
      {
        "balance": "$3,344.40",
        "age": 35,
        "color": "blue",
        "name": "Adeline Flynn",
        "gender": "female",
        "email": "adelineflynn@chorizon.com",
        "phone": "+1 (994) 600-2840",
        "address": "428 Paerdegat Avenue, Hollymead, Pennsylvania, 948"
      },
      {
        "balance": "$3,394.96",
        "age": 25,
        "color": "blue",
        "name": "Aida Kirby",
        "gender": "female",
        "email": "aidakirby@chorizon.com",
        "phone": "+1 (942) 532-2325",
        "address": "797 Engert Avenue, Wilsonia, Idaho, 6532"
      }
    ]);

    let (response, _status_code) = server.search_get(query).await;
    assert_json_eq!(expect, response["hits"].clone(), ordered: false);
}

#[actix_rt::test]
async fn search_with_settings_stop_words() {
    let mut server = common::Server::test_server().await;

    let config = json!({
      "rankingRules": [
        "typo",
        "words",
        "proximity",
        "attribute",
        "wordsPosition",
        "desc(age)",
        "exactness",
        "desc(balance)"
      ],
      "distinctAttribute": null,
      "searchableAttributes": [
        "name",
        "age",
        "color",
        "gender",
        "email",
        "address",
        "about"
      ],
      "displayedAttributes": [
        "name",
        "age",
        "gender",
        "color",
        "email",
        "phone",
        "address",
        "balance"
      ],
      "stopWords": ["ea"],
      "synonyms": null,
    });

    server.update_all_settings(config).await;

    let query = "q=ea%20exercitation&limit=3";
    let expect = json!([
      {
        "balance": "$1,921.58",
        "age": 31,
        "color": "Green",
        "name": "Harper Carson",
        "gender": "male",
        "email": "harpercarson@chorizon.com",
        "phone": "+1 (912) 430-3243",
        "address": "883 Dennett Place, Knowlton, New Mexico, 9219"
      },
      {
        "balance": "$1,706.13",
        "age": 27,
        "color": "Green",
        "name": "Cherry Orr",
        "gender": "female",
        "email": "cherryorr@chorizon.com",
        "phone": "+1 (995) 479-3174",
        "address": "442 Beverly Road, Ventress, New Mexico, 3361"
      },
      {
        "balance": "$1,476.39",
        "age": 28,
        "color": "brown",
        "name": "Maureen Dale",
        "gender": "female",
        "email": "maureendale@chorizon.com",
        "phone": "+1 (984) 538-3684",
        "address": "817 Newton Street, Bannock, Wyoming, 1468"
      }
    ]);

    let (response, _status_code) = server.search_get(query).await;
    assert_json_eq!(expect, response["hits"].clone(), ordered: false);
}

#[actix_rt::test]
async fn search_with_settings_synonyms() {
    let mut server = common::Server::test_server().await;

    let config = json!({
      "rankingRules": [
        "typo",
        "words",
        "proximity",
        "attribute",
        "wordsPosition",
        "desc(age)",
        "exactness",
        "desc(balance)"
      ],
      "distinctAttribute": null,
      "searchableAttributes": [
        "name",
        "age",
        "color",
        "gender",
        "email",
        "address",
        "about"
      ],
      "displayedAttributes": [
        "name",
        "age",
        "gender",
        "color",
        "email",
        "phone",
        "address",
        "balance"
      ],
      "stopWords": null,
      "synonyms": {
          "Application": [
              "Exercitation"
          ]
      },
    });

    server.update_all_settings(config).await;

    let query = "q=application&limit=3";
    let expect = json!([
      {
        "balance": "$1,921.58",
        "age": 31,
        "color": "Green",
        "name": "Harper Carson",
        "gender": "male",
        "email": "harpercarson@chorizon.com",
        "phone": "+1 (912) 430-3243",
        "address": "883 Dennett Place, Knowlton, New Mexico, 9219"
      },
      {
        "balance": "$1,706.13",
        "age": 27,
        "color": "Green",
        "name": "Cherry Orr",
        "gender": "female",
        "email": "cherryorr@chorizon.com",
        "phone": "+1 (995) 479-3174",
        "address": "442 Beverly Road, Ventress, New Mexico, 3361"
      },
      {
        "balance": "$1,476.39",
        "age": 28,
        "color": "brown",
        "name": "Maureen Dale",
        "gender": "female",
        "email": "maureendale@chorizon.com",
        "phone": "+1 (984) 538-3684",
        "address": "817 Newton Street, Bannock, Wyoming, 1468"
      }
    ]);

    let (response, _status_code) = server.search_get(query).await;
    assert_json_eq!(expect, response["hits"].clone(), ordered: false);
}

#[actix_rt::test]
async fn search_with_settings_normalized_synonyms() {
    let mut server = common::Server::test_server().await;

    let config = json!({
      "rankingRules": [
        "typo",
        "words",
        "proximity",
        "attribute",
        "wordsPosition",
        "desc(age)",
        "exactness",
        "desc(balance)"
      ],
      "distinctAttribute": null,
      "searchableAttributes": [
        "name",
        "age",
        "color",
        "gender",
        "email",
        "address",
        "about"
      ],
      "displayedAttributes": [
        "name",
        "age",
        "gender",
        "color",
        "email",
        "phone",
        "address",
        "balance"
      ],
      "stopWords": null,
      "synonyms": {
          "application": [
              "exercitation"
          ]
      },
    });

    server.update_all_settings(config).await;

    let query = "q=application&limit=3";
    let expect = json!([
      {
        "balance": "$1,921.58",
        "age": 31,
        "color": "Green",
        "name": "Harper Carson",
        "gender": "male",
        "email": "harpercarson@chorizon.com",
        "phone": "+1 (912) 430-3243",
        "address": "883 Dennett Place, Knowlton, New Mexico, 9219"
      },
      {
        "balance": "$1,706.13",
        "age": 27,
        "color": "Green",
        "name": "Cherry Orr",
        "gender": "female",
        "email": "cherryorr@chorizon.com",
        "phone": "+1 (995) 479-3174",
        "address": "442 Beverly Road, Ventress, New Mexico, 3361"
      },
      {
        "balance": "$1,476.39",
        "age": 28,
        "color": "brown",
        "name": "Maureen Dale",
        "gender": "female",
        "email": "maureendale@chorizon.com",
        "phone": "+1 (984) 538-3684",
        "address": "817 Newton Street, Bannock, Wyoming, 1468"
      }
    ]);

    let (response, _status_code) = server.search_get(query).await;
    assert_json_eq!(expect, response["hits"].clone(), ordered: false);
}

#[actix_rt::test]
async fn search_with_settings_ranking_rules() {
    let mut server = common::Server::test_server().await;

    let config = json!({
      "rankingRules": [
        "typo",
        "words",
        "proximity",
        "attribute",
        "wordsPosition",
        "desc(age)",
        "exactness",
        "desc(balance)"
      ],
      "distinctAttribute": null,
      "searchableAttributes": [
        "name",
        "age",
        "color",
        "gender",
        "email",
        "address",
        "about"
      ],
      "displayedAttributes": [
        "name",
        "age",
        "gender",
        "color",
        "email",
        "phone",
        "address",
        "balance"
      ],
      "stopWords": null,
      "synonyms": null,
    });

    server.update_all_settings(config).await;

    let query = "q=exarcitation&limit=3";
    let expect = json!([
      {
        "balance": "$1,921.58",
        "age": 31,
        "color": "Green",
        "name": "Harper Carson",
        "gender": "male",
        "email": "harpercarson@chorizon.com",
        "phone": "+1 (912) 430-3243",
        "address": "883 Dennett Place, Knowlton, New Mexico, 9219"
      },
      {
        "balance": "$1,706.13",
        "age": 27,
        "color": "Green",
        "name": "Cherry Orr",
        "gender": "female",
        "email": "cherryorr@chorizon.com",
        "phone": "+1 (995) 479-3174",
        "address": "442 Beverly Road, Ventress, New Mexico, 3361"
      },
      {
        "balance": "$1,476.39",
        "age": 28,
        "color": "brown",
        "name": "Maureen Dale",
        "gender": "female",
        "email": "maureendale@chorizon.com",
        "phone": "+1 (984) 538-3684",
        "address": "817 Newton Street, Bannock, Wyoming, 1468"
      }
    ]);

    let (response, _status_code) = server.search_get(query).await;
    println!("{}", response["hits"].clone());
    assert_json_eq!(expect, response["hits"].clone(), ordered: false);
}

#[actix_rt::test]
async fn search_with_settings_searchable_attributes() {
    let mut server = common::Server::test_server().await;

    let config = json!({
      "rankingRules": [
        "typo",
        "words",
        "proximity",
        "attribute",
        "wordsPosition",
        "desc(age)",
        "exactness",
        "desc(balance)"
      ],
      "distinctAttribute": null,
      "searchableAttributes": [
        "age",
        "color",
        "gender",
        "address",
        "about"
      ],
      "displayedAttributes": [
        "name",
        "age",
        "gender",
        "color",
        "email",
        "phone",
        "address",
        "balance"
      ],
      "stopWords": null,
      "synonyms": {
          "exarcitation": [
              "exercitation"
          ]
      },
    });

    server.update_all_settings(config).await;

    let query = "q=Carol&limit=3";
    let expect = json!([
      {
        "balance": "$1,440.09",
        "age": 40,
        "color": "blue",
        "name": "Levy Whitley",
        "gender": "male",
        "email": "levywhitley@chorizon.com",
        "phone": "+1 (911) 458-2411",
        "address": "187 Thomas Street, Hachita, North Carolina, 2989"
      },
      {
        "balance": "$1,977.66",
        "age": 36,
        "color": "brown",
        "name": "Combs Stanley",
        "gender": "male",
        "email": "combsstanley@chorizon.com",
        "phone": "+1 (827) 419-2053",
        "address": "153 Beverley Road, Siglerville, South Carolina, 3666"
      }
    ]);

    let (response, _status_code) = server.search_get(query).await;
    assert_json_eq!(expect, response["hits"].clone(), ordered: false);
}

#[actix_rt::test]
async fn search_with_settings_displayed_attributes() {
    let mut server = common::Server::test_server().await;

    let config = json!({
      "rankingRules": [
        "typo",
        "words",
        "proximity",
        "attribute",
        "wordsPosition",
        "desc(age)",
        "exactness",
        "desc(balance)"
      ],
      "distinctAttribute": null,
      "searchableAttributes": [
        "age",
        "color",
        "gender",
        "address",
        "about"
      ],
      "displayedAttributes": [
        "name",
        "age",
        "gender",
        "color",
        "email",
        "phone"
      ],
      "stopWords": null,
      "synonyms": null,
    });

    server.update_all_settings(config).await;

    let query = "q=exercitation&limit=3";
    let expect = json!([
      {
        "age": 31,
        "color": "Green",
        "name": "Harper Carson",
        "gender": "male",
        "email": "harpercarson@chorizon.com",
        "phone": "+1 (912) 430-3243"
      },
      {
        "age": 27,
        "color": "Green",
        "name": "Cherry Orr",
        "gender": "female",
        "email": "cherryorr@chorizon.com",
        "phone": "+1 (995) 479-3174"
      },
      {
        "age": 28,
        "color": "brown",
        "name": "Maureen Dale",
        "gender": "female",
        "email": "maureendale@chorizon.com",
        "phone": "+1 (984) 538-3684"
      }
    ]);

    let (response, _status_code) = server.search_get(query).await;
    assert_json_eq!(expect, response["hits"].clone(), ordered: false);
}

#[actix_rt::test]
async fn search_with_settings_searchable_attributes_2() {
    let mut server = common::Server::test_server().await;

    let config = json!({
      "rankingRules": [
        "typo",
        "words",
        "proximity",
        "attribute",
        "wordsPosition",
        "desc(age)",
        "exactness",
        "desc(balance)"
      ],
      "distinctAttribute": null,
      "searchableAttributes": [
        "age",
        "color",
        "gender",
        "address",
        "about"
      ],
      "displayedAttributes": [
        "name",
        "age",
        "gender"
      ],
      "stopWords": null,
      "synonyms": null,
    });

    server.update_all_settings(config).await;

    let query = "q=exercitation&limit=3";
    let expect = json!([
      {
        "age": 31,
        "name": "Harper Carson",
        "gender": "male"
      },
      {
        "age": 27,
        "name": "Cherry Orr",
        "gender": "female"
      },
      {
        "age": 28,
        "name": "Maureen Dale",
        "gender": "female"
      }
    ]);

    let (response, _status_code) = server.search_get(query).await;
    assert_json_eq!(expect, response["hits"].clone(), ordered: false);
}

// issue #798
#[actix_rt::test]
async fn distinct_attributes_returns_name_not_id() {
    let mut server = common::Server::test_server().await;
    let settings = json!({
        "distinctAttribute": "color",
    });
    server.update_all_settings(settings).await;
    let (response, _) = server.get_all_settings().await;
    assert_eq!(response["distinctAttribute"], "color");
    let (response, _) = server.get_distinct_attribute().await;
    assert_eq!(response, "color");
}
