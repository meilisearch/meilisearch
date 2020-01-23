// use std::time::Duration;
// use std::convert::Into;

use assert_json_diff::assert_json_eq;
use async_std::io::prelude::*;
use async_std::task::block_on;
use http_service::Body;
use serde_json::json;
use serde_json::Value;

mod common;

#[test]
fn basic_search() {
    let mut server = common::setup_server().unwrap();

    common::enrich_server_with_movies_index(&mut server).unwrap();
    common::enrich_server_with_movies_settings(&mut server).unwrap();
    common::enrich_server_with_movies_documents(&mut server).unwrap();

    // 1 - Simple search
    // q: Captain
    // limit: 3

    let req = http::Request::get("/indexes/movies/search?q=captain&limit=3")
        .body(Body::empty())
        .unwrap();
    let res = server.simulate(req).unwrap();

    let mut buf = Vec::new();
    block_on(res.into_body().read_to_end(&mut buf)).unwrap();
    let res_value: Value = serde_json::from_slice(&buf).unwrap();
    println!("res: {:?}", res_value);

    let res_expected = json!([
      {
        "id": 299537,
        "popularity": 44.726,
        "vote_average": 7.0,
        "title": "Captain Marvel",
        "tagline": "Higher. Further. Faster.",
        "overview": "The story follows Carol Danvers as she becomes one of the universe’s most powerful heroes when Earth is caught in the middle of a galactic war between two alien races. Set in the 1990s, Captain Marvel is an all-new adventure from a previously unseen period in the history of the Marvel Cinematic Universe.",
        "director": "Ryan Fleck",
        "producer": "Kevin Feige",
        "genres": [
          "Action",
          "Adventure",
          "Science Fiction"
        ],
        "poster_path": "https://image.tmdb.org/t/p/w500/AtsgWhDnHTq68L0lLsUrCnM7TjG.jpg",
        "vote_count": 7858
      },
      {
        "id": 271110,
        "popularity": 37.431,
        "vote_average": 7.4,
        "title": "Captain America: Civil War",
        "tagline": "Divided We Fall",
        "overview": "Following the events of Age of Ultron, the collective governments of the world pass an act designed to regulate all superhuman activity. This polarizes opinion amongst the Avengers, causing two factions to side with Iron Man or Captain America, which causes an epic battle between former allies.",
        "director": "Anthony Russo",
        "producer": "Kevin Feige",
        "genres": [
          "Adventure",
          "Action",
          "Science Fiction"
        ],
        "poster_path": "https://image.tmdb.org/t/p/w500/kSBXou5Ac7vEqKd97wotJumyJvU.jpg",
        "vote_count": 15079
      },
      {
        "id": 1771,
        "popularity": 19.657,
        "vote_average": 6.9,
        "title": "Captain America: The First Avenger",
        "tagline": "When patriots become heroes",
        "overview": "During World War II, Steve Rogers is a sickly man from Brooklyn who's transformed into super-soldier Captain America to aid in the war effort. Rogers must stop the Red Skull – Adolf Hitler's ruthless head of weaponry, and the leader of an organization that intends to use a mysterious device of untold powers for world domination.",
        "director": "Joe Johnston",
        "producer": "Kevin Feige",
        "genres": [
          "Action",
          "Adventure",
          "Science Fiction"
        ],
        "poster_path": "https://image.tmdb.org/t/p/w500/vSNxAJTlD0r02V9sPYpOjqDZXUK.jpg",
        "vote_count": 13853
      }
    ]);

    assert_json_eq!(res_expected, res_value["hits"].clone(), ordered: false);

    // 1 - Simple search with offset
    // q: Captain
    // limit: 3
    // offset: 1

    let req = http::Request::get("/indexes/movies/search?q=captain&limit=3&offset=1")
        .body(Body::empty())
        .unwrap();
    let res = server.simulate(req).unwrap();

    let mut buf = Vec::new();
    block_on(res.into_body().read_to_end(&mut buf)).unwrap();
    let res_value: Value = serde_json::from_slice(&buf).unwrap();
    println!("res: {:?}", res_value);

    let res_expected = json!([
      {
        "id": 271110,
        "popularity": 37.431,
        "vote_average": 7.4,
        "title": "Captain America: Civil War",
        "tagline": "Divided We Fall",
        "overview": "Following the events of Age of Ultron, the collective governments of the world pass an act designed to regulate all superhuman activity. This polarizes opinion amongst the Avengers, causing two factions to side with Iron Man or Captain America, which causes an epic battle between former allies.",
        "director": "Anthony Russo",
        "producer": "Kevin Feige",
        "genres": [
          "Adventure",
          "Action",
          "Science Fiction"
        ],
        "poster_path": "https://image.tmdb.org/t/p/w500/kSBXou5Ac7vEqKd97wotJumyJvU.jpg",
        "vote_count": 15079
      },
      {
        "id": 1771,
        "popularity": 19.657,
        "vote_average": 6.9,
        "title": "Captain America: The First Avenger",
        "tagline": "When patriots become heroes",
        "overview": "During World War II, Steve Rogers is a sickly man from Brooklyn who's transformed into super-soldier Captain America to aid in the war effort. Rogers must stop the Red Skull – Adolf Hitler's ruthless head of weaponry, and the leader of an organization that intends to use a mysterious device of untold powers for world domination.",
        "director": "Joe Johnston",
        "producer": "Kevin Feige",
        "genres": [
          "Action",
          "Adventure",
          "Science Fiction"
        ],
        "poster_path": "https://image.tmdb.org/t/p/w500/vSNxAJTlD0r02V9sPYpOjqDZXUK.jpg",
        "vote_count": 13853
      },
      {
        "id": 268531,
        "popularity": 16.859,
        "vote_average": 6.0,
        "title": "Captain Underpants: The First Epic Movie",
        "tagline": "",
        "overview": "Two mischievous kids hypnotize their mean elementary school principal and turn him into their comic book creation, the kind-hearted and elastic-banded Captain Underpants.",
        "director": "David Soren",
        "producer": "Chris Finnegan",
        "genres": [
          "Action",
          "Animation",
          "Comedy",
          "Family"
        ],
        "poster_path": "https://image.tmdb.org/t/p/w500/AjHZIkzhPXrRNE4VSLVWx6dirK9.jpg",
        "vote_count": 653
      }
    ]);

    assert_json_eq!(res_expected, res_value["hits"].clone(), ordered: false);

    // 1 - Simple search with attribute to highlight all
    // q: Captain
    // limit: 1
    // attributeToHighlight: *

    let query = "q=captain&limit=1&attributesToHighlight=*";

    let req = http::Request::get(format!("/indexes/movies/search?{}", query))
        .body(Body::empty())
        .unwrap();
    let res = server.simulate(req).unwrap();

    let mut buf = Vec::new();
    block_on(res.into_body().read_to_end(&mut buf)).unwrap();
    let res_value: Value = serde_json::from_slice(&buf).unwrap();
    println!("res: {:?}", res_value);

    let res_expected = json!([
      {
        "id": 299537,
        "popularity": 44.726,
        "vote_average": 7.0,
        "title": "Captain Marvel",
        "tagline": "Higher. Further. Faster.",
        "overview": "The story follows Carol Danvers as she becomes one of the universe’s most powerful heroes when Earth is caught in the middle of a galactic war between two alien races. Set in the 1990s, Captain Marvel is an all-new adventure from a previously unseen period in the history of the Marvel Cinematic Universe.",
        "director": "Ryan Fleck",
        "producer": "Kevin Feige",
        "genres": [
          "Action",
          "Adventure",
          "Science Fiction"
        ],
        "poster_path": "https://image.tmdb.org/t/p/w500/AtsgWhDnHTq68L0lLsUrCnM7TjG.jpg",
        "vote_count": 7858,
        "_formatted": {
          "id": 299537,
          "popularity": 44.726,
          "vote_average": 7.0,
          "title": "<em>Captain</em> Marvel",
          "tagline": "Higher. Further. Faster.",
          "overview": "The story follows Carol Danvers as she becomes one of the universe’s most powerful heroes when Earth is caught in the middle of a galactic war between two alien races. Set in the 1990s, <em>Captain</em> Marvel is an all-new adventure from a previously unseen period in the history of the Marvel Cinematic Universe.",
          "director": "Ryan Fleck",
          "producer": "Kevin Feige",
          "genres": [
            "Action",
            "Adventure",
            "Science Fiction"
          ],
          "poster_path": "https://image.tmdb.org/t/p/w500/AtsgWhDnHTq68L0lLsUrCnM7TjG.jpg",
          "vote_count": 7858
        }
      }
    ]);

    assert_json_eq!(res_expected, res_value["hits"].clone(), ordered: false);

    // 1 - Simple search with attribute to highlight title
    // q: Captain
    // limit: 1
    // attributeToHighlight: title

    let query = "q=captain&limit=1&attributesToHighlight=title";

    let req = http::Request::get(format!("/indexes/movies/search?{}", query))
        .body(Body::empty())
        .unwrap();
    let res = server.simulate(req).unwrap();

    let mut buf = Vec::new();
    block_on(res.into_body().read_to_end(&mut buf)).unwrap();
    let res_value: Value = serde_json::from_slice(&buf).unwrap();
    println!("res: {:?}", res_value);

    let res_expected = json!([
      {
        "id": 299537,
        "popularity": 44.726,
        "vote_average": 7.0,
        "title": "Captain Marvel",
        "tagline": "Higher. Further. Faster.",
        "overview": "The story follows Carol Danvers as she becomes one of the universe’s most powerful heroes when Earth is caught in the middle of a galactic war between two alien races. Set in the 1990s, Captain Marvel is an all-new adventure from a previously unseen period in the history of the Marvel Cinematic Universe.",
        "director": "Ryan Fleck",
        "producer": "Kevin Feige",
        "genres": [
          "Action",
          "Adventure",
          "Science Fiction"
        ],
        "poster_path": "https://image.tmdb.org/t/p/w500/AtsgWhDnHTq68L0lLsUrCnM7TjG.jpg",
        "vote_count": 7858,
        "_formatted": {
          "id": 299537,
          "popularity": 44.726,
          "vote_average": 7.0,
          "title": "<em>Captain</em> Marvel",
          "tagline": "Higher. Further. Faster.",
          "overview": "The story follows Carol Danvers as she becomes one of the universe’s most powerful heroes when Earth is caught in the middle of a galactic war between two alien races. Set in the 1990s, Captain Marvel is an all-new adventure from a previously unseen period in the history of the Marvel Cinematic Universe.",
          "director": "Ryan Fleck",
          "producer": "Kevin Feige",
          "genres": [
            "Action",
            "Adventure",
            "Science Fiction"
          ],
          "poster_path": "https://image.tmdb.org/t/p/w500/AtsgWhDnHTq68L0lLsUrCnM7TjG.jpg",
          "vote_count": 7858
        }
      }
    ]);

    assert_json_eq!(res_expected, res_value["hits"].clone(), ordered: false);

    // 1 - Simple search with attribute to highlight title and tagline
    // q: Captain
    // limit: 1
    // attributeToHighlight: title,tagline

    let query = "q=captain&limit=1&attributesToHighlight=title,tagline";

    let req = http::Request::get(format!("/indexes/movies/search?{}", query))
        .body(Body::empty())
        .unwrap();
    let res = server.simulate(req).unwrap();

    let mut buf = Vec::new();
    block_on(res.into_body().read_to_end(&mut buf)).unwrap();
    let res_value: Value = serde_json::from_slice(&buf).unwrap();
    println!("res: {:?}", res_value);

    let res_expected = json!([
      {
        "id": 299537,
        "popularity": 44.726,
        "vote_average": 7.0,
        "title": "Captain Marvel",
        "tagline": "Higher. Further. Faster.",
        "overview": "The story follows Carol Danvers as she becomes one of the universe’s most powerful heroes when Earth is caught in the middle of a galactic war between two alien races. Set in the 1990s, Captain Marvel is an all-new adventure from a previously unseen period in the history of the Marvel Cinematic Universe.",
        "director": "Ryan Fleck",
        "producer": "Kevin Feige",
        "genres": [
          "Action",
          "Adventure",
          "Science Fiction"
        ],
        "poster_path": "https://image.tmdb.org/t/p/w500/AtsgWhDnHTq68L0lLsUrCnM7TjG.jpg",
        "vote_count": 7858,
        "_formatted": {
          "id": 299537,
          "popularity": 44.726,
          "vote_average": 7.0,
          "title": "<em>Captain</em> Marvel",
          "tagline": "Higher. Further. Faster.",
          "overview": "The story follows Carol Danvers as she becomes one of the universe’s most powerful heroes when Earth is caught in the middle of a galactic war between two alien races. Set in the 1990s, Captain Marvel is an all-new adventure from a previously unseen period in the history of the Marvel Cinematic Universe.",
          "director": "Ryan Fleck",
          "producer": "Kevin Feige",
          "genres": [
            "Action",
            "Adventure",
            "Science Fiction"
          ],
          "poster_path": "https://image.tmdb.org/t/p/w500/AtsgWhDnHTq68L0lLsUrCnM7TjG.jpg",
          "vote_count": 7858
        }
      }
    ]);

    assert_json_eq!(res_expected, res_value["hits"].clone(), ordered: false);

    // 1 - Simple search with attribute to highlight title and overview
    // q: Captain
    // limit: 1
    // attributeToHighlight: title,overview

    let query = "q=captain&limit=1&attributesToHighlight=title,overview";

    let req = http::Request::get(format!("/indexes/movies/search?{}", query))
        .body(Body::empty())
        .unwrap();
    let res = server.simulate(req).unwrap();

    let mut buf = Vec::new();
    block_on(res.into_body().read_to_end(&mut buf)).unwrap();
    let res_value: Value = serde_json::from_slice(&buf).unwrap();
    println!("res: {:?}", res_value);

    let res_expected = json!([
      {
        "id": 299537,
        "popularity": 44.726,
        "vote_average": 7.0,
        "title": "Captain Marvel",
        "tagline": "Higher. Further. Faster.",
        "overview": "The story follows Carol Danvers as she becomes one of the universe’s most powerful heroes when Earth is caught in the middle of a galactic war between two alien races. Set in the 1990s, Captain Marvel is an all-new adventure from a previously unseen period in the history of the Marvel Cinematic Universe.",
        "director": "Ryan Fleck",
        "producer": "Kevin Feige",
        "genres": [
          "Action",
          "Adventure",
          "Science Fiction"
        ],
        "poster_path": "https://image.tmdb.org/t/p/w500/AtsgWhDnHTq68L0lLsUrCnM7TjG.jpg",
        "vote_count": 7858,
        "_formatted": {
          "id": 299537,
          "popularity": 44.726,
          "vote_average": 7.0,
          "title": "<em>Captain</em> Marvel",
          "tagline": "Higher. Further. Faster.",
          "overview": "The story follows Carol Danvers as she becomes one of the universe’s most powerful heroes when Earth is caught in the middle of a galactic war between two alien races. Set in the 1990s, <em>Captain</em> Marvel is an all-new adventure from a previously unseen period in the history of the Marvel Cinematic Universe.",
          "director": "Ryan Fleck",
          "producer": "Kevin Feige",
          "genres": [
            "Action",
            "Adventure",
            "Science Fiction"
          ],
          "poster_path": "https://image.tmdb.org/t/p/w500/AtsgWhDnHTq68L0lLsUrCnM7TjG.jpg",
          "vote_count": 7858
        }
      }
    ]);

    assert_json_eq!(res_expected, res_value["hits"].clone(), ordered: false);

    // 1 - Simple search with matches
    // q: Captain
    // limit: 1
    // matches: true

    let query = "q=captain&limit=1&matches=true";

    let req = http::Request::get(format!("/indexes/movies/search?{}", query))
        .body(Body::empty())
        .unwrap();
    let res = server.simulate(req).unwrap();

    let mut buf = Vec::new();
    block_on(res.into_body().read_to_end(&mut buf)).unwrap();
    let res_value: Value = serde_json::from_slice(&buf).unwrap();
    println!("res: {:?}", res_value);

    let res_expected = json!([
      {
        "id": 299537,
        "popularity": 44.726,
        "vote_average": 7.0,
        "title": "Captain Marvel",
        "tagline": "Higher. Further. Faster.",
        "overview": "The story follows Carol Danvers as she becomes one of the universe’s most powerful heroes when Earth is caught in the middle of a galactic war between two alien races. Set in the 1990s, Captain Marvel is an all-new adventure from a previously unseen period in the history of the Marvel Cinematic Universe.",
        "director": "Ryan Fleck",
        "producer": "Kevin Feige",
        "genres": [
          "Action",
          "Adventure",
          "Science Fiction"
        ],
        "poster_path": "https://image.tmdb.org/t/p/w500/AtsgWhDnHTq68L0lLsUrCnM7TjG.jpg",
        "vote_count": 7858,
        "_matchesInfo": {
          "title": [
            {
              "start": 0,
              "length": 7
            }
          ],
          "overview": [
            {
              "start": 186,
              "length": 7
            }
          ]
        }
      }
    ]);

    assert_json_eq!(res_expected, res_value["hits"].clone(), ordered: false);

    // 1 - Simple search with crop
    // q: Captain
    // limit: 1
    // attributesToCrop: overview
    // cropLength: 20

    let query = "q=captain&limit=1&attributesToCrop=overview&cropLength=20";

    let req = http::Request::get(format!("/indexes/movies/search?{}", query))
        .body(Body::empty())
        .unwrap();
    let res = server.simulate(req).unwrap();

    let mut buf = Vec::new();
    block_on(res.into_body().read_to_end(&mut buf)).unwrap();
    let res_value: Value = serde_json::from_slice(&buf).unwrap();
    println!("res: {:?}", res_value);

    let res_expected = json!([
      {
        "id": 299537,
        "popularity": 44.726,
        "vote_average": 7.0,
        "title": "Captain Marvel",
        "tagline": "Higher. Further. Faster.",
        "overview": "The story follows Carol Danvers as she becomes one of the universe’s most powerful heroes when Earth is caught in the middle of a galactic war between two alien races. Set in the 1990s, Captain Marvel is an all-new adventure from a previously unseen period in the history of the Marvel Cinematic Universe.",
        "director": "Ryan Fleck",
        "producer": "Kevin Feige",
        "genres": [
          "Action",
          "Adventure",
          "Science Fiction"
        ],
        "poster_path": "https://image.tmdb.org/t/p/w500/AtsgWhDnHTq68L0lLsUrCnM7TjG.jpg",
        "vote_count": 7858,
        "_formatted": {
          "id": 299537,
          "popularity": 44.726,
          "vote_average": 7.0,
          "title": "Captain Marvel",
          "tagline": "Higher. Further. Faster.",
          "overview": ". Set in the 1990s, Captain Marvel is an",
          "director": "Ryan Fleck",
          "producer": "Kevin Feige",
          "genres": [
            "Action",
            "Adventure",
            "Science Fiction"
          ],
          "poster_path": "https://image.tmdb.org/t/p/w500/AtsgWhDnHTq68L0lLsUrCnM7TjG.jpg",
          "vote_count": 7858
        }
      }
    ]);

    assert_json_eq!(res_expected, res_value["hits"].clone(), ordered: false);

    // 1 - Simple search with attributes to retrieve
    // q: Captain
    // limit: 1
    // attributesToRetrieve: [title,tagline,overview,poster_path]

    let query = "q=captain&limit=1&attributesToRetrieve=title,tagline,overview,poster_path";

    let req = http::Request::get(format!("/indexes/movies/search?{}", query))
        .body(Body::empty())
        .unwrap();
    let res = server.simulate(req).unwrap();

    let mut buf = Vec::new();
    block_on(res.into_body().read_to_end(&mut buf)).unwrap();
    let res_value: Value = serde_json::from_slice(&buf).unwrap();
    println!("res: {:?}", res_value);

    let res_expected = json!([
      {
        "title": "Captain Marvel",
        "tagline": "Higher. Further. Faster.",
        "overview": "The story follows Carol Danvers as she becomes one of the universe’s most powerful heroes when Earth is caught in the middle of a galactic war between two alien races. Set in the 1990s, Captain Marvel is an all-new adventure from a previously unseen period in the history of the Marvel Cinematic Universe.",
        "poster_path": "https://image.tmdb.org/t/p/w500/AtsgWhDnHTq68L0lLsUrCnM7TjG.jpg"
      }
    ]);

    assert_json_eq!(res_expected, res_value["hits"].clone(), ordered: false);

    // 1 - Simple search with filter
    // q: Captain
    // limit: 1
    // filters: director:Anthony%20Russo

    let query = "q=captain&limit=3&filters=director:Anthony%20Russo";

    let req = http::Request::get(format!("/indexes/movies/search?{}", query))
        .body(Body::empty())
        .unwrap();
    let res = server.simulate(req).unwrap();

    let mut buf = Vec::new();
    block_on(res.into_body().read_to_end(&mut buf)).unwrap();
    let res_value: Value = serde_json::from_slice(&buf).unwrap();
    println!("res: {:?}", res_value);

    let res_expected = json!([
      {
        "id": 271110,
        "popularity": 37.431,
        "vote_average": 7.4,
        "title": "Captain America: Civil War",
        "tagline": "Divided We Fall",
        "overview": "Following the events of Age of Ultron, the collective governments of the world pass an act designed to regulate all superhuman activity. This polarizes opinion amongst the Avengers, causing two factions to side with Iron Man or Captain America, which causes an epic battle between former allies.",
        "director": "Anthony Russo",
        "producer": "Kevin Feige",
        "genres": [
          "Adventure",
          "Action",
          "Science Fiction"
        ],
        "poster_path": "https://image.tmdb.org/t/p/w500/kSBXou5Ac7vEqKd97wotJumyJvU.jpg",
        "vote_count": 15079
      },
      {
        "id": 100402,
        "popularity": 16.418,
        "vote_average": 7.7,
        "title": "Captain America: The Winter Soldier",
        "tagline": "In heroes we trust.",
        "overview": "After the cataclysmic events in New York with The Avengers, Steve Rogers, aka Captain America is living quietly in Washington, D.C. and trying to adjust to the modern world. But when a S.H.I.E.L.D. colleague comes under attack, Steve becomes embroiled in a web of intrigue that threatens to put the world at risk. Joining forces with the Black Widow, Captain America struggles to expose the ever-widening conspiracy while fighting off professional assassins sent to silence him at every turn. When the full scope of the villainous plot is revealed, Captain America and the Black Widow enlist the help of a new ally, the Falcon. However, they soon find themselves up against an unexpected and formidable enemy—the Winter Soldier.",
        "director": "Anthony Russo",
        "producer": "Kevin Feige",
        "genres": [
          "Action",
          "Adventure",
          "Science Fiction"
        ],
        "poster_path": "https://image.tmdb.org/t/p/w500/5TQ6YDmymBpnF005OyoB7ohZps9.jpg",
        "vote_count": 11972
      },
      {
        "id": 299534,
        "popularity": 38.659,
        "vote_average": 8.3,
        "title": "Avengers: Endgame",
        "tagline": "Part of the journey is the end.",
        "overview": "After the devastating events of Avengers: Infinity War, the universe is in ruins due to the efforts of the Mad Titan, Thanos. With the help of remaining allies, the Avengers must assemble once more in order to undo Thanos' actions and restore order to the universe once and for all, no matter what consequences may be in store.",
        "director": "Anthony Russo",
        "producer": "Kevin Feige",
        "genres": [
          "Adventure",
          "Science Fiction",
          "Action"
        ],
        "poster_path": "https://image.tmdb.org/t/p/w500/or06FN3Dka5tukK1e9sl16pB3iy.jpg",
        "vote_count": 10497
      }
    ]);

    assert_json_eq!(res_expected, res_value["hits"].clone(), ordered: false);

    // 1 - Simple search with attributes to highlight and matches
    // q: Captain
    // limit: 1
    // attributesToHighlight: [title,overview]
    // matches: true

    let query = "q=captain&limit=1&attributesToHighlight=title,overview&matches=true";

    let req = http::Request::get(format!("/indexes/movies/search?{}", query))
        .body(Body::empty())
        .unwrap();
    let res = server.simulate(req).unwrap();

    let mut buf = Vec::new();
    block_on(res.into_body().read_to_end(&mut buf)).unwrap();
    let res_value: Value = serde_json::from_slice(&buf).unwrap();
    println!("res: {:?}", res_value);

    let res_expected = json!( [
      {
        "id": 299537,
        "popularity": 44.726,
        "vote_average": 7.0,
        "title": "Captain Marvel",
        "tagline": "Higher. Further. Faster.",
        "overview": "The story follows Carol Danvers as she becomes one of the universe’s most powerful heroes when Earth is caught in the middle of a galactic war between two alien races. Set in the 1990s, Captain Marvel is an all-new adventure from a previously unseen period in the history of the Marvel Cinematic Universe.",
        "director": "Ryan Fleck",
        "producer": "Kevin Feige",
        "genres": [
          "Action",
          "Adventure",
          "Science Fiction"
        ],
        "poster_path": "https://image.tmdb.org/t/p/w500/AtsgWhDnHTq68L0lLsUrCnM7TjG.jpg",
        "vote_count": 7858,
        "_formatted": {
          "id": 299537,
          "popularity": 44.726,
          "vote_average": 7.0,
          "title": "<em>Captain</em> Marvel",
          "tagline": "Higher. Further. Faster.",
          "overview": "The story follows Carol Danvers as she becomes one of the universe’s most powerful heroes when Earth is caught in the middle of a galactic war between two alien races. Set in the 1990s, <em>Captain</em> Marvel is an all-new adventure from a previously unseen period in the history of the Marvel Cinematic Universe.",
          "director": "Ryan Fleck",
          "producer": "Kevin Feige",
          "genres": [
            "Action",
            "Adventure",
            "Science Fiction"
          ],
          "poster_path": "https://image.tmdb.org/t/p/w500/AtsgWhDnHTq68L0lLsUrCnM7TjG.jpg",
          "vote_count": 7858
        },
        "_matchesInfo": {
          "overview": [
            {
              "start": 186,
              "length": 7
            }
          ],
          "title": [
            {
              "start": 0,
              "length": 7
            }
          ]
        }
      }
    ]);

    assert_json_eq!(res_expected, res_value["hits"].clone(), ordered: false);

    // 1 - Simple search with attributes to highlight and matches and crop
    // q: Captain
    // limit: 1
    // attributesToHighlight: [title,overview]
    // matches: true
    // cropLength: 20
    // attributesToCrop: overview

    let query = "q=captain&limit=1&attributesToCrop=overview&cropLength=20&attributesToHighlight=title,overview&matches=true";

    let req = http::Request::get(format!("/indexes/movies/search?{}", query))
        .body(Body::empty())
        .unwrap();
    let res = server.simulate(req).unwrap();

    let mut buf = Vec::new();
    block_on(res.into_body().read_to_end(&mut buf)).unwrap();
    let res_value: Value = serde_json::from_slice(&buf).unwrap();
    println!("res: {:?}", res_value);

    let res_expected = json!([
      {
        "id": 299537,
        "popularity": 44.726,
        "vote_average": 7.0,
        "title": "Captain Marvel",
        "tagline": "Higher. Further. Faster.",
        "overview": "The story follows Carol Danvers as she becomes one of the universe’s most powerful heroes when Earth is caught in the middle of a galactic war between two alien races. Set in the 1990s, Captain Marvel is an all-new adventure from a previously unseen period in the history of the Marvel Cinematic Universe.",
        "director": "Ryan Fleck",
        "producer": "Kevin Feige",
        "genres": [
          "Action",
          "Adventure",
          "Science Fiction"
        ],
        "poster_path": "https://image.tmdb.org/t/p/w500/AtsgWhDnHTq68L0lLsUrCnM7TjG.jpg",
        "vote_count": 7858,
        "_formatted": {
          "id": 299537,
          "popularity": 44.726,
          "vote_average": 7.0,
          "title": "<em>Captain</em> Marvel",
          "tagline": "Higher. Further. Faster.",
          "overview": ". Set in the 1990s, <em>Captain</em> Marvel is an",
          "director": "Ryan Fleck",
          "producer": "Kevin Feige",
          "genres": [
            "Action",
            "Adventure",
            "Science Fiction"
          ],
          "poster_path": "https://image.tmdb.org/t/p/w500/AtsgWhDnHTq68L0lLsUrCnM7TjG.jpg",
          "vote_count": 7858
        },
        "_matchesInfo": {
          "overview": [
            {
              "start": 20,
              "length": 7
            }
          ],
          "title": [
            {
              "start": 0,
              "length": 7
            }
          ]
        }
      }
    ]);

    assert_json_eq!(res_expected, res_value["hits"].clone(), ordered: false);
}
