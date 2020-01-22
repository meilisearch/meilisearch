// use std::time::Duration;
// use std::convert::Into;

use async_std::task::{block_on};
use async_std::io::prelude::*;
use http_service::Body;
use serde_json::json;
use serde_json::Value;
use assert_json_diff::assert_json_eq;

mod common;

#[test]
fn basic_search() {
    let mut server = common::setup_server().unwrap();

    common::enrich_server_with_movies_index(&mut server).unwrap();
    common::enrich_server_with_movies_settings(&mut server).unwrap();
    common::enrich_server_with_movies_documents(&mut server).unwrap();

    // 1 - Simple search

    let req = http::Request::get("/indexes/movies/search?q=captain&limit=3").body(Body::empty()).unwrap();
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
}
