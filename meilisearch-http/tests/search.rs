use std::convert::Into;
use std::time::Duration;

use assert_json_diff::assert_json_eq;
use async_std::io::prelude::*;
use async_std::task::{block_on, sleep};
use http_service::Body;
use http_service_mock::TestBackend;
use meilisearch_http::data::Data;
use serde_json::json;
use serde_json::Value;
use tide::server::Service;

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

    let query = "q=captain&limit=3";

    let json = json!([
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

    common::search(&mut server, query, json);

    // 2 - Simple search with offset
    // q: Captain
    // limit: 3
    // offset: 1

    let query = "q=captain&limit=3&offset=1";

    let json = json!([
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

    common::search(&mut server, query, json);

    // 3 - Simple search with attribute to highlight all
    // q: Captain
    // limit: 1
    // attributeToHighlight: *

    let query = "q=captain&limit=1&attributesToHighlight=*";

    let json = json!([
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

    common::search(&mut server, query, json);

    // 4 - Simple search with attribute to highlight title
    // q: Captain
    // limit: 1
    // attributeToHighlight: title

    let query = "q=captain&limit=1&attributesToHighlight=title";

    let json = json!([
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

    common::search(&mut server, query, json);

    // 1 - Simple search with attribute to highlight title and tagline
    // q: Captain
    // limit: 1
    // attributeToHighlight: title,tagline

    let query = "q=captain&limit=1&attributesToHighlight=title,tagline";

    let json = json!([
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

    common::search(&mut server, query, json);

    // 1 - Simple search with attribute to highlight title and overview
    // q: Captain
    // limit: 1
    // attributeToHighlight: title,overview

    let query = "q=captain&limit=1&attributesToHighlight=title,overview";

    let json = json!([
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

    common::search(&mut server, query, json);

    // 1 - Simple search with matches
    // q: Captain
    // limit: 1
    // matches: true

    let query = "q=captain&limit=1&matches=true";

    let json = json!([
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

    common::search(&mut server, query, json);

    // 1 - Simple search with crop
    // q: Captain
    // limit: 1
    // attributesToCrop: overview
    // cropLength: 20

    let query = "q=captain&limit=1&attributesToCrop=overview&cropLength=20";

    let json = json!([
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

    common::search(&mut server, query, json);

    // 1 - Simple search with attributes to retrieve
    // q: Captain
    // limit: 1
    // attributesToRetrieve: [title,tagline,overview,poster_path]

    let query = "q=captain&limit=1&attributesToRetrieve=title,tagline,overview,poster_path";

    let json = json!([
      {
        "title": "Captain Marvel",
        "tagline": "Higher. Further. Faster.",
        "overview": "The story follows Carol Danvers as she becomes one of the universe’s most powerful heroes when Earth is caught in the middle of a galactic war between two alien races. Set in the 1990s, Captain Marvel is an all-new adventure from a previously unseen period in the history of the Marvel Cinematic Universe.",
        "poster_path": "https://image.tmdb.org/t/p/w500/AtsgWhDnHTq68L0lLsUrCnM7TjG.jpg"
      }
    ]);

    common::search(&mut server, query, json);

    // 1 - Simple search with filter
    // q: Captain
    // limit: 1
    // filters: director:Anthony%20Russo

    let query = "q=captain&limit=3&filters=director:Anthony%20Russo";

    let json = json!([
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

    common::search(&mut server, query, json);

    // 1 - Simple search with attributes to highlight and matches
    // q: Captain
    // limit: 1
    // attributesToHighlight: [title,overview]
    // matches: true

    let query = "q=captain&limit=1&attributesToHighlight=title,overview&matches=true";

    let json = json!( [
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

    common::search(&mut server, query, json);

    // 1 - Simple search with attributes to highlight and matches and crop
    // q: Captain
    // limit: 1
    // attributesToHighlight: [title,overview]
    // matches: true
    // cropLength: 20
    // attributesToCrop: overview

    let query = "q=captain&limit=1&attributesToCrop=overview&cropLength=20&attributesToHighlight=title,overview&matches=true";

    let json = json!([
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

    common::search(&mut server, query, json);
}

#[test]
fn search_with_settings_change() {
    let mut server = common::setup_server().unwrap();

    common::enrich_server_with_movies_index(&mut server).unwrap();
    common::enrich_server_with_movies_settings(&mut server).unwrap();
    common::enrich_server_with_movies_documents(&mut server).unwrap();

    // Basic

    let config = json!({
      "rankingRules": [
        "_typo",
        "_words",
        "_proximity",
        "_attribute",
        "_words_position",
        "dsc(popularity)",
        "_exact",
        "dsc(vote_average)"
      ],
      "rankingDistinct": null,
      "attributeIdentifier": "id",
      "attributesSearchable": [
        "title",
        "tagline",
        "overview",
        "cast",
        "director",
        "producer",
        "production_companies",
        "genres"
      ],
      "attributesDisplayed": [
        "title",
        "director",
        "producer",
        "tagline",
        "genres",
        "id",
        "overview",
        "vote_count",
        "vote_average",
        "poster_path",
        "popularity"
      ],
      "stopWords": null,
      "synonyms": null
    });

    common::update_config(&mut server, config);

    let query = "q=the%20avangers&limit=3";
    let response = json!([
      {
        "id": 24428,
        "popularity": 44.506,
        "vote_average": 7.7,
        "title": "The Avengers",
        "tagline": "Some assembly required.",
        "overview": "When an unexpected enemy emerges and threatens global safety and security, Nick Fury, director of the international peacekeeping agency known as S.H.I.E.L.D., finds himself in need of a team to pull the world back from the brink of disaster. Spanning the globe, a daring recruitment effort begins!",
        "director": "Joss Whedon",
        "producer": "Kevin Feige",
        "genres": [
          "Science Fiction",
          "Action",
          "Adventure"
        ],
        "poster_path": "https://image.tmdb.org/t/p/w500/cezWGskPY5x7GaglTTRN4Fugfb8.jpg",
        "vote_count": 21079
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
      },
      {
        "id": 299536,
        "popularity": 65.013,
        "vote_average": 8.3,
        "title": "Avengers: Infinity War",
        "tagline": "An entire universe. Once and for all.",
        "overview": "As the Avengers and their allies have continued to protect the world from threats too large for any one hero to handle, a new danger has emerged from the cosmic shadows: Thanos. A despot of intergalactic infamy, his goal is to collect all six Infinity Stones, artifacts of unimaginable power, and use them to inflict his twisted will on all of reality. Everything the Avengers have fought for has led up to this moment - the fate of Earth and existence itself has never been more uncertain.",
        "director": "Anthony Russo",
        "producer": "Kevin Feige",
        "genres": [
          "Adventure",
          "Action",
          "Science Fiction"
        ],
        "poster_path": "https://image.tmdb.org/t/p/w500/7WsyChQLEftFiDOVTGkv3hFpyyt.jpg",
        "vote_count": 16056
      }
    ]);

    common::search(&mut server, query, response);

    ////////////////////////////////////////////////////////////////////////////////////////////////
    // Set with stop words

    let config = json!({
      "rankingRules": [
        "_typo",
        "_words",
        "_proximity",
        "_attribute",
        "_words_position",
        "dsc(popularity)",
        "_exact",
        "dsc(vote_average)"
      ],
      "rankingDistinct": null,
      "attributeIdentifier": "id",
      "attributesSearchable": [
        "title",
        "tagline",
        "overview",
        "cast",
        "director",
        "producer",
        "production_companies",
        "genres"
      ],
      "attributesDisplayed": [
        "title",
        "director",
        "producer",
        "tagline",
        "genres",
        "id",
        "overview",
        "vote_count",
        "vote_average",
        "poster_path",
        "popularity"
      ],
      "stopWords": ["the"],
      "synonyms": null
    });

    common::update_config(&mut server, config);

    let query = "q=the%20avangers&limit=3";
    let response = json!([
      {
        "id": 299536,
        "popularity": 65.013,
        "vote_average": 8.3,
        "title": "Avengers: Infinity War",
        "tagline": "An entire universe. Once and for all.",
        "overview": "As the Avengers and their allies have continued to protect the world from threats too large for any one hero to handle, a new danger has emerged from the cosmic shadows: Thanos. A despot of intergalactic infamy, his goal is to collect all six Infinity Stones, artifacts of unimaginable power, and use them to inflict his twisted will on all of reality. Everything the Avengers have fought for has led up to this moment - the fate of Earth and existence itself has never been more uncertain.",
        "director": "Anthony Russo",
        "producer": "Kevin Feige",
        "genres": [
          "Adventure",
          "Action",
          "Science Fiction"
        ],
        "poster_path": "https://image.tmdb.org/t/p/w500/7WsyChQLEftFiDOVTGkv3hFpyyt.jpg",
        "vote_count": 16056
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
      },
      {
        "id": 99861,
        "popularity": 33.938,
        "vote_average": 7.3,
        "title": "Avengers: Age of Ultron",
        "tagline": "A New Age Has Come.",
        "overview": "When Tony Stark tries to jumpstart a dormant peacekeeping program, things go awry and Earth’s Mightiest Heroes are put to the ultimate test as the fate of the planet hangs in the balance. As the villainous Ultron emerges, it is up to The Avengers to stop him from enacting his terrible plans, and soon uneasy alliances and unexpected action pave the way for an epic and unique global adventure.",
        "director": "Joss Whedon",
        "producer": "Kevin Feige",
        "genres": [
          "Action",
          "Adventure",
          "Science Fiction"
        ],
        "poster_path": "https://image.tmdb.org/t/p/w500/t90Y3G8UGQp0f0DrP60wRu9gfrH.jpg",
        "vote_count": 14661
      }
    ]);

    common::search(&mut server, query, response);

    ////////////////////////////////////////////////////////////////////////////////////////////////
    // Set with synonyms

    let config = json!({
      "rankingRules": [
        "_typo",
        "_words",
        "_proximity",
        "_attribute",
        "_words_position",
        "dsc(popularity)",
        "_exact",
        "dsc(vote_average)"
      ],
      "rankingDistinct": null,
      "attributeIdentifier": "id",
      "attributesSearchable": [
        "title",
        "tagline",
        "overview",
        "cast",
        "director",
        "producer",
        "production_companies",
        "genres"
      ],
      "attributesDisplayed": [
        "title",
        "director",
        "producer",
        "tagline",
        "genres",
        "id",
        "overview",
        "vote_count",
        "vote_average",
        "poster_path",
        "popularity"
      ],
      "stopWords": null,
      "synonyms": {
        "avangers": [
          "Captain America",
          "Iron Man"
        ]
      }
    });

    common::update_config(&mut server, config);

    let query = "q=avangers&limit=3";
    let response = json!([
      {
        "id": 299536,
        "popularity": 65.013,
        "vote_average": 8.3,
        "title": "Avengers: Infinity War",
        "tagline": "An entire universe. Once and for all.",
        "overview": "As the Avengers and their allies have continued to protect the world from threats too large for any one hero to handle, a new danger has emerged from the cosmic shadows: Thanos. A despot of intergalactic infamy, his goal is to collect all six Infinity Stones, artifacts of unimaginable power, and use them to inflict his twisted will on all of reality. Everything the Avengers have fought for has led up to this moment - the fate of Earth and existence itself has never been more uncertain.",
        "director": "Anthony Russo",
        "producer": "Kevin Feige",
        "genres": [
          "Adventure",
          "Action",
          "Science Fiction"
        ],
        "poster_path": "https://image.tmdb.org/t/p/w500/7WsyChQLEftFiDOVTGkv3hFpyyt.jpg",
        "vote_count": 16056
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
      }
    ]);

    common::search(&mut server, query, response);

    ////////////////////////////////////////////////////////////////////////////////////////////////
    // Set asc(vote_average) in ranking rules

    let config = json!({
      "rankingRules": [
        "_typo",
        "_words",
        "_proximity",
        "_attribute",
        "_words_position",
        "asc(vote_average)",
        "_exact",
        "dsc(popularity)"
      ],
      "rankingDistinct": null,
      "attributeIdentifier": "id",
      "attributesSearchable": [
        "title",
        "tagline",
        "overview",
        "cast",
        "director",
        "producer",
        "production_companies",
        "genres"
      ],
      "attributesDisplayed": [
        "title",
        "director",
        "producer",
        "tagline",
        "genres",
        "id",
        "overview",
        "vote_count",
        "vote_average",
        "poster_path",
        "popularity"
      ],
      "stopWords": null,
      "synonyms": null
    });

    common::update_config(&mut server, config);

    let query = "q=avangers&limit=3";
    let response = json!([
      {
        "id": 99861,
        "popularity": 33.938,
        "vote_average": 7.3,
        "title": "Avengers: Age of Ultron",
        "tagline": "A New Age Has Come.",
        "overview": "When Tony Stark tries to jumpstart a dormant peacekeeping program, things go awry and Earth’s Mightiest Heroes are put to the ultimate test as the fate of the planet hangs in the balance. As the villainous Ultron emerges, it is up to The Avengers to stop him from enacting his terrible plans, and soon uneasy alliances and unexpected action pave the way for an epic and unique global adventure.",
        "director": "Joss Whedon",
        "producer": "Kevin Feige",
        "genres": [
          "Action",
          "Adventure",
          "Science Fiction"
        ],
        "poster_path": "https://image.tmdb.org/t/p/w500/t90Y3G8UGQp0f0DrP60wRu9gfrH.jpg",
        "vote_count": 14661
      },
      {
        "id": 299536,
        "popularity": 65.013,
        "vote_average": 8.3,
        "title": "Avengers: Infinity War",
        "tagline": "An entire universe. Once and for all.",
        "overview": "As the Avengers and their allies have continued to protect the world from threats too large for any one hero to handle, a new danger has emerged from the cosmic shadows: Thanos. A despot of intergalactic infamy, his goal is to collect all six Infinity Stones, artifacts of unimaginable power, and use them to inflict his twisted will on all of reality. Everything the Avengers have fought for has led up to this moment - the fate of Earth and existence itself has never been more uncertain.",
        "director": "Anthony Russo",
        "producer": "Kevin Feige",
        "genres": [
          "Adventure",
          "Action",
          "Science Fiction"
        ],
        "poster_path": "https://image.tmdb.org/t/p/w500/7WsyChQLEftFiDOVTGkv3hFpyyt.jpg",
        "vote_count": 16056
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

    common::search(&mut server, query, response);

    ////////////////////////////////////////////////////////////////////////////////////////////////
    // Remove Title from attributesSearchable

    let config = json!({
      "rankingRules": [
        "_typo",
        "_words",
        "_proximity",
        "_attribute",
        "_words_position",
        "dsc(popularity)",
        "_exact",
        "dsc(vote_average)"
      ],
      "rankingDistinct": null,
      "attributeIdentifier": "id",
      "attributesSearchable": [
        "tagline",
        "overview",
        "cast",
        "director",
        "producer",
        "production_companies",
        "genres"
      ],
      "attributesDisplayed": [
        "title",
        "director",
        "producer",
        "tagline",
        "genres",
        "id",
        "overview",
        "vote_count",
        "vote_average",
        "poster_path",
        "popularity"
      ],
      "stopWords": null,
      "synonyms": null
    });

    common::update_config(&mut server, config);

    let query = "q=avangers&limit=3";
    let response = json!([
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
      },
      {
        "id": 299536,
        "popularity": 65.013,
        "vote_average": 8.3,
        "title": "Avengers: Infinity War",
        "tagline": "An entire universe. Once and for all.",
        "overview": "As the Avengers and their allies have continued to protect the world from threats too large for any one hero to handle, a new danger has emerged from the cosmic shadows: Thanos. A despot of intergalactic infamy, his goal is to collect all six Infinity Stones, artifacts of unimaginable power, and use them to inflict his twisted will on all of reality. Everything the Avengers have fought for has led up to this moment - the fate of Earth and existence itself has never been more uncertain.",
        "director": "Anthony Russo",
        "producer": "Kevin Feige",
        "genres": [
          "Adventure",
          "Action",
          "Science Fiction"
        ],
        "poster_path": "https://image.tmdb.org/t/p/w500/7WsyChQLEftFiDOVTGkv3hFpyyt.jpg",
        "vote_count": 16056
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
      }
    ]);

    common::search(&mut server, query, response);

    ////////////////////////////////////////////////////////////////////////////////////////////////
    // Remove Attributes displayed

    let config = json!({
      "rankingRules": [
        "_typo",
        "_words",
        "_proximity",
        "_attribute",
        "_words_position",
        "dsc(popularity)",
        "_exact",
        "dsc(vote_average)"
      ],
      "rankingDistinct": null,
      "attributeIdentifier": "id",
      "attributesSearchable": [
        "title",
        "tagline",
        "overview",
        "cast",
        "director",
        "producer",
        "production_companies",
        "genres"
      ],
      "attributesDisplayed": [
        "title",
        "tagline",
        "id",
        "overview",
        "poster_path"
      ],
      "stopWords": null,
      "synonyms": null
    });

    common::update_config(&mut server, config);

    let query = "q=avangers&limit=3";
    let response = json!([
      {
        "id": 24428,
        "title": "The Avengers",
        "tagline": "Some assembly required.",
        "overview": "When an unexpected enemy emerges and threatens global safety and security, Nick Fury, director of the international peacekeeping agency known as S.H.I.E.L.D., finds himself in need of a team to pull the world back from the brink of disaster. Spanning the globe, a daring recruitment effort begins!",
        "poster_path": "https://image.tmdb.org/t/p/w500/cezWGskPY5x7GaglTTRN4Fugfb8.jpg"
      },
      {
        "id": 299534,
        "title": "Avengers: Endgame",
        "tagline": "Part of the journey is the end.",
        "overview": "After the devastating events of Avengers: Infinity War, the universe is in ruins due to the efforts of the Mad Titan, Thanos. With the help of remaining allies, the Avengers must assemble once more in order to undo Thanos' actions and restore order to the universe once and for all, no matter what consequences may be in store.",
        "poster_path": "https://image.tmdb.org/t/p/w500/or06FN3Dka5tukK1e9sl16pB3iy.jpg"
      },
      {
        "id": 299536,
        "title": "Avengers: Infinity War",
        "tagline": "An entire universe. Once and for all.",
        "overview": "As the Avengers and their allies have continued to protect the world from threats too large for any one hero to handle, a new danger has emerged from the cosmic shadows: Thanos. A despot of intergalactic infamy, his goal is to collect all six Infinity Stones, artifacts of unimaginable power, and use them to inflict his twisted will on all of reality. Everything the Avengers have fought for has led up to this moment - the fate of Earth and existence itself has never been more uncertain.",
        "poster_path": "https://image.tmdb.org/t/p/w500/7WsyChQLEftFiDOVTGkv3hFpyyt.jpg"
      }
    ]);

    common::search(&mut server, query, response);

    ////////////////////////////////////////////////////////////////////////////////////////////////
    // Reoder attributesSearchable

    let config = json!({
      "rankingRules": [
        "_typo",
        "_words",
        "_proximity",
        "_attribute",
        "_words_position",
        "dsc(popularity)",
        "_exact",
        "dsc(vote_average)"
      ],
      "rankingDistinct": null,
      "attributeIdentifier": "id",
      "attributesSearchable": [
        "tagline",
        "overview",
        "title",
        "cast",
        "director",
        "producer",
        "production_companies",
        "genres"
      ],
      "attributesDisplayed": [
        "title",
        "tagline",
        "id",
        "overview",
        "poster_path"
      ],
      "stopWords": null,
      "synonyms": null
    });

    common::update_config(&mut server, config);

    let query = "q=avangers&limit=3";
    let response = json!([
      {
        "id": 299534,
        "title": "Avengers: Endgame",
        "tagline": "Part of the journey is the end.",
        "overview": "After the devastating events of Avengers: Infinity War, the universe is in ruins due to the efforts of the Mad Titan, Thanos. With the help of remaining allies, the Avengers must assemble once more in order to undo Thanos' actions and restore order to the universe once and for all, no matter what consequences may be in store.",
        "poster_path": "https://image.tmdb.org/t/p/w500/or06FN3Dka5tukK1e9sl16pB3iy.jpg"
      },
      {
        "id": 299536,
        "title": "Avengers: Infinity War",
        "tagline": "An entire universe. Once and for all.",
        "overview": "As the Avengers and their allies have continued to protect the world from threats too large for any one hero to handle, a new danger has emerged from the cosmic shadows: Thanos. A despot of intergalactic infamy, his goal is to collect all six Infinity Stones, artifacts of unimaginable power, and use them to inflict his twisted will on all of reality. Everything the Avengers have fought for has led up to this moment - the fate of Earth and existence itself has never been more uncertain.",
        "poster_path": "https://image.tmdb.org/t/p/w500/7WsyChQLEftFiDOVTGkv3hFpyyt.jpg"
      },
      {
        "id": 100402,
        "title": "Captain America: The Winter Soldier",
        "tagline": "In heroes we trust.",
        "overview": "After the cataclysmic events in New York with The Avengers, Steve Rogers, aka Captain America is living quietly in Washington, D.C. and trying to adjust to the modern world. But when a S.H.I.E.L.D. colleague comes under attack, Steve becomes embroiled in a web of intrigue that threatens to put the world at risk. Joining forces with the Black Widow, Captain America struggles to expose the ever-widening conspiracy while fighting off professional assassins sent to silence him at every turn. When the full scope of the villainous plot is revealed, Captain America and the Black Widow enlist the help of a new ally, the Falcon. However, they soon find themselves up against an unexpected and formidable enemy—the Winter Soldier.",
        "poster_path": "https://image.tmdb.org/t/p/w500/5TQ6YDmymBpnF005OyoB7ohZps9.jpg"
      }
    ]);

    common::search(&mut server, query, response);
}
