use std::convert::Into;
use std::sync::Mutex;

use assert_json_diff::assert_json_eq;
use once_cell::sync::Lazy;
use serde_json::json;

mod common;

static GLOBAL_SERVER: Lazy<Mutex<common::Server>> = Lazy::new(|| {
    let mut server = common::Server::with_uid("movies");
    server.populate_movies();
    Mutex::new(server)
});

// Search
// q: Captain
// limit: 3
#[test]
fn search_with_limit() {
    let query = "q=captain&limit=3";

    let expected = json!([
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

    let (response, _status_code) = GLOBAL_SERVER.lock().unwrap().search(query);
    assert_json_eq!(expected, response["hits"].clone(), ordered: false);
}

// Search with offset
// q: Captain
// limit: 3
// offset: 1
#[test]
fn search_with_offset() {
    let query = "q=captain&limit=3&offset=1";

    let expected = json!([
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

    let (response, _status_code) = GLOBAL_SERVER.lock().unwrap().search(query);
    assert_json_eq!(expected, response["hits"].clone(), ordered: false);
}

// Search with attribute to highlight all
// q: Captain
// limit: 1
// attributeToHighlight: *
#[test]
fn search_with_attribute_to_highlight_wildcard() {
    let query = "q=captain&limit=1&attributesToHighlight=*";

    let expected = json!([
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

    let (response, _status_code) = GLOBAL_SERVER.lock().unwrap().search(query);
    assert_json_eq!(expected, response["hits"].clone(), ordered: false);
}

// Search with attribute to highlight title
// q: Captain
// limit: 1
// attributeToHighlight: title
#[test]
fn search_with_attribute_to_highlight_1() {
    let query = "q=captain&limit=1&attributesToHighlight=title";

    let expected = json!([
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

    let (response, _status_code) = GLOBAL_SERVER.lock().unwrap().search(query);
    assert_json_eq!(expected, response["hits"].clone(), ordered: false);
}

// Search with attribute to highlight title and tagline
// q: Captain
// limit: 1
// attributeToHighlight: title,tagline
#[test]
fn search_with_attribute_to_highlight_title_tagline() {
    let query = "q=captain&limit=1&attributesToHighlight=title,tagline";

    let expected = json!([
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

    let (response, _status_code) = GLOBAL_SERVER.lock().unwrap().search(query);
    assert_json_eq!(expected, response["hits"].clone(), ordered: false);
}

// Search with attribute to highlight title and overview
// q: Captain
// limit: 1
// attributeToHighlight: title,overview
#[test]
fn search_with_attribute_to_highlight_title_overview() {
    let query = "q=captain&limit=1&attributesToHighlight=title,overview";

    let expected = json!([
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

    let (response, _status_code) = GLOBAL_SERVER.lock().unwrap().search(query);
    assert_json_eq!(expected, response["hits"].clone(), ordered: false);
}

// Search with matches
// q: Captain
// limit: 1
// matches: true
#[test]
fn search_with_matches() {
    let query = "q=captain&limit=1&matches=true";

    let expected = json!([
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

    let (response, _status_code) = GLOBAL_SERVER.lock().unwrap().search(query);
    assert_json_eq!(expected, response["hits"].clone(), ordered: false);
}

// Search with crop
// q: Captain
// limit: 1
// attributesToCrop: overview
// cropLength: 20
#[test]
fn search_witch_crop() {
    let query = "q=captain&limit=1&attributesToCrop=overview&cropLength=20";

    let expected = json!([
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

    let (response, _status_code) = GLOBAL_SERVER.lock().unwrap().search(query);
    assert_json_eq!(expected, response["hits"].clone(), ordered: false);
}

// Search with attributes to retrieve
// q: Captain
// limit: 1
// attributesToRetrieve: [title,tagline,overview,poster_path]
#[test]
fn search_with_attributes_to_retrieve() {
    let query = "q=captain&limit=1&attributesToRetrieve=title,tagline,overview,poster_path";

    let expected = json!([
      {
        "title": "Captain Marvel",
        "tagline": "Higher. Further. Faster.",
        "overview": "The story follows Carol Danvers as she becomes one of the universe’s most powerful heroes when Earth is caught in the middle of a galactic war between two alien races. Set in the 1990s, Captain Marvel is an all-new adventure from a previously unseen period in the history of the Marvel Cinematic Universe.",
        "poster_path": "https://image.tmdb.org/t/p/w500/AtsgWhDnHTq68L0lLsUrCnM7TjG.jpg"
      }
    ]);

    let (response, _status_code) = GLOBAL_SERVER.lock().unwrap().search(query);
    assert_json_eq!(expected, response["hits"].clone(), ordered: false);
}

// Search with filter
// q: Captain
// limit: 1
// filters: director:Anthony%20Russo
#[test]
fn search_with_filter() {
    let query = "q=captain&filters=director%20%3D%20%22Anthony%20Russo%22&limit=3";

    let expected = json!([
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

    let (response, _status_code) = GLOBAL_SERVER.lock().unwrap().search(query);
    assert_json_eq!(expected, response["hits"].clone(), ordered: false);
}

// Search with attributes to highlight and matches
// q: Captain
// limit: 1
// attributesToHighlight: [title,overview]
// matches: true
#[test]
fn search_with_attributes_to_highlight_and_matches() {
    let query = "q=captain&limit=1&attributesToHighlight=title,overview&matches=true";

    let expected = json!( [
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

    let (response, _status_code) = GLOBAL_SERVER.lock().unwrap().search(query);
    assert_json_eq!(expected, response["hits"].clone(), ordered: false);
}

// Search with attributes to highlight and matches and crop
// q: Captain
// limit: 1
// attributesToHighlight: [title,overview]
// matches: true
// cropLength: 20
// attributesToCrop: overview
#[test]
fn search_with_attributes_to_highlight_and_matches_and_crop() {
    let query = "q=captain&limit=1&attributesToCrop=overview&cropLength=20&attributesToHighlight=title,overview&matches=true";

    let expected = json!([
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

    let (response, _status_code) = GLOBAL_SERVER.lock().unwrap().search(query);
    assert_json_eq!(expected, response["hits"].clone(), ordered: false);
}

// Search with differents attributes
// q: Captain
// limit: 1
// attributesToRetrieve: [title,producer,director]
// attributesToHighlight: [title]
#[test]
fn search_with_differents_attributes() {
    let query = "q=captain&limit=1&attributesToRetrieve=title,producer,director&attributesToHighlight=title";

    let expected = json!([
      {
        "title": "Captain Marvel",
        "director": "Ryan Fleck",
        "producer": "Kevin Feige",
        "_formatted": {
          "title": "<em>Captain</em> Marvel"
        }
      }
    ]);

    let (response, _status_code) = GLOBAL_SERVER.lock().unwrap().search(query);
    assert_json_eq!(expected, response["hits"].clone(), ordered: false);
}

// Search with attributes to highlight and matches and crop
// q: Captain
// limit: 1
// attributesToRetrieve: [title,producer,director]
// attributesToCrop: [overview]
// cropLength: 10
#[test]
fn search_with_differents_attributes_2() {
    let query = "q=captain&limit=1&attributesToRetrieve=title,producer,director&attributesToCrop=overview&cropLength=10";

    let expected = json!([
      {
      "title": "Captain Marvel",
      "director": "Ryan Fleck",
      "producer": "Kevin Feige",
      "_formatted": {
        "overview": "1990s, Captain Marvel"
      }
    }
    ]);

    let (response, _status_code) = GLOBAL_SERVER.lock().unwrap().search(query);
    assert_json_eq!(expected, response["hits"].clone(), ordered: false);
}

// Search with attributes to highlight and matches and crop
// q: Captain
// limit: 1
// attributesToRetrieve: [title,producer,director]
// attributesToCrop: [overview:10]
#[test]
fn search_with_differents_attributes_3() {
    let query = "q=captain&limit=1&attributesToRetrieve=title,producer,director&attributesToCrop=overview:10";

    let expected = json!([
      {
      "title": "Captain Marvel",
      "director": "Ryan Fleck",
      "producer": "Kevin Feige",
      "_formatted": {
        "overview": "1990s, Captain Marvel"
      }
    }
    ]);

    let (response, _status_code) = GLOBAL_SERVER.lock().unwrap().search(query);
    assert_json_eq!(expected, response["hits"].clone(), ordered: false);
}

// Search with attributes to highlight and matches and crop
// q: Captain
// limit: 1
// attributesToRetrieve: [title,producer,director]
// attributesToCrop: [overview:10,title:0]
#[test]
fn search_with_differents_attributes_4() {
    let query = "q=captain&limit=1&attributesToRetrieve=title,producer,director&attributesToCrop=overview:10,title:0";

    let expected = json!([
    {
      "title": "Captain Marvel",
      "director": "Ryan Fleck",
      "producer": "Kevin Feige",
      "_formatted": {
        "title": "Captain",
        "overview": "1990s, Captain Marvel"
      }
    }
    ]);

    let (response, _status_code) = GLOBAL_SERVER.lock().unwrap().search(query);
    assert_json_eq!(expected, response["hits"].clone(), ordered: false);
}

// Search with attributes to highlight and matches and crop
// q: Captain
// limit: 1
// attributesToRetrieve: [title,producer,director]
// attributesToCrop: [*,overview:10]
#[test]
fn search_with_differents_attributes_5() {
    let query = "q=captain&limit=1&attributesToRetrieve=title,producer,director&attributesToCrop=*,overview:10";

    let expected = json!([
    {
      "title": "Captain Marvel",
      "director": "Ryan Fleck",
      "producer": "Kevin Feige",
      "_formatted": {
        "title": "Captain Marvel",
        "director": "Ryan Fleck",
        "producer": "Kevin Feige",
        "overview": "1990s, Captain Marvel"
      }
    }
    ]);

    let (response, _status_code) = GLOBAL_SERVER.lock().unwrap().search(query);
    assert_json_eq!(expected, response["hits"].clone(), ordered: false);
}

// Search with attributes to highlight and matches and crop
// q: Captain
// limit: 1
// attributesToRetrieve: [title,producer,director]
// attributesToCrop: [*,overview:10]
// attributesToHighlight: [title]
#[test]
fn search_with_differents_attributes_6() {
    let query = "q=captain&limit=1&attributesToRetrieve=title,producer,director&attributesToCrop=*,overview:10&attributesToHighlight=title";

    let expected = json!([
    {
      "title": "Captain Marvel",
      "director": "Ryan Fleck",
      "producer": "Kevin Feige",
      "_formatted": {
        "title": "<em>Captain</em> Marvel",
        "director": "Ryan Fleck",
        "producer": "Kevin Feige",
        "overview": "1990s, Captain Marvel"
      }
    }
    ]);

    let (response, _status_code) = GLOBAL_SERVER.lock().unwrap().search(query);
    assert_json_eq!(expected, response["hits"].clone(), ordered: false);
}

// Search with attributes to highlight and matches and crop
// q: Captain
// limit: 1
// attributesToRetrieve: [title,producer,director]
// attributesToCrop: [*,overview:10]
// attributesToHighlight: [*]
#[test]
fn search_with_differents_attributes_7() {
    let query = "q=captain&limit=1&attributesToRetrieve=title,producer,director&attributesToCrop=*,overview:10&attributesToHighlight=*";

    let expected = json!([
    {
      "title": "Captain Marvel",
      "director": "Ryan Fleck",
      "producer": "Kevin Feige",
      "_formatted": {
        "title": "<em>Captain</em> Marvel",
        "director": "Ryan Fleck",
        "producer": "Kevin Feige",
        "overview": "1990s, Captain Marvel"
      }
    }
    ]);

    let (response, _status_code) = GLOBAL_SERVER.lock().unwrap().search(query);
    assert_json_eq!(expected, response["hits"].clone(), ordered: false);
}

// Search with attributes to highlight and matches and crop
// q: Captain
// limit: 1
// attributesToRetrieve: [title,producer,director]
// attributesToCrop: [*,overview:10]
// attributesToHighlight: [*,tagline]
#[test]
fn search_with_differents_attributes_8() {
    let query = "q=captain&limit=1&attributesToRetrieve=title,producer,director&attributesToCrop=*,overview:10&attributesToHighlight=*,tagline";

    let expected = json!([
    {
      "title": "Captain Marvel",
      "director": "Ryan Fleck",
      "producer": "Kevin Feige",
      "_formatted": {
        "title": "<em>Captain</em> Marvel",
        "director": "Ryan Fleck",
        "producer": "Kevin Feige",
        "tagline": "Higher. Further. Faster.",
        "overview": "1990s, Captain Marvel"
      }
    }
    ]);

    let (response, _status_code) = GLOBAL_SERVER.lock().unwrap().search(query);
    assert_json_eq!(expected, response["hits"].clone(), ordered: false);
}
