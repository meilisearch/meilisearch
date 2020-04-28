use assert_json_diff::assert_json_eq;
use serde_json::json;
use std::convert::Into;

mod common;

#[actix_rt::test]
async fn search_with_settings_basic() {
    let mut server = common::Server::with_uid("movies");
    server.populate_movies().await;

    let config = json!({
      "rankingRules": [
        "typo",
        "words",
        "proximity",
        "attribute",
        "wordsPosition",
        "desc(popularity)",
        "exactness",
        "desc(vote_average)"
      ],
      "distinctAttribute": null,
      "searchableAttributes": [
        "title",
        "tagline",
        "overview",
        "cast",
        "director",
        "producer",
        "production_companies",
        "genres"
      ],
      "displayedAttributes": [
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
      "synonyms": null,
      "acceptNewFields": false,
    });

    server.update_all_settings(config).await;

    let query = "q=the%20avangers&limit=3";
    let expect = json!([
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

    let (response, _status_code) = server.search(query).await;
    assert_json_eq!(expect, response["hits"].clone(), ordered: false);
}

#[actix_rt::test]
async fn search_with_settings_stop_words() {
    let mut server = common::Server::with_uid("movies");
    server.populate_movies().await;

    let config = json!({
      "rankingRules": [
        "typo",
        "words",
        "proximity",
        "attribute",
        "wordsPosition",
        "desc(popularity)",
        "exactness",
        "desc(vote_average)"
      ],
      "distinctAttribute": null,
      "searchableAttributes": [
        "title",
        "tagline",
        "overview",
        "cast",
        "director",
        "producer",
        "production_companies",
        "genres"
      ],
      "displayedAttributes": [
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
      "synonyms": null,
      "acceptNewFields": false,
    });

    server.update_all_settings(config).await;

    let query = "q=the%20avangers&limit=3";
    let expect = json!([
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

    let (response, _status_code) = server.search(query).await;
    assert_json_eq!(expect, response["hits"].clone(), ordered: false);
}

#[actix_rt::test]
async fn search_with_settings_synonyms() {
    let mut server = common::Server::with_uid("movies");
    server.populate_movies().await;

    let config = json!({
      "rankingRules": [
        "typo",
        "words",
        "proximity",
        "attribute",
        "wordsPosition",
        "desc(popularity)",
        "exactness",
        "desc(vote_average)"
      ],
      "distinctAttribute": null,
      "searchableAttributes": [
        "title",
        "tagline",
        "overview",
        "cast",
        "director",
        "producer",
        "production_companies",
        "genres"
      ],
      "displayedAttributes": [
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
      },
      "acceptNewFields": false,
    });

    server.update_all_settings(config).await;

    let query = "q=avangers&limit=3";
    let expect = json!([
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
        "vote_count": 16056,
        "poster_path": "https://image.tmdb.org/t/p/w500/7WsyChQLEftFiDOVTGkv3hFpyyt.jpg"
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
        "vote_count": 10497,
        "poster_path": "https://image.tmdb.org/t/p/w500/or06FN3Dka5tukK1e9sl16pB3iy.jpg"
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
        "vote_count": 14661,
        "poster_path": "https://image.tmdb.org/t/p/w500/t90Y3G8UGQp0f0DrP60wRu9gfrH.jpg"
      }
    ]);

    let (response, _status_code) = server.search(query).await;
    assert_json_eq!(expect, response["hits"].clone(), ordered: false);
}

#[actix_rt::test]
async fn search_with_settings_ranking_rules() {
    let mut server = common::Server::with_uid("movies");
    server.populate_movies().await;

    let config = json!({
      "rankingRules": [
        "typo",
        "words",
        "proximity",
        "attribute",
        "wordsPosition",
        "asc(vote_average)",
        "exactness",
        "desc(popularity)"
      ],
      "distinctAttribute": null,
      "searchableAttributes": [
        "title",
        "tagline",
        "overview",
        "cast",
        "director",
        "producer",
        "production_companies",
        "genres"
      ],
      "displayedAttributes": [
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
      "synonyms": null,
      "acceptNewFields": false,
    });

    server.update_all_settings(config).await;

    let query = "q=avangers&limit=3";
    let expect = json!([
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

    let (response, _status_code) = server.search(query).await;
    assert_json_eq!(expect, response["hits"].clone(), ordered: false);
}

#[actix_rt::test]
async fn search_with_settings_searchable_attributes() {
    let mut server = common::Server::with_uid("movies");
    server.populate_movies().await;

    let config = json!({
      "rankingRules": [
        "typo",
        "words",
        "proximity",
        "attribute",
        "wordsPosition",
        "desc(popularity)",
        "exactness",
        "desc(vote_average)"
      ],
      "distinctAttribute": null,
      "searchableAttributes": [
        "tagline",
        "overview",
        "cast",
        "director",
        "producer",
        "production_companies",
        "genres"
      ],
      "displayedAttributes": [
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
      "synonyms": null,
      "acceptNewFields": false,
    });

    server.update_all_settings(config).await;

    let query = "q=avangers&limit=3";
    let expect = json!([
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

    let (response, _status_code) = server.search(query).await;
    assert_json_eq!(expect, response["hits"].clone(), ordered: false);
}

#[actix_rt::test]
async fn search_with_settings_displayed_attributes() {
    let mut server = common::Server::with_uid("movies");
    server.populate_movies().await;

    let config = json!({
      "rankingRules": [
        "typo",
        "words",
        "proximity",
        "attribute",
        "wordsPosition",
        "desc(popularity)",
        "exactness",
        "desc(vote_average)"
      ],
      "distinctAttribute": null,
      "searchableAttributes": [
        "title",
        "tagline",
        "overview",
        "cast",
        "director",
        "producer",
        "production_companies",
        "genres"
      ],
      "displayedAttributes": [
        "title",
        "tagline",
        "id",
        "overview",
        "poster_path"
      ],
      "stopWords": null,
      "synonyms": null,
      "acceptNewFields": false,
    });

    server.update_all_settings(config).await;

    let query = "q=avangers&limit=3";
    let expect = json!([
      {
        "id": 299536,
        "title": "Avengers: Infinity War",
        "tagline": "An entire universe. Once and for all.",
        "overview": "As the Avengers and their allies have continued to protect the world from threats too large for any one hero to handle, a new danger has emerged from the cosmic shadows: Thanos. A despot of intergalactic infamy, his goal is to collect all six Infinity Stones, artifacts of unimaginable power, and use them to inflict his twisted will on all of reality. Everything the Avengers have fought for has led up to this moment - the fate of Earth and existence itself has never been more uncertain.",
        "poster_path": "https://image.tmdb.org/t/p/w500/7WsyChQLEftFiDOVTGkv3hFpyyt.jpg"
      },
      {
        "id": 299534,
        "title": "Avengers: Endgame",
        "tagline": "Part of the journey is the end.",
        "overview": "After the devastating events of Avengers: Infinity War, the universe is in ruins due to the efforts of the Mad Titan, Thanos. With the help of remaining allies, the Avengers must assemble once more in order to undo Thanos' actions and restore order to the universe once and for all, no matter what consequences may be in store.",
        "poster_path": "https://image.tmdb.org/t/p/w500/or06FN3Dka5tukK1e9sl16pB3iy.jpg"
      },
      {
        "id": 99861,
        "title": "Avengers: Age of Ultron",
        "tagline": "A New Age Has Come.",
        "overview": "When Tony Stark tries to jumpstart a dormant peacekeeping program, things go awry and Earth’s Mightiest Heroes are put to the ultimate test as the fate of the planet hangs in the balance. As the villainous Ultron emerges, it is up to The Avengers to stop him from enacting his terrible plans, and soon uneasy alliances and unexpected action pave the way for an epic and unique global adventure.",
        "poster_path": "https://image.tmdb.org/t/p/w500/t90Y3G8UGQp0f0DrP60wRu9gfrH.jpg"
      }
    ]);

    let (response, _status_code) = server.search(query).await;
    assert_json_eq!(expect, response["hits"].clone(), ordered: false);
}

#[actix_rt::test]
async fn search_with_settings_searchable_attributes_2() {
    let mut server = common::Server::with_uid("movies");
    server.populate_movies().await;

    let config = json!({
      "rankingRules": [
        "typo",
        "words",
        "proximity",
        "attribute",
        "wordsPosition",
        "desc(popularity)",
        "exactness",
        "desc(vote_average)"
      ],
      "distinctAttribute": null,
      "searchableAttributes": [
        "tagline",
        "overview",
        "title",
        "cast",
        "director",
        "producer",
        "production_companies",
        "genres"
      ],
      "displayedAttributes": [
        "title",
        "tagline",
        "id",
        "overview",
        "poster_path"
      ],
      "stopWords": null,
      "synonyms": null,
      "acceptNewFields": false,
    });

    server.update_all_settings(config).await;

    let query = "q=avangers&limit=3";
    let expect = json!([
      {
        "id": 299536,
        "title": "Avengers: Infinity War",
        "tagline": "An entire universe. Once and for all.",
        "overview": "As the Avengers and their allies have continued to protect the world from threats too large for any one hero to handle, a new danger has emerged from the cosmic shadows: Thanos. A despot of intergalactic infamy, his goal is to collect all six Infinity Stones, artifacts of unimaginable power, and use them to inflict his twisted will on all of reality. Everything the Avengers have fought for has led up to this moment - the fate of Earth and existence itself has never been more uncertain.",
        "poster_path": "https://image.tmdb.org/t/p/w500/7WsyChQLEftFiDOVTGkv3hFpyyt.jpg"
      },
      {
        "id": 299534,
        "title": "Avengers: Endgame",
        "tagline": "Part of the journey is the end.",
        "overview": "After the devastating events of Avengers: Infinity War, the universe is in ruins due to the efforts of the Mad Titan, Thanos. With the help of remaining allies, the Avengers must assemble once more in order to undo Thanos' actions and restore order to the universe once and for all, no matter what consequences may be in store.",
        "poster_path": "https://image.tmdb.org/t/p/w500/or06FN3Dka5tukK1e9sl16pB3iy.jpg"
      },
      {
        "id": 100402,
        "title": "Captain America: The Winter Soldier",
        "tagline": "In heroes we trust.",
        "overview": "After the cataclysmic events in New York with The Avengers, Steve Rogers, aka Captain America is living quietly in Washington, D.C. and trying to adjust to the modern world. But when a S.H.I.E.L.D. colleague comes under attack, Steve becomes embroiled in a web of intrigue that threatens to put the world at risk. Joining forces with the Black Widow, Captain America struggles to expose the ever-widening conspiracy while fighting off professional assassins sent to silence him at every turn. When the full scope of the villainous plot is revealed, Captain America and the Black Widow enlist the help of a new ally, the Falcon. However, they soon find themselves up against an unexpected and formidable enemy—the Winter Soldier.",
        "poster_path": "https://image.tmdb.org/t/p/w500/5TQ6YDmymBpnF005OyoB7ohZps9.jpg"
      }
    ]);

    let (response, _status_code) = server.search(query).await;
    assert_json_eq!(expect, response["hits"].clone(), ordered: false);
}
