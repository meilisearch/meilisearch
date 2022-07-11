mod data;

use crate::common::{default_settings, GetAllDocumentsOptions, Server};
use meilisearch_http::Opt;
use serde_json::json;

use self::data::GetDump;

// all the following test are ignored on windows. See #2364
#[actix_rt::test]
#[cfg_attr(target_os = "windows", ignore)]
async fn import_dump_v1() {
    let temp = tempfile::tempdir().unwrap();

    for path in [
        GetDump::MoviesRawV1.path(),
        GetDump::MoviesWithSettingsV1.path(),
        GetDump::RubyGemsWithSettingsV1.path(),
    ] {
        let options = Opt {
            import_dump: Some(path),
            ..default_settings(temp.path())
        };
        let error = Server::new_with_options(options)
            .await
            .map(|_| ())
            .unwrap_err();

        assert_eq!(error.to_string(), "The version 1 of the dumps is not supported anymore. You can re-export your dump from a version between 0.21 and 0.24, or start fresh from a version 0.25 onwards.");
    }
}

#[actix_rt::test]
#[cfg_attr(target_os = "windows", ignore)]
async fn import_dump_v2_movie_raw() {
    let temp = tempfile::tempdir().unwrap();

    let options = Opt {
        import_dump: Some(GetDump::MoviesRawV2.path()),
        ..default_settings(temp.path())
    };
    let server = Server::new_with_options(options).await.unwrap();

    let (indexes, code) = server.list_indexes(None, None).await;
    assert_eq!(code, 200);

    assert_eq!(indexes["results"].as_array().unwrap().len(), 1);
    assert_eq!(indexes["results"][0]["uid"], json!("indexUID"));
    assert_eq!(indexes["results"][0]["primaryKey"], json!("id"));

    let index = server.index("indexUID");

    let (stats, code) = index.stats().await;
    assert_eq!(code, 200);
    assert_eq!(
        stats,
        json!({ "numberOfDocuments": 53, "isIndexing": false, "fieldDistribution": {"genres": 53, "id": 53, "overview": 53, "poster": 53, "release_date": 53, "title": 53 }})
    );

    let (settings, code) = index.settings().await;
    assert_eq!(code, 200);
    assert_eq!(
        settings,
        json!({"displayedAttributes": ["*"], "searchableAttributes": ["*"], "filterableAttributes": [], "sortableAttributes": [], "rankingRules": ["words", "typo", "proximity", "attribute", "exactness"], "stopWords": [], "synonyms": {}, "distinctAttribute": null, "typoTolerance": {"enabled": true, "minWordSizeForTypos": {"oneTypo": 5, "twoTypos": 9}, "disableOnWords": [], "disableOnAttributes": [] }, "faceting": { "maxValuesPerFacet": 100 }, "pagination": { "maxTotalHits": 1000 } })
    );

    let (tasks, code) = index.list_tasks().await;
    assert_eq!(code, 200);
    assert_eq!(
        tasks,
        json!({ "results": [{"uid": 0, "indexUid": "indexUID", "status": "succeeded", "type": "documentAdditionOrUpdate", "details": { "receivedDocuments": 0, "indexedDocuments": 31944 }, "duration": "PT41.751156S", "enqueuedAt": "2021-09-08T08:30:30.550282Z", "startedAt": "2021-09-08T08:30:30.553012Z", "finishedAt": "2021-09-08T08:31:12.304168Z" }], "limit": 20, "from": 0, "next": null })
    );

    // finally we're just going to check that we can still get a few documents by id
    let (document, code) = index.get_document(100, None).await;
    assert_eq!(code, 200);
    assert_eq!(
        document,
        json!({"id": 100, "title": "Lock, Stock and Two Smoking Barrels", "overview": "A card shark and his unwillingly-enlisted friends need to make a lot of cash quick after losing a sketchy poker match. To do this they decide to pull a heist on a small-time gang who happen to be operating out of the flat next door.", "genres": ["Comedy", "Crime"], "poster": "https://image.tmdb.org/t/p/w500/8kSerJrhrJWKLk1LViesGcnrUPE.jpg", "release_date": 889056000})
    );

    let (document, code) = index.get_document(500, None).await;
    assert_eq!(code, 200);
    assert_eq!(
        document,
        json!({"id": 500, "title": "Reservoir Dogs", "overview": "A botched robbery indicates a police informant, and the pressure mounts in the aftermath at a warehouse. Crime begets violence as the survivors -- veteran Mr. White, newcomer Mr. Orange, psychopathic parolee Mr. Blonde, bickering weasel Mr. Pink and Nice Guy Eddie -- unravel.", "genres": ["Crime", "Thriller"], "poster": "https://image.tmdb.org/t/p/w500/AjTtJNumZyUDz33VtMlF1K8JPsE.jpg", "release_date": 715392000})
    );

    let (document, code) = index.get_document(10006, None).await;
    assert_eq!(code, 200);
    assert_eq!(
        document,
        json!({"id": 10006, "title": "Wild Seven", "overview": "In this darkly karmic vision of Arizona, a man who breathes nothing but ill will begins a noxious domino effect as quickly as an uncontrollable virus kills. As he exits Arizona State Penn after twenty-one long years, Wilson has only one thing on the brain, leveling the score with career criminal, Mackey Willis.", "genres": ["Action", "Crime", "Drama"], "poster": "https://image.tmdb.org/t/p/w500/y114dTPoqn8k2Txps4P2tI95YCS.jpg", "release_date": 1136073600})
    );
}

#[actix_rt::test]
#[cfg_attr(target_os = "windows", ignore)]
async fn import_dump_v2_movie_with_settings() {
    let temp = tempfile::tempdir().unwrap();

    let options = Opt {
        import_dump: Some(GetDump::MoviesWithSettingsV2.path()),
        ..default_settings(temp.path())
    };
    let server = Server::new_with_options(options).await.unwrap();

    let (indexes, code) = server.list_indexes(None, None).await;
    assert_eq!(code, 200);

    assert_eq!(indexes["results"].as_array().unwrap().len(), 1);
    assert_eq!(indexes["results"][0]["uid"], json!("indexUID"));
    assert_eq!(indexes["results"][0]["primaryKey"], json!("id"));

    let index = server.index("indexUID");

    let (stats, code) = index.stats().await;
    assert_eq!(code, 200);
    assert_eq!(
        stats,
        json!({ "numberOfDocuments": 53, "isIndexing": false, "fieldDistribution": {"genres": 53, "id": 53, "overview": 53, "poster": 53, "release_date": 53, "title": 53 }})
    );

    let (settings, code) = index.settings().await;
    assert_eq!(code, 200);
    assert_eq!(
        settings,
        json!({ "displayedAttributes": ["title", "genres", "overview", "poster", "release_date"], "searchableAttributes": ["title", "overview"], "filterableAttributes": ["genres"], "sortableAttributes": [], "rankingRules": ["words", "typo", "proximity", "attribute", "exactness"], "stopWords": ["of", "the"], "synonyms": {}, "distinctAttribute": null, "typoTolerance": {"enabled": true, "minWordSizeForTypos": { "oneTypo": 5, "twoTypos": 9 }, "disableOnWords": [], "disableOnAttributes": [] }, "faceting": { "maxValuesPerFacet": 100 }, "pagination": { "maxTotalHits": 1000 } })
    );

    let (tasks, code) = index.list_tasks().await;
    assert_eq!(code, 200);
    assert_eq!(
        tasks,
        json!({ "results": [{ "uid": 1, "indexUid": "indexUID", "status": "succeeded", "type": "settingsUpdate", "details": { "displayedAttributes": ["title", "genres", "overview", "poster", "release_date"], "searchableAttributes": ["title", "overview"], "filterableAttributes": ["genres"], "stopWords": ["of", "the"] }, "duration": "PT37.488777S", "enqueuedAt": "2021-09-08T08:24:02.323444Z", "startedAt": "2021-09-08T08:24:02.324145Z", "finishedAt": "2021-09-08T08:24:39.812922Z" }, { "uid": 0, "indexUid": "indexUID", "status": "succeeded", "type": "documentAdditionOrUpdate", "details": { "receivedDocuments": 0, "indexedDocuments": 31944 }, "duration": "PT39.941318S", "enqueuedAt": "2021-09-08T08:21:14.742672Z", "startedAt": "2021-09-08T08:21:14.750166Z", "finishedAt": "2021-09-08T08:21:54.691484Z" }], "limit": 20, "from": 1, "next": null })
    );

    // finally we're just going to check that we can still get a few documents by id
    let (document, code) = index.get_document(100, None).await;
    assert_eq!(code, 200);
    assert_eq!(
        document,
        json!({ "id": 100, "title": "Lock, Stock and Two Smoking Barrels", "genres": ["Comedy", "Crime"], "overview": "A card shark and his unwillingly-enlisted friends need to make a lot of cash quick after losing a sketchy poker match. To do this they decide to pull a heist on a small-time gang who happen to be operating out of the flat next door.", "poster": "https://image.tmdb.org/t/p/w500/8kSerJrhrJWKLk1LViesGcnrUPE.jpg", "release_date": 889056000 })
    );

    let (document, code) = index.get_document(500, None).await;
    assert_eq!(code, 200);
    assert_eq!(
        document,
        json!({ "id": 500, "title": "Reservoir Dogs", "genres": ["Crime", "Thriller"], "overview": "A botched robbery indicates a police informant, and the pressure mounts in the aftermath at a warehouse. Crime begets violence as the survivors -- veteran Mr. White, newcomer Mr. Orange, psychopathic parolee Mr. Blonde, bickering weasel Mr. Pink and Nice Guy Eddie -- unravel.", "poster": "https://image.tmdb.org/t/p/w500/AjTtJNumZyUDz33VtMlF1K8JPsE.jpg", "release_date": 715392000})
    );

    let (document, code) = index.get_document(10006, None).await;
    assert_eq!(code, 200);
    assert_eq!(
        document,
        json!({ "id": 10006, "title": "Wild Seven", "genres": ["Action", "Crime", "Drama"], "overview": "In this darkly karmic vision of Arizona, a man who breathes nothing but ill will begins a noxious domino effect as quickly as an uncontrollable virus kills. As he exits Arizona State Penn after twenty-one long years, Wilson has only one thing on the brain, leveling the score with career criminal, Mackey Willis.", "poster": "https://image.tmdb.org/t/p/w500/y114dTPoqn8k2Txps4P2tI95YCS.jpg", "release_date": 1136073600})
    );
}

#[actix_rt::test]
#[cfg_attr(target_os = "windows", ignore)]
async fn import_dump_v2_rubygems_with_settings() {
    let temp = tempfile::tempdir().unwrap();

    let options = Opt {
        import_dump: Some(GetDump::RubyGemsWithSettingsV2.path()),
        ..default_settings(temp.path())
    };
    let server = Server::new_with_options(options).await.unwrap();

    let (indexes, code) = server.list_indexes(None, None).await;
    assert_eq!(code, 200);

    assert_eq!(indexes["results"].as_array().unwrap().len(), 1);
    assert_eq!(indexes["results"][0]["uid"], json!("rubygems"));
    assert_eq!(indexes["results"][0]["primaryKey"], json!("id"));

    let index = server.index("rubygems");

    let (stats, code) = index.stats().await;
    assert_eq!(code, 200);
    assert_eq!(
        stats,
        json!({ "numberOfDocuments": 53, "isIndexing": false, "fieldDistribution": {"description": 53, "id": 53, "name": 53, "summary": 53, "total_downloads": 53, "version": 53 }})
    );

    let (settings, code) = index.settings().await;
    assert_eq!(code, 200);
    assert_eq!(
        settings,
        json!({"displayedAttributes": ["name", "summary", "description", "version", "total_downloads"], "searchableAttributes": ["name", "summary"], "filterableAttributes": ["version"], "sortableAttributes": [], "rankingRules": ["typo", "words", "fame:desc", "proximity", "attribute", "exactness", "total_downloads:desc"], "stopWords": [], "synonyms": {}, "distinctAttribute": null, "typoTolerance": {"enabled": true, "minWordSizeForTypos": {"oneTypo": 5, "twoTypos": 9}, "disableOnWords": [], "disableOnAttributes": [] }, "faceting": { "maxValuesPerFacet": 100 }, "pagination": { "maxTotalHits": 1000 }})
    );

    let (tasks, code) = index.list_tasks().await;
    assert_eq!(code, 200);
    assert_eq!(
        tasks["results"][0],
        json!({"uid": 92, "indexUid": "rubygems", "status": "succeeded", "type": "documentAdditionOrUpdate", "details": {"receivedDocuments": 0, "indexedDocuments": 1042}, "duration": "PT14.034672S", "enqueuedAt": "2021-09-08T08:40:31.390775Z", "startedAt": "2021-09-08T08:51:39.060642Z", "finishedAt": "2021-09-08T08:51:53.095314Z"})
    );

    // finally we're just going to check that we can still get a few documents by id
    let (document, code) = index.get_document(188040, None).await;
    assert_eq!(code, 200);
    assert_eq!(
        document,
        json!({ "name": "meilisearch", "summary": "An easy-to-use ruby client for Meilisearch API", "description": "An easy-to-use ruby client for Meilisearch API. See https://github.com/meilisearch/MeiliSearch", "id": "188040", "version": "0.15.2", "total_downloads": "7465"})
    );

    let (document, code) = index.get_document(191940, None).await;
    assert_eq!(code, 200);
    assert_eq!(
        document,
        json!({ "name": "doggo", "summary": "RSpec 3 formatter - documentation, with progress indication", "description": "Similar to \"rspec -f d\", but also indicates progress by showing the current test number and total test count on each line.", "id": "191940", "version": "1.1.0", "total_downloads": "9394"})
    );

    let (document, code) = index.get_document(159227, None).await;
    assert_eq!(code, 200);
    assert_eq!(
        document,
        json!({ "name": "vortex-of-agony", "summary": "You dont need to use nodejs or go, just install this plugin. It will crash your application at random", "description": "You dont need to use nodejs or go, just install this plugin. It will crash your application at random", "id": "159227", "version": "0.1.0", "total_downloads": "1007"})
    );
}

#[actix_rt::test]
#[cfg_attr(target_os = "windows", ignore)]
async fn import_dump_v3_movie_raw() {
    let temp = tempfile::tempdir().unwrap();

    let options = Opt {
        import_dump: Some(GetDump::MoviesRawV3.path()),
        ..default_settings(temp.path())
    };
    let server = Server::new_with_options(options).await.unwrap();

    let (indexes, code) = server.list_indexes(None, None).await;
    assert_eq!(code, 200);

    assert_eq!(indexes["results"].as_array().unwrap().len(), 1);
    assert_eq!(indexes["results"][0]["uid"], json!("indexUID"));
    assert_eq!(indexes["results"][0]["primaryKey"], json!("id"));

    let index = server.index("indexUID");

    let (stats, code) = index.stats().await;
    assert_eq!(code, 200);
    assert_eq!(
        stats,
        json!({ "numberOfDocuments": 53, "isIndexing": false, "fieldDistribution": {"genres": 53, "id": 53, "overview": 53, "poster": 53, "release_date": 53, "title": 53 }})
    );

    let (settings, code) = index.settings().await;
    assert_eq!(code, 200);
    assert_eq!(
        settings,
        json!({"displayedAttributes": ["*"], "searchableAttributes": ["*"], "filterableAttributes": [], "sortableAttributes": [], "rankingRules": ["words", "typo", "proximity", "attribute", "exactness"], "stopWords": [], "synonyms": {}, "distinctAttribute": null, "typoTolerance": {"enabled": true, "minWordSizeForTypos": {"oneTypo": 5, "twoTypos": 9}, "disableOnWords": [], "disableOnAttributes": [] }, "faceting": { "maxValuesPerFacet": 100 }, "pagination": { "maxTotalHits": 1000 } })
    );

    let (tasks, code) = index.list_tasks().await;
    assert_eq!(code, 200);
    assert_eq!(
        tasks,
        json!({ "results": [{"uid": 0, "indexUid": "indexUID", "status": "succeeded", "type": "documentAdditionOrUpdate", "details": { "receivedDocuments": 0, "indexedDocuments": 31944 }, "duration": "PT41.751156S", "enqueuedAt": "2021-09-08T08:30:30.550282Z", "startedAt": "2021-09-08T08:30:30.553012Z", "finishedAt": "2021-09-08T08:31:12.304168Z" }], "limit": 20, "from": 0, "next": null })
    );

    // finally we're just going to check that we can still get a few documents by id
    let (document, code) = index.get_document(100, None).await;
    assert_eq!(code, 200);
    assert_eq!(
        document,
        json!({"id": 100, "title": "Lock, Stock and Two Smoking Barrels", "overview": "A card shark and his unwillingly-enlisted friends need to make a lot of cash quick after losing a sketchy poker match. To do this they decide to pull a heist on a small-time gang who happen to be operating out of the flat next door.", "genres": ["Comedy", "Crime"], "poster": "https://image.tmdb.org/t/p/w500/8kSerJrhrJWKLk1LViesGcnrUPE.jpg", "release_date": 889056000})
    );

    let (document, code) = index.get_document(500, None).await;
    assert_eq!(code, 200);
    assert_eq!(
        document,
        json!({"id": 500, "title": "Reservoir Dogs", "overview": "A botched robbery indicates a police informant, and the pressure mounts in the aftermath at a warehouse. Crime begets violence as the survivors -- veteran Mr. White, newcomer Mr. Orange, psychopathic parolee Mr. Blonde, bickering weasel Mr. Pink and Nice Guy Eddie -- unravel.", "genres": ["Crime", "Thriller"], "poster": "https://image.tmdb.org/t/p/w500/AjTtJNumZyUDz33VtMlF1K8JPsE.jpg", "release_date": 715392000})
    );

    let (document, code) = index.get_document(10006, None).await;
    assert_eq!(code, 200);
    assert_eq!(
        document,
        json!({"id": 10006, "title": "Wild Seven", "overview": "In this darkly karmic vision of Arizona, a man who breathes nothing but ill will begins a noxious domino effect as quickly as an uncontrollable virus kills. As he exits Arizona State Penn after twenty-one long years, Wilson has only one thing on the brain, leveling the score with career criminal, Mackey Willis.", "genres": ["Action", "Crime", "Drama"], "poster": "https://image.tmdb.org/t/p/w500/y114dTPoqn8k2Txps4P2tI95YCS.jpg", "release_date": 1136073600})
    );
}

#[actix_rt::test]
#[cfg_attr(target_os = "windows", ignore)]
async fn import_dump_v3_movie_with_settings() {
    let temp = tempfile::tempdir().unwrap();

    let options = Opt {
        import_dump: Some(GetDump::MoviesWithSettingsV3.path()),
        ..default_settings(temp.path())
    };
    let server = Server::new_with_options(options).await.unwrap();

    let (indexes, code) = server.list_indexes(None, None).await;
    assert_eq!(code, 200);

    assert_eq!(indexes["results"].as_array().unwrap().len(), 1);
    assert_eq!(indexes["results"][0]["uid"], json!("indexUID"));
    assert_eq!(indexes["results"][0]["primaryKey"], json!("id"));

    let index = server.index("indexUID");

    let (stats, code) = index.stats().await;
    assert_eq!(code, 200);
    assert_eq!(
        stats,
        json!({ "numberOfDocuments": 53, "isIndexing": false, "fieldDistribution": {"genres": 53, "id": 53, "overview": 53, "poster": 53, "release_date": 53, "title": 53 }})
    );

    let (settings, code) = index.settings().await;
    assert_eq!(code, 200);
    assert_eq!(
        settings,
        json!({ "displayedAttributes": ["title", "genres", "overview", "poster", "release_date"], "searchableAttributes": ["title", "overview"], "filterableAttributes": ["genres"], "sortableAttributes": [], "rankingRules": ["words", "typo", "proximity", "attribute", "exactness"], "stopWords": ["of", "the"], "synonyms": {}, "distinctAttribute": null, "typoTolerance": {"enabled": true, "minWordSizeForTypos": { "oneTypo": 5, "twoTypos": 9 }, "disableOnWords": [], "disableOnAttributes": [] }, "faceting": { "maxValuesPerFacet": 100 }, "pagination": { "maxTotalHits": 1000 } })
    );

    let (tasks, code) = index.list_tasks().await;
    assert_eq!(code, 200);
    assert_eq!(
        tasks,
        json!({ "results": [{ "uid": 1, "indexUid": "indexUID", "status": "succeeded", "type": "settingsUpdate", "details": { "displayedAttributes": ["title", "genres", "overview", "poster", "release_date"], "searchableAttributes": ["title", "overview"], "filterableAttributes": ["genres"], "stopWords": ["of", "the"] }, "duration": "PT37.488777S", "enqueuedAt": "2021-09-08T08:24:02.323444Z", "startedAt": "2021-09-08T08:24:02.324145Z", "finishedAt": "2021-09-08T08:24:39.812922Z" }, { "uid": 0, "indexUid": "indexUID", "status": "succeeded", "type": "documentAdditionOrUpdate", "details": { "receivedDocuments": 0, "indexedDocuments": 31944 }, "duration": "PT39.941318S", "enqueuedAt": "2021-09-08T08:21:14.742672Z", "startedAt": "2021-09-08T08:21:14.750166Z", "finishedAt": "2021-09-08T08:21:54.691484Z" }], "limit": 20, "from": 1, "next": null })
    );

    // finally we're just going to check that we can["results"] still get a few documents by id
    let (document, code) = index.get_document(100, None).await;
    assert_eq!(code, 200);
    assert_eq!(
        document,
        json!({ "id": 100, "title": "Lock, Stock and Two Smoking Barrels", "genres": ["Comedy", "Crime"], "overview": "A card shark and his unwillingly-enlisted friends need to make a lot of cash quick after losing a sketchy poker match. To do this they decide to pull a heist on a small-time gang who happen to be operating out of the flat next door.", "poster": "https://image.tmdb.org/t/p/w500/8kSerJrhrJWKLk1LViesGcnrUPE.jpg", "release_date": 889056000 })
    );

    let (document, code) = index.get_document(500, None).await;
    assert_eq!(code, 200);
    assert_eq!(
        document,
        json!({ "id": 500, "title": "Reservoir Dogs", "genres": ["Crime", "Thriller"], "overview": "A botched robbery indicates a police informant, and the pressure mounts in the aftermath at a warehouse. Crime begets violence as the survivors -- veteran Mr. White, newcomer Mr. Orange, psychopathic parolee Mr. Blonde, bickering weasel Mr. Pink and Nice Guy Eddie -- unravel.", "poster": "https://image.tmdb.org/t/p/w500/AjTtJNumZyUDz33VtMlF1K8JPsE.jpg", "release_date": 715392000})
    );

    let (document, code) = index.get_document(10006, None).await;
    assert_eq!(code, 200);
    assert_eq!(
        document,
        json!({ "id": 10006, "title": "Wild Seven", "genres": ["Action", "Crime", "Drama"], "overview": "In this darkly karmic vision of Arizona, a man who breathes nothing but ill will begins a noxious domino effect as quickly as an uncontrollable virus kills. As he exits Arizona State Penn after twenty-one long years, Wilson has only one thing on the brain, leveling the score with career criminal, Mackey Willis.", "poster": "https://image.tmdb.org/t/p/w500/y114dTPoqn8k2Txps4P2tI95YCS.jpg", "release_date": 1136073600})
    );
}

#[actix_rt::test]
#[cfg_attr(target_os = "windows", ignore)]
async fn import_dump_v3_rubygems_with_settings() {
    let temp = tempfile::tempdir().unwrap();

    let options = Opt {
        import_dump: Some(GetDump::RubyGemsWithSettingsV3.path()),
        ..default_settings(temp.path())
    };
    let server = Server::new_with_options(options).await.unwrap();

    let (indexes, code) = server.list_indexes(None, None).await;
    assert_eq!(code, 200);

    assert_eq!(indexes["results"].as_array().unwrap().len(), 1);
    assert_eq!(indexes["results"][0]["uid"], json!("rubygems"));
    assert_eq!(indexes["results"][0]["primaryKey"], json!("id"));

    let index = server.index("rubygems");

    let (stats, code) = index.stats().await;
    assert_eq!(code, 200);
    assert_eq!(
        stats,
        json!({ "numberOfDocuments": 53, "isIndexing": false, "fieldDistribution": {"description": 53, "id": 53, "name": 53, "summary": 53, "total_downloads": 53, "version": 53 }})
    );

    let (settings, code) = index.settings().await;
    assert_eq!(code, 200);
    assert_eq!(
        settings,
        json!({"displayedAttributes": ["name", "summary", "description", "version", "total_downloads"], "searchableAttributes": ["name", "summary"], "filterableAttributes": ["version"], "sortableAttributes": [], "rankingRules": ["typo", "words", "fame:desc", "proximity", "attribute", "exactness", "total_downloads:desc"], "stopWords": [], "synonyms": {}, "distinctAttribute": null, "typoTolerance": {"enabled": true, "minWordSizeForTypos": {"oneTypo": 5, "twoTypos": 9}, "disableOnWords": [], "disableOnAttributes": [] }, "faceting": { "maxValuesPerFacet": 100 }, "pagination": { "maxTotalHits": 1000 } })
    );

    let (tasks, code) = index.list_tasks().await;
    assert_eq!(code, 200);
    assert_eq!(
        tasks["results"][0],
        json!({"uid": 92, "indexUid": "rubygems", "status": "succeeded", "type": "documentAdditionOrUpdate", "details": {"receivedDocuments": 0, "indexedDocuments": 1042}, "duration": "PT14.034672S", "enqueuedAt": "2021-09-08T08:40:31.390775Z", "startedAt": "2021-09-08T08:51:39.060642Z", "finishedAt": "2021-09-08T08:51:53.095314Z"})
    );

    // finally we're just going to check that we can still get a few documents by id
    let (document, code) = index.get_document(188040, None).await;
    assert_eq!(code, 200);
    assert_eq!(
        document,
        json!({ "name": "meilisearch", "summary": "An easy-to-use ruby client for Meilisearch API", "description": "An easy-to-use ruby client for Meilisearch API. See https://github.com/meilisearch/MeiliSearch", "id": "188040", "version": "0.15.2", "total_downloads": "7465"})
    );

    let (document, code) = index.get_document(191940, None).await;
    assert_eq!(code, 200);
    assert_eq!(
        document,
        json!({ "name": "doggo", "summary": "RSpec 3 formatter - documentation, with progress indication", "description": "Similar to \"rspec -f d\", but also indicates progress by showing the current test number and total test count on each line.", "id": "191940", "version": "1.1.0", "total_downloads": "9394"})
    );

    let (document, code) = index.get_document(159227, None).await;
    assert_eq!(code, 200);
    assert_eq!(
        document,
        json!({ "name": "vortex-of-agony", "summary": "You dont need to use nodejs or go, just install this plugin. It will crash your application at random", "description": "You dont need to use nodejs or go, just install this plugin. It will crash your application at random", "id": "159227", "version": "0.1.0", "total_downloads": "1007"})
    );
}

#[actix_rt::test]
#[cfg_attr(target_os = "windows", ignore)]
async fn import_dump_v4_movie_raw() {
    let temp = tempfile::tempdir().unwrap();

    let options = Opt {
        import_dump: Some(GetDump::MoviesRawV4.path()),
        ..default_settings(temp.path())
    };
    let server = Server::new_with_options(options).await.unwrap();

    let (indexes, code) = server.list_indexes(None, None).await;
    assert_eq!(code, 200);

    assert_eq!(indexes["results"].as_array().unwrap().len(), 1);
    assert_eq!(indexes["results"][0]["uid"], json!("indexUID"));
    assert_eq!(indexes["results"][0]["primaryKey"], json!("id"));

    let index = server.index("indexUID");

    let (stats, code) = index.stats().await;
    assert_eq!(code, 200);
    assert_eq!(
        stats,
        json!({ "numberOfDocuments": 53, "isIndexing": false, "fieldDistribution": {"genres": 53, "id": 53, "overview": 53, "poster": 53, "release_date": 53, "title": 53 }})
    );

    let (settings, code) = index.settings().await;
    assert_eq!(code, 200);
    assert_eq!(
        settings,
        json!({ "displayedAttributes": ["*"], "searchableAttributes": ["*"], "filterableAttributes": [], "sortableAttributes": [], "rankingRules": ["words", "typo", "proximity", "attribute", "exactness"], "stopWords": [], "synonyms": {}, "distinctAttribute": null, "typoTolerance": {"enabled": true, "minWordSizeForTypos": {"oneTypo": 5, "twoTypos": 9}, "disableOnWords": [], "disableOnAttributes": [] }, "faceting": { "maxValuesPerFacet": 100 }, "pagination": { "maxTotalHits": 1000 } })
    );

    let (tasks, code) = index.list_tasks().await;
    assert_eq!(code, 200);
    assert_eq!(
        tasks,
        json!({ "results": [{"uid": 0, "indexUid": "indexUID", "status": "succeeded", "type": "documentAdditionOrUpdate", "details": { "receivedDocuments": 0, "indexedDocuments": 31944 }, "duration": "PT41.751156S", "enqueuedAt": "2021-09-08T08:30:30.550282Z", "startedAt": "2021-09-08T08:30:30.553012Z", "finishedAt": "2021-09-08T08:31:12.304168Z" }], "limit" : 20, "from": 0, "next": null })
    );

    // finally we're just going to check that we can still get a few documents by id
    let (document, code) = index.get_document(100, None).await;
    assert_eq!(code, 200);
    assert_eq!(
        document,
        json!({ "id": 100, "title": "Lock, Stock and Two Smoking Barrels", "overview": "A card shark and his unwillingly-enlisted friends need to make a lot of cash quick after losing a sketchy poker match. To do this they decide to pull a heist on a small-time gang who happen to be operating out of the flat next door.", "genres": ["Comedy", "Crime"], "poster": "https://image.tmdb.org/t/p/w500/8kSerJrhrJWKLk1LViesGcnrUPE.jpg", "release_date": 889056000})
    );

    let (document, code) = index.get_document(500, None).await;
    assert_eq!(code, 200);
    assert_eq!(
        document,
        json!({ "id": 500, "title": "Reservoir Dogs", "overview": "A botched robbery indicates a police informant, and the pressure mounts in the aftermath at a warehouse. Crime begets violence as the survivors -- veteran Mr. White, newcomer Mr. Orange, psychopathic parolee Mr. Blonde, bickering weasel Mr. Pink and Nice Guy Eddie -- unravel.", "genres": ["Crime", "Thriller"], "poster": "https://image.tmdb.org/t/p/w500/AjTtJNumZyUDz33VtMlF1K8JPsE.jpg", "release_date": 715392000})
    );

    let (document, code) = index.get_document(10006, None).await;
    assert_eq!(code, 200);
    assert_eq!(
        document,
        json!({ "id": 10006, "title": "Wild Seven", "overview": "In this darkly karmic vision of Arizona, a man who breathes nothing but ill will begins a noxious domino effect as quickly as an uncontrollable virus kills. As he exits Arizona State Penn after twenty-one long years, Wilson has only one thing on the brain, leveling the score with career criminal, Mackey Willis.", "genres": ["Action", "Crime", "Drama"], "poster": "https://image.tmdb.org/t/p/w500/y114dTPoqn8k2Txps4P2tI95YCS.jpg", "release_date": 1136073600})
    );
}

#[actix_rt::test]
#[cfg_attr(target_os = "windows", ignore)]
async fn import_dump_v4_movie_with_settings() {
    let temp = tempfile::tempdir().unwrap();

    let options = Opt {
        import_dump: Some(GetDump::MoviesWithSettingsV4.path()),
        ..default_settings(temp.path())
    };
    let server = Server::new_with_options(options).await.unwrap();

    let (indexes, code) = server.list_indexes(None, None).await;
    assert_eq!(code, 200);

    assert_eq!(indexes["results"].as_array().unwrap().len(), 1);
    assert_eq!(indexes["results"][0]["uid"], json!("indexUID"));
    assert_eq!(indexes["results"][0]["primaryKey"], json!("id"));

    let index = server.index("indexUID");

    let (stats, code) = index.stats().await;
    assert_eq!(code, 200);
    assert_eq!(
        stats,
        json!({ "numberOfDocuments": 53, "isIndexing": false, "fieldDistribution": {"genres": 53, "id": 53, "overview": 53, "poster": 53, "release_date": 53, "title": 53 }})
    );

    let (settings, code) = index.settings().await;
    assert_eq!(code, 200);
    assert_eq!(
        settings,
        json!({ "displayedAttributes": ["title", "genres", "overview", "poster", "release_date"], "searchableAttributes": ["title", "overview"], "filterableAttributes": ["genres"], "sortableAttributes": [], "rankingRules": ["words", "typo", "proximity", "attribute", "exactness"], "stopWords": ["of", "the"], "synonyms": {}, "distinctAttribute": null, "typoTolerance": {"enabled": true, "minWordSizeForTypos": { "oneTypo": 5, "twoTypos": 9 }, "disableOnWords": [], "disableOnAttributes": [] }, "faceting": { "maxValuesPerFacet": 100 }, "pagination": { "maxTotalHits": 1000 } })
    );

    let (tasks, code) = index.list_tasks().await;
    assert_eq!(code, 200);
    assert_eq!(
        tasks,
        json!({ "results": [{ "uid": 1, "indexUid": "indexUID", "status": "succeeded", "type": "settingsUpdate", "details": { "displayedAttributes": ["title", "genres", "overview", "poster", "release_date"], "searchableAttributes": ["title", "overview"], "filterableAttributes": ["genres"], "stopWords": ["of", "the"] }, "duration": "PT37.488777S", "enqueuedAt": "2021-09-08T08:24:02.323444Z", "startedAt": "2021-09-08T08:24:02.324145Z", "finishedAt": "2021-09-08T08:24:39.812922Z" }, { "uid": 0, "indexUid": "indexUID", "status": "succeeded", "type": "documentAdditionOrUpdate", "details": { "receivedDocuments": 0, "indexedDocuments": 31944 }, "duration": "PT39.941318S", "enqueuedAt": "2021-09-08T08:21:14.742672Z", "startedAt": "2021-09-08T08:21:14.750166Z", "finishedAt": "2021-09-08T08:21:54.691484Z" }], "limit": 20, "from": 1, "next": null })
    );

    // finally we're just going to check that we can still get a few documents by id
    let (document, code) = index.get_document(100, None).await;
    assert_eq!(code, 200);
    assert_eq!(
        document,
        json!({ "id": 100, "title": "Lock, Stock and Two Smoking Barrels", "genres": ["Comedy", "Crime"], "overview": "A card shark and his unwillingly-enlisted friends need to make a lot of cash quick after losing a sketchy poker match. To do this they decide to pull a heist on a small-time gang who happen to be operating out of the flat next door.", "poster": "https://image.tmdb.org/t/p/w500/8kSerJrhrJWKLk1LViesGcnrUPE.jpg", "release_date": 889056000 })
    );

    let (document, code) = index.get_document(500, None).await;
    assert_eq!(code, 200);
    assert_eq!(
        document,
        json!({ "id": 500, "title": "Reservoir Dogs", "genres": ["Crime", "Thriller"], "overview": "A botched robbery indicates a police informant, and the pressure mounts in the aftermath at a warehouse. Crime begets violence as the survivors -- veteran Mr. White, newcomer Mr. Orange, psychopathic parolee Mr. Blonde, bickering weasel Mr. Pink and Nice Guy Eddie -- unravel.", "poster": "https://image.tmdb.org/t/p/w500/AjTtJNumZyUDz33VtMlF1K8JPsE.jpg", "release_date": 715392000})
    );

    let (document, code) = index.get_document(10006, None).await;
    assert_eq!(code, 200);
    assert_eq!(
        document,
        json!({ "id": 10006, "title": "Wild Seven", "genres": ["Action", "Crime", "Drama"], "overview": "In this darkly karmic vision of Arizona, a man who breathes nothing but ill will begins a noxious domino effect as quickly as an uncontrollable virus kills. As he exits Arizona State Penn after twenty-one long years, Wilson has only one thing on the brain, leveling the score with career criminal, Mackey Willis.", "poster": "https://image.tmdb.org/t/p/w500/y114dTPoqn8k2Txps4P2tI95YCS.jpg", "release_date": 1136073600})
    );
}

#[actix_rt::test]
#[cfg_attr(target_os = "windows", ignore)]
async fn import_dump_v4_rubygems_with_settings() {
    let temp = tempfile::tempdir().unwrap();

    let options = Opt {
        import_dump: Some(GetDump::RubyGemsWithSettingsV4.path()),
        ..default_settings(temp.path())
    };
    let server = Server::new_with_options(options).await.unwrap();

    let (indexes, code) = server.list_indexes(None, None).await;
    assert_eq!(code, 200);

    assert_eq!(indexes["results"].as_array().unwrap().len(), 1);
    assert_eq!(indexes["results"][0]["uid"], json!("rubygems"));
    assert_eq!(indexes["results"][0]["primaryKey"], json!("id"));

    let index = server.index("rubygems");

    let (stats, code) = index.stats().await;
    assert_eq!(code, 200);
    assert_eq!(
        stats,
        json!({ "numberOfDocuments": 53, "isIndexing": false, "fieldDistribution": {"description": 53, "id": 53, "name": 53, "summary": 53, "total_downloads": 53, "version": 53 }})
    );

    let (settings, code) = index.settings().await;
    assert_eq!(code, 200);
    assert_eq!(
        settings,
        json!({ "displayedAttributes": ["name", "summary", "description", "version", "total_downloads"], "searchableAttributes": ["name", "summary"], "filterableAttributes": ["version"], "sortableAttributes": [], "rankingRules": ["typo", "words", "fame:desc", "proximity", "attribute", "exactness", "total_downloads:desc"], "stopWords": [], "synonyms": {}, "distinctAttribute": null, "typoTolerance": {"enabled": true, "minWordSizeForTypos": {"oneTypo": 5, "twoTypos": 9}, "disableOnWords": [], "disableOnAttributes": [] }, "faceting": { "maxValuesPerFacet": 100 }, "pagination": { "maxTotalHits": 1000 } })
    );

    let (tasks, code) = index.list_tasks().await;
    assert_eq!(code, 200);
    assert_eq!(
        tasks["results"][0],
        json!({ "uid": 92, "indexUid": "rubygems", "status": "succeeded", "type": "documentAdditionOrUpdate", "details": {"receivedDocuments": 0, "indexedDocuments": 1042}, "duration": "PT14.034672S", "enqueuedAt": "2021-09-08T08:40:31.390775Z", "startedAt": "2021-09-08T08:51:39.060642Z", "finishedAt": "2021-09-08T08:51:53.095314Z"})
    );

    // finally we're just going to check that we can still get a few documents by id
    let (document, code) = index.get_document(188040, None).await;
    assert_eq!(code, 200);
    assert_eq!(
        document,
        json!({ "name": "meilisearch", "summary": "An easy-to-use ruby client for Meilisearch API", "description": "An easy-to-use ruby client for Meilisearch API. See https://github.com/meilisearch/MeiliSearch", "id": "188040", "version": "0.15.2", "total_downloads": "7465"})
    );

    let (document, code) = index.get_document(191940, None).await;
    assert_eq!(code, 200);
    assert_eq!(
        document,
        json!({ "name": "doggo", "summary": "RSpec 3 formatter - documentation, with progress indication", "description": "Similar to \"rspec -f d\", but also indicates progress by showing the current test number and total test count on each line.", "id": "191940", "version": "1.1.0", "total_downloads": "9394"})
    );

    let (document, code) = index.get_document(159227, None).await;
    assert_eq!(code, 200);
    assert_eq!(
        document,
        json!({ "name": "vortex-of-agony", "summary": "You dont need to use nodejs or go, just install this plugin. It will crash your application at random", "description": "You dont need to use nodejs or go, just install this plugin. It will crash your application at random", "id": "159227", "version": "0.1.0", "total_downloads": "1007"})
    );
}

#[actix_rt::test]
#[cfg_attr(target_os = "windows", ignore)]
async fn import_dump_v5() {
    let temp = tempfile::tempdir().unwrap();

    let options = Opt {
        import_dump: Some(GetDump::TestV5.path()),
        ..default_settings(temp.path())
    };
    let mut server = Server::new_auth_with_options(options, temp).await;
    server.use_api_key("MASTER_KEY");

    let (indexes, code) = server.list_indexes(None, None).await;
    assert_eq!(code, 200, "{indexes}");

    assert_eq!(indexes["results"].as_array().unwrap().len(), 2);
    assert_eq!(indexes["results"][0]["uid"], json!("test"));
    assert_eq!(indexes["results"][1]["uid"], json!("test2"));
    assert_eq!(indexes["results"][0]["primaryKey"], json!("id"));

    let expected_stats = json!({
        "numberOfDocuments": 10,
        "isIndexing": false,
        "fieldDistribution": {
            "cast": 10,
            "director": 10,
            "genres": 10,
            "id": 10,
            "overview": 10,
            "popularity": 10,
            "poster_path": 10,
            "producer": 10,
            "production_companies": 10,
            "release_date": 10,
            "tagline": 10,
            "title": 10,
            "vote_average": 10,
            "vote_count": 10
        }
    });

    let index1 = server.index("test");
    let index2 = server.index("test2");

    let (stats, code) = index1.stats().await;
    assert_eq!(code, 200);
    assert_eq!(stats, expected_stats);

    let (docs, code) = index2
        .get_all_documents(GetAllDocumentsOptions::default())
        .await;
    assert_eq!(code, 200);
    assert_eq!(docs["results"].as_array().unwrap().len(), 10);
    let (docs, code) = index1
        .get_all_documents(GetAllDocumentsOptions::default())
        .await;
    assert_eq!(code, 200);
    assert_eq!(docs["results"].as_array().unwrap().len(), 10);

    let (stats, code) = index2.stats().await;
    assert_eq!(code, 200);
    assert_eq!(stats, expected_stats);

    let (keys, code) = server.list_api_keys().await;
    assert_eq!(code, 200);
    let key = &keys["results"][0];

    assert_eq!(key["name"], "my key");
}
