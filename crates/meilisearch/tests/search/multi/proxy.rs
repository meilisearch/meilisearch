use std::sync::Arc;

use actix_http::StatusCode;
use meili_snap::{json_string, snapshot};
use wiremock::matchers::{method, path, AnyMatcher};
use wiremock::{Mock, MockServer, Request, ResponseTemplate};

use crate::common::{Server, Value, SCORE_DOCUMENTS};
use crate::json;

#[actix_rt::test]
async fn error_feature() {
    let server = Server::new().await;

    let (response, code) = server
        .multi_search(json!({
            "federation": {},
            "queries": [
            {
                "indexUid": "test",
                "federationOptions": {
                    "remote": "toto"
                }
            }
        ]}))
        .await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Performing a remote federated search requires enabling the `network` experimental feature. See https://github.com/orgs/meilisearch/discussions/805",
      "code": "feature_not_enabled",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#feature_not_enabled"
    }
    "###);

    let (response, code) = server
        .multi_search(json!({
            "federation": {},
            "queries": [
            {
                "indexUid": "test",
                "federationOptions": {
                    "queryPosition": 42,
                }
            }
        ]}))
        .await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Using `federationOptions.queryPosition` requires enabling the `network` experimental feature. See https://github.com/orgs/meilisearch/discussions/805",
      "code": "feature_not_enabled",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#feature_not_enabled"
    }
    "###);
}

#[actix_rt::test]
async fn error_params() {
    let server = Server::new().await;

    let (response, code) = server
        .multi_search(json!({
            "federation": {},
            "queries": [
            {
                "indexUid": "test",
                "federationOptions": {
                    "remote": 42
                }
            }
        ]}))
        .await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Invalid value type at `.queries[0].federationOptions.remote`: expected a string, but found a positive integer: `42`",
      "code": "invalid_multi_search_remote",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_multi_search_remote"
    }
    "###);

    let (response, code) = server
        .multi_search(json!({
            "federation": {},
            "queries": [
            {
                "indexUid": "test",
                "federationOptions": {
                    "queryPosition": "toto",
                }
            }
        ]}))
        .await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Invalid value type at `.queries[0].federationOptions.queryPosition`: expected a positive integer, but found a string: `\"toto\"`",
      "code": "invalid_multi_search_query_position",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_multi_search_query_position"
    }
    "###);
}

#[actix_rt::test]
async fn remote_sharding() {
    let ms0 = Server::new().await;
    let ms1 = Server::new().await;
    let ms2 = Server::new().await;

    // enable feature

    let (response, code) = ms0.set_features(json!({"network": true})).await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response["network"]), @"true");
    let (response, code) = ms1.set_features(json!({"network": true})).await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response["network"]), @"true");
    let (response, code) = ms2.set_features(json!({"network": true})).await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response["network"]), @"true");

    // set self

    let (response, code) = ms0.set_network(json!({"self": "ms0"})).await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response), @r###"
    {
      "self": "ms0",
      "remotes": {},
      "sharding": false
    }
    "###);
    let (response, code) = ms1.set_network(json!({"self": "ms1"})).await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response), @r###"
    {
      "self": "ms1",
      "remotes": {},
      "sharding": false
    }
    "###);
    let (response, code) = ms2.set_network(json!({"self": "ms2"})).await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response), @r###"
    {
      "self": "ms2",
      "remotes": {},
      "sharding": false
    }
    "###);

    // add documents
    let documents = SCORE_DOCUMENTS.clone();
    let documents = documents.as_array().unwrap();
    let index0 = ms0.index("test");
    let index1 = ms1.index("test");
    let index2 = ms2.index("test");
    let (task, _status_code) = index0.add_documents(json!(documents[0..2]), None).await;
    ms0.wait_task(task.uid()).await.succeeded();
    let (task, _status_code) = index1.add_documents(json!(documents[2..3]), None).await;
    ms1.wait_task(task.uid()).await.succeeded();
    let (task, _status_code) = index2.add_documents(json!(documents[3..5]), None).await;
    ms2.wait_task(task.uid()).await.succeeded();

    // wrap servers
    let ms0 = Arc::new(ms0);
    let ms1 = Arc::new(ms1);
    let ms2 = Arc::new(ms2);

    let rms0 = LocalMeili::new(ms0.clone()).await;
    let rms1 = LocalMeili::new(ms1.clone()).await;
    let rms2 = LocalMeili::new(ms2.clone()).await;

    // set network
    let network = json!({"remotes": {
        "ms0": {
            "url": rms0.url()
        },
        "ms1": {
            "url": rms1.url()
        },
        "ms2": {
            "url": rms2.url()
        }
    }});

    println!("{}", serde_json::to_string_pretty(&network).unwrap());

    let (_response, status_code) = ms0.set_network(network.clone()).await;
    snapshot!(status_code, @"200 OK");
    let (_response, status_code) = ms1.set_network(network.clone()).await;
    snapshot!(status_code, @"200 OK");
    let (_response, status_code) = ms2.set_network(network.clone()).await;
    snapshot!(status_code, @"200 OK");

    // perform multi-search
    let query = "badman returns";
    let request = json!({
        "federation": {},
        "queries": [
            {
                "q": query,
                "indexUid": "test",
                "federationOptions": {
                    "remote": "ms0"
                }
            },
            {
                "q": query,
                "indexUid": "test",
                "federationOptions": {
                    "remote": "ms1"
                }
            },
            {
                "q": query,
                "indexUid": "test",
                "federationOptions": {
                    "remote": "ms2"
                }
            },
        ]
    });

    let (response, _status_code) = ms0.multi_search(request.clone()).await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response, { ".processingTimeMs" => "[time]", ".requestUid" => "[uuid]" }), @r###"
    {
      "hits": [
        {
          "title": "Batman Returns",
          "id": "C",
          "_federation": {
            "indexUid": "test",
            "queriesPosition": 1,
            "weightedRankingScore": 0.8317901234567902,
            "remote": "ms1"
          }
        },
        {
          "title": "Batman the dark knight returns: Part 1",
          "id": "A",
          "_federation": {
            "indexUid": "test",
            "queriesPosition": 0,
            "weightedRankingScore": 0.7028218694885362,
            "remote": "ms0"
          }
        },
        {
          "title": "Batman the dark knight returns: Part 2",
          "id": "B",
          "_federation": {
            "indexUid": "test",
            "queriesPosition": 0,
            "weightedRankingScore": 0.7028218694885362,
            "remote": "ms0"
          }
        },
        {
          "title": "Badman",
          "id": "E",
          "_federation": {
            "indexUid": "test",
            "queriesPosition": 2,
            "weightedRankingScore": 0.5,
            "remote": "ms2"
          }
        },
        {
          "title": "Batman",
          "id": "D",
          "_federation": {
            "indexUid": "test",
            "queriesPosition": 2,
            "weightedRankingScore": 0.23106060606060605,
            "remote": "ms2"
          }
        }
      ],
      "processingTimeMs": "[time]",
      "limit": 20,
      "offset": 0,
      "estimatedTotalHits": 5,
      "requestUid": "[uuid]",
      "remoteErrors": {}
    }
    "###);
    let (response, _status_code) = ms1.multi_search(request.clone()).await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response, { ".processingTimeMs" => "[time]", ".requestUid" => "[uuid]" }), @r###"
    {
      "hits": [
        {
          "title": "Batman Returns",
          "id": "C",
          "_federation": {
            "indexUid": "test",
            "queriesPosition": 1,
            "weightedRankingScore": 0.8317901234567902,
            "remote": "ms1"
          }
        },
        {
          "title": "Batman the dark knight returns: Part 1",
          "id": "A",
          "_federation": {
            "indexUid": "test",
            "queriesPosition": 0,
            "weightedRankingScore": 0.7028218694885362,
            "remote": "ms0"
          }
        },
        {
          "title": "Batman the dark knight returns: Part 2",
          "id": "B",
          "_federation": {
            "indexUid": "test",
            "queriesPosition": 0,
            "weightedRankingScore": 0.7028218694885362,
            "remote": "ms0"
          }
        },
        {
          "title": "Badman",
          "id": "E",
          "_federation": {
            "indexUid": "test",
            "queriesPosition": 2,
            "weightedRankingScore": 0.5,
            "remote": "ms2"
          }
        },
        {
          "title": "Batman",
          "id": "D",
          "_federation": {
            "indexUid": "test",
            "queriesPosition": 2,
            "weightedRankingScore": 0.23106060606060605,
            "remote": "ms2"
          }
        }
      ],
      "processingTimeMs": "[time]",
      "limit": 20,
      "offset": 0,
      "estimatedTotalHits": 5,
      "requestUid": "[uuid]",
      "remoteErrors": {}
    }
    "###);
    let (response, _status_code) = ms2.multi_search(request.clone()).await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response, { ".processingTimeMs" => "[time]", ".requestUid" => "[uuid]" }), @r###"
    {
      "hits": [
        {
          "title": "Batman Returns",
          "id": "C",
          "_federation": {
            "indexUid": "test",
            "queriesPosition": 1,
            "weightedRankingScore": 0.8317901234567902,
            "remote": "ms1"
          }
        },
        {
          "title": "Batman the dark knight returns: Part 1",
          "id": "A",
          "_federation": {
            "indexUid": "test",
            "queriesPosition": 0,
            "weightedRankingScore": 0.7028218694885362,
            "remote": "ms0"
          }
        },
        {
          "title": "Batman the dark knight returns: Part 2",
          "id": "B",
          "_federation": {
            "indexUid": "test",
            "queriesPosition": 0,
            "weightedRankingScore": 0.7028218694885362,
            "remote": "ms0"
          }
        },
        {
          "title": "Badman",
          "id": "E",
          "_federation": {
            "indexUid": "test",
            "queriesPosition": 2,
            "weightedRankingScore": 0.5,
            "remote": "ms2"
          }
        },
        {
          "title": "Batman",
          "id": "D",
          "_federation": {
            "indexUid": "test",
            "queriesPosition": 2,
            "weightedRankingScore": 0.23106060606060605,
            "remote": "ms2"
          }
        }
      ],
      "processingTimeMs": "[time]",
      "limit": 20,
      "offset": 0,
      "estimatedTotalHits": 5,
      "requestUid": "[uuid]",
      "remoteErrors": {}
    }
    "###);
}

#[actix_rt::test]
async fn remote_sharding_retrieve_vectors() {
    let ms0 = Server::new().await;
    let ms1 = Server::new().await;
    let ms2 = Server::new().await;
    let index0 = ms0.index("test");
    let index1 = ms1.index("test");
    let index2 = ms2.index("test");

    // enable feature

    let (response, code) = ms0.set_features(json!({"network": true})).await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response["network"]), @"true");
    let (response, code) = ms1.set_features(json!({"network": true})).await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response["network"]), @"true");
    let (response, code) = ms2.set_features(json!({"network": true})).await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response["network"]), @"true");

    // set self

    let (response, code) = ms0.set_network(json!({"self": "ms0"})).await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response), @r###"
    {
      "self": "ms0",
      "remotes": {},
      "sharding": false
    }
    "###);
    let (response, code) = ms1.set_network(json!({"self": "ms1"})).await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response), @r###"
    {
      "self": "ms1",
      "remotes": {},
      "sharding": false
    }
    "###);
    let (response, code) = ms2.set_network(json!({"self": "ms2"})).await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response), @r###"
    {
      "self": "ms2",
      "remotes": {},
      "sharding": false
    }
    "###);

    // setup embedders

    let mock_server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/"))
        .respond_with(move |req: &Request| {
            println!("Received request: {:?}", req);
            let text = req.body_json::<String>().unwrap().to_lowercase();
            let patterns = [
                ("batman", [1.0, 0.0, 0.0]),
                ("dark", [0.0, 0.1, 0.0]),
                ("knight", [0.1, 0.1, 0.0]),
                ("returns", [0.0, 0.0, 0.2]),
                ("part", [0.05, 0.1, 0.0]),
                ("1", [0.3, 0.05, 0.0]),
                ("2", [0.2, 0.05, 0.0]),
            ];
            let mut embedding = vec![0.; 3];
            for (pattern, vector) in patterns {
                if text.contains(pattern) {
                    for (i, v) in vector.iter().enumerate() {
                        embedding[i] += v;
                    }
                }
            }
            ResponseTemplate::new(200).set_body_json(json!({ "data": embedding }))
        })
        .mount(&mock_server)
        .await;
    let url = mock_server.uri();

    for (server, index) in [(&ms0, &index0), (&ms1, &index1), (&ms2, &index2)] {
        let (response, code) = index
            .update_settings(json!({
                "embedders": {
                    "rest": {
                        "source": "rest",
                        "url": url,
                        "dimensions": 3,
                        "request": "{{text}}",
                        "response": { "data": "{{embedding}}" },
                        "documentTemplate": "{{doc.name}}",
                    },
                },
            }))
            .await;
        snapshot!(code, @"202 Accepted");
        server.wait_task(response.uid()).await.succeeded();
    }

    // wrap servers
    let ms0 = Arc::new(ms0);
    let ms1 = Arc::new(ms1);
    let ms2 = Arc::new(ms2);

    let rms0 = LocalMeili::new(ms0.clone()).await;
    let rms1 = LocalMeili::new(ms1.clone()).await;
    let rms2 = LocalMeili::new(ms2.clone()).await;

    // set network
    let network = json!({"remotes": {
        "ms0": {
            "url": rms0.url()
        },
        "ms1": {
            "url": rms1.url()
        },
        "ms2": {
            "url": rms2.url()
        }
    }});

    let (_response, status_code) = ms0.set_network(network.clone()).await;
    snapshot!(status_code, @"200 OK");
    let (_response, status_code) = ms1.set_network(network.clone()).await;
    snapshot!(status_code, @"200 OK");
    let (_response, status_code) = ms2.set_network(network.clone()).await;
    snapshot!(status_code, @"200 OK");

    // multi vector search: one query per remote

    let request = json!({
        "federation": {},
        "queries": [
            {
                "q": "batman",
                "indexUid": "test",
                "hybrid": {
                    "semanticRatio": 1.0,
                    "embedder": "rest"
                },
                "retrieveVectors": true,
                "federationOptions": {
                    "remote": "ms0"
                }
            },
            {
                "q": "dark knight",
                "indexUid": "test",
                "hybrid": {
                    "semanticRatio": 1.0,
                    "embedder": "rest"
                },
                "retrieveVectors": true,
                "federationOptions": {
                    "remote": "ms1"
                }
            },
            {
                "q": "returns",
                "indexUid": "test",
                "hybrid": {
                    "semanticRatio": 1.0,
                    "embedder": "rest"
                },
                "retrieveVectors": true,
                "federationOptions": {
                    "remote": "ms2"
                }
            },
        ]
    });

    let (response, _status_code) = ms0.multi_search(request.clone()).await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response, { ".processingTimeMs" => "[time]", ".requestUid" => "[uuid]" }), @r###"
    {
      "hits": [],
      "processingTimeMs": "[time]",
      "limit": 20,
      "offset": 0,
      "estimatedTotalHits": 0,
      "queryVectors": {
        "0": [
          1.0,
          0.0,
          0.0
        ],
        "1": [
          0.1,
          0.2,
          0.0
        ],
        "2": [
          0.0,
          0.0,
          0.2
        ]
      },
      "semanticHitCount": 0,
      "requestUid": "[uuid]",
      "remoteErrors": {}
    }
    "###);

    // multi vector search: two local queries, one remote

    let request = json!({
        "federation": {},
        "queries": [
            {
                "q": "batman",
                "indexUid": "test",
                "hybrid": {
                    "semanticRatio": 1.0,
                    "embedder": "rest"
                },
                "retrieveVectors": true,
                "federationOptions": {
                    "remote": "ms0"
                }
            },
            {
                "q": "dark knight",
                "indexUid": "test",
                "hybrid": {
                    "semanticRatio": 1.0,
                    "embedder": "rest"
                },
                "retrieveVectors": true,
                "federationOptions": {
                    "remote": "ms0"
                }
            },
            {
                "q": "returns",
                "indexUid": "test",
                "hybrid": {
                    "semanticRatio": 1.0,
                    "embedder": "rest"
                },
                "retrieveVectors": true,
                "federationOptions": {
                    "remote": "ms2"
                }
            },
        ]
    });

    let (response, _status_code) = ms0.multi_search(request.clone()).await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response, { ".processingTimeMs" => "[time]", ".requestUid" => "[uuid]" }), @r#"
    {
      "hits": [],
      "processingTimeMs": "[time]",
      "limit": 20,
      "offset": 0,
      "estimatedTotalHits": 0,
      "queryVectors": {
        "0": [
          1.0,
          0.0,
          0.0
        ],
        "1": [
          0.1,
          0.2,
          0.0
        ],
        "2": [
          0.0,
          0.0,
          0.2
        ]
      },
      "semanticHitCount": 0,
      "requestUid": "[uuid]",
      "remoteErrors": {}
    }
    "#);

    // multi vector search: two queries on the same remote

    let request = json!({
        "federation": {},
        "queries": [
            {
                "q": "batman",
                "indexUid": "test",
                "hybrid": {
                    "semanticRatio": 1.0,
                    "embedder": "rest"
                },
                "retrieveVectors": true,
                "federationOptions": {
                    "remote": "ms0"
                }
            },
            {
                "q": "dark knight",
                "indexUid": "test",
                "hybrid": {
                    "semanticRatio": 1.0,
                    "embedder": "rest"
                },
                "retrieveVectors": true,
                "federationOptions": {
                    "remote": "ms1"
                }
            },
            {
                "q": "returns",
                "indexUid": "test",
                "hybrid": {
                    "semanticRatio": 1.0,
                    "embedder": "rest"
                },
                "retrieveVectors": true,
                "federationOptions": {
                    "remote": "ms1"
                }
            },
        ]
    });

    let (response, _status_code) = ms0.multi_search(request.clone()).await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response, { ".processingTimeMs" => "[time]", ".requestUid" => "[uuid]" }), @r#"
    {
      "hits": [],
      "processingTimeMs": "[time]",
      "limit": 20,
      "offset": 0,
      "estimatedTotalHits": 0,
      "queryVectors": {
        "0": [
          1.0,
          0.0,
          0.0
        ],
        "1": [
          0.1,
          0.2,
          0.0
        ],
        "2": [
          0.0,
          0.0,
          0.2
        ]
      },
      "semanticHitCount": 0,
      "requestUid": "[uuid]",
      "remoteErrors": {}
    }
    "#);

    // multi search: two vector, one keyword

    let request = json!({
        "federation": {},
        "queries": [
            {
                "q": "batman",
                "indexUid": "test",
                "hybrid": {
                    "semanticRatio": 1.0,
                    "embedder": "rest"
                },
                "retrieveVectors": true,
                "federationOptions": {
                    "remote": "ms0"
                }
            },
            {
                "q": "dark knight",
                "indexUid": "test",
                "hybrid": {
                    "semanticRatio": 0.0,
                    "embedder": "rest"
                },
                "retrieveVectors": true,
                "federationOptions": {
                    "remote": "ms1"
                }
            },
            {
                "q": "returns",
                "indexUid": "test",
                "hybrid": {
                    "semanticRatio": 1.0,
                    "embedder": "rest"
                },
                "retrieveVectors": true,
                "federationOptions": {
                    "remote": "ms1"
                }
            },
        ]
    });

    let (response, _status_code) = ms0.multi_search(request.clone()).await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response, { ".processingTimeMs" => "[time]", ".requestUid" => "[uuid]" }), @r###"
    {
      "hits": [],
      "processingTimeMs": "[time]",
      "limit": 20,
      "offset": 0,
      "estimatedTotalHits": 0,
      "queryVectors": {
        "0": [
          1.0,
          0.0,
          0.0
        ],
        "2": [
          0.0,
          0.0,
          0.2
        ]
      },
      "semanticHitCount": 0,
      "requestUid": "[uuid]",
      "remoteErrors": {}
    }
    "###);

    // multi vector search: no local queries, all remote

    let request = json!({
        "federation": {},
        "queries": [
            {
                "q": "batman",
                "indexUid": "test",
                "hybrid": {
                    "semanticRatio": 1.0,
                    "embedder": "rest"
                },
                "retrieveVectors": true,
                "federationOptions": {
                    "remote": "ms1"
                }
            },
            {
                "q": "dark knight",
                "indexUid": "test",
                "hybrid": {
                    "semanticRatio": 1.0,
                    "embedder": "rest"
                },
                "retrieveVectors": true,
                "federationOptions": {
                    "remote": "ms1"
                }
            },
            {
                "q": "returns",
                "indexUid": "test",
                "hybrid": {
                    "semanticRatio": 1.0,
                    "embedder": "rest"
                },
                "retrieveVectors": true,
                "federationOptions": {
                    "remote": "ms1"
                }
            },
        ]
    });

    let (response, _status_code) = ms0.multi_search(request.clone()).await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response, { ".processingTimeMs" => "[time]", ".requestUid" => "[uuid]" }), @r###"
    {
      "hits": [],
      "processingTimeMs": "[time]",
      "limit": 20,
      "offset": 0,
      "estimatedTotalHits": 0,
      "queryVectors": {
        "0": [
          1.0,
          0.0,
          0.0
        ],
        "1": [
          0.1,
          0.2,
          0.0
        ],
        "2": [
          0.0,
          0.0,
          0.2
        ]
      },
      "requestUid": "[uuid]",
      "remoteErrors": {}
    }
    "###);
}

#[actix_rt::test]
async fn error_unregistered_remote() {
    let ms0 = Server::new().await;
    let ms1 = Server::new().await;

    // enable feature

    let (response, code) = ms0.set_features(json!({"network": true})).await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response["network"]), @"true");
    let (response, code) = ms1.set_features(json!({"network": true})).await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response["network"]), @"true");

    // set self

    let (response, code) = ms0.set_network(json!({"self": "ms0"})).await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response), @r###"
    {
      "self": "ms0",
      "remotes": {},
      "sharding": false
    }
    "###);
    let (response, code) = ms1.set_network(json!({"self": "ms1"})).await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response), @r###"
    {
      "self": "ms1",
      "remotes": {},
      "sharding": false
    }
    "###);

    // add documents
    let documents = SCORE_DOCUMENTS.clone();
    let documents = documents.as_array().unwrap();
    let index0 = ms0.index("test");
    let index1 = ms1.index("test");
    let (task, _status_code) = index0.add_documents(json!(documents[0..2]), None).await;
    ms0.wait_task(task.uid()).await.succeeded();
    let (task, _status_code) = index1.add_documents(json!(documents[2..3]), None).await;
    ms1.wait_task(task.uid()).await.succeeded();

    // wrap servers
    let ms0 = Arc::new(ms0);
    let ms1 = Arc::new(ms1);

    let rms0 = LocalMeili::new(ms0.clone()).await;
    let rms1 = LocalMeili::new(ms1.clone()).await;

    // set network
    let network = json!({"remotes": {
        "ms0": {
            "url": rms0.url()
        },
        "ms1": {
            "url": rms1.url()
        },
    }});

    println!("{}", serde_json::to_string_pretty(&network).unwrap());

    let (_response, status_code) = ms0.set_network(network.clone()).await;
    snapshot!(status_code, @"200 OK");
    let (_response, status_code) = ms1.set_network(network.clone()).await;
    snapshot!(status_code, @"200 OK");

    // perform multi-search
    let query = "badman returns";
    let request = json!({
        "federation": {},
        "queries": [
            {
                "q": query,
                "indexUid": "test",
                "federationOptions": {
                    "remote": "ms0"
                }
            },
            {
                "q": query,
                "indexUid": "test",
                "federationOptions": {
                    "remote": "ms1"
                }
            },
            {
                "q": query,
                "indexUid": "test",
                "federationOptions": {
                    "remote": "ms2"
                }
            },
        ]
    });

    let (response, _status_code) = ms0.multi_search(request.clone()).await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response, { ".processingTimeMs" => "[time]" }), @r###"
    {
      "message": "Invalid `queries[2].federation_options.remote`: remote `ms2` is not registered",
      "code": "invalid_multi_search_remote",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_multi_search_remote"
    }
    "###);
    let (response, _status_code) = ms1.multi_search(request.clone()).await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response, { ".processingTimeMs" => "[time]" }), @r###"
    {
      "message": "Invalid `queries[2].federation_options.remote`: remote `ms2` is not registered",
      "code": "invalid_multi_search_remote",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_multi_search_remote"
    }
    "###);
}

#[actix_rt::test]
async fn error_no_weighted_score() {
    let ms0 = Server::new().await;
    let ms1 = Server::new().await;

    // enable feature

    let (response, code) = ms0.set_features(json!({"network": true})).await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response["network"]), @"true");
    let (response, code) = ms1.set_features(json!({"network": true})).await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response["network"]), @"true");

    // set self

    let (response, code) = ms0.set_network(json!({"self": "ms0"})).await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response), @r###"
    {
      "self": "ms0",
      "remotes": {},
      "sharding": false
    }
    "###);
    let (response, code) = ms1.set_network(json!({"self": "ms1"})).await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response), @r###"
    {
      "self": "ms1",
      "remotes": {},
      "sharding": false
    }
    "###);

    // add documents
    let documents = SCORE_DOCUMENTS.clone();
    let documents = documents.as_array().unwrap();
    let index0 = ms0.index("test");
    let index1 = ms1.index("test");
    let (task, _status_code) = index0.add_documents(json!(documents[0..2]), None).await;
    ms0.wait_task(task.uid()).await.succeeded();
    let (task, _status_code) = index1.add_documents(json!(documents[2..3]), None).await;
    ms1.wait_task(task.uid()).await.succeeded();

    // wrap servers
    let ms0 = Arc::new(ms0);
    let ms1 = Arc::new(ms1);

    let rms0 = LocalMeili::new(ms0.clone()).await;
    let rms1 = LocalMeili::with_params(
        ms1.clone(),
        LocalMeiliParams { gobble_headers: true, ..Default::default() },
    )
    .await;

    // set network
    let network = json!({"remotes": {
        "ms0": {
            "url": rms0.url()
        },
        "ms1": {
            "url": rms1.url()
        },
    }});

    println!("{}", serde_json::to_string_pretty(&network).unwrap());

    let (_response, status_code) = ms0.set_network(network.clone()).await;
    snapshot!(status_code, @"200 OK");

    // perform multi-search
    let query = "badman returns";
    let request = json!({
        "federation": {},
        "queries": [
            {
                "q": query,
                "indexUid": "test",
                "federationOptions": {
                    "remote": "ms0"
                }
            },
            {
                "q": query,
                "indexUid": "test",
                "federationOptions": {
                    "remote": "ms1"
                }
            },
        ]
    });

    let (response, _status_code) = ms0.multi_search(request.clone()).await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response, { ".processingTimeMs" => "[time]", ".requestUid" => "[uuid]" }), @r###"
    {
      "hits": [
        {
          "title": "Batman the dark knight returns: Part 1",
          "id": "A",
          "_federation": {
            "indexUid": "test",
            "queriesPosition": 0,
            "weightedRankingScore": 0.7028218694885362,
            "remote": "ms0"
          }
        },
        {
          "title": "Batman the dark knight returns: Part 2",
          "id": "B",
          "_federation": {
            "indexUid": "test",
            "queriesPosition": 0,
            "weightedRankingScore": 0.7028218694885362,
            "remote": "ms0"
          }
        }
      ],
      "processingTimeMs": "[time]",
      "limit": 20,
      "offset": 0,
      "estimatedTotalHits": 2,
      "requestUid": "[uuid]",
      "remoteErrors": {
        "ms1": {
          "message": "remote hit does not contain `._federation.weightedScoreValues`\n  - hint: check that the remote instance is a Meilisearch instance running the same version",
          "code": "remote_bad_response",
          "type": "system",
          "link": "https://docs.meilisearch.com/errors#remote_bad_response"
        }
      }
    }
    "###);
}

#[actix_rt::test]
async fn error_bad_response() {
    let ms0 = Server::new().await;
    let ms1 = Server::new().await;

    // enable feature

    let (response, code) = ms0.set_features(json!({"network": true})).await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response["network"]), @"true");
    let (response, code) = ms1.set_features(json!({"network": true})).await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response["network"]), @"true");

    // set self

    let (response, code) = ms0.set_network(json!({"self": "ms0"})).await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response), @r###"
    {
      "self": "ms0",
      "remotes": {},
      "sharding": false
    }
    "###);
    let (response, code) = ms1.set_network(json!({"self": "ms1"})).await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response), @r###"
    {
      "self": "ms1",
      "remotes": {},
      "sharding": false
    }
    "###);

    // add documents
    let documents = SCORE_DOCUMENTS.clone();
    let documents = documents.as_array().unwrap();
    let index0 = ms0.index("test");
    let index1 = ms1.index("test");
    let (task, _status_code) = index0.add_documents(json!(documents[0..2]), None).await;
    ms0.wait_task(task.uid()).await.succeeded();
    let (task, _status_code) = index1.add_documents(json!(documents[2..3]), None).await;
    ms1.wait_task(task.uid()).await.succeeded();

    // wrap servers
    let ms0 = Arc::new(ms0);
    let ms1 = Arc::new(ms1);

    let rms0 = LocalMeili::new(ms0.clone()).await;
    let rms1 = LocalMeili::with_params(
        ms1.clone(),
        LocalMeiliParams {
            override_response_body: Some("<html>Returning an HTML page</html>".into()),
            ..Default::default()
        },
    )
    .await;

    // set network
    let network = json!({"remotes": {
        "ms0": {
            "url": rms0.url()
        },
        "ms1": {
            "url": rms1.url()
        },
    }});

    println!("{}", serde_json::to_string_pretty(&network).unwrap());

    let (_response, status_code) = ms0.set_network(network.clone()).await;
    snapshot!(status_code, @"200 OK");

    // perform multi-search
    let query = "badman returns";
    let request = json!({
        "federation": {},
        "queries": [
            {
                "q": query,
                "indexUid": "test",
                "federationOptions": {
                    "remote": "ms0"
                }
            },
            {
                "q": query,
                "indexUid": "test",
                "federationOptions": {
                    "remote": "ms1"
                }
            },
        ]
    });

    let (response, _status_code) = ms0.multi_search(request.clone()).await;
    snapshot!(code, @"200 OK");

    snapshot!(json_string!(response, { ".processingTimeMs" => "[time]", ".requestUid" => "[uuid]" }), @r###"
    {
      "hits": [
        {
          "title": "Batman the dark knight returns: Part 1",
          "id": "A",
          "_federation": {
            "indexUid": "test",
            "queriesPosition": 0,
            "weightedRankingScore": 0.7028218694885362,
            "remote": "ms0"
          }
        },
        {
          "title": "Batman the dark knight returns: Part 2",
          "id": "B",
          "_federation": {
            "indexUid": "test",
            "queriesPosition": 0,
            "weightedRankingScore": 0.7028218694885362,
            "remote": "ms0"
          }
        }
      ],
      "processingTimeMs": "[time]",
      "limit": 20,
      "offset": 0,
      "estimatedTotalHits": 2,
      "requestUid": "[uuid]",
      "remoteErrors": {
        "ms1": {
          "message": "could not parse response from the remote host as a federated search response:\n  - response from remote: <html>Returning an HTML page</html>\n  - hint: check that the remote instance is a Meilisearch instance running the same version",
          "code": "remote_bad_response",
          "type": "system",
          "link": "https://docs.meilisearch.com/errors#remote_bad_response"
        }
      }
    }
    "###);
}

#[actix_rt::test]
async fn error_bad_request() {
    let ms0 = Server::new().await;
    let ms1 = Server::new().await;

    // enable feature

    let (response, code) = ms0.set_features(json!({"network": true})).await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response["network"]), @"true");
    let (response, code) = ms1.set_features(json!({"network": true})).await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response["network"]), @"true");

    // set self

    let (response, code) = ms0.set_network(json!({"self": "ms0"})).await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response), @r###"
    {
      "self": "ms0",
      "remotes": {},
      "sharding": false
    }
    "###);
    let (response, code) = ms1.set_network(json!({"self": "ms1"})).await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response), @r###"
    {
      "self": "ms1",
      "remotes": {},
      "sharding": false
    }
    "###);

    // add documents
    let documents = SCORE_DOCUMENTS.clone();
    let documents = documents.as_array().unwrap();
    let index0 = ms0.index("test");
    let index1 = ms1.index("test");
    let (task, _status_code) = index0.add_documents(json!(documents[0..2]), None).await;
    ms0.wait_task(task.uid()).await.succeeded();
    let (task, _status_code) = index1.add_documents(json!(documents[2..3]), None).await;
    ms1.wait_task(task.uid()).await.succeeded();

    // wrap servers
    let ms0 = Arc::new(ms0);
    let ms1 = Arc::new(ms1);

    let rms0 = LocalMeili::new(ms0.clone()).await;
    let rms1 = LocalMeili::new(ms1.clone()).await;

    // set network
    let network = json!({"remotes": {
        "ms0": {
            "url": rms0.url()
        },
        "ms1": {
            "url": rms1.url()
        },
    }});

    println!("{}", serde_json::to_string_pretty(&network).unwrap());

    let (_response, status_code) = ms0.set_network(network.clone()).await;
    snapshot!(status_code, @"200 OK");

    // perform multi-search
    let query = "badman returns";
    let request = json!({
        "federation": {},
        "queries": [
            {
                "q": query,
                "indexUid": "test",
                "federationOptions": {
                    "remote": "ms0"
                }
            },
            {
                "q": query,
                "indexUid": "nottest",
                "federationOptions": {
                    "remote": "ms1"
                }
            },
        ]
    });

    let (response, _status_code) = ms0.multi_search(request.clone()).await;
    snapshot!(code, @"200 OK");

    snapshot!(json_string!(response, { ".processingTimeMs" => "[time]", ".requestUid" => "[uuid]" }), @r###"
    {
      "hits": [
        {
          "title": "Batman the dark knight returns: Part 1",
          "id": "A",
          "_federation": {
            "indexUid": "test",
            "queriesPosition": 0,
            "weightedRankingScore": 0.7028218694885362,
            "remote": "ms0"
          }
        },
        {
          "title": "Batman the dark knight returns: Part 2",
          "id": "B",
          "_federation": {
            "indexUid": "test",
            "queriesPosition": 0,
            "weightedRankingScore": 0.7028218694885362,
            "remote": "ms0"
          }
        }
      ],
      "processingTimeMs": "[time]",
      "limit": 20,
      "offset": 0,
      "estimatedTotalHits": 2,
      "requestUid": "[uuid]",
      "remoteErrors": {
        "ms1": {
          "message": "remote host responded with code 400:\n  - response from remote: {\"message\":\"Inside `.queries[1]`: Index `nottest` not found.\",\"code\":\"index_not_found\",\"type\":\"invalid_request\",\"link\":\"https://docs.meilisearch.com/errors#index_not_found\"}\n  - hint: check that the remote instance has the correct index configuration for that request\n  - hint: check that the `network` experimental feature is enabled on the remote instance",
          "code": "remote_bad_request",
          "type": "invalid_request",
          "link": "https://docs.meilisearch.com/errors#remote_bad_request"
        }
      }
    }
    "###);
}

#[actix_rt::test]
async fn error_bad_request_facets_by_index() {
    let ms0 = Server::new().await;
    let ms1 = Server::new().await;

    // enable feature

    let (response, code) = ms0.set_features(json!({"network": true})).await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response["network"]), @"true");
    let (response, code) = ms1.set_features(json!({"network": true})).await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response["network"]), @"true");

    // set self

    let (response, code) = ms0.set_network(json!({"self": "ms0"})).await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response), @r###"
    {
      "self": "ms0",
      "remotes": {},
      "sharding": false
    }
    "###);
    let (response, code) = ms1.set_network(json!({"self": "ms1"})).await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response), @r###"
    {
      "self": "ms1",
      "remotes": {},
      "sharding": false
    }
    "###);

    // add documents
    let documents = SCORE_DOCUMENTS.clone();
    let documents = documents.as_array().unwrap();
    let index0 = ms0.index("test0");
    let index1 = ms1.index("test1");
    let (task, _status_code) = index0.add_documents(json!(documents[0..2]), None).await;
    ms0.wait_task(task.uid()).await.succeeded();

    let (task, _status_code) = index1.add_documents(json!(documents[2..3]), None).await;
    ms1.wait_task(task.uid()).await.succeeded();

    // wrap servers
    let ms0 = Arc::new(ms0);
    let ms1 = Arc::new(ms1);

    let rms0 = LocalMeili::new(ms0.clone()).await;
    let rms1 = LocalMeili::new(ms1.clone()).await;

    // set network
    let network = json!({"remotes": {
        "ms0": {
            "url": rms0.url()
        },
        "ms1": {
            "url": rms1.url()
        },
    }});

    println!("{}", serde_json::to_string_pretty(&network).unwrap());

    let (_response, status_code) = ms0.set_network(network.clone()).await;
    snapshot!(status_code, @"200 OK");

    // perform multi-search
    let query = "badman returns";
    let request = json!({
        "federation": {
          "facetsByIndex": {
            "test0": []
          }
        },
        "queries": [
            {
                "q": query,
                "indexUid": "test0",
                "federationOptions": {
                    "remote": "ms0"
                }
            },
            {
                "q": query,
                "indexUid": "test1",
                "federationOptions": {
                    "remote": "ms1"
                }
            },
        ]
    });

    let (response, _status_code) = ms0.multi_search(request.clone()).await;
    snapshot!(code, @"200 OK");

    snapshot!(json_string!(response, { ".processingTimeMs" => "[time]", ".requestUid" => "[uuid]" }), @r###"
    {
      "hits": [
        {
          "title": "Batman the dark knight returns: Part 1",
          "id": "A",
          "_federation": {
            "indexUid": "test0",
            "queriesPosition": 0,
            "weightedRankingScore": 0.7028218694885362,
            "remote": "ms0"
          }
        },
        {
          "title": "Batman the dark knight returns: Part 2",
          "id": "B",
          "_federation": {
            "indexUid": "test0",
            "queriesPosition": 0,
            "weightedRankingScore": 0.7028218694885362,
            "remote": "ms0"
          }
        }
      ],
      "processingTimeMs": "[time]",
      "limit": 20,
      "offset": 0,
      "estimatedTotalHits": 2,
      "facetsByIndex": {
        "test0": {
          "distribution": {},
          "stats": {}
        }
      },
      "requestUid": "[uuid]",
      "remoteErrors": {
        "ms1": {
          "message": "remote host responded with code 400:\n  - response from remote: {\"message\":\"Inside `.federation.facetsByIndex.test0`: Index `test0` not found.\\n - Note: index `test0` is not used in queries\",\"code\":\"index_not_found\",\"type\":\"invalid_request\",\"link\":\"https://docs.meilisearch.com/errors#index_not_found\"}\n  - hint: check that the remote instance has the correct index configuration for that request\n  - hint: check that the `network` experimental feature is enabled on the remote instance",
          "code": "remote_bad_request",
          "type": "invalid_request",
          "link": "https://docs.meilisearch.com/errors#remote_bad_request"
        }
      }
    }
    "###);
}

#[actix_rt::test]
async fn error_bad_request_facets_by_index_facet() {
    let ms0 = Server::new().await;
    let ms1 = Server::new().await;

    // enable feature

    let (response, code) = ms0.set_features(json!({"network": true})).await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response["network"]), @"true");
    let (response, code) = ms1.set_features(json!({"network": true})).await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response["network"]), @"true");

    // set self

    let (response, code) = ms0.set_network(json!({"self": "ms0"})).await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response), @r###"
    {
      "self": "ms0",
      "remotes": {},
      "sharding": false
    }
    "###);
    let (response, code) = ms1.set_network(json!({"self": "ms1"})).await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response), @r###"
    {
      "self": "ms1",
      "remotes": {},
      "sharding": false
    }
    "###);

    // add documents
    let documents = SCORE_DOCUMENTS.clone();
    let documents = documents.as_array().unwrap();
    let index0 = ms0.index("test");
    let index1 = ms1.index("test");
    let (task, _status_code) = index0.add_documents(json!(documents[0..2]), None).await;
    ms0.wait_task(task.uid()).await.succeeded();

    let (task, _status_code) = index0.update_settings_filterable_attributes(json!(["id"])).await;
    ms0.wait_task(task.uid()).await.succeeded();

    let (task, _status_code) = index1.add_documents(json!(documents[2..3]), None).await;
    ms1.wait_task(task.uid()).await.succeeded();

    // wrap servers
    let ms0 = Arc::new(ms0);
    let ms1 = Arc::new(ms1);

    let rms0 = LocalMeili::new(ms0.clone()).await;
    let rms1 = LocalMeili::new(ms1.clone()).await;

    // set network
    let network = json!({"remotes": {
        "ms0": {
            "url": rms0.url()
        },
        "ms1": {
            "url": rms1.url()
        },
    }});

    println!("{}", serde_json::to_string_pretty(&network).unwrap());

    let (_response, status_code) = ms0.set_network(network.clone()).await;
    snapshot!(status_code, @"200 OK");

    // perform multi-search
    let query = "badman returns";
    let request = json!({
        "federation": {
          "facetsByIndex": {
            "test": ["id"]
          }
        },
        "queries": [
            {
                "q": query,
                "indexUid": "test",
                "federationOptions": {
                    "remote": "ms0"
                }
            },
            {
                "q": query,
                "indexUid": "test",
                "federationOptions": {
                    "remote": "ms1"
                }
            },
        ]
    });

    let (response, _status_code) = ms0.multi_search(request.clone()).await;
    snapshot!(code, @"200 OK");

    snapshot!(json_string!(response, { ".processingTimeMs" => "[time]", ".requestUid" => "[uuid]" }), @r###"
    {
      "hits": [
        {
          "title": "Batman the dark knight returns: Part 1",
          "id": "A",
          "_federation": {
            "indexUid": "test",
            "queriesPosition": 0,
            "weightedRankingScore": 0.7028218694885362,
            "remote": "ms0"
          }
        },
        {
          "title": "Batman the dark knight returns: Part 2",
          "id": "B",
          "_federation": {
            "indexUid": "test",
            "queriesPosition": 0,
            "weightedRankingScore": 0.7028218694885362,
            "remote": "ms0"
          }
        }
      ],
      "processingTimeMs": "[time]",
      "limit": 20,
      "offset": 0,
      "estimatedTotalHits": 2,
      "facetsByIndex": {
        "test": {
          "distribution": {
            "id": {
              "A": 1,
              "B": 1
            }
          },
          "stats": {}
        }
      },
      "requestUid": "[uuid]",
      "remoteErrors": {
        "ms1": {
          "message": "remote host responded with code 400:\n  - response from remote: {\"message\":\"Inside `.federation.facetsByIndex.test`: Invalid facet distribution: Attribute `id` is not filterable. This index does not have configured filterable attributes.\\n - Note: index `test` used in `.queries[1]`\",\"code\":\"invalid_multi_search_facets\",\"type\":\"invalid_request\",\"link\":\"https://docs.meilisearch.com/errors#invalid_multi_search_facets\"}\n  - hint: check that the remote instance has the correct index configuration for that request\n  - hint: check that the `network` experimental feature is enabled on the remote instance",
          "code": "remote_bad_request",
          "type": "invalid_request",
          "link": "https://docs.meilisearch.com/errors#remote_bad_request"
        }
      }
    }
    "###);
}

#[actix_rt::test]
#[ignore]
async fn error_remote_does_not_answer() {
    let ms0 = Server::new().await;
    let ms1 = Server::new().await;

    // enable feature

    let (response, code) = ms0.set_features(json!({"network": true})).await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response["network"]), @"true");
    let (response, code) = ms1.set_features(json!({"network": true})).await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response["network"]), @"true");

    // set self

    let (response, code) = ms0.set_network(json!({"self": "ms0"})).await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response), @r###"
    {
      "self": "ms0",
      "remotes": {},
      "sharding": false
    }
    "###);
    let (response, code) = ms1.set_network(json!({"self": "ms1"})).await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response), @r###"
    {
      "self": "ms1",
      "remotes": {},
      "sharding": false
    }
    "###);

    // add documents
    let documents = SCORE_DOCUMENTS.clone();
    let documents = documents.as_array().unwrap();
    let index0 = ms0.index("test");
    let index1 = ms1.index("test");
    let (task, _status_code) = index0.add_documents(json!(documents[0..2]), None).await;
    ms0.wait_task(task.uid()).await.succeeded();
    let (task, _status_code) = index1.add_documents(json!(documents[2..3]), None).await;
    ms1.wait_task(task.uid()).await.succeeded();

    // wrap servers
    let ms0 = Arc::new(ms0);
    let ms1 = Arc::new(ms1);

    let rms0 = LocalMeili::new(ms0.clone()).await;
    let rms1 = LocalMeili::new(ms1.clone()).await;

    // set network
    let network = json!({"remotes": {
        "ms0": {
            "url": rms0.url()
        },
        "ms1": {
            "url": rms1.url()
        },
        "ms2": {
          "url": "https://thiswebsitedoesnotexist.example"
        }
    }});

    println!("{}", serde_json::to_string_pretty(&network).unwrap());

    let (_response, status_code) = ms0.set_network(network.clone()).await;
    snapshot!(status_code, @"200 OK");
    let (_response, status_code) = ms1.set_network(network.clone()).await;
    snapshot!(status_code, @"200 OK");

    // perform multi-search
    let query = "badman returns";
    let request = json!({
        "federation": {},
        "queries": [
            {
                "q": query,
                "indexUid": "test",
                "federationOptions": {
                    "remote": "ms0"
                }
            },
            {
                "q": query,
                "indexUid": "test",
                "federationOptions": {
                    "remote": "ms1"
                }
            },
            {
                "q": query,
                "indexUid": "test",
                "federationOptions": {
                    "remote": "ms2"
                }
            },
        ]
    });

    let (response, _status_code) = ms0.multi_search(request.clone()).await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response, { ".processingTimeMs" => "[time]" }), @r###"
    {
      "hits": [
        {
          "title": "Batman Returns",
          "id": "C",
          "_federation": {
            "indexUid": "test",
            "queriesPosition": 1,
            "weightedRankingScore": 0.8317901234567902,
            "remote": "ms1"
          }
        },
        {
          "title": "Batman the dark knight returns: Part 1",
          "id": "A",
          "_federation": {
            "indexUid": "test",
            "queriesPosition": 0,
            "weightedRankingScore": 0.7028218694885362,
            "remote": "ms0"
          }
        },
        {
          "title": "Batman the dark knight returns: Part 2",
          "id": "B",
          "_federation": {
            "indexUid": "test",
            "queriesPosition": 0,
            "weightedRankingScore": 0.7028218694885362,
            "remote": "ms0"
          }
        }
      ],
      "processingTimeMs": "[time]",
      "limit": 20,
      "offset": 0,
      "estimatedTotalHits": 3,
      "remoteErrors": {
        "ms2": {
          "message": "error sending request",
          "code": "remote_could_not_send_request",
          "type": "system",
          "link": "https://docs.meilisearch.com/errors#remote_could_not_send_request"
        }
      }
    }
    "###);
    let (response, _status_code) = ms1.multi_search(request.clone()).await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response, { ".processingTimeMs" => "[time]" }), @r#"
    {
      "hits": [
        {
          "title": "Batman Returns",
          "id": "C",
          "_federation": {
            "indexUid": "test",
            "queriesPosition": 1,
            "weightedRankingScore": 0.8317901234567902,
            "remote": "ms1"
          }
        },
        {
          "title": "Batman the dark knight returns: Part 1",
          "id": "A",
          "_federation": {
            "indexUid": "test",
            "queriesPosition": 0,
            "weightedRankingScore": 0.7028218694885362,
            "remote": "ms0"
          }
        },
        {
          "title": "Batman the dark knight returns: Part 2",
          "id": "B",
          "_federation": {
            "indexUid": "test",
            "queriesPosition": 0,
            "weightedRankingScore": 0.7028218694885362,
            "remote": "ms0"
          }
        }
      ],
      "processingTimeMs": "[time]",
      "limit": 20,
      "offset": 0,
      "estimatedTotalHits": 3,
      "remoteErrors": {
        "ms2": {
          "message": "error sending request",
          "code": "remote_could_not_send_request",
          "type": "system",
          "link": "https://docs.meilisearch.com/errors#remote_could_not_send_request"
        }
      }
    }
    "#);
}

#[actix_rt::test]
async fn error_remote_404() {
    let ms0 = Server::new().await;
    let ms1 = Server::new().await;

    // enable feature

    let (response, code) = ms0.set_features(json!({"network": true})).await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response["network"]), @"true");
    let (response, code) = ms1.set_features(json!({"network": true})).await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response["network"]), @"true");

    // set self

    let (response, code) = ms0.set_network(json!({"self": "ms0"})).await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response), @r###"
    {
      "self": "ms0",
      "remotes": {},
      "sharding": false
    }
    "###);
    let (response, code) = ms1.set_network(json!({"self": "ms1"})).await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response), @r###"
    {
      "self": "ms1",
      "remotes": {},
      "sharding": false
    }
    "###);

    // add documents
    let documents = SCORE_DOCUMENTS.clone();
    let documents = documents.as_array().unwrap();
    let index0 = ms0.index("test");
    let index1 = ms1.index("test");
    let (task, _status_code) = index0.add_documents(json!(documents[0..2]), None).await;
    ms0.wait_task(task.uid()).await.succeeded();
    let (task, _status_code) = index1.add_documents(json!(documents[2..3]), None).await;
    ms1.wait_task(task.uid()).await.succeeded();

    // wrap servers
    let ms0 = Arc::new(ms0);
    let ms1 = Arc::new(ms1);

    let rms0 = LocalMeili::new(ms0.clone()).await;
    let rms1 = LocalMeili::new(ms1.clone()).await;

    // set network
    let network = json!({"remotes": {
        "ms0": {
            "url": rms0.url()
        },
        "ms1": {
            "url": format!("{}/this-route-does-not-exists/", rms1.url())
        },
    }});

    println!("{}", serde_json::to_string_pretty(&network).unwrap());

    let (_response, status_code) = ms0.set_network(network.clone()).await;
    snapshot!(status_code, @"200 OK");
    let (_response, status_code) = ms1.set_network(network.clone()).await;
    snapshot!(status_code, @"200 OK");

    // perform multi-search
    let query = "badman returns";
    let request = json!({
        "federation": {},
        "queries": [
            {
                "q": query,
                "indexUid": "test",
                "federationOptions": {
                    "remote": "ms0"
                }
            },
            {
                "q": query,
                "indexUid": "test",
                "federationOptions": {
                    "remote": "ms1"
                }
            },
        ]
    });

    let (response, _status_code) = ms0.multi_search(request.clone()).await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response, { ".processingTimeMs" => "[time]", ".requestUid" => "[uuid]" }), @r###"
    {
      "hits": [
        {
          "title": "Batman the dark knight returns: Part 1",
          "id": "A",
          "_federation": {
            "indexUid": "test",
            "queriesPosition": 0,
            "weightedRankingScore": 0.7028218694885362,
            "remote": "ms0"
          }
        },
        {
          "title": "Batman the dark knight returns: Part 2",
          "id": "B",
          "_federation": {
            "indexUid": "test",
            "queriesPosition": 0,
            "weightedRankingScore": 0.7028218694885362,
            "remote": "ms0"
          }
        }
      ],
      "processingTimeMs": "[time]",
      "limit": 20,
      "offset": 0,
      "estimatedTotalHits": 2,
      "requestUid": "[uuid]",
      "remoteErrors": {
        "ms1": {
          "message": "remote host responded with code 404:\n  - response from remote: null\n  - hint: check that the remote instance has the correct index configuration for that request\n  - hint: check that the `network` experimental feature is enabled on the remote instance",
          "code": "remote_bad_request",
          "type": "invalid_request",
          "link": "https://docs.meilisearch.com/errors#remote_bad_request"
        }
      }
    }
    "###);
    let (response, _status_code) = ms1.multi_search(request.clone()).await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response, { ".processingTimeMs" => "[time]", ".requestUid" => "[uuid]" }), @r###"
    {
      "hits": [
        {
          "title": "Batman Returns",
          "id": "C",
          "_federation": {
            "indexUid": "test",
            "queriesPosition": 1,
            "weightedRankingScore": 0.8317901234567902,
            "remote": "ms1"
          }
        },
        {
          "title": "Batman the dark knight returns: Part 1",
          "id": "A",
          "_federation": {
            "indexUid": "test",
            "queriesPosition": 0,
            "weightedRankingScore": 0.7028218694885362,
            "remote": "ms0"
          }
        },
        {
          "title": "Batman the dark knight returns: Part 2",
          "id": "B",
          "_federation": {
            "indexUid": "test",
            "queriesPosition": 0,
            "weightedRankingScore": 0.7028218694885362,
            "remote": "ms0"
          }
        }
      ],
      "processingTimeMs": "[time]",
      "limit": 20,
      "offset": 0,
      "estimatedTotalHits": 3,
      "requestUid": "[uuid]",
      "remoteErrors": {}
    }
    "###);
}

#[actix_rt::test]
async fn error_remote_sharding_auth() {
    let ms0 = Server::new().await;
    let mut ms1 = Server::new_auth().await;
    ms1.use_api_key("MASTER_KEY");

    let (search_api_key_not_enough_indexes, code) = ms1
        .add_api_key(json!({
          "actions": ["search"],
          "indexes": ["nottest"],
          "expiresAt": serde_json::Value::Null
        }))
        .await;
    meili_snap::snapshot!(code, @"201 Created");
    let search_api_key_not_enough_indexes = search_api_key_not_enough_indexes["key"].clone();

    let (api_key_not_search, code) = ms1
        .add_api_key(json!({
          "actions": ["documents.*"],
          "indexes": ["*"],
          "expiresAt": serde_json::Value::Null
        }))
        .await;
    meili_snap::snapshot!(code, @"201 Created");
    let api_key_not_search = api_key_not_search["key"].clone();

    // enable feature

    let (response, code) = ms0.set_features(json!({"network": true})).await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response["network"]), @"true");
    let (response, code) = ms1.set_features(json!({"network": true})).await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response["network"]), @"true");

    // set self

    let (response, code) = ms0.set_network(json!({"self": "ms0"})).await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response), @r###"
    {
      "self": "ms0",
      "remotes": {},
      "sharding": false
    }
    "###);
    let (response, code) = ms1.set_network(json!({"self": "ms1"})).await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response), @r###"
    {
      "self": "ms1",
      "remotes": {},
      "sharding": false
    }
    "###);

    // add documents
    let documents = SCORE_DOCUMENTS.clone();
    let documents = documents.as_array().unwrap();
    let index0 = ms0.index("test");
    let index1 = ms1.index("test");
    let (task, _status_code) = index0.add_documents(json!(documents[0..2]), None).await;
    ms0.wait_task(task.uid()).await.succeeded();
    let (task, _status_code) = index1.add_documents(json!(documents[2..3]), None).await;
    ms1.wait_task(task.uid()).await.succeeded();

    // wrap servers
    ms1.clear_api_key();

    let ms0 = Arc::new(ms0);
    let ms1 = Arc::new(ms1);

    let rms0 = LocalMeili::new(ms0.clone()).await;
    let rms1 = LocalMeili::new(ms1.clone()).await;

    // set network
    let network = json!({"remotes": {
        "ms0": {
            "url": rms0.url()
        },
        "ms1-nottest": {
            "url": rms1.url(),
            "searchApiKey": search_api_key_not_enough_indexes
        },
        "ms1-notsearch": {
          "url": rms1.url(),
          "searchApiKey": api_key_not_search
        }
    }});

    println!("{}", serde_json::to_string_pretty(&network).unwrap());

    let (_response, status_code) = ms0.set_network(network.clone()).await;
    snapshot!(status_code, @"200 OK");

    // perform multi-search
    let query = "badman returns";
    let request = json!({
        "federation": {},
        "queries": [
            {
                "q": query,
                "indexUid": "test",
                "federationOptions": {
                    "remote": "ms0"
                }
            },
            {
                "q": query,
                "indexUid": "test",
                "federationOptions": {
                    "remote": "ms1-nottest"
                }
            },
            {
                "q": query,
                "indexUid": "test",
                "federationOptions": {
                    "remote": "ms1-notsearch"
                }
            },
        ]
    });

    let (response, _status_code) = ms0.multi_search(request.clone()).await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response, { ".processingTimeMs" => "[time]", ".requestUid" => "[uuid]" }), @r###"
    {
      "hits": [
        {
          "title": "Batman the dark knight returns: Part 1",
          "id": "A",
          "_federation": {
            "indexUid": "test",
            "queriesPosition": 0,
            "weightedRankingScore": 0.7028218694885362,
            "remote": "ms0"
          }
        },
        {
          "title": "Batman the dark knight returns: Part 2",
          "id": "B",
          "_federation": {
            "indexUid": "test",
            "queriesPosition": 0,
            "weightedRankingScore": 0.7028218694885362,
            "remote": "ms0"
          }
        }
      ],
      "processingTimeMs": "[time]",
      "limit": 20,
      "offset": 0,
      "estimatedTotalHits": 2,
      "requestUid": "[uuid]",
      "remoteErrors": {
        "ms1-notsearch": {
          "message": "could not authenticate against the remote host\n  - hint: check that the remote instance was registered with a valid API key having the `search` action",
          "code": "remote_invalid_api_key",
          "type": "auth",
          "link": "https://docs.meilisearch.com/errors#remote_invalid_api_key"
        },
        "ms1-nottest": {
          "message": "could not authenticate against the remote host\n  - hint: check that the remote instance was registered with a valid API key having the `search` action",
          "code": "remote_invalid_api_key",
          "type": "auth",
          "link": "https://docs.meilisearch.com/errors#remote_invalid_api_key"
        }
      }
    }
    "###);
}

#[actix_rt::test]
async fn remote_sharding_auth() {
    let ms0 = Server::new().await;
    let mut ms1 = Server::new_auth().await;
    ms1.use_api_key("MASTER_KEY");

    let (search_api_key, code) = ms1
        .add_api_key(json!({
          "actions": ["search"],
          "indexes": ["*"],
          "expiresAt": serde_json::Value::Null
        }))
        .await;
    meili_snap::snapshot!(code, @"201 Created");
    let search_api_key = search_api_key["key"].clone();

    // enable feature

    let (response, code) = ms0.set_features(json!({"network": true})).await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response["network"]), @"true");
    let (response, code) = ms1.set_features(json!({"network": true})).await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response["network"]), @"true");

    // set self

    let (response, code) = ms0.set_network(json!({"self": "ms0"})).await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response), @r###"
    {
      "self": "ms0",
      "remotes": {},
      "sharding": false
    }
    "###);
    let (response, code) = ms1.set_network(json!({"self": "ms1"})).await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response), @r###"
    {
      "self": "ms1",
      "remotes": {},
      "sharding": false
    }
    "###);

    // add documents
    let documents = SCORE_DOCUMENTS.clone();
    let documents = documents.as_array().unwrap();
    let index0 = ms0.index("test");
    let index1 = ms1.index("test");
    let (task, _status_code) = index0.add_documents(json!(documents[0..2]), None).await;
    ms0.wait_task(task.uid()).await.succeeded();
    let (task, _status_code) = index1.add_documents(json!(documents[2..3]), None).await;
    ms1.wait_task(task.uid()).await.succeeded();

    // wrap servers
    ms1.clear_api_key();
    let ms0 = Arc::new(ms0);
    let ms1 = Arc::new(ms1);

    let rms0 = LocalMeili::new(ms0.clone()).await;
    let rms1 = LocalMeili::new(ms1.clone()).await;

    // set network
    let network = json!({"remotes": {
        "ms0": {
            "url": rms0.url()
        },
        "ms1": {
            "url": rms1.url(),
            "searchApiKey": "MASTER_KEY"
        },
        "ms1-alias": {
          "url": rms1.url(),
          "searchApiKey": search_api_key
        }
    }});

    println!("{}", serde_json::to_string_pretty(&network).unwrap());

    let (_response, status_code) = ms0.set_network(network.clone()).await;
    snapshot!(status_code, @"200 OK");

    // perform multi-search
    let query = "badman returns";
    let request = json!({
        "federation": {},
        "queries": [
            {
                "q": query,
                "indexUid": "test",
                "federationOptions": {
                    "remote": "ms0"
                }
            },
            {
                "q": query,
                "indexUid": "test",
                "federationOptions": {
                    "remote": "ms1"
                }
            },
            {
                "q": query,
                "indexUid": "test",
                "federationOptions": {
                    "remote": "ms1-alias"
                }
            },
        ]
    });

    let (response, _status_code) = ms0.multi_search(request.clone()).await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response, { ".processingTimeMs" => "[time]", ".requestUid" => "[uuid]" }), @r###"
    {
      "hits": [
        {
          "title": "Batman Returns",
          "id": "C",
          "_federation": {
            "indexUid": "test",
            "queriesPosition": 1,
            "weightedRankingScore": 0.8317901234567902,
            "remote": "ms1"
          }
        },
        {
          "title": "Batman Returns",
          "id": "C",
          "_federation": {
            "indexUid": "test",
            "queriesPosition": 2,
            "weightedRankingScore": 0.8317901234567902,
            "remote": "ms1-alias"
          }
        },
        {
          "title": "Batman the dark knight returns: Part 1",
          "id": "A",
          "_federation": {
            "indexUid": "test",
            "queriesPosition": 0,
            "weightedRankingScore": 0.7028218694885362,
            "remote": "ms0"
          }
        },
        {
          "title": "Batman the dark knight returns: Part 2",
          "id": "B",
          "_federation": {
            "indexUid": "test",
            "queriesPosition": 0,
            "weightedRankingScore": 0.7028218694885362,
            "remote": "ms0"
          }
        }
      ],
      "processingTimeMs": "[time]",
      "limit": 20,
      "offset": 0,
      "estimatedTotalHits": 4,
      "requestUid": "[uuid]",
      "remoteErrors": {}
    }
    "###);
}

#[actix_rt::test]
async fn error_remote_500() {
    let ms0 = Server::new().await;
    let ms1 = Server::new().await;

    // enable feature

    let (response, code) = ms0.set_features(json!({"network": true})).await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response["network"]), @"true");
    let (response, code) = ms1.set_features(json!({"network": true})).await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response["network"]), @"true");

    // set self

    let (response, code) = ms0.set_network(json!({"self": "ms0"})).await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response), @r###"
    {
      "self": "ms0",
      "remotes": {},
      "sharding": false
    }
    "###);
    let (response, code) = ms1.set_network(json!({"self": "ms1"})).await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response), @r###"
    {
      "self": "ms1",
      "remotes": {},
      "sharding": false
    }
    "###);

    // add documents
    let documents = SCORE_DOCUMENTS.clone();
    let documents = documents.as_array().unwrap();
    let index0 = ms0.index("test");
    let index1 = ms1.index("test");
    let (task, _status_code) = index0.add_documents(json!(documents[0..2]), None).await;
    ms0.wait_task(task.uid()).await.succeeded();
    let (task, _status_code) = index1.add_documents(json!(documents[2..3]), None).await;
    ms1.wait_task(task.uid()).await.succeeded();

    // wrap servers
    let ms0 = Arc::new(ms0);
    let ms1 = Arc::new(ms1);

    let rms0 = LocalMeili::new(ms0.clone()).await;
    let rms1 = LocalMeili::with_params(
        ms1.clone(),
        LocalMeiliParams { fails: FailurePolicy::Always, ..Default::default() },
    )
    .await;

    // set network
    let network = json!({"remotes": {
        "ms0": {
            "url": rms0.url()
        },
        "ms1": {
            "url": rms1.url()
        },
    }});

    println!("{}", serde_json::to_string_pretty(&network).unwrap());

    let (_response, status_code) = ms0.set_network(network.clone()).await;
    snapshot!(status_code, @"200 OK");
    let (_response, status_code) = ms1.set_network(network.clone()).await;
    snapshot!(status_code, @"200 OK");

    // perform multi-search
    let query = "badman returns";
    let request = json!({
        "federation": {},
        "queries": [
            {
                "q": query,
                "indexUid": "test",
                "federationOptions": {
                    "remote": "ms0"
                }
            },
            {
                "q": query,
                "indexUid": "test",
                "federationOptions": {
                    "remote": "ms1"
                }
            }
        ]
    });

    let (response, _status_code) = ms0.multi_search(request.clone()).await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response, { ".processingTimeMs" => "[time]", ".requestUid" => "[uuid]" }), @r###"
    {
      "hits": [
        {
          "title": "Batman the dark knight returns: Part 1",
          "id": "A",
          "_federation": {
            "indexUid": "test",
            "queriesPosition": 0,
            "weightedRankingScore": 0.7028218694885362,
            "remote": "ms0"
          }
        },
        {
          "title": "Batman the dark knight returns: Part 2",
          "id": "B",
          "_federation": {
            "indexUid": "test",
            "queriesPosition": 0,
            "weightedRankingScore": 0.7028218694885362,
            "remote": "ms0"
          }
        }
      ],
      "processingTimeMs": "[time]",
      "limit": 20,
      "offset": 0,
      "estimatedTotalHits": 2,
      "requestUid": "[uuid]",
      "remoteErrors": {
        "ms1": {
          "message": "remote host responded with code 500:\n  - response from remote: {\"error\":\"provoked error\",\"code\":\"test_error\",\"link\":\"https://docs.meilisearch.com/errors#test_error\"}",
          "code": "remote_remote_error",
          "type": "system",
          "link": "https://docs.meilisearch.com/errors#remote_remote_error"
        }
      }
    }
    "###);
    let (response, _status_code) = ms1.multi_search(request.clone()).await;
    snapshot!(code, @"200 OK");
    // the response if full because we queried the instance that works
    snapshot!(json_string!(response, { ".processingTimeMs" => "[time]", ".requestUid" => "[uuid]" }), @r###"
    {
      "hits": [
        {
          "title": "Batman Returns",
          "id": "C",
          "_federation": {
            "indexUid": "test",
            "queriesPosition": 1,
            "weightedRankingScore": 0.8317901234567902,
            "remote": "ms1"
          }
        },
        {
          "title": "Batman the dark knight returns: Part 1",
          "id": "A",
          "_federation": {
            "indexUid": "test",
            "queriesPosition": 0,
            "weightedRankingScore": 0.7028218694885362,
            "remote": "ms0"
          }
        },
        {
          "title": "Batman the dark knight returns: Part 2",
          "id": "B",
          "_federation": {
            "indexUid": "test",
            "queriesPosition": 0,
            "weightedRankingScore": 0.7028218694885362,
            "remote": "ms0"
          }
        }
      ],
      "processingTimeMs": "[time]",
      "limit": 20,
      "offset": 0,
      "estimatedTotalHits": 3,
      "requestUid": "[uuid]",
      "remoteErrors": {}
    }
    "###);
}

#[actix_rt::test]
async fn error_remote_500_once() {
    let ms0 = Server::new().await;
    let ms1 = Server::new().await;

    // enable feature

    let (response, code) = ms0.set_features(json!({"network": true})).await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response["network"]), @"true");
    let (response, code) = ms1.set_features(json!({"network": true})).await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response["network"]), @"true");

    // set self

    let (response, code) = ms0.set_network(json!({"self": "ms0"})).await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response), @r###"
    {
      "self": "ms0",
      "remotes": {},
      "sharding": false
    }
    "###);
    let (response, code) = ms1.set_network(json!({"self": "ms1"})).await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response), @r###"
    {
      "self": "ms1",
      "remotes": {},
      "sharding": false
    }
    "###);

    // add documents
    let documents = SCORE_DOCUMENTS.clone();
    let documents = documents.as_array().unwrap();
    let index0 = ms0.index("test");
    let index1 = ms1.index("test");
    let (task, _status_code) = index0.add_documents(json!(documents[0..2]), None).await;
    ms0.wait_task(task.uid()).await.succeeded();
    let (task, _status_code) = index1.add_documents(json!(documents[2..3]), None).await;
    ms1.wait_task(task.uid()).await.succeeded();

    // wrap servers
    let ms0 = Arc::new(ms0);
    let ms1 = Arc::new(ms1);

    let rms0 = LocalMeili::new(ms0.clone()).await;
    let rms1 = LocalMeili::with_params(
        ms1.clone(),
        LocalMeiliParams { fails: FailurePolicy::Once, ..Default::default() },
    )
    .await;

    // set network
    let network = json!({"remotes": {
        "ms0": {
            "url": rms0.url()
        },
        "ms1": {
            "url": rms1.url()
        },
    }});

    println!("{}", serde_json::to_string_pretty(&network).unwrap());

    let (_response, status_code) = ms0.set_network(network.clone()).await;
    snapshot!(status_code, @"200 OK");
    let (_response, status_code) = ms1.set_network(network.clone()).await;
    snapshot!(status_code, @"200 OK");

    // perform multi-search
    let query = "badman returns";
    let request = json!({
        "federation": {},
        "queries": [
            {
                "q": query,
                "indexUid": "test",
                "federationOptions": {
                    "remote": "ms0"
                }
            },
            {
                "q": query,
                "indexUid": "test",
                "federationOptions": {
                    "remote": "ms1"
                }
            }
        ]
    });

    // Meilisearch is tolerant to a single failure
    let (response, _status_code) = ms0.multi_search(request.clone()).await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response, { ".processingTimeMs" => "[time]", ".requestUid" => "[uuid]" }), @r###"
    {
      "hits": [
        {
          "title": "Batman Returns",
          "id": "C",
          "_federation": {
            "indexUid": "test",
            "queriesPosition": 1,
            "weightedRankingScore": 0.8317901234567902,
            "remote": "ms1"
          }
        },
        {
          "title": "Batman the dark knight returns: Part 1",
          "id": "A",
          "_federation": {
            "indexUid": "test",
            "queriesPosition": 0,
            "weightedRankingScore": 0.7028218694885362,
            "remote": "ms0"
          }
        },
        {
          "title": "Batman the dark knight returns: Part 2",
          "id": "B",
          "_federation": {
            "indexUid": "test",
            "queriesPosition": 0,
            "weightedRankingScore": 0.7028218694885362,
            "remote": "ms0"
          }
        }
      ],
      "processingTimeMs": "[time]",
      "limit": 20,
      "offset": 0,
      "estimatedTotalHits": 3,
      "requestUid": "[uuid]",
      "remoteErrors": {}
    }
    "###);
    let (response, _status_code) = ms1.multi_search(request.clone()).await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response, { ".processingTimeMs" => "[time]", ".requestUid" => "[uuid]" }), @r###"
    {
      "hits": [
        {
          "title": "Batman Returns",
          "id": "C",
          "_federation": {
            "indexUid": "test",
            "queriesPosition": 1,
            "weightedRankingScore": 0.8317901234567902,
            "remote": "ms1"
          }
        },
        {
          "title": "Batman the dark knight returns: Part 1",
          "id": "A",
          "_federation": {
            "indexUid": "test",
            "queriesPosition": 0,
            "weightedRankingScore": 0.7028218694885362,
            "remote": "ms0"
          }
        },
        {
          "title": "Batman the dark knight returns: Part 2",
          "id": "B",
          "_federation": {
            "indexUid": "test",
            "queriesPosition": 0,
            "weightedRankingScore": 0.7028218694885362,
            "remote": "ms0"
          }
        }
      ],
      "processingTimeMs": "[time]",
      "limit": 20,
      "offset": 0,
      "estimatedTotalHits": 3,
      "requestUid": "[uuid]",
      "remoteErrors": {}
    }
    "###);
}

#[actix_rt::test]
#[ignore]
async fn error_remote_timeout() {
    let ms0 = Server::new().await;
    let ms1 = Server::new().await;

    // enable feature

    let (response, code) = ms0.set_features(json!({"network": true})).await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response["network"]), @"true");
    let (response, code) = ms1.set_features(json!({"network": true})).await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response["network"]), @"true");

    // set self

    let (response, code) = ms0.set_network(json!({"self": "ms0"})).await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response), @r###"
    {
      "self": "ms0",
      "remotes": {},
      "sharding": false
    }
    "###);
    let (response, code) = ms1.set_network(json!({"self": "ms1"})).await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response), @r###"
    {
      "self": "ms1",
      "remotes": {},
      "sharding": false
    }
    "###);

    // add documents
    let documents = SCORE_DOCUMENTS.clone();
    let documents = documents.as_array().unwrap();
    let index0 = ms0.index("test");
    let index1 = ms1.index("test");
    let (task, _status_code) = index0.add_documents(json!(documents[0..2]), None).await;
    ms0.wait_task(task.uid()).await.succeeded();
    let (task, _status_code) = index1.add_documents(json!(documents[2..3]), None).await;
    ms1.wait_task(task.uid()).await.succeeded();

    // wrap servers
    let ms0 = Arc::new(ms0);
    let ms1 = Arc::new(ms1);

    let rms0 = LocalMeili::new(ms0.clone()).await;
    let rms1 = LocalMeili::with_params(
        ms1.clone(),
        LocalMeiliParams { delay: Some(std::time::Duration::from_secs(31)), ..Default::default() },
    )
    .await;

    // set network
    let network = json!({"remotes": {
        "ms0": {
            "url": rms0.url()
        },
        "ms1": {
            "url": rms1.url()
        },
    }});

    println!("{}", serde_json::to_string_pretty(&network).unwrap());

    let (_response, status_code) = ms0.set_network(network.clone()).await;
    snapshot!(status_code, @"200 OK");
    let (_response, status_code) = ms1.set_network(network.clone()).await;
    snapshot!(status_code, @"200 OK");

    // perform multi-search
    let query = "badman returns";
    let request = json!({
        "federation": {},
        "queries": [
            {
                "q": query,
                "indexUid": "test",
                "federationOptions": {
                    "remote": "ms0"
                }
            },
            {
                "q": query,
                "indexUid": "test",
                "federationOptions": {
                    "remote": "ms1"
                }
            }
        ]
    });

    let (response, _status_code) = ms0.multi_search(request.clone()).await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response, { ".processingTimeMs" => "[time]" }), @r###"
    {
      "hits": [
        {
          "title": "Batman the dark knight returns: Part 1",
          "id": "A",
          "_federation": {
            "indexUid": "test",
            "queriesPosition": 0,
            "weightedRankingScore": 0.7028218694885362,
            "remote": "ms0"
          }
        },
        {
          "title": "Batman the dark knight returns: Part 2",
          "id": "B",
          "_federation": {
            "indexUid": "test",
            "queriesPosition": 0,
            "weightedRankingScore": 0.7028218694885362,
            "remote": "ms0"
          }
        }
      ],
      "processingTimeMs": "[time]",
      "limit": 20,
      "offset": 0,
      "estimatedTotalHits": 2,
      "remoteErrors": {
        "ms1": {
          "message": "remote host did not answer before the deadline",
          "code": "remote_timeout",
          "type": "system",
          "link": "https://docs.meilisearch.com/errors#remote_timeout"
        }
      }
    }
    "###);
    let (response, _status_code) = ms1.multi_search(request.clone()).await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response, { ".processingTimeMs" => "[time]" }), @r###"
    {
      "hits": [
        {
          "title": "Batman Returns",
          "id": "C",
          "_federation": {
            "indexUid": "test",
            "queriesPosition": 1,
            "weightedRankingScore": 0.8317901234567902,
            "remote": "ms1"
          }
        },
        {
          "title": "Batman the dark knight returns: Part 1",
          "id": "A",
          "_federation": {
            "indexUid": "test",
            "queriesPosition": 0,
            "weightedRankingScore": 0.7028218694885362,
            "remote": "ms0"
          }
        },
        {
          "title": "Batman the dark knight returns: Part 2",
          "id": "B",
          "_federation": {
            "indexUid": "test",
            "queriesPosition": 0,
            "weightedRankingScore": 0.7028218694885362,
            "remote": "ms0"
          }
        }
      ],
      "processingTimeMs": "[time]",
      "limit": 20,
      "offset": 0,
      "estimatedTotalHits": 3,
      "remoteErrors": {}
    }
    "###);
}

// test: try all the flattened structs in queries

// working facet tests with and without merge

#[derive(Default)]
pub enum FailurePolicy {
    #[default]
    Never,
    Once,
    Always,
}

/// Parameters to change the behavior of the [`LocalMeili`] server.
#[derive(Default)]
pub struct LocalMeiliParams {
    /// delay the response by the specified duration
    pub delay: Option<std::time::Duration>,
    pub fails: FailurePolicy,
    /// replace the reponse body with the provided String
    pub override_response_body: Option<String>,
    pub gobble_headers: bool,
}

/// A server that exploits [`MockServer`] to provide an URL for testing network and the network.
pub struct LocalMeili {
    mock_server: &'static MockServer,
}

impl LocalMeili {
    pub async fn new(server: Arc<Server>) -> Self {
        Self::with_params(server, Default::default()).await
    }

    pub async fn with_params(server: Arc<Server>, params: LocalMeiliParams) -> Self {
        let mock_server = Box::leak(Box::new(MockServer::start().await));

        // tokio won't let us execute asynchronous code from a sync function inside of an async test,
        // so instead we spawn another thread that will call the service on a brand new tokio runtime
        // and communicate via channels...
        let (request_sender, request_receiver) = crossbeam_channel::bounded::<wiremock::Request>(0);
        let (response_sender, response_receiver) =
            crossbeam_channel::bounded::<(Value, StatusCode)>(0);
        std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread().build().unwrap();
            while let Ok(req) = request_receiver.recv() {
                let body = std::str::from_utf8(&req.body).unwrap();
                let headers: Vec<(&str, &str)> = if params.gobble_headers {
                    vec![("Content-Type", "application/json")]
                } else {
                    req.headers
                        .iter()
                        .map(|(name, value)| (name.as_str(), value.to_str().unwrap()))
                        .collect()
                };
                let (value, code) = rt.block_on(async {
                    match req.method.as_str() {
                        "POST" => server.service.post_str(&req.url, body, headers.clone()).await,
                        "PUT" => server.service.put_str(&req.url, body, headers).await,
                        "PATCH" => server.service.patch(&req.url, req.body_json().unwrap()).await,
                        "GET" => server.service.get(&req.url).await,
                        "DELETE" => server.service.delete(&req.url).await,
                        _ => unimplemented!(),
                    }
                });
                if response_sender.send((value, code)).is_err() {
                    break;
                }
            }
            println!("exiting mock thread")
        });

        let failed_already = std::sync::atomic::AtomicBool::new(false);

        Mock::given(AnyMatcher)
            .respond_with(move |req: &wiremock::Request| {
                if let Some(delay) = params.delay {
                    std::thread::sleep(delay);
                }
                match params.fails {
                    FailurePolicy::Never => {}
                    FailurePolicy::Once => {
                        let failed_already =
                            failed_already.fetch_or(true, std::sync::atomic::Ordering::AcqRel);
                        if !failed_already {
                            return fail(params.override_response_body.as_deref());
                        }
                    }
                    FailurePolicy::Always => return fail(params.override_response_body.as_deref()),
                }
                request_sender.send(req.clone()).unwrap();
                let (value, code) = response_receiver.recv().unwrap();
                let response = ResponseTemplate::new(code.as_u16());
                if let Some(override_response_body) = params.override_response_body.as_deref() {
                    response.set_body_string(override_response_body)
                } else {
                    response.set_body_json(value)
                }
            })
            .mount(mock_server)
            .await;
        Self { mock_server }
    }

    pub fn url(&self) -> String {
        self.mock_server.uri()
    }
}

fn fail(override_response_body: Option<&str>) -> ResponseTemplate {
    let response = ResponseTemplate::new(500);
    if let Some(override_response_body) = override_response_body {
        response.set_body_string(override_response_body)
    } else {
        response.set_body_json(json!({"error": "provoked error", "code": "test_error", "link": "https://docs.meilisearch.com/errors#test_error"}))
    }
}

#[actix_rt::test]
async fn remote_auto_sharding() {
    let ms0 = Server::new().await;
    let ms1 = Server::new().await;
    let ms2 = Server::new().await;

    // enable feature

    let (response, code) = ms0.set_features(json!({"network": true})).await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response["network"]), @"true");
    let (response, code) = ms1.set_features(json!({"network": true})).await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response["network"]), @"true");
    let (response, code) = ms2.set_features(json!({"network": true})).await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response["network"]), @"true");

    // set self & sharding

    let (response, code) = ms0.set_network(json!({"self": "ms0", "sharding": true})).await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response), @r###"
    {
      "self": "ms0",
      "remotes": {},
      "sharding": true
    }
    "###);
    let (response, code) = ms1.set_network(json!({"self": "ms1", "sharding": true})).await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response), @r###"
    {
      "self": "ms1",
      "remotes": {},
      "sharding": true
    }
    "###);
    let (response, code) = ms2.set_network(json!({"self": "ms2", "sharding": true})).await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response), @r###"
    {
      "self": "ms2",
      "remotes": {},
      "sharding": true
    }
    "###);

    // wrap servers
    let ms0 = Arc::new(ms0);
    let ms1 = Arc::new(ms1);
    let ms2 = Arc::new(ms2);

    let rms0 = LocalMeili::new(ms0.clone()).await;
    let rms1 = LocalMeili::new(ms1.clone()).await;
    let rms2 = LocalMeili::new(ms2.clone()).await;

    // set network
    let network = json!({"remotes": {
        "ms0": {
            "url": rms0.url()
        },
        "ms1": {
            "url": rms1.url()
        },
        "ms2": {
            "url": rms2.url()
        }
    }});

    println!("{}", serde_json::to_string_pretty(&network).unwrap());

    let (_response, status_code) = ms0.set_network(network.clone()).await;
    snapshot!(status_code, @"200 OK");
    let (_response, status_code) = ms1.set_network(network.clone()).await;
    snapshot!(status_code, @"200 OK");
    let (_response, status_code) = ms2.set_network(network.clone()).await;
    snapshot!(status_code, @"200 OK");

    // add documents
    let documents = SCORE_DOCUMENTS.clone();
    let documents = documents.as_array().unwrap();
    let index0 = ms0.index("test");
    let _index1 = ms1.index("test");
    let _index2 = ms2.index("test");

    let (task, _status_code) = index0.add_documents(json!(documents), None).await;

    let t0 = task.uid();
    let (t, _) = ms0.get_task(task.uid()).await;
    let t1 = t["network"]["remote_tasks"]["ms1"]["taskUid"].as_u64().unwrap();
    let t2 = t["network"]["remote_tasks"]["ms2"]["taskUid"].as_u64().unwrap();

    ms0.wait_task(t0).await.succeeded();
    ms1.wait_task(t1).await.succeeded();
    ms2.wait_task(t2).await.succeeded();

    // perform multi-search
    let query = "badman returns";
    let request = json!({
        "federation": {},
        "queries": [
            {
                "q": query,
                "indexUid": "test",
                "federationOptions": {
                    "remote": "ms0"
                }
            },
            {
                "q": query,
                "indexUid": "test",
                "federationOptions": {
                    "remote": "ms1"
                }
            },
            {
                "q": query,
                "indexUid": "test",
                "federationOptions": {
                    "remote": "ms2"
                }
            },
        ]
    });

    let (response, _status_code) = ms0.multi_search(request.clone()).await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response, { ".processingTimeMs" => "[time]", ".requestUid" => "[uuid]" }), @r###"
    {
      "hits": [
        {
          "title": "Batman Returns",
          "id": "C",
          "_federation": {
            "indexUid": "test",
            "queriesPosition": 2,
            "weightedRankingScore": 0.8317901234567902,
            "remote": "ms2"
          }
        },
        {
          "title": "Batman the dark knight returns: Part 1",
          "id": "A",
          "_federation": {
            "indexUid": "test",
            "queriesPosition": 1,
            "weightedRankingScore": 0.7028218694885362,
            "remote": "ms1"
          }
        },
        {
          "title": "Batman the dark knight returns: Part 2",
          "id": "B",
          "_federation": {
            "indexUid": "test",
            "queriesPosition": 1,
            "weightedRankingScore": 0.7028218694885362,
            "remote": "ms1"
          }
        },
        {
          "title": "Badman",
          "id": "E",
          "_federation": {
            "indexUid": "test",
            "queriesPosition": 2,
            "weightedRankingScore": 0.5,
            "remote": "ms2"
          }
        },
        {
          "title": "Batman",
          "id": "D",
          "_federation": {
            "indexUid": "test",
            "queriesPosition": 0,
            "weightedRankingScore": 0.23106060606060605,
            "remote": "ms0"
          }
        }
      ],
      "processingTimeMs": "[time]",
      "limit": 20,
      "offset": 0,
      "estimatedTotalHits": 5,
      "requestUid": "[uuid]",
      "remoteErrors": {}
    }
    "###);
    let (response, _status_code) = ms1.multi_search(request.clone()).await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response, { ".processingTimeMs" => "[time]", ".requestUid" => "[uuid]" }), @r###"
    {
      "hits": [
        {
          "title": "Batman Returns",
          "id": "C",
          "_federation": {
            "indexUid": "test",
            "queriesPosition": 2,
            "weightedRankingScore": 0.8317901234567902,
            "remote": "ms2"
          }
        },
        {
          "title": "Batman the dark knight returns: Part 1",
          "id": "A",
          "_federation": {
            "indexUid": "test",
            "queriesPosition": 1,
            "weightedRankingScore": 0.7028218694885362,
            "remote": "ms1"
          }
        },
        {
          "title": "Batman the dark knight returns: Part 2",
          "id": "B",
          "_federation": {
            "indexUid": "test",
            "queriesPosition": 1,
            "weightedRankingScore": 0.7028218694885362,
            "remote": "ms1"
          }
        },
        {
          "title": "Badman",
          "id": "E",
          "_federation": {
            "indexUid": "test",
            "queriesPosition": 2,
            "weightedRankingScore": 0.5,
            "remote": "ms2"
          }
        },
        {
          "title": "Batman",
          "id": "D",
          "_federation": {
            "indexUid": "test",
            "queriesPosition": 0,
            "weightedRankingScore": 0.23106060606060605,
            "remote": "ms0"
          }
        }
      ],
      "processingTimeMs": "[time]",
      "limit": 20,
      "offset": 0,
      "estimatedTotalHits": 5,
      "requestUid": "[uuid]",
      "remoteErrors": {}
    }
    "###);
    let (response, _status_code) = ms2.multi_search(request.clone()).await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response, { ".processingTimeMs" => "[time]", ".requestUid" => "[uuid]" }), @r###"
    {
      "hits": [
        {
          "title": "Batman Returns",
          "id": "C",
          "_federation": {
            "indexUid": "test",
            "queriesPosition": 2,
            "weightedRankingScore": 0.8317901234567902,
            "remote": "ms2"
          }
        },
        {
          "title": "Batman the dark knight returns: Part 1",
          "id": "A",
          "_federation": {
            "indexUid": "test",
            "queriesPosition": 1,
            "weightedRankingScore": 0.7028218694885362,
            "remote": "ms1"
          }
        },
        {
          "title": "Batman the dark knight returns: Part 2",
          "id": "B",
          "_federation": {
            "indexUid": "test",
            "queriesPosition": 1,
            "weightedRankingScore": 0.7028218694885362,
            "remote": "ms1"
          }
        },
        {
          "title": "Badman",
          "id": "E",
          "_federation": {
            "indexUid": "test",
            "queriesPosition": 2,
            "weightedRankingScore": 0.5,
            "remote": "ms2"
          }
        },
        {
          "title": "Batman",
          "id": "D",
          "_federation": {
            "indexUid": "test",
            "queriesPosition": 0,
            "weightedRankingScore": 0.23106060606060605,
            "remote": "ms0"
          }
        }
      ],
      "processingTimeMs": "[time]",
      "limit": 20,
      "offset": 0,
      "estimatedTotalHits": 5,
      "requestUid": "[uuid]",
      "remoteErrors": {}
    }
    "###);
}

#[actix_rt::test]
async fn remote_auto_sharding_with_custom_metadata() {
    let ms0 = Server::new().await;
    let ms1 = Server::new().await;
    let ms2 = Server::new().await;

    // enable feature

    let (response, code) = ms0.set_features(json!({"network": true})).await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response["network"]), @"true");
    let (response, code) = ms1.set_features(json!({"network": true})).await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response["network"]), @"true");
    let (response, code) = ms2.set_features(json!({"network": true})).await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response["network"]), @"true");

    // set self & sharding

    let (response, code) = ms0.set_network(json!({"self": "ms0", "sharding": true})).await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response), @r###"
    {
      "self": "ms0",
      "remotes": {},
      "sharding": true
    }
    "###);
    let (response, code) = ms1.set_network(json!({"self": "ms1", "sharding": true})).await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response), @r###"
    {
      "self": "ms1",
      "remotes": {},
      "sharding": true
    }
    "###);
    let (response, code) = ms2.set_network(json!({"self": "ms2", "sharding": true})).await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response), @r###"
    {
      "self": "ms2",
      "remotes": {},
      "sharding": true
    }
    "###);

    // wrap servers
    let ms0 = Arc::new(ms0);
    let ms1 = Arc::new(ms1);
    let ms2 = Arc::new(ms2);

    let rms0 = LocalMeili::new(ms0.clone()).await;
    let rms1 = LocalMeili::new(ms1.clone()).await;
    let rms2 = LocalMeili::new(ms2.clone()).await;

    // set network
    let network = json!({"remotes": {
        "ms0": {
            "url": rms0.url()
        },
        "ms1": {
            "url": rms1.url()
        },
        "ms2": {
            "url": rms2.url()
        }
    }});

    println!("{}", serde_json::to_string_pretty(&network).unwrap());

    let (_response, status_code) = ms0.set_network(network.clone()).await;
    snapshot!(status_code, @"200 OK");
    let (_response, status_code) = ms1.set_network(network.clone()).await;
    snapshot!(status_code, @"200 OK");
    let (_response, status_code) = ms2.set_network(network.clone()).await;
    snapshot!(status_code, @"200 OK");

    // add documents
    let documents = SCORE_DOCUMENTS.clone();
    let documents = documents.as_array().unwrap();
    let index0 = ms0.index("test");
    let _index1 = ms1.index("test");
    let _index2 = ms2.index("test");

    let (task, _status_code) = index0
        .add_documents_with_custom_metadata(
            json!(documents),
            None,
            Some("remote_auto_sharding_with_custom_metadata"),
        )
        .await;

    let t0 = task.uid();
    let (t, _) = ms0.get_task(task.uid()).await;
    let t1 = t["network"]["remote_tasks"]["ms1"]["taskUid"].as_u64().unwrap();
    let t2 = t["network"]["remote_tasks"]["ms2"]["taskUid"].as_u64().unwrap();

    let t = ms0.wait_task(t0).await.succeeded();
    snapshot!(t, @r###"
    {
      "uid": "[uid]",
      "batchUid": "[batch_uid]",
      "indexUid": "test",
      "status": "succeeded",
      "type": "documentAdditionOrUpdate",
      "canceledBy": null,
      "details": {
        "receivedDocuments": 5,
        "indexedDocuments": 1
      },
      "error": null,
      "duration": "[duration]",
      "enqueuedAt": "[date]",
      "startedAt": "[date]",
      "finishedAt": "[date]",
      "network": {
        "remote_tasks": {
          "ms1": {
            "taskUid": 0,
            "error": null
          },
          "ms2": {
            "taskUid": 0,
            "error": null
          }
        }
      },
      "customMetadata": "remote_auto_sharding_with_custom_metadata"
    }
    "###);

    let t = ms1.wait_task(t1).await.succeeded();
    snapshot!(t, @r###"
    {
      "uid": "[uid]",
      "batchUid": "[batch_uid]",
      "indexUid": "test",
      "status": "succeeded",
      "type": "documentAdditionOrUpdate",
      "canceledBy": null,
      "details": {
        "receivedDocuments": 5,
        "indexedDocuments": 2
      },
      "error": null,
      "duration": "[duration]",
      "enqueuedAt": "[date]",
      "startedAt": "[date]",
      "finishedAt": "[date]",
      "network": {
        "origin": {
          "remoteName": "ms0",
          "taskUid": 0
        }
      },
      "customMetadata": "remote_auto_sharding_with_custom_metadata"
    }
    "###);

    let t = ms2.wait_task(t2).await.succeeded();
    snapshot!(t, @r###"
    {
      "uid": "[uid]",
      "batchUid": "[batch_uid]",
      "indexUid": "test",
      "status": "succeeded",
      "type": "documentAdditionOrUpdate",
      "canceledBy": null,
      "details": {
        "receivedDocuments": 5,
        "indexedDocuments": 2
      },
      "error": null,
      "duration": "[duration]",
      "enqueuedAt": "[date]",
      "startedAt": "[date]",
      "finishedAt": "[date]",
      "network": {
        "origin": {
          "remoteName": "ms0",
          "taskUid": 0
        }
      },
      "customMetadata": "remote_auto_sharding_with_custom_metadata"
    }
    "###);
}
