use serde_json::Value::Null;

use crate::common::Server;
use crate::json;

#[actix_rt::test]
async fn error_network_not_enabled() {
    let server = Server::new().await;

    let (response, code) = server.get_network().await;

    meili_snap::snapshot!(code, @"400 Bad Request");
    meili_snap::snapshot!(meili_snap::json_string!(response), @r###"
    {
      "message": "Using the /network route requires enabling the `network` experimental feature. See https://github.com/orgs/meilisearch/discussions/805",
      "code": "feature_not_enabled",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#feature_not_enabled"
    }
    "###);

    let (response, code) = server.set_network(json!({"self": "myself"})).await;

    meili_snap::snapshot!(code, @"400 Bad Request");
    meili_snap::snapshot!(meili_snap::json_string!(response), @r###"
    {
      "message": "Using the /network route requires enabling the `network` experimental feature. See https://github.com/orgs/meilisearch/discussions/805",
      "code": "feature_not_enabled",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#feature_not_enabled"
    }
    "###);
}

#[actix_rt::test]
async fn errors_on_param() {
    let server = Server::new().await;

    let (response, code) = server.set_features(json!({"network": true})).await;
    meili_snap::snapshot!(code, @"200 OK");
    meili_snap::snapshot!(meili_snap::json_string!(response["network"]), @r#"true"#);

    // non-existing param
    let (response, code) = server.set_network(json!({"selfie": "myself"})).await;

    meili_snap::snapshot!(code, @"400 Bad Request");
    meili_snap::snapshot!(meili_snap::json_string!(response), @r###"
    {
      "message": "Unknown field `selfie`: expected one of `remotes`, `self`",
      "code": "bad_request",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#bad_request"
    }
    "###);

    // self not a string
    let (response, code) = server.set_network(json!({"self": 42})).await;

    meili_snap::snapshot!(code, @"400 Bad Request");
    meili_snap::snapshot!(meili_snap::json_string!(response), @r###"
    {
      "message": "Invalid value type at `.self`: expected a string, but found a positive integer: `42`",
      "code": "invalid_network_self",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_network_self"
    }
    "###);

    // remotes not an object
    let (response, code) = server.set_network(json!({"remotes": 42})).await;

    meili_snap::snapshot!(code, @"400 Bad Request");
    meili_snap::snapshot!(meili_snap::json_string!(response), @r###"
    {
      "message": "Invalid value type at `.remotes`: expected an object, but found a positive integer: `42`",
      "code": "invalid_network_remotes",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_network_remotes"
    }
    "###);

    // new remote without url
    let (response, code) = server
        .set_network(json!({"remotes": {
            "new": {
                "searchApiKey": "http://localhost:7700"
            }
        }}))
        .await;

    meili_snap::snapshot!(code, @"400 Bad Request");
    meili_snap::snapshot!(meili_snap::json_string!(response), @r###"
    {
      "message": "Missing field `.remotes.new.url`",
      "code": "missing_network_url",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#missing_network_url"
    }
    "###);

    // remote with url not a string
    let (response, code) = server
        .set_network(json!({"remotes": {
            "new": {
                "url": 7700
            }
        }}))
        .await;

    meili_snap::snapshot!(code, @"400 Bad Request");
    meili_snap::snapshot!(meili_snap::json_string!(response), @r###"
    {
      "message": "Invalid value type at `.remotes.new.url`: expected a string, but found a positive integer: `7700`",
      "code": "invalid_network_url",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_network_url"
    }
    "###);

    // remote with non-existing param
    let (response, code) = server
        .set_network(json!({"remotes": {
            "new": {
                "url": "http://localhost:7700",
                "doggo": "Intel the Beagle"
            }
        }}))
        .await;

    meili_snap::snapshot!(code, @"400 Bad Request");
    meili_snap::snapshot!(meili_snap::json_string!(response), @r###"
    {
      "message": "Unknown field `doggo` inside `.remotes.new`: expected one of `url`, `searchApiKey`",
      "code": "invalid_network_remotes",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_network_remotes"
    }
    "###);

    // remote with non-string searchApiKey
    let (response, code) = server
        .set_network(json!({"remotes": {
            "new": {
                "url": "http://localhost:7700",
                "searchApiKey": 1204664602099962445u64,
            }
        }}))
        .await;

    meili_snap::snapshot!(code, @"400 Bad Request");
    meili_snap::snapshot!(meili_snap::json_string!(response), @r###"
    {
      "message": "Invalid value type at `.remotes.new.searchApiKey`: expected a string, but found a positive integer: `1204664602099962445`",
      "code": "invalid_network_search_api_key",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_network_search_api_key"
    }
    "###);

    // setting `null` on URL a posteriori
    let (response, code) = server
        .set_network(json!({"remotes": {
            "kefir": {
                "url": "http://localhost:7700",
            }
        }}))
        .await;

    meili_snap::snapshot!(code, @"200 OK");
    meili_snap::snapshot!(meili_snap::json_string!(response), @r###"
    {
      "self": null,
      "remotes": {
        "kefir": {
          "url": "http://localhost:7700",
          "searchApiKey": null
        }
      }
    }
    "###);
    let (response, code) = server
        .set_network(json!({"remotes": {
            "kefir": {
                "url": Null,
            }
        }}))
        .await;

    meili_snap::snapshot!(code, @"400 Bad Request");
    meili_snap::snapshot!(meili_snap::json_string!(response), @r###"
    {
      "message": "Field `.remotes.kefir.url` cannot be set to `null`",
      "code": "invalid_network_url",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_network_url"
    }
    "###);
}

#[actix_rt::test]
async fn auth() {
    let mut server = Server::new_auth().await;
    server.use_api_key("MASTER_KEY");

    let (response, code) = server.set_features(json!({"network": true})).await;
    meili_snap::snapshot!(code, @"200 OK");
    meili_snap::snapshot!(meili_snap::json_string!(response["network"]), @r#"true"#);

    let (get_network_key, code) = server
        .add_api_key(json!({
          "actions": ["network.get"],
          "indexes": ["*"],
          "expiresAt": serde_json::Value::Null
        }))
        .await;
    meili_snap::snapshot!(code, @"201 Created");
    let get_network_key = get_network_key["key"].clone();

    let (update_network_key, code) = server
        .add_api_key(json!({
          "actions": ["network.update"],
          "indexes": ["*"],
          "expiresAt": serde_json::Value::Null
        }))
        .await;
    meili_snap::snapshot!(code, @"201 Created");
    let update_network_key = update_network_key["key"].clone();

    let (search_api_key, code) = server
        .add_api_key(json!({
          "actions": ["search"],
          "indexes": ["*"],
          "expiresAt": serde_json::Value::Null
        }))
        .await;
    meili_snap::snapshot!(code, @"201 Created");
    let search_api_key = search_api_key["key"].clone();

    // try with master key
    let (response, code) = server
        .set_network(json!({
          "self": "master"
        }))
        .await;

    meili_snap::snapshot!(code, @"200 OK");
    meili_snap::snapshot!(meili_snap::json_string!(response), @r###"
    {
      "self": "master",
      "remotes": {}
    }
    "###);

    let (response, code) = server.get_network().await;

    meili_snap::snapshot!(code, @"200 OK");
    meili_snap::snapshot!(meili_snap::json_string!(response), @r###"
{
  "self": "master",
  "remotes": {}
}
"###);

    // try get with get permission
    server.use_api_key(get_network_key.as_str().unwrap());
    let (response, code) = server.get_network().await;

    meili_snap::snapshot!(code, @"200 OK");
    meili_snap::snapshot!(meili_snap::json_string!(response), @r###"
{
  "self": "master",
  "remotes": {}
}
"###);

    // try update with update permission
    server.use_api_key(update_network_key.as_str().unwrap());

    let (response, code) = server
        .set_network(json!({
          "self": "api_key"
        }))
        .await;

    meili_snap::snapshot!(code, @"200 OK");
    meili_snap::snapshot!(meili_snap::json_string!(response), @r###"
{
  "self": "api_key",
  "remotes": {}
}
"###);

    // try with the other's permission
    let (response, code) = server.get_network().await;

    meili_snap::snapshot!(code, @"403 Forbidden");
    meili_snap::snapshot!(meili_snap::json_string!(response), @r###"
    {
      "message": "The provided API key is invalid.",
      "code": "invalid_api_key",
      "type": "auth",
      "link": "https://docs.meilisearch.com/errors#invalid_api_key"
    }
    "###);

    server.use_api_key(get_network_key.as_str().unwrap());
    let (response, code) = server
        .set_network(json!({
          "self": "get_api_key"
        }))
        .await;

    meili_snap::snapshot!(code, @"403 Forbidden");
    meili_snap::snapshot!(meili_snap::json_string!(response), @r###"
    {
      "message": "The provided API key is invalid.",
      "code": "invalid_api_key",
      "type": "auth",
      "link": "https://docs.meilisearch.com/errors#invalid_api_key"
    }
    "###);
    // try either with bad permission
    server.use_api_key(search_api_key.as_str().unwrap());
    let (response, code) = server.get_network().await;

    meili_snap::snapshot!(code, @"403 Forbidden");
    meili_snap::snapshot!(meili_snap::json_string!(response), @r###"
    {
      "message": "The provided API key is invalid.",
      "code": "invalid_api_key",
      "type": "auth",
      "link": "https://docs.meilisearch.com/errors#invalid_api_key"
    }
    "###);

    let (response, code) = server
        .set_network(json!({
          "self": "get_api_key"
        }))
        .await;

    meili_snap::snapshot!(code, @"403 Forbidden");
    meili_snap::snapshot!(meili_snap::json_string!(response), @r###"
    {
      "message": "The provided API key is invalid.",
      "code": "invalid_api_key",
      "type": "auth",
      "link": "https://docs.meilisearch.com/errors#invalid_api_key"
    }
    "###);
}

#[actix_rt::test]
async fn get_and_set_network() {
    let server = Server::new().await;

    let (response, code) = server.set_features(json!({"network": true})).await;
    meili_snap::snapshot!(code, @"200 OK");
    meili_snap::snapshot!(meili_snap::json_string!(response["network"]), @r#"true"#);

    let (response, code) = server.get_network().await;

    meili_snap::snapshot!(code, @"200 OK");
    meili_snap::snapshot!(meili_snap::json_string!(response), @r###"
    {
      "self": null,
      "remotes": {}
    }
    "###);

    // adding self
    let (response, code) = server.set_network(json!({"self": "myself"})).await;
    meili_snap::snapshot!(code, @"200 OK");
    meili_snap::snapshot!(meili_snap::json_string!(response), @r###"
    {
      "self": "myself",
      "remotes": {}
    }
    "###);

    // adding remotes
    let (response, code) = server
        .set_network(json!({"remotes": {
            "myself": {
                "url": "http://localhost:7700"
            },
            "thy": {
                "url": "http://localhost:7701",
                "searchApiKey": "foo"
            }
        }}))
        .await;

    meili_snap::snapshot!(code, @"200 OK");
    meili_snap::snapshot!(meili_snap::json_string!(response), @r###"
    {
      "self": "myself",
      "remotes": {
        "myself": {
          "url": "http://localhost:7700",
          "searchApiKey": null
        },
        "thy": {
          "url": "http://localhost:7701",
          "searchApiKey": "foo"
        }
      }
    }
    "###);

    // partially updating one remote
    let (response, code) = server
        .set_network(json!({"remotes": {
            "thy": {
                "searchApiKey": "bar"
            }
        }}))
        .await;

    meili_snap::snapshot!(code, @"200 OK");
    meili_snap::snapshot!(meili_snap::json_string!(response), @r###"
    {
      "self": "myself",
      "remotes": {
        "myself": {
          "url": "http://localhost:7700",
          "searchApiKey": null
        },
        "thy": {
          "url": "http://localhost:7701",
          "searchApiKey": "bar"
        }
      }
    }
    "###);

    // adding one remote
    let (response, code) = server
        .set_network(json!({"remotes": {
            "them": {
                "url": "http://localhost:7702",
                "searchApiKey": "baz"
            }
        }}))
        .await;

    meili_snap::snapshot!(code, @"200 OK");
    meili_snap::snapshot!(meili_snap::json_string!(response), @r###"
    {
      "self": "myself",
      "remotes": {
        "myself": {
          "url": "http://localhost:7700",
          "searchApiKey": null
        },
        "them": {
          "url": "http://localhost:7702",
          "searchApiKey": "baz"
        },
        "thy": {
          "url": "http://localhost:7701",
          "searchApiKey": "bar"
        }
      }
    }
    "###);

    // deleting one remote
    let (response, code) = server
        .set_network(json!({"remotes": {
            "myself": Null,
        }}))
        .await;

    meili_snap::snapshot!(code, @"200 OK");
    meili_snap::snapshot!(meili_snap::json_string!(response), @r###"
    {
      "self": "myself",
      "remotes": {
        "them": {
          "url": "http://localhost:7702",
          "searchApiKey": "baz"
        },
        "thy": {
          "url": "http://localhost:7701",
          "searchApiKey": "bar"
        }
      }
    }
    "###);

    // removing self
    let (response, code) = server.set_network(json!({"self": Null})).await;

    meili_snap::snapshot!(code, @"200 OK");
    meili_snap::snapshot!(meili_snap::json_string!(response), @r###"
    {
      "self": null,
      "remotes": {
        "them": {
          "url": "http://localhost:7702",
          "searchApiKey": "baz"
        },
        "thy": {
          "url": "http://localhost:7701",
          "searchApiKey": "bar"
        }
      }
    }
    "###);

    // setting self again
    let (response, code) = server.set_network(json!({"self": "thy"})).await;

    meili_snap::snapshot!(code, @"200 OK");
    meili_snap::snapshot!(meili_snap::json_string!(response), @r###"
    {
      "self": "thy",
      "remotes": {
        "them": {
          "url": "http://localhost:7702",
          "searchApiKey": "baz"
        },
        "thy": {
          "url": "http://localhost:7701",
          "searchApiKey": "bar"
        }
      }
    }
    "###);

    // doing nothing
    let (response, code) = server.set_network(json!({})).await;

    meili_snap::snapshot!(code, @"200 OK");
    meili_snap::snapshot!(meili_snap::json_string!(response), @r###"
        {
          "self": "thy",
          "remotes": {
            "them": {
              "url": "http://localhost:7702",
              "searchApiKey": "baz"
            },
            "thy": {
              "url": "http://localhost:7701",
              "searchApiKey": "bar"
            }
          }
        }
        "###);

    // still doing nothing
    let (response, code) = server.set_network(json!({"remotes": {}})).await;

    meili_snap::snapshot!(code, @"200 OK");
    meili_snap::snapshot!(meili_snap::json_string!(response), @r###"
        {
          "self": "thy",
          "remotes": {
            "them": {
              "url": "http://localhost:7702",
              "searchApiKey": "baz"
            },
            "thy": {
              "url": "http://localhost:7701",
              "searchApiKey": "bar"
            }
          }
        }
        "###);

    // good time to check GET
    let (response, code) = server.get_network().await;

    meili_snap::snapshot!(code, @"200 OK");
    meili_snap::snapshot!(meili_snap::json_string!(response), @r###"
        {
          "self": "thy",
          "remotes": {
            "them": {
              "url": "http://localhost:7702",
              "searchApiKey": "baz"
            },
            "thy": {
              "url": "http://localhost:7701",
              "searchApiKey": "bar"
            }
          }
        }
        "###);

    // deleting everything
    let (response, code) = server
        .set_network(json!({
            "remotes": Null,
        }))
        .await;

    meili_snap::snapshot!(code, @"200 OK");
    meili_snap::snapshot!(meili_snap::json_string!(response), @r###"
    {
      "self": "thy",
      "remotes": {}
    }
    "###);
}
