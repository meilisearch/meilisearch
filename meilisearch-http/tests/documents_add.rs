use serde_json::json;

mod common;

// Test issue https://github.com/meilisearch/MeiliSearch/issues/519
#[actix_rt::test]
async fn check_add_documents_with_primary_key_param() {
    let mut server = common::Server::with_uid("movies");

    // 1 - Create the index with no primary_key

    let body = json!({
        "uid": "movies",
    });
    let (response, status_code) = server.create_index(body).await;
    assert_eq!(status_code, 201);
    assert_eq!(response["primaryKey"], json!(null));

    // 2 - Add documents

    let body = json!([{
      "title": "Test",
      "comment": "comment test"
    }]);

    let url = "/indexes/movies/documents?primaryKey=title";
    let (response, status_code) = server.post_request(&url, body).await;
    eprintln!("{:#?}", response);
    assert_eq!(status_code, 202);
    let update_id = response["updateId"].as_u64().unwrap();
    server.wait_update_id(update_id).await;

    // 3 - Check update success

    let (response, status_code) = server.get_update_status(update_id).await;
    assert_eq!(status_code, 200);
    assert_eq!(response["status"], "processed");
}

// Test issue https://github.com/meilisearch/MeiliSearch/issues/568
#[actix_rt::test]
async fn check_add_documents_with_nested_boolean() {
    let mut server = common::Server::with_uid("tasks");

    // 1 - Create the index with no primary_key

    let body = json!({ "uid": "tasks" });
    let (response, status_code) = server.create_index(body).await;
    assert_eq!(status_code, 201);
    assert_eq!(response["primaryKey"], json!(null));

    // 2 - Add a document that contains a boolean in a nested object

    let body = json!([{
        "id": 12161,
        "created_at": "2019-04-10T14:57:57.522Z",
        "foo": {
            "bar": {
                "id": 121,
                "crash": false
            },
            "id": 45912
        }
    }]);

    let url = "/indexes/tasks/documents";
    let (response, status_code) = server.post_request(&url, body).await;
    eprintln!("{:#?}", response);
    assert_eq!(status_code, 202);
    let update_id = response["updateId"].as_u64().unwrap();
    server.wait_update_id(update_id).await;

    // 3 - Check update success

    let (response, status_code) = server.get_update_status(update_id).await;
    assert_eq!(status_code, 200);
    assert_eq!(response["status"], "processed");
}

// Test issue https://github.com/meilisearch/MeiliSearch/issues/571
#[actix_rt::test]
async fn check_add_documents_with_nested_null() {
    let mut server = common::Server::with_uid("tasks");

    // 1 - Create the index with no primary_key

    let body = json!({ "uid": "tasks" });
    let (response, status_code) = server.create_index(body).await;
    assert_eq!(status_code, 201);
    assert_eq!(response["primaryKey"], json!(null));

    // 2 - Add a document that contains a null in a nested object

    let body = json!([{
        "id": 0,
        "foo": {
            "bar": null
        }
    }]);

    let url = "/indexes/tasks/documents";
    let (response, status_code) = server.post_request(&url, body).await;
    eprintln!("{:#?}", response);
    assert_eq!(status_code, 202);
    let update_id = response["updateId"].as_u64().unwrap();
    server.wait_update_id(update_id).await;

    // 3 - Check update success

    let (response, status_code) = server.get_update_status(update_id).await;
    assert_eq!(status_code, 200);
    assert_eq!(response["status"], "processed");
}

// Test issue https://github.com/meilisearch/MeiliSearch/issues/574
#[actix_rt::test]
async fn check_add_documents_with_nested_sequence() {
    let mut server = common::Server::with_uid("tasks");

    // 1 - Create the index with no primary_key

    let body = json!({ "uid": "tasks" });
    let (response, status_code) = server.create_index(body).await;
    assert_eq!(status_code, 201);
    assert_eq!(response["primaryKey"], json!(null));

    // 2 - Add a document that contains a seq in a nested object

    let body = json!([{
        "id": 0,
        "foo": {
            "bar": [123,456],
            "fez": [{
                "id": 255,
                "baz": "leesz",
                "fuzz": {
                    "fax": [234]
                },
                "sas": []
            }],
            "foz": [{
                "id": 255,
                "baz": "leesz",
                "fuzz": {
                    "fax": [234]
                },
                "sas": []
            },
            {
                "id": 256,
                "baz": "loss",
                "fuzz": {
                    "fax": [235]
                },
                "sas": [321, 321]
            }]
        }
    }]);

    let url = "/indexes/tasks/documents";
    let (response, status_code) = server.post_request(&url, body.clone()).await;
    eprintln!("{:#?}", response);
    assert_eq!(status_code, 202);
    let update_id = response["updateId"].as_u64().unwrap();
    server.wait_update_id(update_id).await;

    // 3 - Check update success

    let (response, status_code) = server.get_update_status(update_id).await;
    assert_eq!(status_code, 200);
    assert_eq!(response["status"], "processed");

    let url = "/indexes/tasks/search?q=leesz";
    let (response, status_code) = server.get_request(&url).await;
    assert_eq!(status_code, 200);
    assert_eq!(response["hits"], body);
}

#[actix_rt::test]
// test sample from #807
async fn add_document_with_long_field() {
    let mut server = common::Server::with_uid("test");
    server.create_index(json!({ "uid": "test" })).await;
    let body = json!([{
        "documentId":"de1c2adbb897effdfe0deae32a01035e46f932ce",
        "rank":1,
        "relurl":"/configuration/app/web.html#locations",
        "section":"Web",
        "site":"docs",
        "text":" The locations block is the most powerful, and potentially most involved, section of the .platform.app.yaml file. It allows you to control how the application container responds to incoming requests at a very fine-grained level. Common patterns also vary between language containers due to the way PHP-FPM handles incoming requests.\nEach entry of the locations block is an absolute URI path (with leading /) and its value includes the configuration directives for how the web server should handle matching requests. That is, if your domain is example.com then '/' means &ldquo;requests for example.com/&rdquo;, while '/admin' means &ldquo;requests for example.com/admin&rdquo;. If multiple blocks could match an incoming request then the most-specific will apply.\nweb:locations:&#39;/&#39;:# Rules for all requests that don&#39;t otherwise match....&#39;/sites/default/files&#39;:# Rules for any requests that begin with /sites/default/files....The simplest possible locations configuration is one that simply passes all requests on to your application unconditionally:\nweb:locations:&#39;/&#39;:passthru:trueThat is, all requests to /* should be forwarded to the process started by web.commands.start above. Note that for PHP containers the passthru key must specify what PHP file the request should be forwarded to, and must also specify a docroot under which the file lives. For example:\nweb:locations:&#39;/&#39;:root:&#39;web&#39;passthru:&#39;/app.php&#39;This block will serve requests to / from the web directory in the application, and if a file doesn&rsquo;t exist on disk then the request will be forwarded to the /app.php script.\nA full list of the possible subkeys for locations is below.\n  root: The folder from which to serve static assets for this location relative to the application root. The application root is the directory in which the .platform.app.yaml file is located. Typical values for this property include public or web. Setting it to '' is not recommended, and its behavior may vary depending on the type of application. Absolute paths are not supported.\n  passthru: Whether to forward disallowed and missing resources from this location to the application and can be true, false or an absolute URI path (with leading /). The default value is false. For non-PHP applications it will generally be just true or false. In a PHP application this will typically be the front controller such as /index.php or /app.php. This entry works similar to mod_rewrite under Apache. Note: If the value of passthru does not begin with the same value as the location key it is under, the passthru may evaluate to another entry. That may be useful when you want different cache settings for different paths, for instance, but want missing files in all of them to map back to the same front controller. See the example block below.\n  index: The files to consider when serving a request for a directory: an array of file names or null. (typically ['index.html']). Note that in order for this to work, access to the static files named must be allowed by the allow or rules keys for this location.\n  expires: How long to allow static assets from this location to be cached (this enables the Cache-Control and Expires headers) and can be a time or -1 for no caching (default). Times can be suffixed with &ldquo;ms&rdquo; (milliseconds), &ldquo;s&rdquo; (seconds), &ldquo;m&rdquo; (minutes), &ldquo;h&rdquo; (hours), &ldquo;d&rdquo; (days), &ldquo;w&rdquo; (weeks), &ldquo;M&rdquo; (months, 30d) or &ldquo;y&rdquo; (years, 365d).\n  scripts: Whether to allow loading scripts in that location (true or false). This directive is only meaningful on PHP.\n  allow: Whether to allow serving files which don&rsquo;t match a rule (true or false, default: true).\n  headers: Any additional headers to apply to static assets. This section is a mapping of header names to header values. Responses from the application aren&rsquo;t affected, to avoid overlap with the application&rsquo;s own ability to include custom headers in the response.\n  rules: Specific overrides for a specific location. The key is a PCRE (regular expression) that is matched against the full request path.\n  request_buffering: Most application servers do not support chunked requests (e.g. fpm, uwsgi), so Platform.sh enables request_buffering by default to handle them. That default configuration would look like this if it was present in .platform.app.yaml:\nweb:locations:&#39;/&#39;:passthru:truerequest_buffering:enabled:truemax_request_size:250mIf the application server can already efficiently handle chunked requests, the request_buffering subkey can be modified to disable it entirely (enabled: false). Additionally, applications that frequently deal with uploads greater than 250MB in size can update the max_request_size key to the application&rsquo;s needs. Note that modifications to request_buffering will need to be specified at each location where it is desired.\n ",
        "title":"Locations",
        "url":"/configuration/app/web.html#locations"
    }]);
    server.add_or_replace_multiple_documents(body).await;
    let (response, _status) = server
        .search_post(json!({ "q": "request_buffering" }))
        .await;
    assert!(!response["hits"].as_array().unwrap().is_empty());
}

#[actix_rt::test]
async fn documents_with_same_id_are_overwritten() {
    let mut server = common::Server::with_uid("test");
    server.create_index(json!({ "uid": "test"})).await;
    let documents = json!([
        {
            "id": 1,
            "content": "test1"
        },
        {
            "id": 1,
            "content": "test2"
        },
    ]);
    server.add_or_replace_multiple_documents(documents).await;
    let (response, _status) = server.get_all_documents().await;
    assert_eq!(response.as_array().unwrap().len(), 1);
    assert_eq!(
        response.as_array().unwrap()[0].as_object().unwrap()["content"],
        "test2"
    );
}

#[actix_rt::test]
async fn create_index_lazy_by_pushing_documents() {
    let mut server = common::Server::with_uid("movies");

    // 1 - Add documents

    let body = json!([{
      "title": "Test",
      "comment": "comment test"
    }]);

    let url = "/indexes/movies/documents?primaryKey=title";
    let (response, status_code) = server.post_request(&url, body).await;
    eprintln!("{:#?}", response);
    assert_eq!(status_code, 202);
    let update_id = response["updateId"].as_u64().unwrap();
    server.wait_update_id(update_id).await;

    // 3 - Check update success

    let (response, status_code) = server.get_update_status(update_id).await;
    assert_eq!(status_code, 200);
    assert_eq!(response["status"], "processed");
}

#[actix_rt::test]
async fn create_index_lazy_by_pushing_documents_and_discover_pk() {
    let mut server = common::Server::with_uid("movies");

    // 1 - Add documents

    let body = json!([{
      "id": 1,
      "title": "Test",
      "comment": "comment test"
    }]);

    let url = "/indexes/movies/documents";
    let (response, status_code) = server.post_request(&url, body).await;
    eprintln!("{:#?}", response);
    assert_eq!(status_code, 202);
    let update_id = response["updateId"].as_u64().unwrap();
    server.wait_update_id(update_id).await;

    // 3 - Check update success

    let (response, status_code) = server.get_update_status(update_id).await;
    assert_eq!(status_code, 200);
    assert_eq!(response["status"], "processed");
}

#[actix_rt::test]
async fn create_index_lazy_by_pushing_documents_with_wwrong_name() {
    let mut server = common::Server::with_uid("wrong&name");

    let body = json!([{
      "title": "Test",
      "comment": "comment test"
    }]);

    let url = "/indexes/wrong&name/documents?primaryKey=title";
    let (response, status_code) = server.post_request(&url, body).await;
    eprintln!("{:#?}", response);
    assert_eq!(status_code, 400);
    assert_eq!(response["errorCode"], "invalid_index_uid");
}
