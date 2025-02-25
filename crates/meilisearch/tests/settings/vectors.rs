use meili_snap::{json_string, snapshot};

use crate::common::{Server, Value};

macro_rules! parameter_test {
    ($server:ident, $source:tt, $param:tt) => {
        let source = stringify!($source);
        let param = stringify!($param);
        let index = $server.index("test");

        let (response, _code) = index
        .update_settings(crate::json!({
            "embedders": {
                "test": null,
            }
        }))
        .await;
    $server.wait_task(response.uid()).await.succeeded();

    let mut value = base_for_source(source);
    value[param] = valid_parameter(source, param).0;
    let (response, code) = index
        .update_settings(crate::json!({
            "embedders": {
                "test": value
            }
        }))
        .await;
    snapshot!(code, name: concat!(stringify!($source), "-", stringify!($param), "-sending_code"));
    snapshot!(json_string!(response, {".enqueuedAt" => "[enqueuedAt]", ".taskUid" => "[taskUid]"}), name: concat!(stringify!($source), "-", stringify!($param), "-sending_result"));

    if response.has_uid() {
        let response = $server.wait_task(response.uid()).await;
        snapshot!(json_string!(response, {".enqueuedAt" => "[enqueuedAt]",
        ".uid" => "[uid]", ".batchUid" => "[batchUid]",
        ".duration" => "[duration]",
        ".startedAt" => "[startedAt]",
        ".finishedAt" => "[finishedAt]"}), name: concat!(stringify!($source), "-", stringify!($param), "-task_result"));
    }

    };
}

#[actix_rt::test]
async fn bad_parameters() {
    let server = Server::new().await;

    // for each source, check which parameters are allowed/disallowed
    // model
    // - openai
    parameter_test!(server, openAi, model);
    // - huggingFace
    parameter_test!(server, huggingFace, model);
    // - userProvided
    parameter_test!(server, userProvided, model);
    // - ollama
    parameter_test!(server, ollama, model);
    // - rest
    parameter_test!(server, rest, model);
    // ==

    // revision
    // - openai
    parameter_test!(server, openAi, revision);
    // - huggingFace
    parameter_test!(server, huggingFace, revision);
    // - userProvided
    parameter_test!(server, userProvided, revision);
    // - ollama
    parameter_test!(server, ollama, revision);
    // - rest
    parameter_test!(server, rest, revision);
    // ==

    // pooling
    // - openai
    parameter_test!(server, openAi, pooling);
    // - huggingFace
    parameter_test!(server, huggingFace, pooling);
    // - userProvided
    parameter_test!(server, userProvided, pooling);
    // - ollama
    parameter_test!(server, ollama, pooling);
    // - rest
    parameter_test!(server, rest, pooling);
    // ==

    // apiKey
    // - openai
    parameter_test!(server, openAi, apiKey);
    // - huggingFace
    parameter_test!(server, huggingFace, apiKey);
    // - userProvided
    parameter_test!(server, userProvided, apiKey);
    // - ollama
    parameter_test!(server, ollama, apiKey);
    // - rest
    parameter_test!(server, rest, apiKey);
    // ==

    // dimensions
    // - openai
    parameter_test!(server, openAi, dimensions);
    // - huggingFace
    parameter_test!(server, huggingFace, dimensions);
    // - userProvided
    parameter_test!(server, userProvided, dimensions);
    // - ollama
    parameter_test!(server, ollama, dimensions);
    // - rest
    parameter_test!(server, rest, dimensions);
    // ==

    // binaryQuantized
    // - openai
    parameter_test!(server, openAi, binaryQuantized);
    // - huggingFace
    parameter_test!(server, huggingFace, binaryQuantized);
    // - userProvided
    parameter_test!(server, userProvided, binaryQuantized);
    // - ollama
    parameter_test!(server, ollama, binaryQuantized);
    // - rest
    parameter_test!(server, rest, binaryQuantized);
    // ==

    // for each source, check that removing mandatory parameters is a failure
}

#[actix_rt::test]
async fn bad_parameters_2() {
    let server = Server::new().await;

    // documentTemplate
    // - openai
    parameter_test!(server, openAi, documentTemplate);
    // - huggingFace
    parameter_test!(server, huggingFace, documentTemplate);
    // - userProvided
    parameter_test!(server, userProvided, documentTemplate);
    // - ollama
    parameter_test!(server, ollama, documentTemplate);
    // - rest
    parameter_test!(server, rest, documentTemplate);
    // ==

    // documentTemplateMaxBytes
    // - openai
    parameter_test!(server, openAi, documentTemplateMaxBytes);
    // - huggingFace
    parameter_test!(server, huggingFace, documentTemplateMaxBytes);
    // - userProvided
    parameter_test!(server, userProvided, documentTemplateMaxBytes);
    // - ollama
    parameter_test!(server, ollama, documentTemplateMaxBytes);
    // - rest
    parameter_test!(server, rest, documentTemplateMaxBytes);
    // ==

    // url
    // - openai
    parameter_test!(server, openAi, url);
    // - huggingFace
    parameter_test!(server, huggingFace, url);
    // - userProvided
    parameter_test!(server, userProvided, url);
    // - ollama
    parameter_test!(server, ollama, url);
    // - rest
    parameter_test!(server, rest, url);
    // ==

    // request
    // - openai
    parameter_test!(server, openAi, request);
    // - huggingFace
    parameter_test!(server, huggingFace, request);
    // - userProvided
    parameter_test!(server, userProvided, request);
    // - ollama
    parameter_test!(server, ollama, request);
    // - rest
    parameter_test!(server, rest, request);
    // ==

    // response
    // - openai
    parameter_test!(server, openAi, response);
    // - huggingFace
    parameter_test!(server, huggingFace, response);
    // - userProvided
    parameter_test!(server, userProvided, response);
    // - ollama
    parameter_test!(server, ollama, response);
    // - rest
    parameter_test!(server, rest, response);
    // ==

    // headers
    // - openai
    parameter_test!(server, openAi, headers);
    // - huggingFace
    parameter_test!(server, huggingFace, headers);
    // - userProvided
    parameter_test!(server, userProvided, headers);
    // - ollama
    parameter_test!(server, ollama, headers);
    // - rest
    parameter_test!(server, rest, headers);
    // ==

    // distribution
    // - openai
    parameter_test!(server, openAi, distribution);
    // - huggingFace
    parameter_test!(server, huggingFace, distribution);
    // - userProvided
    parameter_test!(server, userProvided, distribution);
    // - ollama
    parameter_test!(server, ollama, distribution);
    // - rest
    parameter_test!(server, rest, distribution);
    // ==
}

fn base_for_source(source: &'static str) -> Value {
    let base_parameters = maplit::btreemap! {
        "openAi" => vec![],
        "huggingFace" => vec![],
        "userProvided" => vec!["dimensions"],
        "ollama" => vec!["model",
        // add dimensions to avoid actually fetching the model from ollama
        "dimensions"],
        "rest" => vec!["url", "request", "response",
        // add dimensions to avoid actually fetching the model from ollama
        "dimensions"],
    };

    let mut value = crate::json!({
        "source": source
    });

    let mandatory_parameters = base_parameters.get(source).unwrap();
    for mandatory_parameter in mandatory_parameters {
        value[mandatory_parameter] = valid_parameter(source, mandatory_parameter).0;
    }
    value
}

fn valid_parameter(source: &'static str, parameter: &'static str) -> Value {
    match (source, parameter) {
        ("openAi", "model") => crate::json!("text-embedding-3-small"),
        ("huggingFace", "model") => crate::json!("sentence-transformers/all-MiniLM-L6-v2"),
        (_, "model") => crate::json!("all-minilm"),
        (_, "revision") => crate::json!("e4ce9877abf3edfe10b0d82785e83bdcb973e22e"),
        (_, "pooling") => crate::json!("forceMean"),
        (_, "apiKey") => crate::json!("foo"),
        (_, "dimensions") => crate::json!(768),
        (_, "binaryQuantized") => crate::json!(false),
        (_, "documentTemplate") => crate::json!("toto"),
        (_, "documentTemplateMaxBytes") => crate::json!(200),
        (_, "url") => crate::json!("http://rest.example/"),
        (_, "request") => crate::json!({"text": "{{text}}"}),
        (_, "response") => crate::json!({"embedding": "{{embedding}}"}),
        (_, "headers") => crate::json!({"custom": "value"}),
        (_, "distribution") => crate::json!({"mean": 0.4, "sigma": 0.1}),
        _ => panic!("unknown parameter"),
    }
}
