mod errors;

use meili_snap::{json_string, snapshot};
use serde_json::json;

use crate::common::{GetAllDocumentsOptions, Server};

#[actix_rt::test]
async fn swap_indexes() {
    let server = Server::new().await;
    let a = server.index("a");
    let (_, code) = a.add_documents(json!({ "id": 1, "index": "a"}), None).await;
    snapshot!(code, @"202 Accepted");
    let b = server.index("b");
    let (res, code) = b.add_documents(json!({ "id": 1, "index": "b"}), None).await;
    snapshot!(code, @"202 Accepted");
    snapshot!(res["taskUid"], @"1");
    server.wait_task(1).await;

    let (tasks, code) = server.tasks().await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(tasks, { ".results[].duration" => "[duration]", ".results[].enqueuedAt" => "[date]", ".results[].startedAt" => "[date]", ".results[].finishedAt" => "[date]" }), @r###"
    {
      "results": [
        {
          "uid": 1,
          "indexUid": "b",
          "status": "succeeded",
          "type": "documentAdditionOrUpdate",
          "canceledBy": null,
          "details": {
            "receivedDocuments": 1,
            "indexedDocuments": 1
          },
          "error": null,
          "duration": "[duration]",
          "enqueuedAt": "[date]",
          "startedAt": "[date]",
          "finishedAt": "[date]"
        },
        {
          "uid": 0,
          "indexUid": "a",
          "status": "succeeded",
          "type": "documentAdditionOrUpdate",
          "canceledBy": null,
          "details": {
            "receivedDocuments": 1,
            "indexedDocuments": 1
          },
          "error": null,
          "duration": "[duration]",
          "enqueuedAt": "[date]",
          "startedAt": "[date]",
          "finishedAt": "[date]"
        }
      ],
      "limit": 20,
      "from": 1,
      "next": null
    }
    "###);

    let (res, code) = server.index_swap(json!([{ "indexes": ["a", "b"] }])).await;
    snapshot!(code, @"202 Accepted");
    snapshot!(res["taskUid"], @"2");
    server.wait_task(2).await;

    let (tasks, code) = server.tasks().await;
    snapshot!(code, @"200 OK");

    // Notice how the task 0 which was initially representing the creation of the index `A` now represents the creation of the index `B`.
    snapshot!(json_string!(tasks, { ".results[].duration" => "[duration]", ".results[].enqueuedAt" => "[date]", ".results[].startedAt" => "[date]", ".results[].finishedAt" => "[date]" }), @r###"
    {
      "results": [
        {
          "uid": 2,
          "indexUid": null,
          "status": "succeeded",
          "type": "indexSwap",
          "canceledBy": null,
          "details": {
            "swaps": [
              {
                "indexes": [
                  "a",
                  "b"
                ]
              }
            ]
          },
          "error": null,
          "duration": "[duration]",
          "enqueuedAt": "[date]",
          "startedAt": "[date]",
          "finishedAt": "[date]"
        },
        {
          "uid": 1,
          "indexUid": "a",
          "status": "succeeded",
          "type": "documentAdditionOrUpdate",
          "canceledBy": null,
          "details": {
            "receivedDocuments": 1,
            "indexedDocuments": 1
          },
          "error": null,
          "duration": "[duration]",
          "enqueuedAt": "[date]",
          "startedAt": "[date]",
          "finishedAt": "[date]"
        },
        {
          "uid": 0,
          "indexUid": "b",
          "status": "succeeded",
          "type": "documentAdditionOrUpdate",
          "canceledBy": null,
          "details": {
            "receivedDocuments": 1,
            "indexedDocuments": 1
          },
          "error": null,
          "duration": "[duration]",
          "enqueuedAt": "[date]",
          "startedAt": "[date]",
          "finishedAt": "[date]"
        }
      ],
      "limit": 20,
      "from": 2,
      "next": null
    }
    "###);

    // BUT, the data in `a` should now points to the data that was in `b`.
    // And the opposite is true as well
    let (res, _) = a.get_all_documents(GetAllDocumentsOptions::default()).await;
    snapshot!(res["results"], @r###"[{"id":1,"index":"b"}]"###);
    let (res, _) = b.get_all_documents(GetAllDocumentsOptions::default()).await;
    snapshot!(res["results"], @r###"[{"id":1,"index":"a"}]"###);

    // ================
    // And now we're going to attempt the famous and dangerous DOUBLE index swap 🤞

    let c = server.index("c");
    let (res, code) = c.add_documents(json!({ "id": 1, "index": "c"}), None).await;
    snapshot!(code, @"202 Accepted");
    snapshot!(res["taskUid"], @"3");
    let d = server.index("d");
    let (res, code) = d.add_documents(json!({ "id": 1, "index": "d"}), None).await;
    snapshot!(code, @"202 Accepted");
    snapshot!(res["taskUid"], @"4");
    server.wait_task(4).await;

    // ensure the index creation worked properly
    let (tasks, code) = server.tasks_filter("limit=2").await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(tasks, { ".results[].duration" => "[duration]", ".results[].enqueuedAt" => "[date]", ".results[].startedAt" => "[date]", ".results[].finishedAt" => "[date]" }), @r###"
    {
      "results": [
        {
          "uid": 4,
          "indexUid": "d",
          "status": "succeeded",
          "type": "documentAdditionOrUpdate",
          "canceledBy": null,
          "details": {
            "receivedDocuments": 1,
            "indexedDocuments": 1
          },
          "error": null,
          "duration": "[duration]",
          "enqueuedAt": "[date]",
          "startedAt": "[date]",
          "finishedAt": "[date]"
        },
        {
          "uid": 3,
          "indexUid": "c",
          "status": "succeeded",
          "type": "documentAdditionOrUpdate",
          "canceledBy": null,
          "details": {
            "receivedDocuments": 1,
            "indexedDocuments": 1
          },
          "error": null,
          "duration": "[duration]",
          "enqueuedAt": "[date]",
          "startedAt": "[date]",
          "finishedAt": "[date]"
        }
      ],
      "limit": 2,
      "from": 4,
      "next": 2
    }
    "###);

    // It's happening 😲

    let (res, code) =
        server.index_swap(json!([{ "indexes": ["a", "b"] }, { "indexes": ["c", "d"] } ])).await;
    snapshot!(res["taskUid"], @"5");
    snapshot!(code, @"202 Accepted");
    server.wait_task(5).await;

    // ensure the index creation worked properly
    let (tasks, code) = server.tasks().await;
    snapshot!(code, @"200 OK");

    // What should we check for each tasks in this test:
    // Task number;
    // 0. should have the indexUid `a` again
    // 1. should have the indexUid `b` again
    // 2. stays unchanged
    // 3. now have the indexUid `d` instead of `c`
    // 4. now have the indexUid `c` instead of `d`
    snapshot!(json_string!(tasks, { ".results[].duration" => "[duration]", ".results[].enqueuedAt" => "[date]", ".results[].startedAt" => "[date]", ".results[].finishedAt" => "[date]" }), @r###"
    {
      "results": [
        {
          "uid": 5,
          "indexUid": null,
          "status": "succeeded",
          "type": "indexSwap",
          "canceledBy": null,
          "details": {
            "swaps": [
              {
                "indexes": [
                  "a",
                  "b"
                ]
              },
              {
                "indexes": [
                  "c",
                  "d"
                ]
              }
            ]
          },
          "error": null,
          "duration": "[duration]",
          "enqueuedAt": "[date]",
          "startedAt": "[date]",
          "finishedAt": "[date]"
        },
        {
          "uid": 4,
          "indexUid": "c",
          "status": "succeeded",
          "type": "documentAdditionOrUpdate",
          "canceledBy": null,
          "details": {
            "receivedDocuments": 1,
            "indexedDocuments": 1
          },
          "error": null,
          "duration": "[duration]",
          "enqueuedAt": "[date]",
          "startedAt": "[date]",
          "finishedAt": "[date]"
        },
        {
          "uid": 3,
          "indexUid": "d",
          "status": "succeeded",
          "type": "documentAdditionOrUpdate",
          "canceledBy": null,
          "details": {
            "receivedDocuments": 1,
            "indexedDocuments": 1
          },
          "error": null,
          "duration": "[duration]",
          "enqueuedAt": "[date]",
          "startedAt": "[date]",
          "finishedAt": "[date]"
        },
        {
          "uid": 2,
          "indexUid": null,
          "status": "succeeded",
          "type": "indexSwap",
          "canceledBy": null,
          "details": {
            "swaps": [
              {
                "indexes": [
                  "b",
                  "a"
                ]
              }
            ]
          },
          "error": null,
          "duration": "[duration]",
          "enqueuedAt": "[date]",
          "startedAt": "[date]",
          "finishedAt": "[date]"
        },
        {
          "uid": 1,
          "indexUid": "b",
          "status": "succeeded",
          "type": "documentAdditionOrUpdate",
          "canceledBy": null,
          "details": {
            "receivedDocuments": 1,
            "indexedDocuments": 1
          },
          "error": null,
          "duration": "[duration]",
          "enqueuedAt": "[date]",
          "startedAt": "[date]",
          "finishedAt": "[date]"
        },
        {
          "uid": 0,
          "indexUid": "a",
          "status": "succeeded",
          "type": "documentAdditionOrUpdate",
          "canceledBy": null,
          "details": {
            "receivedDocuments": 1,
            "indexedDocuments": 1
          },
          "error": null,
          "duration": "[duration]",
          "enqueuedAt": "[date]",
          "startedAt": "[date]",
          "finishedAt": "[date]"
        }
      ],
      "limit": 20,
      "from": 5,
      "next": null
    }
    "###);

    // - The data in `a` should point to `a`.
    // - The data in `b` should point to `b`.
    // - The data in `c` should point to `d`.
    // - The data in `d` should point to `c`.
    let (res, _) = a.get_all_documents(GetAllDocumentsOptions::default()).await;
    snapshot!(res["results"], @r###"[{"id":1,"index":"a"}]"###);
    let (res, _) = b.get_all_documents(GetAllDocumentsOptions::default()).await;
    snapshot!(res["results"], @r###"[{"id":1,"index":"b"}]"###);
    let (res, _) = c.get_all_documents(GetAllDocumentsOptions::default()).await;
    snapshot!(res["results"], @r###"[{"id":1,"index":"d"}]"###);
    let (res, _) = d.get_all_documents(GetAllDocumentsOptions::default()).await;
    snapshot!(res["results"], @r###"[{"id":1,"index":"c"}]"###);
}
