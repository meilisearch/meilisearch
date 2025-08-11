mod errors;

use meili_snap::{json_string, snapshot};

use crate::common::{GetAllDocumentsOptions, Server};
use crate::json;

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
    server.wait_task(res.uid()).await;

    let (tasks, code) = server.tasks().await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(tasks, { ".results[].duration" => "[duration]", ".results[].enqueuedAt" => "[date]", ".results[].startedAt" => "[date]", ".results[].finishedAt" => "[date]" }), @r###"
    {
      "results": [
        {
          "uid": 1,
          "batchUid": 1,
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
          "batchUid": 0,
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
      "total": 2,
      "limit": 20,
      "from": 1,
      "next": null
    }
    "###);

    let (res, code) = server.index_swap(json!([{ "indexes": ["a", "b"] }])).await;
    snapshot!(code, @"202 Accepted");
    snapshot!(res["taskUid"], @"2");
    server.wait_task(res.uid()).await;

    let (tasks, code) = server.tasks().await;
    snapshot!(code, @"200 OK");

    // Notice how the task 0 which was initially representing the creation of the index `A` now represents the creation of the index `B`.
    snapshot!(json_string!(tasks, { ".results[].duration" => "[duration]", ".results[].enqueuedAt" => "[date]", ".results[].startedAt" => "[date]", ".results[].finishedAt" => "[date]" }), @r#"
    {
      "results": [
        {
          "uid": 2,
          "batchUid": 2,
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
                ],
                "rename": false
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
          "batchUid": 1,
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
          "batchUid": 0,
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
      "total": 3,
      "limit": 20,
      "from": 2,
      "next": null
    }
    "#);

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
    server.wait_task(res.uid()).await;

    // ensure the index creation worked properly
    let (tasks, code) = server.tasks_filter("limit=2").await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(tasks, { ".results[].duration" => "[duration]", ".results[].enqueuedAt" => "[date]", ".results[].startedAt" => "[date]", ".results[].finishedAt" => "[date]" }), @r###"
    {
      "results": [
        {
          "uid": 4,
          "batchUid": 4,
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
          "batchUid": 3,
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
      "total": 5,
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
    server.wait_task(res.uid()).await;

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
    snapshot!(json_string!(tasks, { ".results[].duration" => "[duration]", ".results[].enqueuedAt" => "[date]", ".results[].startedAt" => "[date]", ".results[].finishedAt" => "[date]" }), @r#"
    {
      "results": [
        {
          "uid": 5,
          "batchUid": 5,
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
                ],
                "rename": false
              },
              {
                "indexes": [
                  "c",
                  "d"
                ],
                "rename": false
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
          "batchUid": 4,
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
          "batchUid": 3,
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
          "batchUid": 2,
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
                ],
                "rename": false
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
          "batchUid": 1,
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
          "batchUid": 0,
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
      "total": 6,
      "limit": 20,
      "from": 5,
      "next": null
    }
    "#);

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

#[actix_rt::test]
async fn swap_rename_indexes() {
    let server = Server::new().await;
    let a = server.index("a");
    let b = server.index("b");
    a.create(None).await;
    a.add_documents(json!({ "id": 1, "index": "a"}), None).await;

    let (res, _code) = server.index_swap(json!([{ "indexes": ["a", "b"], "rename": true }])).await;
    server.wait_task(res.uid()).await.succeeded();

    let (tasks, _code) = server.tasks().await;

    // Notice how the task 0 which was initially representing the creation of the index `A` now represents the creation of the index `B`.
    snapshot!(json_string!(tasks, { ".results[].duration" => "[duration]", ".results[].enqueuedAt" => "[date]", ".results[].startedAt" => "[date]", ".results[].finishedAt" => "[date]" }), @r#"
    {
      "results": [
        {
          "uid": 2,
          "batchUid": 2,
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
                ],
                "rename": true
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
          "batchUid": 1,
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
          "batchUid": 0,
          "indexUid": "b",
          "status": "succeeded",
          "type": "indexCreation",
          "canceledBy": null,
          "details": {
            "primaryKey": null
          },
          "error": null,
          "duration": "[duration]",
          "enqueuedAt": "[date]",
          "startedAt": "[date]",
          "finishedAt": "[date]"
        }
      ],
      "total": 3,
      "limit": 20,
      "from": 2,
      "next": null
    }
    "#);

    // BUT, `a` should not exists
    let (res, code) = a.get_all_documents(GetAllDocumentsOptions::default()).await;
    snapshot!(code, @"404 Not Found");
    snapshot!(res["results"], @"null");

    // And its data should be in b
    let (res, code) = b.get_all_documents(GetAllDocumentsOptions::default()).await;
    snapshot!(code, @"200 OK");
    snapshot!(res["results"], @r#"[{"id":1,"index":"a"}]"#);

    // No tasks should be linked to the index a
    let (tasks, _code) = server.tasks_filter("indexUids=a").await;
    snapshot!(json_string!(tasks, { ".results[].duration" => "[duration]", ".results[].enqueuedAt" => "[date]", ".results[].startedAt" => "[date]", ".results[].finishedAt" => "[date]" }), @r#"
    {
      "results": [],
      "total": 1,
      "limit": 20,
      "from": null,
      "next": null
    }
    "#);

    // They should be linked to the index b
    let (tasks, _code) = server.tasks_filter("indexUids=b").await;
    snapshot!(json_string!(tasks, { ".results[].duration" => "[duration]", ".results[].enqueuedAt" => "[date]", ".results[].startedAt" => "[date]", ".results[].finishedAt" => "[date]" }), @r#"
    {
      "results": [
        {
          "uid": 1,
          "batchUid": 1,
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
          "batchUid": 0,
          "indexUid": "b",
          "status": "succeeded",
          "type": "indexCreation",
          "canceledBy": null,
          "details": {
            "primaryKey": null
          },
          "error": null,
          "duration": "[duration]",
          "enqueuedAt": "[date]",
          "startedAt": "[date]",
          "finishedAt": "[date]"
        }
      ],
      "total": 3,
      "limit": 20,
      "from": 1,
      "next": null
    }
    "#);

    // ===== Now, we can delete the index `b`, but its tasks will stays
    //       if we then make a new `b` index and rename it to be called `a`
    //       the tasks currently available in `b` should not be moved

    let (res, _) = b.delete().await;
    server.wait_task(res.uid()).await.succeeded();

    b.create(Some("kefir")).await;
    let (res, _code) = server.index_swap(json!([{ "indexes": ["b", "a"], "rename": true }])).await;
    server.wait_task(res.uid()).await.succeeded();

    // `a` now contains everything
    let (tasks, _code) = server.tasks_filter("indexUids=a").await;
    snapshot!(json_string!(tasks, { ".results[].duration" => "[duration]", ".results[].enqueuedAt" => "[date]", ".results[].startedAt" => "[date]", ".results[].finishedAt" => "[date]" }), @r#"
    {
      "results": [
        {
          "uid": 4,
          "batchUid": 4,
          "indexUid": "a",
          "status": "succeeded",
          "type": "indexCreation",
          "canceledBy": null,
          "details": {
            "primaryKey": "kefir"
          },
          "error": null,
          "duration": "[duration]",
          "enqueuedAt": "[date]",
          "startedAt": "[date]",
          "finishedAt": "[date]"
        },
        {
          "uid": 3,
          "batchUid": 3,
          "indexUid": "a",
          "status": "succeeded",
          "type": "indexDeletion",
          "canceledBy": null,
          "details": {
            "deletedDocuments": 1
          },
          "error": null,
          "duration": "[duration]",
          "enqueuedAt": "[date]",
          "startedAt": "[date]",
          "finishedAt": "[date]"
        },
        {
          "uid": 1,
          "batchUid": 1,
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
          "batchUid": 0,
          "indexUid": "a",
          "status": "succeeded",
          "type": "indexCreation",
          "canceledBy": null,
          "details": {
            "primaryKey": null
          },
          "error": null,
          "duration": "[duration]",
          "enqueuedAt": "[date]",
          "startedAt": "[date]",
          "finishedAt": "[date]"
        }
      ],
      "total": 6,
      "limit": 20,
      "from": 4,
      "next": null
    }
    "#);

    // And `b` is empty
    let (tasks, _code) = server.tasks_filter("indexUids=b").await;
    snapshot!(json_string!(tasks, { ".results[].duration" => "[duration]", ".results[].enqueuedAt" => "[date]", ".results[].startedAt" => "[date]", ".results[].finishedAt" => "[date]" }), @r#"
    {
      "results": [],
      "total": 2,
      "limit": 20,
      "from": null,
      "next": null
    }
    "#);
}
