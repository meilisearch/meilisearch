use crate::common::{GetAllDocumentsOptions, Server};
use crate::json;
use meili_snap::snapshot;

#[actix_rt::test]
async fn hf_bge_m3_force_cls_settings() {
    let server = Server::new_shared();
    let index = server.unique_index();

    let (response, code) = index
        .update_settings(json!({
          "embedders": {
              "default": {
                  "source": "huggingFace",
                  "model": "baai/bge-m3",
                  "revision": "5617a9f61b028005a4858fdac845db406aefb181",
                  "pooling": "forceCls",
                  // minimal template to allow potential document embedding if used later
                  "documentTemplate": "{{doc.title}}"
              }
          }
        }))
        .await;
    snapshot!(code, @"202 Accepted");
    server.wait_task(response.uid()).await.succeeded();

    // Try to embed one simple document
    let (task, code) = index.add_documents(json!([{ "id": 1, "title": "Hello world" }]), None).await;
    snapshot!(code, @"202 Accepted");
    server.wait_task(task.uid()).await.succeeded();

    // Retrieve the document with vectors and assert embeddings were produced
    let (documents, _code) = index
        .get_all_documents(GetAllDocumentsOptions { retrieve_vectors: true, ..Default::default() })
        .await;
    let has_vectors = documents["results"][0]["_vectors"]["default"]["embeddings"]
        .as_array()
        .map(|a| !a.is_empty())
        .unwrap_or(false);
    snapshot!(has_vectors, @"true");
}
