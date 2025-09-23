use meili_snap::{json_string, snapshot};

use crate::common::Server;
use crate::json;

#[actix_rt::test]
async fn set_reset_chat_issue_5772() {
    let server = Server::new().await;
    let index = server.unique_index();

    let (_, code) = server
        .set_features(json!({
            "chatCompletions": true,
        }))
        .await;
    snapshot!(code, @r#"200 OK"#);

    let (task1, _code) = index.update_settings_chat(json!({
        "description": "test!",
        "documentTemplate": "{% for field in fields %}{% if field.is_searchable and field.value != nil %}{{ field.name }}: {{ field.value }}\n{% endif %}{% endfor %}",
        "documentTemplateMaxBytes": 400,
        "searchParameters": {
            "limit": 15,
            "sort": [],
            "attributesToSearchOn": []
        }
    })).await;
    server.wait_task(task1.uid()).await.succeeded();

    let (response, _) = index.settings().await;
    snapshot!(json_string!(response["chat"]), @r#"
    {
      "description": "test!",
      "documentTemplate": "{% for field in fields %}{% if field.is_searchable and field.value != nil %}{{ field.name }}: {{ field.value }}\n{% endif %}{% endfor %}",
      "documentTemplateMaxBytes": 400,
      "searchParameters": {
        "limit": 15,
        "sort": [],
        "attributesToSearchOn": []
      }
    }
    "#);

    let (task2, _status_code) = index.update_settings_chat(json!({
        "description": "test!",
        "documentTemplate": "{% for field in fields %}{% if field.is_searchable and field.value != nil %}{{ field.name }}: {{ field.value }}\n{% endif %}{% endfor %}",
        "documentTemplateMaxBytes": 400,
        "searchParameters": {
            "limit": 16
        }
    })).await;
    server.wait_task(task2.uid()).await.succeeded();

    let (response, _) = index.settings().await;
    snapshot!(json_string!(response["chat"]), @r#"
    {
      "description": "test!",
      "documentTemplate": "{% for field in fields %}{% if field.is_searchable and field.value != nil %}{{ field.name }}: {{ field.value }}\n{% endif %}{% endfor %}",
      "documentTemplateMaxBytes": 400,
      "searchParameters": {
        "limit": 16,
        "sort": [],
        "attributesToSearchOn": []
      }
    }
    "#);
}
