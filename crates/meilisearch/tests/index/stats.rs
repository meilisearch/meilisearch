use crate::common::{shared_does_not_exists_index, Server};

use crate::json;

#[actix_rt::test]
async fn stats() {
    let server = Server::new_shared();
    let index = server.unique_index();
    let (task, code) = index.create(Some("id")).await;

    assert_eq!(code, 202);

    server.wait_task(task.uid()).await.succeeded();

    let (response, code) = index.stats().await;

    assert_eq!(code, 200);
    assert_eq!(response["numberOfDocuments"], 0);
    assert_eq!(response["isIndexing"], false);
    assert!(response["fieldDistribution"].as_object().unwrap().is_empty());

    let documents = json!([
        {
            "id": 1,
            "name": "Alexey",
        },
        {
            "id": 2,
            "age": 45,
        }
    ]);

    let (response, code) = index.add_documents(documents, None).await;
    assert_eq!(code, 202);

    server.wait_task(response.uid()).await.succeeded();

    let (response, code) = index.stats().await;

    assert_eq!(code, 200);
    assert_eq!(response["numberOfDocuments"], 2);
    assert_eq!(response["isIndexing"], false);
    assert_eq!(response["fieldDistribution"]["id"], 2);
    assert_eq!(response["fieldDistribution"]["name"], 1);
    assert_eq!(response["fieldDistribution"]["age"], 1);
}

#[actix_rt::test]
async fn error_get_stats_unexisting_index() {
    let index = shared_does_not_exists_index().await;
    let (response, code) = index.stats().await;

    let expected_response = json!({
        "message": format!("Index `{}` not found.", index.uid),
        "code": "index_not_found",
        "type": "invalid_request",
        "link": "https://docs.meilisearch.com/errors#index_not_found"
    });

    assert_eq!(response, expected_response);
    assert_eq!(code, 404);
}

#[actix_rt::test]
async fn fields() {
    let server = Server::new_shared();
    let index = server.unique_index();
    let (task, code) = index.create(None).await;

    assert_eq!(code, 202);
    server.wait_task(task.uid()).await.succeeded();

    // Test empty index
    let (response, code) = index.fields().await;
    assert_eq!(code, 200);
    assert!(response.as_array().unwrap().is_empty());

    // Test with documents containing nested fields
    let documents = json!([
        {
            "id": 1,
            "name": "John",
            "user": {
                "email": "john@example.com",
                "profile": {
                    "age": 30,
                    "location": "Paris"
                }
            },
            "tags": ["developer", "rust"]
        },
        {
            "id": 2,
            "title": "Article",
            "metadata": {
                "category": "tech",
                "author": {
                    "name": "Jane",
                    "id": 123
                }
            }
        }
    ]);

    let (response, code) = index.add_documents(documents, None).await;
    assert_eq!(code, 202);
    server.wait_task(response.uid()).await.succeeded();

    // Test fields including nested fields
    let (response, code) = index.fields().await;
    assert_eq!(code, 200);

    let fields = response.as_array().unwrap();
    let field_names: Vec<&str> = fields.iter().map(|f| f.as_str().unwrap()).collect();

    // Check that all expected fields are present (including nested fields)
    assert!(field_names.contains(&"id"));
    assert!(field_names.contains(&"name"));
    assert!(field_names.contains(&"title"));
    assert!(field_names.contains(&"user.email"));
    assert!(field_names.contains(&"user.profile.age"));
    assert!(field_names.contains(&"user.profile.location"));
    assert!(field_names.contains(&"tags"));
    assert!(field_names.contains(&"metadata.category"));
    assert!(field_names.contains(&"metadata.author.name"));
    assert!(field_names.contains(&"metadata.author.id"));

    // Verify the response is a simple array of strings
    for field in fields {
        assert!(field.is_string());
    }
}

#[actix_rt::test]
async fn fields_nested_complex() {
    let server = Server::new_shared();
    let index = server.unique_index();
    let (task, code) = index.create(Some("id")).await;

    assert_eq!(code, 202);
    server.wait_task(task.uid()).await.succeeded();

    // Test with complex deeply nested structures
    let documents = json!([
        {
            "id": 1,
            "product": {
                "name": "Laptop",
                "specifications": {
                    "hardware": {
                        "cpu": {
                            "brand": "Intel",
                            "model": "i7-12700K",
                            "cores": {
                                "physical": 12,
                                "logical": 20
                            }
                        },
                        "memory": {
                            "ram": 16,
                            "type": "DDR4"
                        },
                        "storage": {
                            "primary": {
                                "type": "SSD",
                                "capacity": 512
                            },
                            "secondary": {
                                "type": "HDD",
                                "capacity": 1000
                            }
                        }
                    },
                    "software": {
                        "os": "Windows 11",
                        "applications": ["Chrome", "VS Code", "Docker"]
                    }
                },
                "pricing": {
                    "base": 1299.99,
                    "currency": "USD",
                    "discounts": {
                        "student": 0.1,
                        "bulk": 0.05
                    }
                }
            },
            "customer": {
                "info": {
                    "name": "Alice",
                    "contact": {
                        "email": "alice@example.com",
                        "phone": {
                            "country": "+1",
                            "number": "555-1234"
                        }
                    }
                },
                "preferences": {
                    "notifications": {
                        "email": true,
                        "sms": false,
                        "push": {
                            "enabled": true,
                            "frequency": "daily"
                        }
                    }
                }
            }
        },
        {
            "id": 2,
            "order": {
                "items": [
                    {
                        "product_id": "ABC123",
                        "quantity": 2
                    },
                    {
                        "product_id": "DEF456",
                        "quantity": 1
                    }
                ],
                "shipping": {
                    "address": {
                        "street": "123 Main St",
                        "city": "New York",
                        "state": "NY",
                        "zip": "10001",
                        "country": "USA"
                    },
                    "method": "express",
                    "tracking": {
                        "number": "1Z999AA1234567890",
                        "carrier": "UPS"
                    }
                }
            }
        }
    ]);

    let (response, code) = index.add_documents(documents, None).await;
    assert_eq!(code, 202);
    server.wait_task(response.uid()).await.succeeded();

    // Test fields with complex nested structures
    let (response, code) = index.fields().await;
    assert_eq!(code, 200);

    let fields = response.as_array().unwrap();
    let field_names: Vec<&str> = fields.iter().map(|f| f.as_str().unwrap()).collect();

    // Test deeply nested fields from the first document
    assert!(field_names.contains(&"product.name"));
    assert!(field_names.contains(&"product.specifications.hardware.cpu.brand"));
    assert!(field_names.contains(&"product.specifications.hardware.cpu.model"));
    assert!(field_names.contains(&"product.specifications.hardware.cpu.cores.physical"));
    assert!(field_names.contains(&"product.specifications.hardware.cpu.cores.logical"));
    assert!(field_names.contains(&"product.specifications.hardware.memory.ram"));
    assert!(field_names.contains(&"product.specifications.hardware.memory.type"));
    assert!(field_names.contains(&"product.specifications.hardware.storage.primary.type"));
    assert!(field_names.contains(&"product.specifications.hardware.storage.primary.capacity"));
    assert!(field_names.contains(&"product.specifications.hardware.storage.secondary.type"));
    assert!(field_names.contains(&"product.specifications.hardware.storage.secondary.capacity"));
    assert!(field_names.contains(&"product.specifications.software.os"));
    assert!(field_names.contains(&"product.specifications.software.applications"));
    assert!(field_names.contains(&"product.pricing.base"));
    assert!(field_names.contains(&"product.pricing.currency"));
    assert!(field_names.contains(&"product.pricing.discounts.student"));
    assert!(field_names.contains(&"product.pricing.discounts.bulk"));

    // Test deeply nested fields from the second document
    assert!(field_names.contains(&"order.items"));
    assert!(field_names.contains(&"order.shipping.address.street"));
    assert!(field_names.contains(&"order.shipping.address.city"));
    assert!(field_names.contains(&"order.shipping.address.state"));
    assert!(field_names.contains(&"order.shipping.address.zip"));
    assert!(field_names.contains(&"order.shipping.address.country"));
    assert!(field_names.contains(&"order.shipping.method"));
    assert!(field_names.contains(&"order.shipping.tracking.number"));
    assert!(field_names.contains(&"order.shipping.tracking.carrier"));

    // Test customer fields
    assert!(field_names.contains(&"customer.info.name"));
    assert!(field_names.contains(&"customer.info.contact.email"));
    assert!(field_names.contains(&"customer.info.contact.phone.country"));
    assert!(field_names.contains(&"customer.info.contact.phone.number"));
    assert!(field_names.contains(&"customer.preferences.notifications.email"));
    assert!(field_names.contains(&"customer.preferences.notifications.sms"));
    assert!(field_names.contains(&"customer.preferences.notifications.push.enabled"));
    assert!(field_names.contains(&"customer.preferences.notifications.push.frequency"));

    // Verify all fields are strings and no duplicates
    let unique_fields: std::collections::HashSet<&str> = field_names.iter().cloned().collect();
    assert_eq!(unique_fields.len(), field_names.len(), "No duplicate fields should exist");

    // Verify the response is a simple array of strings
    for field in fields {
        assert!(field.is_string());
    }

    // Test that we have a reasonable number of fields (should be more than just top-level)
    assert!(field_names.len() > 10, "Should have more than 10 fields including nested ones");
}

#[actix_rt::test]
async fn error_get_fields_unexisting_index() {
    let index = shared_does_not_exists_index().await;
    let (response, code) = index.fields().await;

    let expected_response = json!({
        "message": format!("Index `{}` not found.", index.uid),
        "code": "index_not_found",
        "type": "invalid_request",
        "link": "https://docs.meilisearch.com/errors#index_not_found"
    });

    assert_eq!(response, expected_response);
    assert_eq!(code, 404);
}
