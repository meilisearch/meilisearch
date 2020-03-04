mod common;

#[test]
fn delete() {
    let mut server = common::Server::with_uid("movies");
    server.populate_movies();

    let (_response, status_code) = server.get_document(419704);
    assert_eq!(status_code, 200);

    server.delete_document(419704);

    let (_response, status_code) = server.get_document(419704);
    assert_eq!(status_code, 404);
}

#[test]
fn delete_batch() {
    let mut server = common::Server::with_uid("movies");
    server.populate_movies();

    let (_response, status_code) = server.get_document(419704);
    assert_eq!(status_code, 200);

    let body = serde_json::json!([419704,512200,181812]);
    server.delete_multiple_documents(body);

    let (_response, status_code) = server.get_document(419704);
    assert_eq!(status_code, 404);
}
