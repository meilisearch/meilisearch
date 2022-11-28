use crate::common::Server;

#[cfg(feature = "mini-dashboard")]
#[actix_rt::test]
async fn dashboard_assets_load() {
    let server = Server::new().await;

    mod generated {
        include!(concat!(env!("OUT_DIR"), "/generated.rs"));
    }

    let generated = generated::generate();

    for (path, _) in generated.into_iter() {
        let path = if path == "index.html" {
            // "index.html" redirects to "/"
            "/".to_owned()
        } else {
            "/".to_owned() + path
        };

        let (_, status_code) = server.service.get(&path).await;
        assert_eq!(status_code, 200);
    }
}
