use rocket::http::Status;
use rocket::request::Outcome;
use rocket::request::Request;
use rocket::response::content::RawHtml;
use rocket::response::stream::{Event, EventStream};
use rocket::tokio::time::{self, Duration};
use rocket::{get, launch, routes};
use std::time::SystemTime;

pub struct LastEventId(pub usize);

#[rocket::async_trait]
impl<'r> rocket::request::FromRequest<'r> for LastEventId {
    type Error = std::num::ParseIntError;
    async fn from_request(req: &'r Request<'_>) -> Outcome<Self, Self::Error> {
        if let Some(id) = req.headers().get("Last-Event-ID").next() {
            if id.is_empty() {
                Outcome::Success(LastEventId(0))
            } else {
                match id.parse() {
                    Ok(id) => Outcome::Success(LastEventId(id)),
                    Err(err) => Outcome::Error((Status::BadRequest, err)),
                }
            }
        } else {
            Outcome::Success(LastEventId(0))
        }
    }
}

#[get("/events")]
fn events(id: LastEventId) -> EventStream![] {
    let mut id = id.0;
    let mut interval = time::interval(Duration::from_secs(2));
    EventStream! {
        loop {
            interval.tick().await;
            let unix_time = SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_secs();
            id += 1;
            yield Event::data(unix_time.to_string()).id(id.to_string());
        }
    }
}

#[get("/")]
fn index() -> RawHtml<&'static str> {
    RawHtml(
        r#"
Open Console
<script>
    const es = new EventSource("http://localhost:8000/events");
    es.onopen = () => console.log("Connection Open!");
    es.onmessage = (e) => console.log("Message:", e);
    es.onerror = (e) => {
        console.log("Error:", e);
        // es.close();
    };
</script>
"#,
    )
}

#[launch]
fn rocket() -> _ {
    rocket::build().mount("/", routes![events, index])
}
