use futures::stream::StreamExt;
use reqwest_eventsource::{Event, EventSource};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut es = EventSource::get("http://localhost:8000/events");
    while let Some(event) = es.next().await {
        match event {
            Ok(Event::Open) => println!("Connection Open!"),
            Ok(Event::Message(message)) => println!("Message: {:#?}", message),
            Err(err) => {
                println!("Error: {}", err);
                // es.close();
            }
        }
    }
    Ok(())
}
