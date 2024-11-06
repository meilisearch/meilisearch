use std::io::{Read, Write};

use entry::Entry;

pub mod entry;
mod error;
pub mod layer;
pub mod processor;

pub use error::Error;

pub struct TraceWriter<W: Write> {
    writer: W,
    receiver: tokio::sync::mpsc::UnboundedReceiver<Entry>,
}

pub struct Trace {
    receiver: tokio::sync::mpsc::UnboundedReceiver<Entry>,
}

impl Trace {
    pub fn into_receiver(self) -> tokio::sync::mpsc::UnboundedReceiver<Entry> {
        self.receiver
    }

    pub fn into_writer<W: Write>(self, writer: W) -> TraceWriter<W> {
        TraceWriter { writer, receiver: self.receiver }
    }
}

pub struct TraceReader<R: Read> {
    reader: R,
}

impl<R: Read> TraceReader<R> {
    pub fn new(reader: R) -> Self {
        Self { reader }
    }

    fn read(&mut self) -> Option<Result<Entry, Error>> {
        serde_json::Deserializer::from_reader(&mut self.reader)
            .into_iter()
            .next()
            .map(|res| res.map_err(Into::into))
    }
}

impl<R: Read> Iterator for TraceReader<R> {
    type Item = Result<Entry, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        self.read()
    }
}
