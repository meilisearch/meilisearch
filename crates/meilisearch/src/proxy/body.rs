use std::fs::File;

use meilisearch_types::network::Remote;

pub enum Body<T, F>
where
    T: serde::Serialize,
    F: FnMut(&str, &Remote, &mut T),
{
    NdJsonPayload(File),
    Inline(T),
    Generated(T, F),
    None,
}

impl Body<(), fn(&str, &Remote, &mut ())> {
    pub fn with_ndjson_payload(file: File) -> Self {
        Self::NdJsonPayload(file)
    }

    pub fn none() -> Self {
        Self::None
    }
}

impl<T> Body<T, fn(&str, &Remote, &mut T)>
where
    T: serde::Serialize,
{
    pub fn inline(payload: T) -> Self {
        Self::Inline(payload)
    }
}

impl<T, F> Body<T, F>
where
    T: serde::Serialize,
    F: FnMut(&str, &Remote, &mut T),
{
    pub fn generated(initial: T, f: F) -> Self {
        Self::Generated(initial, f)
    }
}
