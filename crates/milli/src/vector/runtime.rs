use std::collections::HashMap;
use std::sync::Arc;

use super::Embedder;
use crate::prompt::Prompt;
use crate::vector::json_template::JsonTemplate;
use crate::vector::url_fetcher::{ResolvedFetchMapping, UrlFetcher};

/// Map of runtime embedder data.
#[derive(Clone, Default)]
pub struct RuntimeEmbedders(HashMap<String, Arc<RuntimeEmbedder>>);

pub struct RuntimeEmbedder {
    pub embedder: Arc<Embedder>,
    pub document_template: Prompt,
    fragments: Vec<RuntimeFragment>,
    pub is_quantized: bool,
    /// URL fetcher for downloading content from URLs during embedding extraction.
    url_fetcher: Option<UrlFetcher>,
    /// Mapping from document URL field to virtual field for fetched content.
    fetch_mapping: Option<ResolvedFetchMapping>,
}

impl RuntimeEmbedder {
    pub fn new(
        embedder: Arc<Embedder>,
        document_template: Prompt,
        mut fragments: Vec<RuntimeFragment>,
        is_quantized: bool,
        url_fetcher: Option<UrlFetcher>,
        fetch_mapping: Option<ResolvedFetchMapping>,
    ) -> Self {
        fragments.sort_unstable_by(|left, right| left.name.cmp(&right.name));
        Self { embedder, document_template, fragments, is_quantized, url_fetcher, fetch_mapping }
    }

    /// The runtime fragments sorted by name.
    pub fn fragments(&self) -> &[RuntimeFragment] {
        self.fragments.as_slice()
    }

    /// The URL fetcher for downloading content during embedding extraction.
    pub fn url_fetcher(&self) -> Option<&UrlFetcher> {
        self.url_fetcher.as_ref()
    }

    /// The mapping from document URL field to virtual field.
    pub fn fetch_mapping(&self) -> Option<&ResolvedFetchMapping> {
        self.fetch_mapping.as_ref()
    }
}
pub struct RuntimeFragment {
    pub name: String,
    pub id: u8,
    pub template: JsonTemplate,
}

impl RuntimeEmbedders {
    /// Create the map from its internal component.s
    pub fn new(data: HashMap<String, Arc<RuntimeEmbedder>>) -> Self {
        Self(data)
    }

    pub fn contains(&self, name: &str) -> bool {
        self.0.contains_key(name)
    }

    /// Get an embedder configuration and template from its name.
    pub fn get(&self, name: &str) -> Option<&Arc<RuntimeEmbedder>> {
        self.0.get(name)
    }

    pub fn inner_as_ref(&self) -> &HashMap<String, Arc<RuntimeEmbedder>> {
        &self.0
    }

    pub fn into_inner(self) -> HashMap<String, Arc<RuntimeEmbedder>> {
        self.0
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl IntoIterator for RuntimeEmbedders {
    type Item = (String, Arc<RuntimeEmbedder>);

    type IntoIter = std::collections::hash_map::IntoIter<String, Arc<RuntimeEmbedder>>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}
