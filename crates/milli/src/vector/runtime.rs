use std::collections::HashMap;
use std::sync::Arc;

use super::Embedder;
use crate::prompt::Prompt;
use crate::vector::json_template::JsonTemplate;

/// Map of runtime embedder data.
#[derive(Clone, Default)]
pub struct RuntimeEmbedders(HashMap<String, Arc<RuntimeEmbedder>>);

pub struct RuntimeEmbedder {
    pub embedder: Arc<Embedder>,
    pub document_template: Prompt,
    fragments: Vec<RuntimeFragment>,
    pub is_quantized: bool,
}

impl RuntimeEmbedder {
    pub fn new(
        embedder: Arc<Embedder>,
        document_template: Prompt,
        mut fragments: Vec<RuntimeFragment>,
        is_quantized: bool,
    ) -> Self {
        fragments.sort_unstable_by(|left, right| left.name.cmp(&right.name));
        Self { embedder, document_template, fragments, is_quantized }
    }

    /// The runtime fragments sorted by name.
    pub fn fragments(&self) -> &[RuntimeFragment] {
        self.fragments.as_slice()
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
