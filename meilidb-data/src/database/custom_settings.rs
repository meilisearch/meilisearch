use std::sync::Arc;
use std::ops::Deref;

#[derive(Clone)]
pub struct CustomSettings(pub Arc<sled::Tree>);

impl Deref for CustomSettings {
    type Target = sled::Tree;

    fn deref(&self) -> &sled::Tree {
        &self.0
    }
}
