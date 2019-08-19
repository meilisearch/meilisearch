use std::sync::Arc;
use std::ops::Deref;

#[derive(Clone)]
pub struct CustomSettingsIndex(pub(crate) Arc<sled::Tree>);

impl Deref for CustomSettingsIndex {
    type Target = sled::Tree;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
