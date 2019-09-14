use std::ops::Deref;

#[derive(Clone)]
pub struct CustomSettingsIndex(pub(crate) crate::CfTree);

impl Deref for CustomSettingsIndex {
    type Target = crate::CfTree;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
