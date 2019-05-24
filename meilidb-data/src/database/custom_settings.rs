use std::ops::Deref;
use crate::database::raw_index::InnerRawIndex;

#[derive(Clone)]
pub struct CustomSettings(pub(crate) InnerRawIndex);

impl Deref for CustomSettings {
    type Target = InnerRawIndex;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
