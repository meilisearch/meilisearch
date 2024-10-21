use serde::Deserialize;
use uuid::Uuid;

use super::Settings;

#[derive(Deserialize, Debug, Clone)]
#[cfg_attr(test, derive(serde::Serialize))]
pub struct IndexUuid {
    pub uid: String,
    pub uuid: Uuid,
}

#[derive(Deserialize)]
#[cfg_attr(test, derive(serde::Serialize))]
pub struct DumpMeta {
    pub settings: Settings<super::Unchecked>,
    pub primary_key: Option<String>,
}
