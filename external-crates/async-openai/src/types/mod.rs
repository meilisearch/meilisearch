//! Types used in OpenAI API requests and responses.
//! These types are created from component schemas in the [OpenAPI spec](https://github.com/openai/openai-openapi)
mod assistant;
mod assistant_impls;
mod assistant_stream;
mod audio;
mod audit_log;
mod batch;
mod chat;
mod common;
mod completion;
mod embedding;
mod file;
mod fine_tuning;
mod image;
mod invites;
mod message;
mod model;
mod moderation;
mod project_api_key;
mod project_service_account;
mod project_users;
mod projects;
#[cfg_attr(docsrs, doc(cfg(feature = "realtime")))]
#[cfg(feature = "realtime")]
pub mod realtime;
mod run;
mod step;
mod thread;
mod upload;
mod users;
mod vector_store;

pub use assistant::*;
pub use assistant_stream::*;
pub use audio::*;
pub use audit_log::*;
pub use batch::*;
pub use chat::*;
pub use common::*;
pub use completion::*;
pub use embedding::*;
pub use file::*;
pub use fine_tuning::*;
pub use image::*;
pub use invites::*;
pub use message::*;
pub use model::*;
pub use moderation::*;
pub use project_api_key::*;
pub use project_service_account::*;
pub use project_users::*;
pub use projects::*;
pub use run::*;
pub use step::*;
pub use thread::*;
pub use upload::*;
pub use users::*;
pub use vector_store::*;

mod impls;
use derive_builder::UninitializedFieldError;

use crate::error::OpenAIError;

impl From<UninitializedFieldError> for OpenAIError {
    fn from(value: UninitializedFieldError) -> Self {
        OpenAIError::InvalidArgument(value.to_string())
    }
}
