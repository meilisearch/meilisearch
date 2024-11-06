use serde::{Deserialize, Serialize};
use time::{Date, OffsetDateTime, Time, UtcOffset};

pub type FieldDistribution = std::collections::BTreeMap<String, u64>;

/// The statistics that can be computed from an `Index` object.
#[derive(serde::Deserialize, Debug)]
pub struct IndexStats {
    /// Number of documents in the index.
    pub number_of_documents: u64,
    /// Size taken up by the index' DB, in bytes.
    ///
    /// This includes the size taken by both the used and free pages of the DB, and as the free pages
    /// are not returned to the disk after a deletion, this number is typically larger than
    /// `used_database_size` that only includes the size of the used pages.
    pub database_size: u64,
    /// Size taken by the used pages of the index' DB, in bytes.
    ///
    /// As the DB backend does not return to the disk the pages that are not currently used by the DB,
    /// this value is typically smaller than `database_size`.
    pub used_database_size: u64,
    /// Association of every field name with the number of times it occurs in the documents.
    pub field_distribution: FieldDistribution,
    /// Creation date of the index.
    pub created_at: LegacyDateTime,
    /// Date of the last update of the index.
    pub updated_at: LegacyDateTime,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct IndexEmbeddingConfig {
    pub name: String,
    pub config: EmbeddingConfig,
}

#[derive(Debug, Clone, Default, serde::Deserialize, serde::Serialize)]
pub struct EmbeddingConfig {
    /// Options of the embedder, specific to each kind of embedder
    pub embedder_options: EmbedderOptions,
}

/// Options of an embedder, specific to each kind of embedder.
#[derive(Debug, Clone, Hash, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub enum EmbedderOptions {
    HuggingFace(hf::EmbedderOptions),
    OpenAi(openai::EmbedderOptions),
    Ollama(ollama::EmbedderOptions),
    UserProvided(manual::EmbedderOptions),
    Rest(rest::EmbedderOptions),
}

impl Default for EmbedderOptions {
    fn default() -> Self {
        Self::OpenAi(openai::EmbedderOptions { api_key: None, dimensions: None })
    }
}

mod hf {
    #[derive(Debug, Clone, Hash, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
    pub struct EmbedderOptions {
        pub model: String,
        pub revision: Option<String>,
    }
}
mod openai {

    #[derive(Debug, Clone, Hash, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
    pub struct EmbedderOptions {
        pub api_key: Option<String>,
        pub dimensions: Option<usize>,
    }
}
mod ollama {
    #[derive(Debug, Clone, Hash, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
    pub struct EmbedderOptions {
        pub embedding_model: String,
        pub url: Option<String>,
        pub api_key: Option<String>,
    }
}
mod manual {
    #[derive(Debug, Clone, Hash, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
    pub struct EmbedderOptions {
        pub dimensions: usize,
    }
}
mod rest {
    #[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize, serde::Serialize, Hash)]
    pub struct EmbedderOptions {
        pub api_key: Option<String>,
        pub dimensions: Option<usize>,
        pub url: String,
        pub input_field: Vec<String>,
        // path to the array of embeddings
        pub path_to_embeddings: Vec<String>,
        // shape of a single embedding
        pub embedding_object: Vec<String>,
    }
}

/// A datetime from Meilisearch v1.9 with an unspecified format.
#[derive(Debug)]
pub struct LegacyDateTime(pub OffsetDateTime);

impl<'de> Deserialize<'de> for LegacyDateTime {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct Visitor;
        impl<'de> serde::de::Visitor<'de> for Visitor {
            type Value = OffsetDateTime;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                write!(formatter, "a valid datetime")
            }

            // Comes from a binary. The legacy format is:
            // 2024-11-04 13:32:08.48368 +00:00:00
            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                let format = time::macros::format_description!("[year]-[month]-[day] [hour]:[minute]:[second].[subsecond] [offset_hour sign:mandatory]:[offset_minute]:[offset_second]");
                OffsetDateTime::parse(v, format).map_err(E::custom)
            }

            // Comes from the docker image, the legacy format is:
            // [2024,        309,     17,     15,   1, 698184971, 0,0,0]
            // year,  day in year,  hour, minute, sec, subsec   , offset stuff
            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: serde::de::SeqAccess<'de>,
            {
                let mut vec = Vec::new();
                // We must deserialize the value as `i64` because the largest values are `u32` and `i32`
                while let Some(el) = seq.next_element::<i64>()? {
                    vec.push(el);
                }
                if vec.len() != 9 {
                    return Err(serde::de::Error::custom(format!(
                        "Invalid datetime, received an array of {} elements instead of 9",
                        vec.len()
                    )));
                }
                Ok(OffsetDateTime::new_in_offset(
                    Date::from_ordinal_date(vec[0] as i32, vec[1] as u16)
                        .map_err(serde::de::Error::custom)?,
                    Time::from_hms_nano(vec[2] as u8, vec[3] as u8, vec[4] as u8, vec[5] as u32)
                        .map_err(serde::de::Error::custom)?,
                    UtcOffset::from_hms(vec[6] as i8, vec[7] as i8, vec[8] as i8)
                        .map_err(serde::de::Error::custom)?,
                ))
            }
        }
        deserializer.deserialize_any(Visitor).map(LegacyDateTime)
    }
}
