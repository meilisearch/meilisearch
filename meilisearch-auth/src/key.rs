use crate::action::Action;
use crate::error::{AuthControllerError, Result};
use crate::store::KeyId;

use meilisearch_types::index_uid::IndexUid;
use meilisearch_types::star_or::StarOr;
use serde::{Deserialize, Serialize};
use serde_json::{from_value, Value};
use time::format_description::well_known::Rfc3339;
use time::macros::{format_description, time};
use time::{Date, OffsetDateTime, PrimitiveDateTime};
use uuid::Uuid;

#[derive(Debug, Deserialize, Serialize)]
pub struct Key {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    pub uid: KeyId,
    pub actions: Vec<Action>,
    pub indexes: Vec<StarOr<IndexUid>>,
    #[serde(with = "time::serde::rfc3339::option")]
    pub expires_at: Option<OffsetDateTime>,
    #[serde(with = "time::serde::rfc3339")]
    pub created_at: OffsetDateTime,
    #[serde(with = "time::serde::rfc3339")]
    pub updated_at: OffsetDateTime,
}

impl Key {
    pub fn create_from_value(value: Value) -> Result<Self> {
        let name = match value.get("name") {
            None | Some(Value::Null) => None,
            Some(des) => from_value(des.clone())
                .map(Some)
                .map_err(|_| AuthControllerError::InvalidApiKeyName(des.clone()))?,
        };

        let description = match value.get("description") {
            None | Some(Value::Null) => None,
            Some(des) => from_value(des.clone())
                .map(Some)
                .map_err(|_| AuthControllerError::InvalidApiKeyDescription(des.clone()))?,
        };

        let uid = value.get("uid").map_or_else(
            || Ok(Uuid::new_v4()),
            |uid| {
                from_value(uid.clone())
                    .map_err(|_| AuthControllerError::InvalidApiKeyUid(uid.clone()))
            },
        )?;

        let actions = value
            .get("actions")
            .map(|act| {
                from_value(act.clone())
                    .map_err(|_| AuthControllerError::InvalidApiKeyActions(act.clone()))
            })
            .ok_or(AuthControllerError::MissingParameter("actions"))??;

        let indexes = value
            .get("indexes")
            .map(|ind| {
                from_value(ind.clone())
                    .map_err(|_| AuthControllerError::InvalidApiKeyIndexes(ind.clone()))
            })
            .ok_or(AuthControllerError::MissingParameter("indexes"))??;

        let expires_at = value
            .get("expiresAt")
            .map(parse_expiration_date)
            .ok_or(AuthControllerError::MissingParameter("expiresAt"))??;

        let created_at = OffsetDateTime::now_utc();
        let updated_at = created_at;

        Ok(Self {
            name,
            description,
            uid,
            actions,
            indexes,
            expires_at,
            created_at,
            updated_at,
        })
    }

    pub fn update_from_value(&mut self, value: Value) -> Result<()> {
        if let Some(des) = value.get("description") {
            let des = from_value(des.clone())
                .map_err(|_| AuthControllerError::InvalidApiKeyDescription(des.clone()));
            self.description = des?;
        }

        if let Some(des) = value.get("name") {
            let des = from_value(des.clone())
                .map_err(|_| AuthControllerError::InvalidApiKeyName(des.clone()));
            self.name = des?;
        }

        if value.get("uid").is_some() {
            return Err(AuthControllerError::ImmutableField("uid".to_string()));
        }

        if value.get("actions").is_some() {
            return Err(AuthControllerError::ImmutableField("actions".to_string()));
        }

        if value.get("indexes").is_some() {
            return Err(AuthControllerError::ImmutableField("indexes".to_string()));
        }

        if value.get("expiresAt").is_some() {
            return Err(AuthControllerError::ImmutableField("expiresAt".to_string()));
        }

        if value.get("createdAt").is_some() {
            return Err(AuthControllerError::ImmutableField("createdAt".to_string()));
        }

        if value.get("updatedAt").is_some() {
            return Err(AuthControllerError::ImmutableField("updatedAt".to_string()));
        }

        self.updated_at = OffsetDateTime::now_utc();

        Ok(())
    }

    pub(crate) fn default_admin() -> Self {
        let now = OffsetDateTime::now_utc();
        let uid = Uuid::new_v4();
        Self {
            name: Some("Default Admin API Key".to_string()),
            description: Some("Use it for anything that is not a search operation. Caution! Do not expose it on a public frontend".to_string()),
            uid,
            actions: vec![Action::All],
            indexes: vec![StarOr::Star],
            expires_at: None,
            created_at: now,
            updated_at: now,
        }
    }

    pub(crate) fn default_search() -> Self {
        let now = OffsetDateTime::now_utc();
        let uid = Uuid::new_v4();
        Self {
            name: Some("Default Search API Key".to_string()),
            description: Some("Use it to search from the frontend".to_string()),
            uid,
            actions: vec![Action::Search],
            indexes: vec![StarOr::Star],
            expires_at: None,
            created_at: now,
            updated_at: now,
        }
    }
}

fn parse_expiration_date(value: &Value) -> Result<Option<OffsetDateTime>> {
    match value {
        Value::String(string) => OffsetDateTime::parse(string, &Rfc3339)
            .or_else(|_| {
                PrimitiveDateTime::parse(
                    string,
                    format_description!(
                        "[year repr:full base:calendar]-[month repr:numerical]-[day]T[hour]:[minute]:[second]"
                    ),
                ).map(|datetime| datetime.assume_utc())
            })
            .or_else(|_| {
                PrimitiveDateTime::parse(
                    string,
                    format_description!(
                        "[year repr:full base:calendar]-[month repr:numerical]-[day] [hour]:[minute]:[second]"
                    ),
                ).map(|datetime| datetime.assume_utc())
            })
            .or_else(|_| {
                    Date::parse(string, format_description!(
                        "[year repr:full base:calendar]-[month repr:numerical]-[day]"
                    )).map(|date| PrimitiveDateTime::new(date, time!(00:00)).assume_utc())
            })
            .map_err(|_| AuthControllerError::InvalidApiKeyExpiresAt(value.clone()))
            // check if the key is already expired.
            .and_then(|d| {
                if d > OffsetDateTime::now_utc() {
                    Ok(d)
                } else {
                    Err(AuthControllerError::InvalidApiKeyExpiresAt(value.clone()))
                }
            })
            .map(Option::Some),
        Value::Null => Ok(None),
        _otherwise => Err(AuthControllerError::InvalidApiKeyExpiresAt(value.clone())),
    }
}
