use crate::action::Action;
use crate::error::{AuthControllerError, Result};
use crate::store::{KeyId, KEY_ID_LENGTH};
use chrono::{DateTime, NaiveDateTime, Utc};
use rand::Rng;
use serde::{Deserialize, Serialize};
use serde_json::{from_value, Value};

#[derive(Debug, Deserialize, Serialize)]
pub struct Key {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    pub id: KeyId,
    pub actions: Vec<Action>,
    pub indexes: Vec<String>,
    pub expires_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl Key {
    pub fn create_from_value(value: Value) -> Result<Self> {
        let description = value
            .get("description")
            .map(|des| {
                from_value(des.clone())
                    .map_err(|_| AuthControllerError::InvalidApiKeyDescription(des.clone()))
            })
            .transpose()?;

        let id = generate_id();

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

        let created_at = Utc::now();
        let updated_at = Utc::now();

        Ok(Self {
            description,
            id,
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

        if let Some(act) = value.get("actions") {
            let act = from_value(act.clone())
                .map_err(|_| AuthControllerError::InvalidApiKeyActions(act.clone()));
            self.actions = act?;
        }

        if let Some(ind) = value.get("indexes") {
            let ind = from_value(ind.clone())
                .map_err(|_| AuthControllerError::InvalidApiKeyIndexes(ind.clone()));
            self.indexes = ind?;
        }

        if let Some(exp) = value.get("expiresAt") {
            self.expires_at = parse_expiration_date(exp)?;
        }

        self.updated_at = Utc::now();

        Ok(())
    }

    pub(crate) fn default_admin() -> Self {
        Self {
            description: Some("Default Admin API Key (Use it for all other operations. Caution! Do not use it on a public frontend)".to_string()),
            id: generate_id(),
            actions: vec![Action::All],
            indexes: vec!["*".to_string()],
            expires_at: None,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        }
    }

    pub(crate) fn default_search() -> Self {
        Self {
            description: Some(
                "Default Search API Key (Use it to search from the frontend)".to_string(),
            ),
            id: generate_id(),
            actions: vec![Action::Search],
            indexes: vec!["*".to_string()],
            expires_at: None,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        }
    }
}

/// Generate a printable key of 64 characters using thread_rng.
fn generate_id() -> [u8; KEY_ID_LENGTH] {
    const CHARSET: &[u8] = b"abcdefghijklmnopqrstuvwxyz0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";

    let mut rng = rand::thread_rng();
    let mut bytes = [0; KEY_ID_LENGTH];
    for byte in bytes.iter_mut() {
        *byte = CHARSET[rng.gen_range(0..CHARSET.len())];
    }

    bytes
}

fn parse_expiration_date(value: &Value) -> Result<Option<DateTime<Utc>>> {
    match value {
        Value::String(string) => DateTime::parse_from_rfc3339(string)
            .map(|d| d.into())
            .or_else(|_| {
                NaiveDateTime::parse_from_str(string, "%Y-%m-%dT%H:%M:%S")
                    .map(|naive| DateTime::from_utc(naive, Utc))
            })
            .or_else(|_| {
                NaiveDateTime::parse_from_str(string, "%Y-%m-%d")
                    .map(|naive| DateTime::from_utc(naive, Utc))
            })
            .map_err(|_| AuthControllerError::InvalidApiKeyExpiresAt(value.clone()))
            // check if the key is already expired.
            .and_then(|d| {
                if d > Utc::now() {
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
