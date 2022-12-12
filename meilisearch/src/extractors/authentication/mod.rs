mod error;

use std::marker::PhantomData;
use std::ops::Deref;
use std::pin::Pin;

use actix_web::FromRequest;
pub use error::AuthenticationError;
use futures::future::err;
use futures::Future;
use meilisearch_auth::{AuthController, AuthFilter};
use meilisearch_types::error::{Code, ResponseError};

pub struct GuardedData<P, D> {
    data: D,
    filters: AuthFilter,
    _marker: PhantomData<P>,
}

impl<P, D> GuardedData<P, D> {
    pub fn filters(&self) -> &AuthFilter {
        &self.filters
    }

    async fn auth_bearer(
        auth: AuthController,
        token: String,
        index: Option<String>,
        data: Option<D>,
    ) -> Result<Self, ResponseError>
    where
        P: Policy + 'static,
    {
        let missing_master_key = auth.get_master_key().is_none();

        match Self::authenticate(auth, token, index).await? {
            Some(filters) => match data {
                Some(data) => Ok(Self { data, filters, _marker: PhantomData }),
                None => Err(AuthenticationError::IrretrievableState.into()),
            },
            None if missing_master_key => Err(AuthenticationError::MissingMasterKey.into()),
            None => Err(AuthenticationError::InvalidToken.into()),
        }
    }

    async fn auth_token(auth: AuthController, data: Option<D>) -> Result<Self, ResponseError>
    where
        P: Policy + 'static,
    {
        let missing_master_key = auth.get_master_key().is_none();

        match Self::authenticate(auth, String::new(), None).await? {
            Some(filters) => match data {
                Some(data) => Ok(Self { data, filters, _marker: PhantomData }),
                None => Err(AuthenticationError::IrretrievableState.into()),
            },
            None if missing_master_key => Err(AuthenticationError::MissingMasterKey.into()),
            None => Err(AuthenticationError::MissingAuthorizationHeader.into()),
        }
    }

    async fn authenticate(
        auth: AuthController,
        token: String,
        index: Option<String>,
    ) -> Result<Option<AuthFilter>, ResponseError>
    where
        P: Policy + 'static,
    {
        tokio::task::spawn_blocking(move || P::authenticate(auth, token.as_ref(), index.as_deref()))
            .await
            .map_err(|e| ResponseError::from_msg(e.to_string(), Code::Internal))
    }
}

impl<P, D> Deref for GuardedData<P, D> {
    type Target = D;

    fn deref(&self) -> &Self::Target {
        &self.data
    }
}

impl<P: Policy + 'static, D: 'static + Clone> FromRequest for GuardedData<P, D> {
    type Error = ResponseError;

    type Future = Pin<Box<dyn Future<Output = Result<Self, Self::Error>>>>;

    fn from_request(
        req: &actix_web::HttpRequest,
        _payload: &mut actix_web::dev::Payload,
    ) -> Self::Future {
        match req.app_data::<AuthController>().cloned() {
            Some(auth) => match req
                .headers()
                .get("Authorization")
                .map(|type_token| type_token.to_str().unwrap_or_default().splitn(2, ' '))
            {
                Some(mut type_token) => match type_token.next() {
                    Some("Bearer") => {
                        // TODO: find a less hardcoded way?
                        let index = req.match_info().get("index_uid");
                        match type_token.next() {
                            Some(token) => Box::pin(Self::auth_bearer(
                                auth,
                                token.to_string(),
                                index.map(String::from),
                                req.app_data::<D>().cloned(),
                            )),
                            None => Box::pin(err(AuthenticationError::InvalidToken.into())),
                        }
                    }
                    _otherwise => {
                        Box::pin(err(AuthenticationError::MissingAuthorizationHeader.into()))
                    }
                },
                None => Box::pin(Self::auth_token(auth, req.app_data::<D>().cloned())),
            },
            None => Box::pin(err(AuthenticationError::IrretrievableState.into())),
        }
    }
}

pub trait Policy {
    fn authenticate(auth: AuthController, token: &str, index: Option<&str>) -> Option<AuthFilter>;
}

pub mod policies {
    use jsonwebtoken::{decode, Algorithm, DecodingKey, Validation};
    use meilisearch_auth::{AuthController, AuthFilter, SearchRules};
    // reexport actions in policies in order to be used in routes configuration.
    pub use meilisearch_types::keys::{actions, Action};
    use serde::{Deserialize, Serialize};
    use time::OffsetDateTime;
    use uuid::Uuid;

    use crate::extractors::authentication::Policy;

    fn tenant_token_validation() -> Validation {
        let mut validation = Validation::default();
        validation.validate_exp = false;
        validation.required_spec_claims.remove("exp");
        validation.algorithms = vec![Algorithm::HS256, Algorithm::HS384, Algorithm::HS512];
        validation
    }

    /// Extracts the key id used to sign the payload, without performing any validation.
    fn extract_key_id(token: &str) -> Option<Uuid> {
        let mut validation = tenant_token_validation();
        validation.insecure_disable_signature_validation();
        let dummy_key = DecodingKey::from_secret(b"secret");
        let token_data = decode::<Claims>(token, &dummy_key, &validation).ok()?;

        // get token fields without validating it.
        let Claims { api_key_uid, .. } = token_data.claims;
        Some(api_key_uid)
    }

    fn is_keys_action(action: u8) -> bool {
        use actions::*;
        matches!(action, KEYS_GET | KEYS_CREATE | KEYS_UPDATE | KEYS_DELETE)
    }

    pub struct ActionPolicy<const A: u8>;

    impl<const A: u8> Policy for ActionPolicy<A> {
        fn authenticate(
            auth: AuthController,
            token: &str,
            index: Option<&str>,
        ) -> Option<AuthFilter> {
            // authenticate if token is the master key.
            // master key can only have access to keys routes.
            // if master key is None only keys routes are inaccessible.
            if auth.get_master_key().map_or_else(|| !is_keys_action(A), |mk| mk == token) {
                return Some(AuthFilter::default());
            }

            // Tenant token
            if let Some(filters) = ActionPolicy::<A>::authenticate_tenant_token(&auth, token, index)
            {
                return Some(filters);
            } else if let Some(action) = Action::from_repr(A) {
                // API key
                if let Ok(Some(uid)) = auth.get_optional_uid_from_encoded_key(token.as_bytes()) {
                    if let Ok(true) = auth.is_key_authorized(uid, action, index) {
                        return auth.get_key_filters(uid, None).ok();
                    }
                }
            }

            None
        }
    }

    impl<const A: u8> ActionPolicy<A> {
        fn authenticate_tenant_token(
            auth: &AuthController,
            token: &str,
            index: Option<&str>,
        ) -> Option<AuthFilter> {
            // Only search action can be accessed by a tenant token.
            if A != actions::SEARCH {
                return None;
            }

            let uid = extract_key_id(token)?;
            // check if parent key is authorized to do the action.
            if auth.is_key_authorized(uid, Action::Search, index).ok()? {
                // Check if tenant token is valid.
                let key = auth.generate_key(uid)?;
                let data = decode::<Claims>(
                    token,
                    &DecodingKey::from_secret(key.as_bytes()),
                    &tenant_token_validation(),
                )
                .ok()?;

                // Check index access if an index restriction is provided.
                if let Some(index) = index {
                    if !data.claims.search_rules.is_index_authorized(index) {
                        return None;
                    }
                }

                // Check if token is expired.
                if let Some(exp) = data.claims.exp {
                    if OffsetDateTime::now_utc().unix_timestamp() > exp {
                        return None;
                    }
                }

                return auth.get_key_filters(uid, Some(data.claims.search_rules)).ok();
            }

            None
        }
    }

    #[derive(Debug, Serialize, Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct Claims {
        search_rules: SearchRules,
        exp: Option<i64>,
        api_key_uid: Uuid,
    }
}
