use crate::error::{ResponseError, SResult};
use crate::models::token::*;
use crate::Data;
use chrono::Utc;
use heed::types::{SerdeBincode, Str};
use meilidb_core::Index;
use serde_json::Value;
use tide::Context;

pub trait ContextExt {
    fn is_allowed(&self, acl: ACL) -> SResult<()>;
    fn header(&self, name: &str) -> Result<String, ResponseError>;
    fn url_param(&self, name: &str) -> Result<String, ResponseError>;
    fn index(&self) -> Result<Index, ResponseError>;
    fn identifier(&self) -> Result<String, ResponseError>;
}

impl ContextExt for Context<Data> {
    fn is_allowed(&self, acl: ACL) -> SResult<()> {
        let api_key = match &self.state().api_key {
            Some(api_key) => api_key,
            None => return Ok(()),
        };

        let user_api_key = self.header("X-Meili-API-Key")?;
        if user_api_key == *api_key {
            return Ok(());
        }
        let request_index: Option<String> = None; //self.param::<String>("index").ok();

        let db = &self.state().db;
        let env = &db.env;
        let reader = env.read_txn().map_err(ResponseError::internal)?;

        let token_key = format!("{}{}", TOKEN_PREFIX_KEY, user_api_key);

        let token_config = db
            .common_store()
            .get::<Str, SerdeBincode<Token>>(&reader, &token_key)
            .map_err(ResponseError::internal)?
            .ok_or(ResponseError::invalid_token(format!(
                "Api key does not exist: {}",
                user_api_key
            )))?;

        if token_config.revoked {
            return Err(ResponseError::invalid_token("token revoked"));
        }

        if let Some(index) = request_index {
            if !token_config
                .indexes
                .iter()
                .any(|r| match_wildcard(&r, &index))
            {
                return Err(ResponseError::invalid_token(
                    "token is not allowed to access to this index",
                ));
            }
        }

        if token_config.expires_at < Utc::now() {
            return Err(ResponseError::invalid_token("token expired"));
        }

        if token_config.acl.contains(&ACL::All) {
            return Ok(());
        }

        if !token_config.acl.contains(&acl) {
            return Err(ResponseError::invalid_token("token do not have this ACL"));
        }

        Ok(())
    }

    fn header(&self, name: &str) -> Result<String, ResponseError> {
        let header = self
            .headers()
            .get(name)
            .ok_or(ResponseError::missing_header(name))?
            .to_str()
            .map_err(|_| ResponseError::missing_header("X-Meili-API-Key"))?
            .to_string();
        Ok(header)
    }

    fn url_param(&self, name: &str) -> Result<String, ResponseError> {
        let param = self
            .param::<String>(name)
            .map_err(|e| ResponseError::bad_parameter(name, e))?;
        Ok(param)
    }

    fn index(&self) -> Result<Index, ResponseError> {
        let index_uid = self.url_param("index")?;
        let index = self
            .state()
            .db
            .open_index(&index_uid)
            .ok_or(ResponseError::index_not_found(index_uid))?;
        Ok(index)
    }

    fn identifier(&self) -> Result<String, ResponseError> {
        let name = self
            .param::<Value>("identifier")
            .as_ref()
            .map(meilidb_core::serde::value_to_string)
            .map_err(|e| ResponseError::bad_parameter("identifier", e))?
            .ok_or(ResponseError::bad_parameter(
                "identifier",
                "missing parameter",
            ))?;

        Ok(name)
    }
}
