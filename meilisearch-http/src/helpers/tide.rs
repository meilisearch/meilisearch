use crate::error::{ResponseError, SResult};
use crate::Data;
use meilisearch_core::Index;
use tide::Request;

pub enum ACL {
    Admin,
    Private,
    Public
}

pub trait RequestExt {
    fn is_allowed(&self, acl: ACL) -> SResult<()>;
    fn url_param(&self, name: &str) -> SResult<String>;
    fn index(&self) -> SResult<Index>;
    fn identifier(&self) -> SResult<String>;
}

impl RequestExt for Request<Data> {
    fn is_allowed(&self, acl: ACL) -> SResult<()> {
        let user_api_key = self.header("X-Meili-API-Key");

        match acl {
            ACL::Admin => {
                if user_api_key == self.state().api_keys.master.as_deref() {
                    return Ok(())
                }
            },
            ACL::Private => {
                if user_api_key == self.state().api_keys.master.as_deref() {
                    return Ok(())
                }
                if user_api_key == self.state().api_keys.private.as_deref() {
                    return Ok(())
                }
            },
            ACL::Public => {
                if user_api_key == self.state().api_keys.master.as_deref() {
                    return Ok(())
                }
                if user_api_key == self.state().api_keys.private.as_deref() {
                    return Ok(())
                }
                if user_api_key == self.state().api_keys.public.as_deref() {
                    return Ok(())
                }
            }
        }

        Err(ResponseError::InvalidToken(user_api_key.unwrap_or("Need a token").to_owned()))
    }

    fn url_param(&self, name: &str) -> SResult<String> {
        let param = self
            .param::<String>(name)
            .map_err(|_| ResponseError::bad_parameter("identifier", name))?;
        Ok(param)
    }

    fn index(&self) -> SResult<Index> {
        let index_uid = self.url_param("index")?;
        let index = self
            .state()
            .db
            .open_index(&index_uid)
            .ok_or(ResponseError::index_not_found(index_uid))?;
        Ok(index)
    }

    fn identifier(&self) -> SResult<String> {
        let name = self
            .param::<String>("identifier")
            .map_err(|_| ResponseError::bad_parameter("identifier", "identifier"))?;

        Ok(name)
    }
}
