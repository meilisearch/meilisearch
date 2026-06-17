use std::collections::BTreeMap;

use base64::Engine as _;
use liquid_derive::{Display_filter, FilterReflection, ParseFilter};
use uuid::Uuid;

use crate::prompt::error::RenderPromptError;

#[derive(Clone, ParseFilter, FilterReflection)]
#[filter(
    name = "fetchurl",
    description = "GET the bytes from a remote URL, must be the final filter",
    parsed(FetchUrlFilter) // A struct that implements `Filter` (must implement `Default`)
)]
pub struct FetchUrl;

#[derive(Clone, Default)]
pub struct FetchUrlTickets {
    tickets_urls: BTreeMap<Uuid, String>,
}

impl FetchUrlTickets {
    pub fn resolve_url(
        self,
        client: &http_client::ureq::Agent,
        rendered: &str,
    ) -> Result<Option<String>, RenderPromptError> {
        let mut replaced: Option<String> = None;
        for (ticket, url) in self.tickets_urls {
            let mut response = match client.get(&url).call() {
                Ok(response) => response,
                Err(err) => {
                    return Err(RenderPromptError::fetching_url_failed(url, err.to_string()));
                }
            };

            let bytes = match response.body_mut().read_to_vec() {
                Ok(bytes) => bytes,
                Err(err) => {
                    return Err(RenderPromptError::fetching_url_failed(url, err.to_string()));
                }
            };
            let mediatype = infer::get(&bytes).expect("this is a demo").mime_type();
            let encoded = base64::prelude::BASE64_STANDARD.encode(&bytes);
            let data_url = format!("data:{mediatype};base64,{encoded}");
            let replace_from = match &replaced {
                Some(replaced) => replaced.as_str(),
                None => rendered,
            };
            replaced = Some(replace_from.replace(&to_ticket(ticket), &data_url));
        }
        Ok(replaced)
    }
}

#[derive(Debug, Default, Clone, Display_filter)]
#[name = "fetchurl"]
struct FetchUrlFilter;

impl liquid_core::Filter for FetchUrlFilter {
    fn evaluate(
        &self,
        input: &dyn liquid::ValueView,
        runtime: &dyn liquid_core::Runtime,
    ) -> liquid_core::Result<liquid_core::Value> {
        let url = input
            .as_scalar()
            .ok_or_else(|| {
                liquid_core::Error::with_msg(format!("expected string, got {}", input.type_name()))
            })?
            .into_string();

        let ticket = Uuid::now_v7();

        runtime
            .registers()
            .get_mut::<FetchUrlTickets>()
            .tickets_urls
            .insert(ticket, url.to_string());

        Ok(liquid_core::Value::Scalar(to_ticket(ticket).into()))
    }
}

fn to_ticket(ticket: Uuid) -> String {
    format!("_meili_fetchurlticket_{ticket}")
}
