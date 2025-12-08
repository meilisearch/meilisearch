#[cfg(not(feature = "enterprise"))]
pub mod community_edition;
#[cfg(feature = "enterprise")]
pub mod enterprise_edition;
#[cfg(not(feature = "enterprise"))]
pub use community_edition::{proxy, task_network_and_check_leader_and_version};
#[cfg(feature = "enterprise")]
pub use enterprise_edition::{
    import_data_from_req, import_metadata_from_req, origin_from_req, proxy, send_request,
    task_network_and_check_leader_and_version,
};

mod body;
mod error;

pub use body::Body;
pub use error::{ProxyError, ReqwestErrorWithoutUrl};
