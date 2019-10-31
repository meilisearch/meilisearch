#[macro_use]
extern crate envconfig_derive;

pub mod data;
pub mod error;
pub mod helpers;
pub mod models;
pub mod option;
pub mod routes;

use self::data::Data;
