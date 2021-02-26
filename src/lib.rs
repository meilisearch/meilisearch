#![allow(clippy::or_fun_call)]
#![allow(unused_must_use)]
#![allow(unused_variables)]
#![allow(dead_code)]

pub mod data;
pub mod error;
pub mod helpers;
pub mod option;
pub mod routes;
mod index_controller;

pub use option::Opt;
pub use self::data::Data;
