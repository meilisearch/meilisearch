mod autobatcher;
mod batch;
pub mod error;
mod index_mapper;
mod index_scheduler;
pub mod task;
mod utils;

pub type Result<T> = std::result::Result<T, Error>;
pub type TaskId = u32;

pub use crate::index_scheduler::{IndexScheduler, Query};
pub use error::Error;
pub use task::{Kind, KindWithContent, Status, TaskView};

#[cfg(test)]
mod tests {
    #[macro_export]
    macro_rules! assert_smol_debug_snapshot {
        ($value:expr, @$snapshot:literal) => {{
            let value = format!("{:?}", $value);
            insta::assert_snapshot!(value, stringify!($value), @$snapshot);
        }};
        ($name:expr, $value:expr) => {{
            let value = format!("{:?}", $value);
            insta::assert_snapshot!(Some($name), value, stringify!($value));
        }};
        ($value:expr) => {{
            let value = format!("{:?}", $value);
            insta::assert_snapshot!($crate::_macro_support::AutoName, value, stringify!($value));
        }};
    }

    #[test]
    fn simple_new() {
        crate::IndexScheduler::test();
    }
}
