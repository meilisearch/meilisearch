mod autobatcher;
mod batch;
pub mod error;
mod index_mapper;
mod index_scheduler;
pub mod task;
mod utils;

pub type Result<T> = std::result::Result<T, Error>;
pub type TaskId = u32;

pub use crate::index_scheduler::IndexScheduler;
pub use error::Error;
/// from the exterior you don't need to know there is multiple type of `Kind`
pub use task::KindWithContent as TaskKind;
/// from the exterior you don't need to know there is multiple type of `Task`
pub use task::TaskView as Task;

#[cfg(test)]
mod tests {
    use milli::update::IndexerConfig;
    use tempfile::TempDir;

    use crate::IndexScheduler;

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

    pub fn index_scheduler() -> (IndexScheduler, TempDir) {
        let dir = TempDir::new().unwrap();

        (
            IndexScheduler::new(
                dir.path().join("db_path"),
                dir.path().join("file_store"),
                dir.path().join("indexes"),
                1024 * 1024,
                IndexerConfig::default(),
            )
            .unwrap(),
            dir,
        )
    }

    #[test]
    fn simple_new() {
        index_scheduler();
    }
}
