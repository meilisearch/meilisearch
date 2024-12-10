use crate::utils::ProcessingBatch;
use enum_iterator::Sequence;
use meilisearch_types::milli::progress::{AtomicSubStep, NamedStep, Progress, ProgressView, Step};
use roaring::RoaringBitmap;
use std::{borrow::Cow, sync::Arc};

#[derive(Clone)]
pub struct ProcessingTasks {
    pub batch: Option<Arc<ProcessingBatch>>,
    /// The list of tasks ids that are currently running.
    pub processing: Arc<RoaringBitmap>,
    /// The progress on processing tasks
    pub progress: Option<Progress>,
}

impl ProcessingTasks {
    /// Creates an empty `ProcessingAt` struct.
    pub fn new() -> ProcessingTasks {
        ProcessingTasks { batch: None, processing: Arc::new(RoaringBitmap::new()), progress: None }
    }

    pub fn get_progress_view(&self) -> Option<ProgressView> {
        Some(self.progress.as_ref()?.as_progress_view())
    }

    /// Stores the currently processing tasks, and the date time at which it started.
    pub fn start_processing(
        &mut self,
        processing_batch: ProcessingBatch,
        processing: RoaringBitmap,
    ) -> Progress {
        self.batch = Some(Arc::new(processing_batch));
        self.processing = Arc::new(processing);
        let progress = Progress::default();
        progress.update_progress(BatchProgress::ProcessingTasks);
        self.progress = Some(progress.clone());

        progress
    }

    /// Set the processing tasks to an empty list
    pub fn stop_processing(&mut self) -> Self {
        self.progress = None;

        Self {
            batch: std::mem::take(&mut self.batch),
            processing: std::mem::take(&mut self.processing),
            progress: None,
        }
    }

    /// Returns `true` if there, at least, is one task that is currently processing that we must stop.
    pub fn must_cancel_processing_tasks(&self, canceled_tasks: &RoaringBitmap) -> bool {
        !self.processing.is_disjoint(canceled_tasks)
    }
}

macro_rules! make_enum_progress {
    (enum $name:ident: $(- $variant:ident)+ ) => {
        #[repr(u8)]
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Sequence)]
        #[allow(clippy::enum_variant_names)]
        pub enum $name {
            $($variant),+
        }

        impl Step for $name {
            fn name(&self) -> Cow<'static, str> {
                use convert_case::Casing;

                match self {
                    $(
                        $name::$variant => stringify!($variant).from_case(convert_case::Case::Camel).to_case(convert_case::Case::Lower).into()
                    ),+
                }
            }

            fn current(&self) -> u32 {
                *self as u32
            }

            fn total(&self) -> u32 {
                Self::CARDINALITY as u32
            }
        }
    };
}

macro_rules! make_atomic_progress {
    ($struct_name:ident alias $atomic_struct_name:ident => $step_name:literal) => {
        #[derive(Default, Debug, Clone, Copy)]
        pub struct $struct_name {}
        impl NamedStep for $struct_name {
            fn name(&self) -> &'static str {
                $step_name
            }
        }
        pub type $atomic_struct_name = AtomicSubStep<$struct_name>;
    };
}

make_enum_progress! {
    enum BatchProgress:
        - ProcessingTasks
        - WritingTasksToDisk
}

make_enum_progress! {
    enum TaskCancelationProgress:
        - RetrievingTasks
        - UpdatingTasks
}

make_enum_progress! {
    enum TaskDeletionProgress:
        - DeletingTasksDateTime
        - DeletingTasksMetadata
        - DeletingTasks
        - DeletingBatches
}

make_atomic_progress!(Task alias AtomicTaskStep => "task" );
make_atomic_progress!(Batch alias AtomicBatchStep => "batch" );

#[cfg(test)]
mod test {
    use std::sync::atomic::Ordering;

    use meili_snap::{json_string, snapshot};

    use super::*;

    #[test]
    fn one_level() {
        let mut processing = ProcessingTasks::new();
        processing.start_processing(ProcessingBatch::new(0), RoaringBitmap::new());
        snapshot!(json_string!(processing.get_progress_view()), @r#"
        {
          "steps": [
            {
              "name": "processing tasks",
              "finished": 0,
              "total": 2
            }
          ],
          "percentage": 0.0
        }
        "#);
        processing.progress.as_ref().unwrap().update_progress(BatchProgress::WritingTasksToDisk);
        snapshot!(json_string!(processing.get_progress_view()), @r#"
        {
          "steps": [
            {
              "name": "writing tasks to disk",
              "finished": 1,
              "total": 2
            }
          ],
          "percentage": 50.0
        }
        "#);
    }

    #[test]
    fn task_progress() {
        let mut processing = ProcessingTasks::new();
        processing.start_processing(ProcessingBatch::new(0), RoaringBitmap::new());
        let (atomic, tasks) = AtomicTaskStep::new(10);
        processing.progress.as_ref().unwrap().update_progress(tasks);
        snapshot!(json_string!(processing.get_progress_view()), @r#"
        {
          "steps": [
            {
              "name": "processing tasks",
              "finished": 0,
              "total": 2
            },
            {
              "name": "task",
              "finished": 0,
              "total": 10
            }
          ],
          "percentage": 0.0
        }
        "#);
        atomic.fetch_add(6, Ordering::Relaxed);
        snapshot!(json_string!(processing.get_progress_view()), @r#"
        {
          "steps": [
            {
              "name": "processing tasks",
              "finished": 0,
              "total": 2
            },
            {
              "name": "task",
              "finished": 6,
              "total": 10
            }
          ],
          "percentage": 30.000002
        }
        "#);
        processing.progress.as_ref().unwrap().update_progress(BatchProgress::WritingTasksToDisk);
        snapshot!(json_string!(processing.get_progress_view()), @r#"
        {
          "steps": [
            {
              "name": "writing tasks to disk",
              "finished": 1,
              "total": 2
            }
          ],
          "percentage": 50.0
        }
        "#);
        let (atomic, tasks) = AtomicTaskStep::new(5);
        processing.progress.as_ref().unwrap().update_progress(tasks);
        atomic.fetch_add(4, Ordering::Relaxed);
        snapshot!(json_string!(processing.get_progress_view()), @r#"
        {
          "steps": [
            {
              "name": "writing tasks to disk",
              "finished": 1,
              "total": 2
            },
            {
              "name": "task",
              "finished": 4,
              "total": 5
            }
          ],
          "percentage": 90.0
        }
        "#);
    }
}
