use std::cmp::Ordering;
use std::collections::{hash_map::Entry, BinaryHeap, HashMap, VecDeque};
use std::ops::{Deref, DerefMut};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use atomic_refcell::AtomicRefCell;
use milli::update::IndexDocumentsMethod;
use time::OffsetDateTime;
use tokio::sync::{watch, RwLock};

use crate::options::SchedulerConfig;
use crate::update_file_store::UpdateFileStore;

use super::batch::Batch;
use super::error::Result;
use super::task::{Job, Task, TaskContent, TaskEvent, TaskId};
use super::update_loop::UpdateLoop;
use super::{TaskFilter, TaskPerformer, TaskStore};

#[derive(Eq, Debug, Clone, Copy)]
enum TaskType {
    DocumentAddition {
        number: usize,
    },
    DocumentUpdate {
        number: usize,
    },
    /// Any other kind of task, including Dumps
    Other,
}

/// Two tasks are equal if they have the same type.
impl PartialEq for TaskType {
    fn eq(&self, other: &Self) -> bool {
        matches!(
            (self, other),
            (Self::DocumentAddition { .. }, Self::DocumentAddition { .. })
                | (Self::DocumentUpdate { .. }, Self::DocumentUpdate { .. })
        )
    }
}

#[derive(Eq, Debug, Clone, Copy)]
struct PendingTask {
    kind: TaskType,
    id: TaskId,
}

impl PartialEq for PendingTask {
    fn eq(&self, other: &Self) -> bool {
        self.id.eq(&other.id)
    }
}

impl PartialOrd for PendingTask {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for PendingTask {
    fn cmp(&self, other: &Self) -> Ordering {
        self.id.cmp(&other.id).reverse()
    }
}

#[derive(Debug)]
struct TaskList {
    id: TaskListIdentifier,
    tasks: BinaryHeap<PendingTask>,
}

impl Deref for TaskList {
    type Target = BinaryHeap<PendingTask>;

    fn deref(&self) -> &Self::Target {
        &self.tasks
    }
}

impl DerefMut for TaskList {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.tasks
    }
}

impl TaskList {
    fn new(id: TaskListIdentifier) -> Self {
        Self {
            id,
            tasks: Default::default(),
        }
    }
}

impl PartialEq for TaskList {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Eq for TaskList {}

impl Ord for TaskList {
    fn cmp(&self, other: &Self) -> Ordering {
        match (&self.id, &other.id) {
            (TaskListIdentifier::Index(_), TaskListIdentifier::Index(_)) => {
                match (self.peek(), other.peek()) {
                    (None, None) => Ordering::Equal,
                    (None, Some(_)) => Ordering::Less,
                    (Some(_), None) => Ordering::Greater,
                    (Some(lhs), Some(rhs)) => lhs.cmp(rhs),
                }
            }
            (TaskListIdentifier::Index(_), TaskListIdentifier::Dump) => Ordering::Greater,
            (TaskListIdentifier::Dump, TaskListIdentifier::Index(_)) => Ordering::Less,
            (TaskListIdentifier::Dump, TaskListIdentifier::Dump) => {
                unreachable!("There should be only one Dump task list")
            }
        }
    }
}

impl PartialOrd for TaskList {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(PartialEq, Eq, Hash, Debug, Clone)]
enum TaskListIdentifier {
    Index(String),
    Dump,
}

#[derive(Default)]
struct TaskQueue {
    /// Maps index uids to their TaskList, for quick access
    index_tasks: HashMap<TaskListIdentifier, Arc<AtomicRefCell<TaskList>>>,
    /// A queue that orders TaskList by the priority of their fist update
    queue: BinaryHeap<Arc<AtomicRefCell<TaskList>>>,
}

impl TaskQueue {
    fn insert(&mut self, task: Task) {
        let id = task.id;
        let uid = match task.index_uid {
            Some(uid) => TaskListIdentifier::Index(uid.into_inner()),
            None if matches!(task.content, TaskContent::Dump { .. }) => TaskListIdentifier::Dump,
            None => unreachable!("invalid task state"),
        };
        let kind = match task.content {
            TaskContent::DocumentAddition {
                documents_count,
                merge_strategy: IndexDocumentsMethod::ReplaceDocuments,
                ..
            } => TaskType::DocumentAddition {
                number: documents_count,
            },
            TaskContent::DocumentAddition {
                documents_count,
                merge_strategy: IndexDocumentsMethod::UpdateDocuments,
                ..
            } => TaskType::DocumentUpdate {
                number: documents_count,
            },
            _ => TaskType::Other,
        };
        let task = PendingTask { kind, id };

        match self.index_tasks.entry(uid) {
            Entry::Occupied(entry) => {
                // A task list already exists for this index, all we have to to is to push the new
                // update to the end of the list. This won't change the order since ids are
                // monotically increasing.
                let mut list = entry.get().borrow_mut();

                // We only need the first element to be lower than the one we want to
                // insert to preserve the order in the queue.
                assert!(list.peek().map(|old_id| id >= old_id.id).unwrap_or(true));

                list.push(task);
            }
            Entry::Vacant(entry) => {
                let mut task_list = TaskList::new(entry.key().clone());
                task_list.push(task);
                let task_list = Arc::new(AtomicRefCell::new(task_list));
                entry.insert(task_list.clone());
                self.queue.push(task_list);
            }
        }
    }

    /// Passes a context with a view to the task list of the next index to schedule. It is
    /// guaranteed that the first id from task list will be the lowest pending task id.
    fn head_mut<R>(&mut self, mut f: impl FnMut(&mut TaskList) -> R) -> Option<R> {
        let head = self.queue.pop()?;
        let result = {
            let mut ref_head = head.borrow_mut();
            f(&mut *ref_head)
        };
        if !head.borrow().tasks.is_empty() {
            // After being mutated, the head is reinserted to the correct position.
            self.queue.push(head);
        } else {
            self.index_tasks.remove(&head.borrow().id);
        }

        Some(result)
    }

    pub fn is_empty(&self) -> bool {
        self.queue.is_empty() && self.index_tasks.is_empty()
    }
}

pub struct Scheduler {
    jobs: VecDeque<Job>,
    tasks: TaskQueue,

    store: TaskStore,
    processing: Vec<TaskId>,
    next_fetched_task_id: TaskId,
    config: SchedulerConfig,
    /// Notifies the update loop that a new task was received
    notifier: watch::Sender<()>,
}

impl Scheduler {
    pub fn new<P>(
        store: TaskStore,
        performer: Arc<P>,
        mut config: SchedulerConfig,
    ) -> Result<Arc<RwLock<Self>>>
    where
        P: TaskPerformer,
    {
        let (notifier, rcv) = watch::channel(());

        let debounce_time = config.debounce_duration_sec;

        // Disable autobatching
        if !config.enable_auto_batching {
            config.max_batch_size = Some(1);
        }

        let this = Self {
            jobs: VecDeque::new(),
            tasks: TaskQueue::default(),

            store,
            processing: Vec::new(),
            next_fetched_task_id: 0,
            config,
            notifier,
        };

        // Notify update loop to start processing pending updates immediately after startup.
        this.notify();

        let this = Arc::new(RwLock::new(this));

        let update_loop = UpdateLoop::new(
            this.clone(),
            performer,
            debounce_time.filter(|&v| v > 0).map(Duration::from_secs),
            rcv,
        );

        tokio::task::spawn_local(update_loop.run());

        Ok(this)
    }

    pub async fn dump(&self, path: &Path, file_store: UpdateFileStore) -> Result<()> {
        self.store.dump(path, file_store).await
    }

    fn register_task(&mut self, task: Task) {
        assert!(!task.is_finished());
        self.tasks.insert(task);
    }

    /// Clears the processing list, this method should be called when the processing of a batch is finished.
    pub fn finish(&mut self) {
        self.processing.clear();
    }

    pub fn notify(&self) {
        let _ = self.notifier.send(());
    }

    fn notify_if_not_empty(&self) {
        if !self.jobs.is_empty() || !self.tasks.is_empty() {
            self.notify();
        }
    }

    pub async fn update_tasks(&self, tasks: Vec<Task>) -> Result<Vec<Task>> {
        self.store.update_tasks(tasks).await
    }

    pub async fn get_task(&self, id: TaskId, filter: Option<TaskFilter>) -> Result<Task> {
        self.store.get_task(id, filter).await
    }

    pub async fn list_tasks(
        &self,
        offset: Option<TaskId>,
        filter: Option<TaskFilter>,
        limit: Option<usize>,
    ) -> Result<Vec<Task>> {
        self.store.list_tasks(offset, filter, limit).await
    }

    pub async fn get_processing_tasks(&self) -> Result<Vec<Task>> {
        let mut tasks = Vec::new();

        for id in self.processing.iter() {
            let task = self.store.get_task(*id, None).await?;
            tasks.push(task);
        }

        Ok(tasks)
    }

    pub async fn schedule_job(&mut self, job: Job) {
        self.jobs.push_back(job);
        self.notify();
    }

    async fn fetch_pending_tasks(&mut self) -> Result<()> {
        // We must NEVER re-enqueue an already processed task! It's content uuid would point to an unexisting file.
        //
        // TODO(marin): This may create some latency when the first batch lazy loads the pending updates.
        let mut filter = TaskFilter::default();
        filter.filter_fn(|task| !task.is_finished());

        self.store
            .list_tasks(Some(self.next_fetched_task_id), Some(filter), None)
            .await?
            .into_iter()
            // The tasks arrive in reverse order, and we need to insert them in order.
            .rev()
            .for_each(|t| {
                self.next_fetched_task_id = t.id + 1;
                self.register_task(t);
            });

        Ok(())
    }

    /// Prepare the next batch, and set `processing` to the ids in that batch.
    pub async fn prepare(&mut self) -> Result<Pending> {
        // If there is a job to process, do it first.
        if let Some(job) = self.jobs.pop_front() {
            // There is more work to do, notify the update loop
            self.notify_if_not_empty();
            return Ok(Pending::Job(job));
        }
        // Try to fill the queue with pending tasks.
        self.fetch_pending_tasks().await?;

        make_batch(&mut self.tasks, &mut self.processing, &self.config);

        log::debug!("prepared batch with {} tasks", self.processing.len());

        if !self.processing.is_empty() {
            let ids = std::mem::take(&mut self.processing);

            let (ids, mut tasks) = self.store.get_pending_tasks(ids).await?;

            // The batch id is the id of the first update it contains
            let id = match tasks.first() {
                Some(Task { id, .. }) => *id,
                _ => panic!("invalid batch"),
            };

            tasks.iter_mut().for_each(|t| {
                t.events.push(TaskEvent::Batched {
                    batch_id: id,
                    timestamp: OffsetDateTime::now_utc(),
                })
            });

            self.processing = ids;

            let batch = Batch {
                id,
                created_at: OffsetDateTime::now_utc(),
                tasks,
            };

            // There is more work to do, notify the update loop
            self.notify_if_not_empty();

            Ok(Pending::Batch(batch))
        } else {
            Ok(Pending::Nothing)
        }
    }
}

#[derive(Debug)]
pub enum Pending {
    Batch(Batch),
    Job(Job),
    Nothing,
}

fn make_batch(tasks: &mut TaskQueue, processing: &mut Vec<TaskId>, config: &SchedulerConfig) {
    processing.clear();

    let mut doc_count = 0;
    tasks.head_mut(|list| match list.peek().copied() {
        Some(PendingTask {
            kind: TaskType::Other,
            id,
        }) => {
            processing.push(id);
            list.pop();
        }
        Some(PendingTask { kind, .. }) => loop {
            match list.peek() {
                Some(pending) if pending.kind == kind => {
                    // We always need to process at least one task for the scheduler to make progress.
                    if processing.len() >= config.max_batch_size.unwrap_or(usize::MAX).max(1) {
                        break;
                    }
                    let pending = list.pop().unwrap();
                    processing.push(pending.id);

                    // We add the number of documents to the count if we are scheduling document additions and
                    // stop adding if we already have enough.
                    //
                    // We check that bound only after adding the current task to the batch, so that a batch contains at least one task.
                    match pending.kind {
                        TaskType::DocumentUpdate { number }
                        | TaskType::DocumentAddition { number } => {
                            doc_count += number;

                            if doc_count >= config.max_documents_per_batch.unwrap_or(usize::MAX) {
                                break;
                            }
                        }
                        _ => (),
                    }
                }
                _ => break,
            }
        },
        None => (),
    });
}

#[cfg(test)]
mod test {
    use milli::update::IndexDocumentsMethod;
    use uuid::Uuid;

    use crate::{index_resolver::IndexUid, tasks::task::TaskContent};

    use super::*;

    fn gen_task(id: TaskId, index_uid: &str, content: TaskContent) -> Task {
        Task {
            id,
            index_uid: Some(IndexUid::new_unchecked(index_uid)),
            content,
            events: vec![],
        }
    }

    #[test]
    fn register_updates_multiples_indexes() {
        let mut queue = TaskQueue::default();
        queue.insert(gen_task(0, "test1", TaskContent::IndexDeletion));
        queue.insert(gen_task(1, "test2", TaskContent::IndexDeletion));
        queue.insert(gen_task(2, "test2", TaskContent::IndexDeletion));
        queue.insert(gen_task(3, "test2", TaskContent::IndexDeletion));
        queue.insert(gen_task(4, "test1", TaskContent::IndexDeletion));
        queue.insert(gen_task(5, "test1", TaskContent::IndexDeletion));
        queue.insert(gen_task(6, "test2", TaskContent::IndexDeletion));

        let test1_tasks = queue
            .head_mut(|tasks| tasks.drain().map(|t| t.id).collect::<Vec<_>>())
            .unwrap();

        assert_eq!(test1_tasks, &[0, 4, 5]);

        let test2_tasks = queue
            .head_mut(|tasks| tasks.drain().map(|t| t.id).collect::<Vec<_>>())
            .unwrap();

        assert_eq!(test2_tasks, &[1, 2, 3, 6]);

        assert!(queue.index_tasks.is_empty());
        assert!(queue.queue.is_empty());
    }

    #[test]
    fn test_make_batch() {
        let mut queue = TaskQueue::default();
        let content = TaskContent::DocumentAddition {
            content_uuid: Uuid::new_v4(),
            merge_strategy: IndexDocumentsMethod::ReplaceDocuments,
            primary_key: Some("test".to_string()),
            documents_count: 0,
            allow_index_creation: true,
        };
        queue.insert(gen_task(0, "test1", content.clone()));
        queue.insert(gen_task(1, "test2", content.clone()));
        queue.insert(gen_task(2, "test2", TaskContent::IndexDeletion));
        queue.insert(gen_task(3, "test2", content.clone()));
        queue.insert(gen_task(4, "test1", content.clone()));
        queue.insert(gen_task(5, "test1", TaskContent::IndexDeletion));
        queue.insert(gen_task(6, "test2", content.clone()));
        queue.insert(gen_task(7, "test1", content));

        let mut batch = Vec::new();

        let config = SchedulerConfig::default();
        make_batch(&mut queue, &mut batch, &config);
        assert_eq!(batch, &[0, 4]);

        batch.clear();
        make_batch(&mut queue, &mut batch, &config);
        assert_eq!(batch, &[1]);

        batch.clear();
        make_batch(&mut queue, &mut batch, &config);
        assert_eq!(batch, &[2]);

        batch.clear();
        make_batch(&mut queue, &mut batch, &config);
        assert_eq!(batch, &[3, 6]);

        batch.clear();
        make_batch(&mut queue, &mut batch, &config);
        assert_eq!(batch, &[5]);

        batch.clear();
        make_batch(&mut queue, &mut batch, &config);
        assert_eq!(batch, &[7]);

        assert!(queue.is_empty());
    }
}
