use chrono::{Utc, DateTime};
use serde::{Serialize, Deserialize};

#[derive(Debug, PartialEq, Eq, Hash, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Pending<M> {
    pub update_id: u64,
    pub meta: M,
    pub enqueued_at: DateTime<Utc>,
}

impl<M> Pending<M> {
    pub fn new(meta: M, update_id: u64) -> Self {
        Self {
            enqueued_at: Utc::now(),
            meta,
            update_id,
        }
    }

    pub fn processing(self) -> Processing<M> {
        Processing {
            from: self,
            started_processing_at: Utc::now(),
        }
    }

    pub fn abort(self) -> Aborted<M> {
        Aborted {
            from: self,
            aborted_at: Utc::now(),
        }
    }

    pub fn meta(&self) -> &M {
        &self.meta
    }

    pub fn id(&self) -> u64 {
        self.update_id
    }
}

#[derive(Debug, PartialEq, Eq, Hash, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Processed<M, N> {
    pub success: N,
    pub processed_at: DateTime<Utc>,
    #[serde(flatten)]
    pub from: Processing<M>,
}

impl<M, N> Processed<M, N> {
    pub fn id(&self) -> u64 {
        self.from.id()
    }
}

#[derive(Debug, PartialEq, Eq, Hash, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Processing<M> {
    #[serde(flatten)]
    pub from: Pending<M>,
    pub started_processing_at: DateTime<Utc>,
}

impl<M> Processing<M> {
    pub fn id(&self) -> u64 {
        self.from.id()
    }

    pub fn meta(&self) -> &M {
        self.from.meta()
    }

    pub fn process<N>(self, meta: N) -> Processed<M, N> {
        Processed {
            success: meta,
            from: self,
            processed_at: Utc::now(),
        }
    }

    pub fn fail<E>(self, error: E) -> Failed<M, E> {
        Failed {
            from: self,
            error,
            failed_at: Utc::now(),
        }
    }
}

#[derive(Debug, PartialEq, Eq, Hash, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Aborted<M> {
    #[serde(flatten)]
    from: Pending<M>,
    aborted_at: DateTime<Utc>,
}

impl<M> Aborted<M> {
    pub fn id(&self) -> u64 {
        self.from.id()
    }
}

#[derive(Debug, PartialEq, Eq, Hash, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Failed<M, E> {
    #[serde(flatten)]
    from: Processing<M>,
    error: E,
    failed_at: DateTime<Utc>,
}

impl<M, E> Failed<M, E> {
    pub fn id(&self) -> u64 {
        self.from.id()
    }
}

#[derive(Debug, PartialEq, Eq, Hash, Serialize)]
#[serde(tag = "status", rename_all = "camelCase")]
pub enum UpdateStatus<M, N, E> {
    Processing(Processing<M>),
    Pending(Pending<M>),
    Processed(Processed<M, N>),
    Aborted(Aborted<M>),
    Failed(Failed<M, E>),
}

impl<M, N, E> UpdateStatus<M, N, E> {
    pub fn id(&self) -> u64 {
        match self {
            UpdateStatus::Processing(u) => u.id(),
            UpdateStatus::Pending(u) => u.id(),
            UpdateStatus::Processed(u) => u.id(),
            UpdateStatus::Aborted(u) => u.id(),
            UpdateStatus::Failed(u) => u.id(),
        }
    }

    pub fn processed(&self) -> Option<&Processed<M, N>> {
        match self {
            UpdateStatus::Processed(p) => Some(p),
            _ => None,
        }
    }
}

impl<M, N, E> From<Pending<M>> for UpdateStatus<M, N, E> {
    fn from(other: Pending<M>) -> Self {
        Self::Pending(other)
    }
}

impl<M, N, E> From<Aborted<M>> for UpdateStatus<M, N, E> {
    fn from(other: Aborted<M>) -> Self {
        Self::Aborted(other)
    }
}

impl<M, N, E> From<Processed<M, N>> for UpdateStatus<M, N, E> {
    fn from(other: Processed<M, N>) -> Self {
        Self::Processed(other)
    }
}

impl<M, N, E> From<Processing<M>> for UpdateStatus<M, N, E> {
    fn from(other: Processing<M>) -> Self {
        Self::Processing(other)
    }
}

impl<M, N, E> From<Failed<M, E>> for UpdateStatus<M, N, E> {
    fn from(other: Failed<M, E>) -> Self {
        Self::Failed(other)
    }
}
