use std::{
    any::TypeId,
    borrow::Cow,
    sync::{
        atomic::{AtomicU32, Ordering},
        Arc, RwLock,
    },
};

use serde::Serialize;

pub trait Step: 'static + Send + Sync {
    fn name(&self) -> Cow<'static, str>;
    fn current(&self) -> u32;
    fn total(&self) -> u32;
}

#[derive(Clone, Default)]
pub struct Progress {
    steps: Arc<RwLock<Vec<(TypeId, Box<dyn Step>)>>>,
}

impl Progress {
    pub fn update_progress<P: Step>(&self, sub_progress: P) {
        let mut steps = self.steps.write().unwrap();
        let step_type = TypeId::of::<P>();
        if let Some(idx) = steps.iter().position(|(id, _)| *id == step_type) {
            steps.truncate(idx);
        }
        steps.push((step_type, Box::new(sub_progress)));
    }

    // TODO: This code should be in meilisearch_types but cannot because milli can't depend on meilisearch_types
    pub fn as_progress_view(&self) -> ProgressView {
        let steps = self.steps.read().unwrap();

        let mut percentage = 0.0;
        let mut prev_factors = 1.0;

        let mut step_view = Vec::new();
        for (_, step) in steps.iter() {
            prev_factors *= step.total() as f32;
            percentage += step.current() as f32 / prev_factors;

            step_view.push(ProgressStepView {
                name: step.name(),
                finished: step.current(),
                total: step.total(),
            });
        }

        ProgressView { steps: step_view, percentage: percentage * 100.0 }
    }
}

/// This trait lets you use the AtomicSubStep defined right below.
/// The name must be a const that never changed but that can't be enforced by the type system because it make the trait non object-safe.
/// By forcing the Default trait + the &'static str we make it harder to miss-use the trait.
pub trait NamedStep: 'static + Send + Sync + Default {
    fn name(&self) -> &'static str;
}

/// Structure to quickly define steps that need very quick, lockless updating of their current step.
/// You can use this struct if:
/// - The name of the step doesn't change
/// - The total number of steps doesn't change
pub struct AtomicSubStep<Name: NamedStep> {
    name: Name,
    current: Arc<AtomicU32>,
    total: u32,
}

impl<Name: NamedStep> AtomicSubStep<Name> {
    pub fn new(total: u32) -> (Arc<AtomicU32>, Self) {
        let current = Arc::new(AtomicU32::new(0));
        (current.clone(), Self { current, total, name: Name::default() })
    }
}

impl<Name: NamedStep> Step for AtomicSubStep<Name> {
    fn name(&self) -> Cow<'static, str> {
        self.name.name().into()
    }

    fn current(&self) -> u32 {
        self.current.load(Ordering::Relaxed)
    }

    fn total(&self) -> u32 {
        self.total
    }
}

#[derive(Default)]
pub struct Document {}

impl NamedStep for Document {
    fn name(&self) -> &'static str {
        "document"
    }
}

pub type AtomicDocumentStep = AtomicSubStep<Document>;

#[derive(Debug, Serialize, Clone)]
pub struct ProgressView {
    steps: Vec<ProgressStepView>,
    percentage: f32,
}

#[derive(Debug, Serialize, Clone)]
pub struct ProgressStepView {
    name: Cow<'static, str>,
    finished: u32,
    total: u32,
}
