use std::any::TypeId;
use std::borrow::Cow;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use enum_iterator::Sequence;
use indexmap::IndexMap;
use itertools::Itertools;
use serde::Serialize;
use utoipa::ToSchema;

pub trait Step: 'static + Send + Sync {
    fn name(&self) -> Cow<'static, str>;
    fn current(&self) -> u64;
    fn total(&self) -> u64;
}

#[derive(Clone, Default)]
pub struct Progress {
    steps: Arc<RwLock<InnerProgress>>,
}

#[derive(Default)]
pub struct EmbedderStats {
    pub errors: Arc<RwLock<(Option<String>, u32)>>,
    pub total_count: AtomicUsize,
}

impl std::fmt::Debug for EmbedderStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let guard = self.errors.read().unwrap_or_else(|p| p.into_inner());
        let (error, count) = (guard.0.clone(), guard.1);
        std::mem::drop(guard);
        f.debug_struct("EmbedderStats")
            .field("last_error", &error)
            .field("total_count", &self.total_count.load(Ordering::Relaxed))
            .field("error_count", &count)
            .finish()
    }
}

#[derive(Default)]
struct InnerProgress {
    /// The hierarchy of steps.
    steps: Vec<(TypeId, Box<dyn Step>, Instant)>,
    /// The durations associated to each steps.
    durations: Vec<(String, Duration)>,
}

impl Progress {
    pub fn update_progress<P: Step>(&self, sub_progress: P) {
        let mut inner = self.steps.write().unwrap();
        let InnerProgress { steps, durations } = &mut *inner;

        let now = Instant::now();
        let step_type = TypeId::of::<P>();
        if let Some(idx) = steps.iter().position(|(id, _, _)| *id == step_type) {
            push_steps_durations(steps, durations, now, idx);
            steps.truncate(idx);
        }

        steps.push((step_type, Box::new(sub_progress), now));
    }

    // TODO: This code should be in meilisearch_types but cannot because milli can't depend on meilisearch_types
    pub fn as_progress_view(&self) -> ProgressView {
        let inner = self.steps.read().unwrap();
        let InnerProgress { steps, .. } = &*inner;

        let mut percentage = 0.0;
        let mut prev_factors = 1.0;

        let mut step_view = Vec::with_capacity(steps.len());
        for (_, step, _) in steps.iter() {
            prev_factors *= step.total() as f32;
            percentage += step.current() as f32 / prev_factors;

            step_view.push(ProgressStepView {
                current_step: step.name(),
                finished: step.current(),
                total: step.total(),
            });
        }

        ProgressView { steps: step_view, percentage: percentage * 100.0 }
    }

    pub fn accumulated_durations(&self) -> IndexMap<String, String> {
        let mut inner = self.steps.write().unwrap();
        let InnerProgress { steps, durations, .. } = &mut *inner;

        let now = Instant::now();
        push_steps_durations(steps, durations, now, 0);

        durations.drain(..).map(|(name, duration)| (name, format!("{duration:.2?}"))).collect()
    }

    // TODO: ideally we should expose the progress in a way that let arroy use it directly
    pub(crate) fn update_progress_from_arroy(&self, progress: arroy::WriterProgress) {
        self.update_progress(progress.main);
        if let Some(sub) = progress.sub {
            self.update_progress(sub);
        }
    }
}

/// Generate the names associated with the durations and push them.
fn push_steps_durations(
    steps: &[(TypeId, Box<dyn Step>, Instant)],
    durations: &mut Vec<(String, Duration)>,
    now: Instant,
    idx: usize,
) {
    for (i, (_, _, started_at)) in steps.iter().skip(idx).enumerate().rev() {
        let full_name = steps.iter().take(idx + i + 1).map(|(_, s, _)| s.name()).join(" > ");
        durations.push((full_name, now.duration_since(*started_at)));
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
    unit_name: Name,
    current: Arc<AtomicU64>,
    total: u64,
}

impl<Name: NamedStep> AtomicSubStep<Name> {
    pub fn new(total: u64) -> (Arc<AtomicU64>, Self) {
        let current = Arc::new(AtomicU64::new(0));
        (current.clone(), Self { current, total, unit_name: Name::default() })
    }
}

impl<Name: NamedStep> Step for AtomicSubStep<Name> {
    fn name(&self) -> Cow<'static, str> {
        self.unit_name.name().into()
    }

    fn current(&self) -> u64 {
        self.current.load(Ordering::Relaxed)
    }

    fn total(&self) -> u64 {
        self.total
    }
}

#[doc(hidden)]
pub use convert_case as _private_convert_case;
#[doc(hidden)]
pub use enum_iterator as _private_enum_iterator;

#[macro_export]
macro_rules! make_enum_progress {
    ($visibility:vis enum $name:ident { $($variant:ident,)+ }) => {
        #[repr(u8)]
        #[derive(Debug, Clone, Copy, PartialEq, Eq, $crate::progress::_private_enum_iterator::Sequence)]
        #[allow(clippy::enum_variant_names)]
        $visibility enum $name {
            $($variant),+
        }

        impl $crate::progress::Step for $name {
            fn name(&self) -> std::borrow::Cow<'static, str> {
                use $crate::progress::_private_convert_case::Casing;

                match self {
                    $(
                        $name::$variant => stringify!($variant).from_case(convert_case::Case::Camel).to_case(convert_case::Case::Lower).into()
                    ),+
                }
            }

            fn current(&self) -> u64 {
                *self as u64
            }

            fn total(&self) -> u64 {
                use $crate::progress::_private_enum_iterator::Sequence;
                Self::CARDINALITY as u64
            }
        }
    };
}

#[macro_export]
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

make_atomic_progress!(Document alias AtomicDocumentStep => "document");
make_atomic_progress!(Payload alias AtomicPayloadStep => "payload");

make_enum_progress! {
    pub enum MergingWordCache {
        WordDocids,
        WordFieldIdDocids,
        ExactWordDocids,
        WordPositionDocids,
        FieldIdWordCountDocids,
    }
}

#[derive(Debug, Serialize, Clone, ToSchema)]
#[serde(rename_all = "camelCase")]
#[schema(rename_all = "camelCase")]
pub struct ProgressView {
    pub steps: Vec<ProgressStepView>,
    pub percentage: f32,
}

#[derive(Debug, Serialize, Clone, ToSchema)]
#[serde(rename_all = "camelCase")]
#[schema(rename_all = "camelCase")]
pub struct ProgressStepView {
    pub current_step: Cow<'static, str>,
    pub finished: u64,
    pub total: u64,
}

/// Used when the name can change but it's still the same step.
/// To avoid conflicts on the `TypeId`, create a unique type every time you use this step:
/// ```text
/// enum UpgradeVersion {}
///
/// progress.update_progress(VariableNameStep::<UpgradeVersion>::new(
///     "v1 to v2",
///     0,
///     10,
/// ));
/// ```
pub struct VariableNameStep<U: Send + Sync + 'static> {
    name: String,
    current: u64,
    total: u64,
    phantom: PhantomData<U>,
}

impl<U: Send + Sync + 'static> VariableNameStep<U> {
    pub fn new(name: impl Into<String>, current: u64, total: u64) -> Self {
        Self { name: name.into(), current, total, phantom: PhantomData }
    }
}

impl<U: Send + Sync + 'static> Step for VariableNameStep<U> {
    fn name(&self) -> Cow<'static, str> {
        self.name.clone().into()
    }

    fn current(&self) -> u64 {
        self.current
    }

    fn total(&self) -> u64 {
        self.total
    }
}

impl Step for arroy::MainStep {
    fn name(&self) -> Cow<'static, str> {
        match self {
            arroy::MainStep::PreProcessingTheItems => "pre processing the items",
            arroy::MainStep::WritingTheDescendantsAndMetadata => {
                "writing the descendants and metadata"
            }
            arroy::MainStep::RetrieveTheUpdatedItems => "retrieve the updated items",
            arroy::MainStep::WriteTheMetadata => "write the metadata",
            arroy::MainStep::RetrievingTheItemsIds => "retrieving the items ids",
            arroy::MainStep::RetrievingTheUsedTreeNodes => "retrieving the used tree nodes",
            arroy::MainStep::DeletingExtraTrees => "deleting extra trees",
            arroy::MainStep::RemoveItemsFromExistingTrees => "remove items from existing trees",
            arroy::MainStep::InsertItemsInCurrentTrees => "insert items in current trees",
            arroy::MainStep::RetrievingTheItems => "retrieving the items",
            arroy::MainStep::RetrievingTheTreeNodes => "retrieving the tree nodes",
            arroy::MainStep::RetrieveTheLargeDescendants => "retrieve the large descendants",
            arroy::MainStep::CreateTreesForItems => "create trees for items",
        }
        .into()
    }

    fn current(&self) -> u64 {
        *self as u64
    }

    fn total(&self) -> u64 {
        Self::CARDINALITY as u64
    }
}

impl Step for arroy::SubStep {
    fn name(&self) -> Cow<'static, str> {
        self.unit.into()
    }

    fn current(&self) -> u64 {
        self.current.load(Ordering::Relaxed)
    }

    fn total(&self) -> u64 {
        self.max
    }
}
