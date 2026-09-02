use std::any::TypeId;
use std::borrow::Cow;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use enum_iterator::Sequence as _;
use indexmap::IndexMap;
use itertools::Itertools;
use serde::Serialize;
use utoipa::ToSchema;

pub trait Step: 'static + Send + Sync {
    fn name(&self) -> Cow<'static, str>;
    fn current(&self) -> u32;
    fn total(&self) -> u32;
    fn verbosity_mode(&self) -> ProgressVerbosityMode {
        ProgressVerbosityMode::Info
    }
}

/// The mode of a step.
/// The order is important, the higher the mode, the more verbose the step.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default, PartialOrd, Ord)]
pub enum ProgressVerbosityMode {
    Quiet = 0,
    #[default]
    Info = 1,
    Trace = 2,
}

/// The mode of the timestamp computation.
///
/// `Precise` is the default mode and uses the std::time::Instant type.
/// Based on the wall clock.
///
/// `Fast` is a faster mode that uses the fastant::Instant type.
/// Based on TSC on Linux x86_64/x86 but fallback to the wall clock on other platforms.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum ProgressTimestampMode {
    #[default]
    Precise,
    Fast,
}

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ProgressInstant {
    Precise(std::time::Instant),
    Fast(fastant::Instant),
}

impl ProgressInstant {
    pub fn now(timestamp_mode: ProgressTimestampMode) -> Self {
        match timestamp_mode {
            ProgressTimestampMode::Precise => ProgressInstant::Precise(std::time::Instant::now()),
            ProgressTimestampMode::Fast => ProgressInstant::Fast(fastant::Instant::now()),
        }
    }

    pub fn elapsed(&self) -> Duration {
        match self {
            ProgressInstant::Precise(instant) => instant.elapsed(),
            ProgressInstant::Fast(instant) => instant.elapsed(),
        }
    }

    pub fn duration_since(&self, other: ProgressInstant) -> Duration {
        match (self, other) {
            (ProgressInstant::Precise(instant), ProgressInstant::Precise(other)) => {
                instant.duration_since(other)
            }
            (ProgressInstant::Fast(instant), ProgressInstant::Fast(other)) => {
                instant.duration_since(other)
            }
            (ProgressInstant::Precise(_), ProgressInstant::Fast(_))
            | (ProgressInstant::Fast(_), ProgressInstant::Precise(_)) => {
                unreachable!("Cannot compute the duration between a precise and a fast instant")
            }
        }
    }
}

#[derive(Clone)]
pub struct Progress {
    steps: Arc<RwLock<InnerProgress>>,
    verbosity_mode: ProgressVerbosityMode,
    timestamp_mode: ProgressTimestampMode,
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
    steps: Vec<(TypeId, Box<dyn Step>, ProgressInstant)>,
    /// The durations associated to each steps.
    durations: Vec<(String, Duration)>,
}

impl Progress {
    fn new(verbosity_mode: ProgressVerbosityMode, timestamp_mode: ProgressTimestampMode) -> Self {
        Self {
            steps: Arc::new(RwLock::new(InnerProgress::default())),
            verbosity_mode,
            timestamp_mode,
        }
    }

    /// Create a new progress with precise timestamp mode.
    pub fn precise(verbosity_mode: ProgressVerbosityMode) -> Self {
        Self::new(verbosity_mode, ProgressTimestampMode::Precise)
    }

    /// Create a new progress with fast timestamp mode.
    pub fn fast(verbosity_mode: ProgressVerbosityMode) -> Self {
        Self::new(verbosity_mode, ProgressTimestampMode::Fast)
    }

    /// Create a new progress with quiet verbosity mode.
    /// This will not register any steps.
    pub fn quiet() -> Self {
        Self::new(ProgressVerbosityMode::Quiet, ProgressTimestampMode::Fast)
    }

    /// Recreate the progress with the same verbosity and timestamp mode.
    pub fn recreate(&self) -> Self {
        Self::new(self.verbosity_mode, self.timestamp_mode)
    }

    /// Update the progress and return `Updated` if the step was started, `NotUpdated` if it was already started.
    /// Return `Failed` if the RWLock failed to lock.
    pub fn update_progress<P: Step>(&self, sub_progress: P) -> UpdateStepStatus {
        // If the step is more verbose than the progress mode, we skip it.
        if sub_progress.verbosity_mode() > self.verbosity_mode {
            return UpdateStepStatus::Skipped;
        }

        let mut inner = match self.steps.write() {
            Ok(inner) => inner,
            Err(error) => {
                tracing::error!("Failed to start progress step `{}`: {error}", sub_progress.name());
                return UpdateStepStatus::NotUpdated;
            }
        };
        let InnerProgress { steps, durations } = &mut *inner;

        let step_type = TypeId::of::<P>();
        if let Some(idx) = steps.iter().position(|(id, _, _)| *id == step_type) {
            if steps[idx].1.name() == sub_progress.name() {
                // The step is already started, so we don't need to start it again.
                return UpdateStepStatus::NotUpdated;
            }

            let now = ProgressInstant::now(self.timestamp_mode);
            push_steps_durations(steps, durations, now, idx);
            steps.truncate(idx);
            steps.push((step_type, Box::new(sub_progress), now));
        } else {
            steps.push((
                step_type,
                Box::new(sub_progress),
                ProgressInstant::now(self.timestamp_mode),
            ));
        }

        UpdateStepStatus::Updated
    }

    /// End a step that has been started without having to start a new step.
    /// Update the progress and return `Updated` if the step was ended, `NotUpdated` if it was already ended.
    /// Return `Failed` if the RWLock failed to lock.
    fn end_progress_step<P: Step>(&self, sub_progress: P) -> UpdateStepStatus {
        let mut inner = match self.steps.write() {
            Ok(inner) => inner,
            Err(error) => {
                tracing::error!("Failed to end progress step `{}`: {error}", sub_progress.name());
                return UpdateStepStatus::NotUpdated;
            }
        };

        let InnerProgress { steps, durations } = &mut *inner;

        let step_type = TypeId::of::<P>();
        match steps
            .iter()
            .position(|(id, s, _)| *id == step_type && s.name() == sub_progress.name())
        {
            Some(idx) => {
                let now = ProgressInstant::now(self.timestamp_mode);
                push_steps_durations(steps, durations, now, idx);
                steps.truncate(idx);
                UpdateStepStatus::Updated
            }
            None => UpdateStepStatus::NotUpdated,
        }
    }

    /// Update the progress and return a scoped progress step that will end the progress step when dropped.
    pub fn update_progress_scoped<P: Step + Copy>(&self, step: P) -> ScopedProgressStep<'_, P> {
        match self.update_progress(step) {
            UpdateStepStatus::Updated => ScopedProgressStep { progress: self, step: Some(step) },
            UpdateStepStatus::NotUpdated => {
                tracing::warn!(
                    "Step `{}` can't be scoped because it was already started",
                    step.name()
                );
                ScopedProgressStep { progress: self, step: None }
            }
            UpdateStepStatus::Skipped => ScopedProgressStep { progress: self, step: None },
        }
    }

    // TODO: This code should be in meilisearch_types but cannot because milli can't depend on meilisearch_types
    pub fn as_progress_view(&self) -> Option<ProgressView> {
        let inner = match self.steps.read() {
            Ok(inner) => inner,
            Err(error) => {
                tracing::error!("Failed to read progress: {error}");
                return None;
            }
        };
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

        Some(ProgressView { steps: step_view, percentage: percentage * 100.0 })
    }

    pub fn accumulated_durations(&self) -> IndexMap<String, String> {
        let inner = match self.steps.read() {
            Ok(inner) => inner,
            Err(error) => {
                tracing::error!("Failed to read progress: {error}");
                return IndexMap::new();
            }
        };
        let InnerProgress { steps, durations, .. } = &*inner;
        let mut durations = durations.clone();

        let now = ProgressInstant::now(self.timestamp_mode);
        push_steps_durations(steps, &mut durations, now, 0);

        let mut accumulated_durations = IndexMap::new();
        for (name, duration) in durations.drain(..) {
            accumulated_durations.entry(name).and_modify(|d| *d += duration).or_insert(duration);
        }

        accumulated_durations
            .into_iter()
            .map(|(name, duration)| (name, format!("{duration:.2?}")))
            .collect()
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
    steps: &[(TypeId, Box<dyn Step>, ProgressInstant)],
    durations: &mut Vec<(String, Duration)>,
    now: ProgressInstant,
    idx: usize,
) {
    for (i, (_, _, started_at)) in steps.iter().skip(idx).enumerate().rev() {
        let full_name = steps.iter().take(idx + i + 1).map(|(_, s, _)| s.name()).join(" > ");
        durations.push((full_name, now.duration_since(*started_at)));
    }
}

/// This trait lets you use the AtomicSubStep defined right below.
/// The name must be a const that never changed but that can't be enforced
/// by the type system because it make the trait non object-safe. By forcing
/// the Default trait + the &'static str we make it harder to miss-use the
/// trait.
pub trait NamedStep: 'static + Send + Sync + Default {
    fn name(&self) -> &'static str;
}

/// Structure to quickly define steps that need very quick, lockless
/// updating of their current step.
/// You can use this struct if:
/// - The name of the step doesn't change
/// - The total number of steps doesn't change
pub struct AtomicSubStep<Name: NamedStep> {
    unit_name: Name,
    current: Arc<AtomicU32>,
    total: u32,
}

impl<Name: NamedStep> AtomicSubStep<Name> {
    pub fn new(total: u32) -> (Arc<AtomicU32>, Self) {
        let current = Arc::new(AtomicU32::new(0));
        (current.clone(), Self { current, total, unit_name: Name::default() })
    }
}

impl<Name: NamedStep> Step for AtomicSubStep<Name> {
    fn name(&self) -> Cow<'static, str> {
        self.unit_name.name().into()
    }

    fn current(&self) -> u32 {
        self.current.load(Ordering::Relaxed)
    }

    fn total(&self) -> u32 {
        self.total
    }
}

#[doc(hidden)]
pub use convert_case as _private_convert_case;
#[doc(hidden)]
pub use enum_iterator as _private_enum_iterator;

#[macro_export]
macro_rules! make_enum_progress {
    ($visibility:vis enum $name:ident { $($variant:ident: $mode:ident,)+ }) => {
        #[repr(u8)]
        #[derive(Debug, Clone, Copy, PartialEq, Eq, $crate::progress::_private_enum_iterator::Sequence)]
        #[allow(clippy::enum_variant_names)]
        $visibility enum $name {
            $($variant),+
        }

        impl $crate::progress::Step for $name {
            fn verbosity_mode(&self) -> $crate::progress::ProgressVerbosityMode {
                match self {
                    $(
                        $name::$variant => $crate::progress::ProgressVerbosityMode::$mode,
                    )+
                }
            }

            fn name(&self) -> std::borrow::Cow<'static, str> {
                use $crate::progress::_private_convert_case::Casing;

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
                use $crate::progress::_private_enum_iterator::Sequence;
                Self::CARDINALITY as u32
            }
        }
    };
    ($visibility:vis enum $name:ident { $($variant:ident,)+ }) => {
        $crate::make_enum_progress!($visibility enum $name { $($variant: Info,)+ });
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
make_atomic_progress!(Database alias AtomicDatabaseStep => "database");
make_atomic_progress!(Payload alias AtomicPayloadStep => "payload");

/// Real-time progress information for a batch or task that is currently
/// being processed. Use this to display progress bars or status updates to
/// users.
#[derive(Debug, Serialize, Clone, ToSchema)]
#[serde(rename_all = "camelCase")]
#[schema(rename_all = "camelCase")]
pub struct ProgressView {
    /// A hierarchical list of processing steps currently being executed.
    /// Steps are listed from outermost to innermost, with each step
    /// representing a more granular operation within its parent step.
    pub steps: Vec<ProgressStepView>,
    /// The overall completion percentage of the operation (0.0 to 100.0).
    /// This is calculated by combining the progress of all nested steps,
    /// weighted by their relative importance.
    pub percentage: f32,
}

/// Information about a single processing step within a batch or task. Each
/// step has a name, current progress, and total items to process.
#[derive(Debug, Serialize, Clone, ToSchema)]
#[serde(rename_all = "camelCase")]
#[schema(rename_all = "camelCase")]
pub struct ProgressStepView {
    /// A human-readable name describing what this processing step is doing.
    /// Examples include "indexing documents", "computing embeddings",
    /// "building word cache", etc.
    pub current_step: Cow<'static, str>,
    /// The number of items that have been processed so far in this step.
    /// Compare with `total` to calculate the percentage complete for this
    /// specific step.
    pub finished: u32,
    /// The total number of items to process in this step. When `finished`
    /// equals `total`, this step is complete and processing moves to the
    /// next step.
    pub total: u32,
}

/// Used when the name can change but it's still the same step.
/// To avoid conflicts on the `TypeId`, create a unique type every time you
/// use this step:
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
    current: u32,
    total: u32,
    phantom: PhantomData<U>,
}

impl<U: Send + Sync + 'static> VariableNameStep<U> {
    pub fn new(name: impl Into<String>, current: u32, total: u32) -> Self {
        Self { name: name.into(), current, total, phantom: PhantomData }
    }
}

impl<U: Send + Sync + 'static> Step for VariableNameStep<U> {
    fn name(&self) -> Cow<'static, str> {
        self.name.clone().into()
    }

    fn current(&self) -> u32 {
        self.current
    }

    fn total(&self) -> u32 {
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
            arroy::MainStep::RetrievingTheTreeAndItemNodes => "retrieving the tree and item nodes",
            arroy::MainStep::UpdatingTheTrees => "updating the trees",
            arroy::MainStep::CreateNewTrees => "create new trees",
            arroy::MainStep::WritingNodesToDatabase => "writing nodes to database",
            arroy::MainStep::DeleteExtraneousTrees => "delete extraneous trees",
            arroy::MainStep::WriteTheMetadata => "write the metadata",
            arroy::MainStep::ConvertingHannoyToArroy => "converting hannoy to arroy",
        }
        .into()
    }

    fn current(&self) -> u32 {
        *self as u32
    }

    fn total(&self) -> u32 {
        Self::CARDINALITY as u32
    }
}

impl Step for arroy::SubStep {
    fn name(&self) -> Cow<'static, str> {
        self.unit.into()
    }

    fn current(&self) -> u32 {
        self.current.load(Ordering::Relaxed)
    }

    fn total(&self) -> u32 {
        self.max
    }
}

// Integration with steppe

impl steppe::Progress for Progress {
    fn update(&self, sub_progress: impl steppe::Step) {
        self.update_progress(Compat(sub_progress));
    }
}

struct Compat<T: steppe::Step>(T);

impl<T: steppe::Step> Step for Compat<T> {
    fn name(&self) -> Cow<'static, str> {
        self.0.name()
    }

    fn current(&self) -> u32 {
        self.0.current().try_into().unwrap_or(u32::MAX)
    }

    fn total(&self) -> u32 {
        self.0.total().try_into().unwrap_or(u32::MAX)
    }
}

pub struct ScopedProgressStep<'a, P: Step + Copy> {
    progress: &'a Progress,
    step: Option<P>,
}

impl<'a, P: Step + Copy> Drop for ScopedProgressStep<'a, P> {
    fn drop(&mut self) {
        if let Some(step) = self.step {
            if self.progress.end_progress_step(step) == UpdateStepStatus::NotUpdated {
                tracing::warn!("Step `{}` has already been ended", step.name());
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UpdateStepStatus {
    /// The step was updated.
    Updated,
    /// The step did not change.
    NotUpdated,
    /// The step as been skipped.
    Skipped,
}
