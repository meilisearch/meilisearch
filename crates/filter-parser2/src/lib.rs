//! Instruction-oriented Meilisearch filters.
//!
//! This crate is a rewrite of Meilisearch filters with the following characteristics:
//!
//! - Parsing a filter does not build an Abstract Syntax Tree, but instead a single path in the AST,
//!   represented as contiguous instructions.
//!   The tradeoff here is that the resulting path is harder to optimize than a tree,
//!   but is a more lightweight representation
//!   (fewer allocations)
//! - Instructions keep indirect references to the original data via a [`Span`] type,
//!   which encapsulates an opaque handle to some [`FilterSource`] and a span consisting of a range.
//!   No lifetimes are kept, liveness is ensured by bundling together the tokens and sources,
//!   and providing views at access time.
//! - Semantics is fully decoupled from the parsing via a [`Semantics`] trait that implementors can use to interpret the filter.
//! - Parsing is done with a virtual stack rather than on the actual stack, reducing the risk of stack overflow.
//! - Parsing is done with an explicit state machine that matches on permissible tokens depending on the current state of the parser.
//!
//! # Possible tokens
//!
//!
//! 1. `word         = (alphanumeric | _ | - |.)+`
//! 2. `singleQuoted = ' ([']^*|\') '`
//! 3. `doubleQuoted = " (["]^*|\") "`
//! 4. a keyword:
//!   1. OR
//!   2. AND
//!   3. NOT
//!   4. IN
//!   5. EXISTS
//!   6. IS NULL
//!   7. IS NOT NULL
//!   8. TO
//! 4. A reserved field:
//!   1. _geoRadius
//!   2. _geoBoundingBox
//!   3. _geoPolygon
//!   4. _vectors
//!   5. _foreign
//! 12. left or right parens `(`, `)`
//! 13. comma `,`
//! 14. left or right square bracket `[`, `]`
//! 15. equality and comparison operators: `=`, `!=`, `>`, `>=`, `<`, `<=`
//!
//! # Possible states
//!
//! - Terminal context:
//!   - Value -> left-operand context
//!   - NOT -> Terminal context
//!   - Reserved Field -> Reserved syntax
//!   - left parens -> Terminal context
//! - left-operand context
//!   - IS NOT NULL -> Link context
//!   - IS NULL -> Link context
//!   - IS EMPTY -> LInk context
//!   - IS NOT EMPTY -> Link Context
//!   - EXISTS -> Link Context
//!   - NOT EXISTS -> Link Context
//!   - IN -> IN Context
//!   - Value -> TO Context
//!   - (cmp operator) -> right-operand context
//! - link context
//! - IN context
//! - TO context

use std::collections::BTreeMap;

mod span;
mod token;

pub use span::{Span, SpanView, SpanViewError};

type SourceHandle = u16;

/// Sources for a filter.
///
/// A filter is built of multiple sources, that know how they relate with each others
pub struct FilterSources {
    sources: Vec<FilterSource>,
    labels: BTreeMap<String, SourceHandle>,
}

/// A source for a filter.
///
/// Consists of a label, the source itself, and its relation to the previous and next source.
pub struct FilterSource {
    label: String,
    source: String,
    previous_link: Link,
    next_down: u16,
}

/// A filter contains sources, and instructions obtained by parsing the sources.
pub struct Filter {
    sources: FilterSources,
    instructions: Vec<Instruction>,
}

impl Filter {
    pub fn parse(sources: FilterSources) -> Self {
        if sources.sources.len() > u16::MAX.into() {
            wip::wip!("too many sources")
        }
        let mut instructions = Vec::new();
        for (source_handle, source) in sources.sources.iter().enumerate() {
            // unwrap: sources.len() <= u16::MAX
            let source_handle: u16 = source_handle.try_into().unwrap();
            parse_source(&mut instructions, source, source_handle);
        }
        Self { sources, instructions }
    }

    pub fn execute<S: Semantics>(&self, mut semantics: S) -> Result<(), S::Error> {
        for Instruction { origin, terminal, previous_link, next_down } in &self.instructions {
            if previous_link.is_push() {
                semantics.push_from_left(*previous_link)?;
            }

            // improve readability with a few small lambdas
            // unwrap: the sources and the span have the same origin
            let view = |span: &Span| SpanView::from_sources(*span, &self.sources).unwrap();
            let point_view = |spans: &[Span; 2]| [view(&spans[0]), view(&spans[1])];

            match &terminal.kind {
                TerminalKind::VectorExists { embedder, filter } => semantics.vector_exists(
                    embedder.as_ref().map(view),
                    VectorFilterView::from_sources(*filter, &self.sources).unwrap(),
                )?,
                TerminalKind::GeoLowerThan { point, radius, resolution } => semantics
                    .geo_lower_than(
                        point_view(point),
                        view(radius),
                        resolution.as_ref().map(view),
                    )?,
                TerminalKind::GeoBoundingBox { top_right_point, bottom_left_point } => semantics
                    .geo_bounding_box(point_view(top_right_point), point_view(bottom_left_point))?,
                TerminalKind::GeoPolygon { points } => semantics
                    .geo_polygon(points.iter().map(|point| point_view(point)), points.len())?,
                TerminalKind::GreaterThan { left, right } => {
                    semantics.greater_than(view(left), view(right))?
                }
                TerminalKind::GreaterThanOrEqual { left, right } => {
                    semantics.greater_than_or_equal(view(left), view(right))?;
                }
                TerminalKind::Equal { left, right } => {
                    semantics.equal(view(left), view(right))?;
                }
                TerminalKind::Null { operand } => {
                    semantics.null(view(operand))?;
                }
                TerminalKind::Empty { operand } => {
                    semantics.empty(view(operand))?;
                }
                TerminalKind::Exists { operand } => {
                    semantics.exists(view(operand))?;
                }
                TerminalKind::LowerThan { left, right } => {
                    semantics.lower_than(view(left), view(right))?;
                }
                TerminalKind::LowerThanOrEqual { left, right } => {
                    semantics.lower_than_or_equal(view(left), view(right))?;
                }
                TerminalKind::Between { operand, lower, upper } => {
                    semantics.between(view(operand), view(lower), view(upper))?;
                }
                TerminalKind::Contains { left, right } => {
                    semantics.contains(view(left), view(right))?;
                }
                TerminalKind::StartsWith { left, right } => {
                    semantics.starts_with(view(left), view(right))?;
                }
                TerminalKind::ForeignGroup { foreign_field, foreign_source } => {
                    semantics.foreign(view(foreign_field), view(foreign_source))?;
                }
            }

            if previous_link.is_not() {
                semantics.not_right()?;
            }

            if previous_link.is_and() {
                semantics.and()?;
            }
            if previous_link.is_or() {
                semantics.or()?;
            }

            if *next_down != 0 {
                semantics.pop_to_left(*next_down)?;
            }
        }
        Ok(())
    }
}

pub trait Semantics {
    /// Represents a semantic error that can happen while executing a filter.
    type Error;

    /// pushes the left value to the stack, leaving a new initial value in left
    fn push_from_left(&mut self, previous_link: Link) -> Result<(), Self::Error>;
    /// pop one or multiple values from the stack and fold them into left by applying their link
    ///
    /// the implementing type **MUST** allow popping more than pushing.
    fn pop_to_left(&mut self, count: u16) -> Result<(), Self::Error>;

    /// reverses the right value
    fn not_right(&mut self) -> Result<(), Self::Error>;

    /// intersects the left and right values
    fn and(&mut self) -> Result<(), Self::Error>;
    /// unions left and right values
    fn or(&mut self) -> Result<(), Self::Error>;

    // terminals
    // each of the following functions compute a new current value as the right value

    fn vector_exists(
        &mut self,
        embedder: Option<SpanView<'_>>,
        filter: VectorFilterView<'_>,
    ) -> Result<(), Self::Error>;
    fn geo_lower_than(
        &mut self,
        point: [SpanView<'_>; 2],
        radius: SpanView<'_>,
        resolution: Option<SpanView<'_>>,
    ) -> Result<(), Self::Error>;
    fn geo_bounding_box(
        &mut self,
        top_right_point: [SpanView<'_>; 2],
        bottom_left_point: [SpanView<'_>; 2],
    ) -> Result<(), Self::Error>;
    fn geo_polygon<'a>(
        &mut self,
        points: impl Iterator<Item = [SpanView<'a>; 2]>,
        point_count: usize,
    ) -> Result<(), Self::Error>;
    fn greater_than(&mut self, left: SpanView<'_>, right: SpanView<'_>) -> Result<(), Self::Error>;
    fn greater_than_or_equal(
        &mut self,
        left: SpanView<'_>,
        right: SpanView<'_>,
    ) -> Result<(), Self::Error>;
    fn lower_than(&mut self, left: SpanView<'_>, right: SpanView<'_>) -> Result<(), Self::Error>;
    fn lower_than_or_equal(
        &mut self,
        left: SpanView<'_>,
        right: SpanView<'_>,
    ) -> Result<(), Self::Error>;

    fn equal(&mut self, left: SpanView<'_>, right: SpanView<'_>) -> Result<(), Self::Error>;
    fn null(&mut self, operand: SpanView<'_>) -> Result<(), Self::Error>;
    fn empty(&mut self, operand: SpanView<'_>) -> Result<(), Self::Error>;
    fn exists(&mut self, operand: SpanView<'_>) -> Result<(), Self::Error>;
    fn between(
        &mut self,
        operand: SpanView<'_>,
        lower: SpanView<'_>,
        upper: SpanView<'_>,
    ) -> Result<(), Self::Error>;
    fn contains(&mut self, left: SpanView<'_>, right: SpanView<'_>) -> Result<(), Self::Error>;
    fn starts_with(&mut self, left: SpanView<'_>, right: SpanView<'_>) -> Result<(), Self::Error>;
    fn foreign(
        &mut self,
        foreign_field: SpanView<'_>,
        foreign_source: SpanView<'_>,
    ) -> Result<(), Self::Error>;
}

fn parse_source(
    instructions: &mut Vec<Instruction>,
    source: &FilterSource,
    source_handle: SourceHandle,
) {
    wip::fixme!("first instruction inherits the previous_link from the source");
    SpanView::from_entire_source(source_handle, source);
    wip::fixme!("last instruction inherits the downgroup from the source")
}

fn parse_expression(
    span: SpanView<'_>,
    instructions: &mut Vec<Instruction>,
    source: &FilterSource,
    source_handle: SourceHandle,
) {
}

enum ParsingState<'a> {
    Terminal,
    Operator { left_operand: SpanView<'a> },
    In { left_operand: SpanView<'a>, next_link: Link },
    Link { terminal: Terminal },
    To { left_operand: SpanView<'a> },
}

struct ParsingContext<'a> {
    polarity: bool,
    state: ParsingState<'a>,
    previous_link: Link,
    in_foreign: bool,
    next: SpanView<'a>,
}

struct SourceParser<'a> {
    source: &'a FilterSource,
    source_handle: SourceHandle,
    current: ParsingContext<'a>,
    nested: Vec<ParsingContext<'a>>,
}

impl<'a> ParsingContext<'a> {
    fn new(parse: SpanView<'a>) -> Self {
        Self {
            polarity: true,
            state: ParsingState::Terminal,
            previous_link: Link::And,
            in_foreign: false,
            next: parse,
        }
    }
}

impl<'a> SourceParser<'a> {
    fn new(source: &'a FilterSource, source_handle: SourceHandle) -> Self {
        Self {
            source,
            source_handle,
            current: ParsingContext::new(SpanView::from_entire_source(source_handle, source)),
            nested: Default::default(),
        }
    }
}

/// A filter instruction
///
/// It consists of a Terminal, and the links to the previous and next instructions
struct Instruction {
    /// span of the entire instruction
    origin: Span,
    /// terminal to compute in the instruction
    terminal: Terminal,
    /// link to previous instruction
    previous_link: Link,
    /// move up the stack n times after executing the instruction
    next_down: u16,
}

/// A link between sources or instructions
///
/// Filter instructions represent a path inside of an expression tree.
/// Links indicate how the path progresses inside of the tree.
#[derive(Debug, Clone, Copy)]
pub enum Link {
    /// & with previous, move up the stack
    AndUp,
    /// & with previous, move up the stack, switch polarity
    AndUpNot,
    /// | with previous, move up the stack
    OrUp,
    /// | with previous, move up the stack, switch polarity
    OrUpNot,
    /// & terminal with previous, no move in stack
    And,
    /// & terminal with previous, no move in stack, switch polarity
    AndNot,
    /// | terminal with previous, no move in stack
    Or,
    /// | terminal with previous, no move in stack, switch polarity
    OrNot,
}

impl Link {
    pub fn is_push(&self) -> bool {
        match self {
            Link::AndUp | Link::AndUpNot | Link::OrUp | Link::OrUpNot => true,
            Link::And | Link::AndNot | Link::Or | Link::OrNot => true,
        }
    }

    pub fn is_not(&self) -> bool {
        match self {
            Link::AndUpNot | Link::OrUpNot | Link::AndNot | Link::OrNot => true,
            Link::AndUp | Link::OrUp | Link::And | Link::Or => false,
        }
    }

    pub fn is_and(&self) -> bool {
        match self {
            Link::AndUp | Link::AndUpNot | Link::And | Link::AndNot => true,
            Link::OrUp | Link::OrUpNot | Link::Or | Link::OrNot => false,
        }
    }

    pub fn is_or(&self) -> bool {
        match self {
            Link::OrUp | Link::OrUpNot | Link::Or | Link::OrNot => true,
            Link::AndUp | Link::AndUpNot | Link::And | Link::AndNot => false,
        }
    }
}

struct Terminal {
    kind: TerminalKind,
}

/// A terminal typically represents the leaf objects of a filter
///
/// In Meilisearch's case, it generally resolves to roaring bitmaps representing lists of docids.
enum TerminalKind {
    VectorExists { embedder: Option<Span>, filter: VectorFilter },
    GeoLowerThan { point: [Span; 2], radius: Span, resolution: Option<Span> },
    GeoBoundingBox { top_right_point: [Span; 2], bottom_left_point: [Span; 2] },
    GeoPolygon { points: Vec<[Span; 2]> },
    GreaterThan { left: Span, right: Span },
    GreaterThanOrEqual { left: Span, right: Span },
    Equal { left: Span, right: Span },
    Null { operand: Span },
    Empty { operand: Span },
    Exists { operand: Span },
    LowerThan { left: Span, right: Span },
    LowerThanOrEqual { left: Span, right: Span },
    Between { operand: Span, lower: Span, upper: Span },
    Contains { left: Span, right: Span },
    StartsWith { left: Span, right: Span },
    ForeignGroup { foreign_field: Span, foreign_source: Span },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VectorFilter {
    Fragment(Span),
    DocumentTemplate,
    UserProvided,
    Regenerate,
    None,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VectorFilterView<'a> {
    Fragment(SpanView<'a>),
    DocumentTemplate,
    UserProvided,
    Regenerate,
    None,
}

impl<'a> VectorFilterView<'a> {
    pub fn from_sources(
        filter: VectorFilter,
        sources: &'a FilterSources,
    ) -> Result<Self, SpanViewError> {
        Ok(match filter {
            VectorFilter::Fragment(span) => {
                VectorFilterView::Fragment(SpanView::from_sources(span, sources)?)
            }
            VectorFilter::DocumentTemplate => VectorFilterView::DocumentTemplate,
            VectorFilter::UserProvided => VectorFilterView::UserProvided,
            VectorFilter::Regenerate => VectorFilterView::Regenerate,
            VectorFilter::None => VectorFilterView::None,
        })
    }
}

fn wip() {
    wip::fixme!("source label should be a type rather than a string. perhaps a trait so that it can be injected from Meilisearch");
}
