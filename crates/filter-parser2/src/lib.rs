use std::borrow::Cow;
use std::collections::BTreeMap;

use nom::combinator::eof;
use nom::sequence::terminated;
use wip::WipResultExt;

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

/// A reference inside of a source.
///
/// Consists of a handle to the source and the span inside the source.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Token {
    source: SourceHandle,
    span: Span,
}

/// A span inside a text that is smaller than 4GBs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Span {
    pub start: u32,
    pub end: u32,
}

impl Span {
    pub fn from_entire_str(s: &str) -> Span {
        let end = s.len().try_into().unwrap_wip();
        Span { start: 0, end }
    }
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

    pub fn execute(&self, mut semantics: impl Semantics) {
        for Instruction { origin, terminal, previous_link, next_down } in &self.instructions {
            if previous_link.is_push() {
                semantics.push_from_left(*previous_link);
            }

            // improve readability with a few small lambdas
            // unwrap: the sources and the token have the same origin
            let view = |token: &Token| TokenView::from_sources(*token, &self.sources).unwrap();
            let point_view = |tokens: &[Token; 2]| [view(&tokens[0]), view(&tokens[1])];

            match &terminal.kind {
                TerminalKind::VectorExists { embedder, filter } => semantics.vector_exists(
                    embedder.as_ref().map(view),
                    VectorFilterView::from_sources(*filter, &self.sources).unwrap(),
                ),
                TerminalKind::GeoLowerThan { point, radius, resolution } => semantics
                    .geo_lower_than(point_view(point), view(radius), resolution.as_ref().map(view)),
                TerminalKind::GeoBoundingBox { top_right_point, bottom_left_point } => {
                    semantics.geo_bounding_box(
                        point_view(top_right_point),
                        point_view(bottom_left_point),
                    );
                }
                TerminalKind::GeoPolygon { points } => {
                    semantics
                        .geo_polygon(points.iter().map(|point| point_view(point)), points.len());
                }
                TerminalKind::GreaterThan { left, right } => {
                    semantics.greater_than(view(left), view(right));
                }
                TerminalKind::GreaterThanOrEqual { left, right } => {
                    semantics.greater_than_or_equal(view(left), view(right));
                }
                TerminalKind::Equal { left, right } => {
                    semantics.equal(view(left), view(right));
                }
                TerminalKind::Null { operand } => {
                    semantics.null(view(operand));
                }
                TerminalKind::Empty { operand } => {
                    semantics.empty(view(operand));
                }
                TerminalKind::Exists { operand } => {
                    semantics.exists(view(operand));
                }
                TerminalKind::LowerThan { left, right } => {
                    semantics.lower_than(view(left), view(right));
                }
                TerminalKind::LowerThanOrEqual { left, right } => {
                    semantics.lower_than_or_equal(view(left), view(right));
                }
                TerminalKind::Between { operand, lower, upper } => {
                    semantics.between(view(operand), view(lower), view(upper));
                }
                TerminalKind::Contains { left, right } => {
                    semantics.contains(view(left), view(right));
                }
                TerminalKind::StartsWith { left, right } => {
                    semantics.starts_with(view(left), view(right));
                }
                TerminalKind::ForeignGroup { id } => {
                    semantics.foreign(view(id));
                }
            }

            if previous_link.is_not() {
                semantics.not_right();
            }

            if previous_link.is_and() {
                semantics.and();
            }
            if previous_link.is_or() {
                semantics.or();
            }

            if *next_down != 0 {
                semantics.pop_to_left(*next_down);
            }
        }
    }
}

pub trait Semantics {
    /// pushes the left value to the stack, leaving a new initial value in left
    fn push_from_left(&mut self, previous_link: Link);
    /// pop one or multiple values from the stack and fold them into left by applying their link
    ///
    /// the implementing type **MUST** allow popping more than pushing.
    fn pop_to_left(&mut self, count: u16);

    /// reverses the right value
    fn not_right(&mut self);

    /// intersects the left and right values
    fn and(&mut self);
    /// unions left and right values
    fn or(&mut self);

    // terminals
    // each of the following functions compute a new current value as the right value

    fn vector_exists(&mut self, embedder: Option<TokenView<'_>>, filter: VectorFilterView<'_>);
    fn geo_lower_than(
        &mut self,
        point: [TokenView<'_>; 2],
        radius: TokenView<'_>,
        resolution: Option<TokenView<'_>>,
    );
    fn geo_bounding_box(
        &mut self,
        top_right_point: [TokenView<'_>; 2],
        bottom_left_point: [TokenView<'_>; 2],
    );
    fn geo_polygon<'a>(
        &mut self,
        points: impl Iterator<Item = [TokenView<'a>; 2]>,
        point_count: usize,
    );
    fn greater_than(&mut self, left: TokenView<'_>, right: TokenView<'_>);
    fn greater_than_or_equal(&mut self, left: TokenView<'_>, right: TokenView<'_>);
    fn lower_than(&mut self, left: TokenView<'_>, right: TokenView<'_>);
    fn lower_than_or_equal(&mut self, left: TokenView<'_>, right: TokenView<'_>);

    fn equal(&mut self, left: TokenView<'_>, right: TokenView<'_>);
    fn null(&mut self, operand: TokenView<'_>);
    fn empty(&mut self, operand: TokenView<'_>);
    fn exists(&mut self, operand: TokenView<'_>);
    fn between(&mut self, operand: TokenView<'_>, lower: TokenView<'_>, upper: TokenView<'_>);
    fn contains(&mut self, left: TokenView<'_>, right: TokenView<'_>);
    fn starts_with(&mut self, left: TokenView<'_>, right: TokenView<'_>);
    fn foreign(&mut self, id: TokenView<'_>);
}

fn parse_source(
    instructions: &mut Vec<Instruction>,
    source: &FilterSource,
    source_handle: SourceHandle,
) {
    wip::fixme!("first instruction inherits the previous_link from the source");
    terminated(|input| parse_expression(input, instructions, source, source_handle), eof)(
        Span::from_entire_str(&source.source),
    );
    wip::fixme!("last instruction inherits the downgroup from the source")
}

fn parse_expression(
    span: Span,
    instructions: &mut Vec<Instruction>,
    source: &FilterSource,
    source_handle: SourceHandle,
) {
}

/// A filter instruction
///
/// It consists of a Terminal, and the links to the previous and next instructions
struct Instruction {
    /// span of the entire instruction
    origin: Token,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// A view of a token that contains the text of that token.
pub struct TokenView<'a> {
    token: Token,
    text: &'a str,
}

impl<'a> TokenView<'a> {
    /// build a token view from a token and the sources.
    ///
    /// The sources and the token must match
    pub fn from_sources(token: Token, sources: &'a FilterSources) -> Result<Self, TokenViewError> {
        let Some(source): Option<&FilterSource> = sources.sources.get(token.source as usize) else {
            return Err(TokenViewError::UnknownSourceHandle(token));
        };

        let Some(text) = source.source.get(token.span.start as usize..token.span.end as usize)
        else {
            return Err(TokenViewError::SpanOutOfRange(token));
        };

        Ok(Self { token, text })
    }

    /// If the text of this token is quoted in single or double quotes, then returns
    /// the inner portion, unescaping any contained `\'`, `\"` and `\\`.
    ///
    /// If any unescaping occurs, then the returned Cow will be owned. Otherwise, it will be borrowed.
    ///
    /// If the text of this token is not quoted, returns the entire text of the token as borrowed, without unescaping anything.
    pub fn unqote(&self) -> Cow<'a, str> {
        if self.text.len() < 2 {
            return Cow::Borrowed(self.text);
        }
        if self.text.starts_with('\'') && self.text.ends_with('\'') {
            let inner = &self.text[1..(self.text.len() - 1)];
            if !inner.contains('\\') {
                return Cow::Borrowed(inner);
            } else {
                let inner: String = serde_json::from_reader(std::io::Read::chain(
                    std::io::Read::chain("\"".as_bytes(), inner.as_bytes()),
                    "\"".as_bytes(),
                ))
                .unwrap();
                Cow::Owned(inner)
            }
        } else if self.text.starts_with('"') && self.text.ends_with('"') {
            let inner: Cow<'_, str> = serde_json::from_str(self.text).unwrap();
            inner
        } else {
            Cow::Borrowed(self.text)
        }
    }
}

/// Error when trying to obtain a view of a token.
///
/// Should typically be an internal error, as it happens if the token doesn't match the source,
/// which is a code error.
#[derive(Debug, Clone, Copy)]
pub enum TokenViewError {
    UnknownSourceHandle(Token),
    SpanOutOfRange(Token),
}

/// A terminal typically represents the leaf objects of a filter
///
/// In Meilisearch's case, it generally resolves to roaring bitmaps representing lists of docids.
enum TerminalKind {
    VectorExists {
        embedder: Option<Token>,
        filter: VectorFilter,
    },
    GeoLowerThan {
        point: [Token; 2],
        radius: Token,
        resolution: Option<Token>,
    },
    GeoBoundingBox {
        top_right_point: [Token; 2],
        bottom_left_point: [Token; 2],
    },
    GeoPolygon {
        points: Vec<[Token; 2]>,
    },
    GreaterThan {
        left: Token,
        right: Token,
    },
    GreaterThanOrEqual {
        left: Token,
        right: Token,
    },
    Equal {
        left: Token,
        right: Token,
    },
    Null {
        operand: Token,
    },
    Empty {
        operand: Token,
    },
    Exists {
        operand: Token,
    },
    LowerThan {
        left: Token,
        right: Token,
    },
    LowerThanOrEqual {
        left: Token,
        right: Token,
    },
    Between {
        operand: Token,
        lower: Token,
        upper: Token,
    },
    Contains {
        left: Token,
        right: Token,
    },
    StartsWith {
        left: Token,
        right: Token,
    },
    /// Starts a new foreign filter until the stack is moved down again
    ForeignGroup {
        id: Token,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VectorFilter {
    Fragment(Token),
    DocumentTemplate,
    UserProvided,
    Regenerate,
    None,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VectorFilterView<'a> {
    Fragment(TokenView<'a>),
    DocumentTemplate,
    UserProvided,
    Regenerate,
    None,
}

impl<'a> VectorFilterView<'a> {
    pub fn from_sources(
        filter: VectorFilter,
        sources: &'a FilterSources,
    ) -> Result<Self, TokenViewError> {
        Ok(match filter {
            VectorFilter::Fragment(token) => {
                VectorFilterView::Fragment(TokenView::from_sources(token, sources)?)
            }
            VectorFilter::DocumentTemplate => VectorFilterView::DocumentTemplate,
            VectorFilter::UserProvided => VectorFilterView::UserProvided,
            VectorFilter::Regenerate => VectorFilterView::Regenerate,
            VectorFilter::None => VectorFilterView::None,
        })
    }
}

fn wip() {
    wip::fixme!("foreign should be a terminal with id, and we should tuck the parsed instructions in a special object of the filter");
    wip::fixme!("source label should be a type rather than a string. perhaps a trait so that it can be injected from Meilisearch");
}
