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

use std::borrow::Cow;
use std::collections::BTreeMap;

use itertools::Itertools;
use nom::branch::alt;
use nom::bytes::complete::{escaped, tag, take_while, take_while1};
use nom::character::complete::{anychar, multispace0, one_of};
use nom::combinator::cut;
use nom::number::complete::recognize_float;
use nom::sequence::{preceded, terminated};
use nom::{IResult, Input, Parser};
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
/// The span must be smaller than 4GB.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Span {
    source: SourceHandle,
    start: u32,
    end: u32,
}

impl Span {
    /// Build a span from a source and its handle, spanning the entire source  
    fn from_entire_source(source_handle: SourceHandle, source: &FilterSource) -> Self {
        let end = source.source.len().try_into().unwrap_wip();
        Self { source: source_handle, start: 0, end }
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

#[derive(Debug, Clone, Copy)]
struct Token<'a> {
    span: SpanView<'a>,
    kind: TokenKind,
}

struct ParseOutput<'a> {
    parsed_token: Token<'a>,
    remaining_input: SpanView<'a>,
}

impl<'a> Token<'a> {
    /// Parses the first token of an input span, returning the parsed token and the remaining input.
    pub fn parse_next(input: SpanView<'a>) -> Result<ParseOutput<'a>, ParseTokenError> {
        let input = ignore_whitespace(input);
        let (remaining_input, parsed_token) = parse_token().parse(input)?;
        Ok(ParseOutput { parsed_token, remaining_input })
    }

    /// Iterates over all parsed tokens from input until reaching EOF.
    pub fn iter_tokens(
        input: SpanView<'a>,
    ) -> impl Iterator<Item = Result<Token<'a>, ParseTokenError>> + 'a {
        let input = std::cell::Cell::new(input);
        std::iter::from_fn(move || {
            let token = (|| {
                let ParseOutput { parsed_token, remaining_input } = Self::parse_next(input.get())?;
                input.set(remaining_input);
                Ok(parsed_token)
            })();
            Some(token)
        })
        .take_while_inclusive(|token| {
            let Ok(token) = token else {
                return false;
            };
            !matches!(token.kind, TokenKind::Eof)
        })
    }
}

impl<'a> From<nom::Err<nom::error::Error<SpanView<'a>>>> for ParseTokenError {
    fn from(value: nom::Err<nom::error::Error<SpanView<'a>>>) -> Self {
        match value {
            nom::Err::Incomplete(needed) => ParseTokenError::TruncatedInput(needed),
            nom::Err::Error(err) | nom::Err::Failure(err) => {
                wip::fixme!("proper structured error handling");
                ParseTokenError::Error(format!("{err:?}"))
            }
        }
    }
}

fn ignore_whitespace(input: SpanView<'_>) -> SpanView<'_> {
    match nom::character::complete::multispace0::<_, ()>(input) {
        Ok((rest, _)) => rest,
        Err(_) => input,
    }
}

fn is_value_component(c: char) -> bool {
    c.is_alphanumeric() || ['_', '-', '.'].contains(&c)
}

fn parse_token<'a>(
) -> impl Parser<SpanView<'a>, Output = Token<'a>, Error = nom::error::Error<SpanView<'a>>> {
    parse_all_syntax()
        .or(parse_keywords())
        .or(parse_reserved_fields())
        .or(parse_eof)
        .or(parse_value)
        .or(parse_illegal)
}

fn parse_illegal<'a>(input: SpanView<'a>) -> IResult<SpanView<'a>, Token<'a>> {
    let (remaining, illegal) = input.take_split(1);
    Ok((remaining, Token { span: illegal, kind: TokenKind::IllegalCharacter }))
}

fn parse_keywords<'a>(
) -> impl Parser<SpanView<'a>, Output = Token<'a>, Error = nom::error::Error<SpanView<'a>>> {
    alt((
        parse_keyword("OR", TokenKind::Or),
        parse_keyword("AND", TokenKind::And),
        parse_keyword("NOT", TokenKind::Not),
        parse_keyword("NULL", TokenKind::Null),
        parse_keyword("EXISTS", TokenKind::Exists),
        parse_keyword("IS", TokenKind::Is),
        parse_keyword("TO", TokenKind::To),
        parse_keyword("IN", TokenKind::In),
    ))
}

fn parse_all_syntax<'a>(
) -> impl Parser<SpanView<'a>, Output = Token<'a>, Error = nom::error::Error<SpanView<'a>>> {
    alt((
        // start with two-characters syntax to ensure we don't parse them as separate tokens
        parse_syntax("!=", TokenKind::Different),
        parse_syntax(">=", TokenKind::GreaterOrEqual),
        parse_syntax("<=", TokenKind::LowerOrEqual),
        // single-char tokens
        parse_syntax("(", TokenKind::LeftParens),
        parse_syntax(")", TokenKind::RightParens),
        parse_syntax("[", TokenKind::LeftSquareBracket),
        parse_syntax("]", TokenKind::RightSquareBracket),
        parse_syntax(",", TokenKind::Comma),
        parse_syntax("=", TokenKind::Equal),
        parse_syntax(">", TokenKind::GreaterThan),
        parse_syntax("<", TokenKind::LowerThan),
    ))
}

fn parse_reserved_fields<'a>(
) -> impl Parser<SpanView<'a>, Output = Token<'a>, Error = nom::error::Error<SpanView<'a>>> {
    alt((
        parse_reserved_field("_geoRadius", TokenKind::GeoRadius),
        parse_reserved_field("_geoBoundingBox", TokenKind::GeoBoundingBox),
        parse_reserved_field("_geoPolygon", TokenKind::GeoPolygon),
        parse_reserved_field("_vectors", TokenKind::Vectors),
        parse_reserved_field("_foreign", TokenKind::Foreign),
    ))
}

/// Parse the specified keyword, which should NOT be followed by a value component
fn parse_keyword<'a>(
    keyword: &str,
    keyword_kind: TokenKind,
) -> impl Fn(SpanView<'_>) -> IResult<SpanView<'_>, Token<'_>> + '_ {
    move |input| {
        let (rest, parsed) = nom::bytes::complete::tag(keyword)(input)?;
        if let Some(next) = rest.iter_elements().next() {
            if is_value_component(next) {
                return Err(nom::Err::Error(nom::error::Error::new(
                    input,
                    nom::error::ErrorKind::Tag,
                )));
            }
        }
        Ok((rest, Token { span: parsed, kind: keyword_kind }))
    }
}

/// Parse the specified syntax character, which can be followed by anything
fn parse_syntax<'a>(
    c: &str,
    kind: TokenKind,
) -> impl Fn(SpanView<'_>) -> IResult<SpanView<'_>, Token<'_>> + '_ {
    move |input| {
        let (rest, parsed) = nom::bytes::complete::tag(c)(input)?;
        Ok((rest, Token { span: parsed, kind }))
    }
}

/// Parse the specified reserved field name, which can be NOT be followed by a value component except dot
fn parse_reserved_field<'a>(
    field: &str,
    kind: TokenKind,
) -> impl Fn(SpanView<'_>) -> IResult<SpanView<'_>, Token<'_>> + '_ {
    wip::fixme!("parse_float");
    move |input| {
        let (rest, parsed) = nom::bytes::complete::tag(field)(input)?;
        if let Some(next) = rest.iter_elements().next() {
            if next != '.' && is_value_component(next) {
                return Err(nom::Err::Error(nom::error::Error::new(
                    input,
                    nom::error::ErrorKind::Tag,
                )));
            }
        }
        Ok((rest, Token { span: parsed, kind }))
    }
}

fn parse_eof<'a>(input: SpanView<'a>) -> IResult<SpanView<'a>, Token<'a>> {
    let (rest, parsed) = nom::combinator::eof(input)?;
    Ok((rest, Token { span: parsed, kind: TokenKind::Eof }))
}

fn parse_value<'a>(input: SpanView<'a>) -> IResult<SpanView<'a>, Token<'a>> {
    let value = nom::combinator::map(take_while1(is_value_component), |span| Token {
        span,
        kind: TokenKind::Value,
    });

    let float_value =
        nom::combinator::map(recognize_float, |span| Token { span, kind: TokenKind::FloatValue });

    let (rest, parsed) =
        terminated(alt((float_value, value, parse_quoted_value)), multispace0).parse(input)?;
    Ok((rest, parsed))
}

/// Inspired by <https://github.com/rust-bakery/nom/blob/main/examples/string.rs>, *sans* the output as String
/// because we're solely interested in emitting tokens at this stage
fn parse_quoted_value<'a>(input: SpanView<'a>) -> IResult<SpanView<'a>, Token<'a>> {
    let not_double_quote_slash = nom::bytes::streaming::is_not("\"\\");
    let not_single_quote_slash = nom::bytes::streaming::is_not("'\\");
    let parse_escaped_double_quote = alt((tag("\\\""), tag("\\\\")));
    let parse_escaped_single_quote = alt((tag("\\'"), tag("\\\\")));

    let parse_double_quote_fragment = alt((
        nom::combinator::map(not_double_quote_slash, |_| TokenKind::Value),
        nom::combinator::map(parse_escaped_double_quote, |_| TokenKind::Value),
        nom::combinator::map(tag("\\"), |_| TokenKind::IllegalDoubleQuoted),
    ));

    let parse_single_quote_fragment = alt((
        nom::combinator::map(not_single_quote_slash, |_| TokenKind::Value),
        nom::combinator::map(parse_escaped_single_quote, |_| TokenKind::Value),
        nom::combinator::map(tag("\\"), |_| TokenKind::IllegalSingleQuoted),
    ));

    let parse_single_quoted_value = nom::multi::fold(
        0..,
        parse_single_quote_fragment,
        || TokenKind::Value,
        |previous, kind| match (previous, kind) {
            (TokenKind::IllegalSingleQuoted, _) => TokenKind::IllegalSingleQuoted,
            (_, TokenKind::IllegalSingleQuoted) => TokenKind::IllegalSingleQuoted,
            (_, _) => kind,
        },
    );

    let parse_double_quoted_value = nom::multi::fold(
        0..,
        parse_double_quote_fragment,
        || TokenKind::Value,
        |previous, kind: TokenKind| match (previous, kind) {
            (TokenKind::IllegalDoubleQuoted, _) => TokenKind::IllegalDoubleQuoted,
            (_, TokenKind::IllegalDoubleQuoted) => TokenKind::IllegalDoubleQuoted,
            (_, _) => kind,
        },
    );

    let (rest, (first, token_kind, third)) = alt((
        (tag("'"), parse_single_quoted_value, cut(tag("'"))),
        (tag(r#"""#), parse_double_quoted_value, tag(r#"""#)),
    ))
    .parse(input)?;

    let parsed = Span { source: input.span.source, start: first.span.start, end: third.span.end };
    // shorten the input text by the length of parsed
    // this works because the original text is a slice [parsed.start..input.end] of the source
    // so we are taking the subslice [(parsed.start+0)..parsed.end] of the source
    let parsed =
        SpanView { span: parsed, text: &input.text[..(parsed.end - parsed.start) as usize] };
    Ok((rest, Token { span: parsed, kind: token_kind }))
}

#[derive(Debug)]
enum ParseTokenError {
    Error(String),
    TruncatedInput(nom::Needed),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TokenKind {
    /// field name, directly or single or double quoted
    Value,
    /// Single quoted value containing illegal characters
    IllegalSingleQuoted,
    /// Double quoted value containing illegal characters
    IllegalDoubleQuoted,
    /// Illegal character
    IllegalCharacter,
    /// number
    FloatValue,
    /// (
    LeftParens,
    /// )
    RightParens,
    /// [
    LeftSquareBracket,
    /// ]
    RightSquareBracket,
    /// OR
    Or,
    /// AND
    And,
    /// NOT
    Not,
    /// IN
    In,
    /// EXISTS
    Exists,
    /// IS
    Is,
    /// NULL
    Null,
    /// TO
    To,
    /// _geoRadius
    GeoRadius,
    /// _geoBoundingBox
    GeoBoundingBox,
    /// _geoPolygon
    GeoPolygon,
    /// _vectors
    Vectors,
    /// _foreign
    Foreign,
    /// ,
    Comma,
    /// =
    Equal,
    /// !=
    Different,
    /// >
    GreaterThan,
    /// >=
    GreaterOrEqual,
    /// <
    LowerThan,
    /// <=
    LowerOrEqual,
    /// End of input
    Eof,
}

impl TokenKind {}

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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// A view of a span that contains the text of that span.
pub struct SpanView<'a> {
    span: Span,
    text: &'a str,
}

impl<'a> SpanView<'a> {
    /// Build a view from a source and its handle, spanning the entire source
    fn from_entire_source(source_handle: SourceHandle, source: &'a FilterSource) -> Self {
        let span = Span::from_entire_source(source_handle, source);
        Self { span, text: &source.source }
    }

    /// A copy of the underlying span.
    pub fn span(&self) -> Span {
        self.span
    }

    /// build a span view from a span and the sources.
    ///
    /// The sources and the span must match
    pub fn from_sources(span: Span, sources: &'a FilterSources) -> Result<Self, SpanViewError> {
        let Some(source): Option<&FilterSource> = sources.sources.get(span.source as usize) else {
            return Err(SpanViewError::UnknownSourceHandle(span));
        };

        let Some(text) = source.source.get(span.start as usize..span.end as usize) else {
            return Err(SpanViewError::SpanOutOfRange(span));
        };

        Ok(Self { span, text })
    }

    /// If the text of this span is quoted in single or double quotes, then returns
    /// the inner portion, unescaping any contained `\'`, `\"` and `\\`.
    ///
    /// If any unescaping occurs, then the returned Cow will be owned. Otherwise, it will be borrowed.
    ///
    /// If the text of this span is not quoted, returns the entire text of the span as borrowed, without unescaping anything.
    pub fn unquote(&self) -> Cow<'a, str> {
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

    /// A reference to the text corresponding to this span in the underlying source.
    ///
    /// As the span might be quoted and contain escaped characters, the text returned by
    /// this method is not suitable for e.g. looking a field in a fields ids map.
    ///
    /// For these uses, the text returned by [`Self::unquote`] is suitable, but may incur an allocation.
    fn raw_possibly_escaped_text(&self) -> &'a str {
        self.text
    }
}

// nom trait implementations

impl<'a> Input for SpanView<'a> {
    type Item = <&'a str as Input>::Item;

    type Iter = <&'a str as Input>::Iter;

    type IterIndices = <&'a str as Input>::IterIndices;

    fn input_len(&self) -> usize {
        self.text.len()
    }

    fn take(&self, index: usize) -> Self {
        let split: u32 = self.span.start + u32::try_from(index).unwrap_wip();
        Self {
            text: Input::take(&self.text, index),
            span: Span { source: self.span.source, start: self.span.start, end: split },
        }
    }

    fn take_from(&self, index: usize) -> Self {
        let split: u32 = self.span.start + u32::try_from(index).unwrap_wip();
        Self {
            text: Input::take_from(&self.text, index),
            span: Span { source: self.span.source, start: split, end: self.span.end },
        }
    }

    fn take_split(&self, index: usize) -> (Self, Self) {
        let split: u32 = self.span.start + u32::try_from(index).unwrap_wip();
        let (after, before) = Input::take_split(&self.text, index);
        (
            Self {
                text: after,
                span: Span { source: self.span.source, start: split, end: self.span.end },
            },
            Self {
                text: before,
                span: Span { source: self.span.source, start: self.span.start, end: split },
            },
        )
    }

    fn position<P>(&self, predicate: P) -> Option<usize>
    where
        P: Fn(Self::Item) -> bool,
    {
        Input::position(&self.text, predicate)
    }

    fn iter_elements(&self) -> Self::Iter {
        Input::iter_elements(&self.text)
    }

    fn iter_indices(&self) -> Self::IterIndices {
        Input::iter_indices(&self.text)
    }

    fn slice_index(&self, count: usize) -> Result<usize, nom::Needed> {
        Input::slice_index(&self.text, count)
    }
}

impl<'a, 'b> nom::Compare<&'a str> for SpanView<'b> {
    fn compare(&self, t: &'a str) -> nom::CompareResult {
        nom::Compare::compare(&self.text, t)
    }

    fn compare_no_case(&self, t: &'a str) -> nom::CompareResult {
        nom::Compare::compare_no_case(&self.text, t)
    }
}

impl<'a> nom::Offset for SpanView<'a> {
    fn offset(&self, second: &Self) -> usize {
        let second = second.span.start as usize;
        let first = self.span.start as usize;
        second - first
    }
}

/// Error when trying to obtain a view inside a source.
///
/// Should typically be an internal error, as it happens if the span doesn't match the source,
/// which is a code error.
#[derive(Debug, Clone, Copy)]
pub enum SpanViewError {
    UnknownSourceHandle(Span),
    SpanOutOfRange(Span),
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
    wip::fixme!("foreign should be a terminal with id, and we should tuck the parsed instructions in a special object of the filter");
    wip::fixme!("source label should be a type rather than a string. perhaps a trait so that it can be injected from Meilisearch");
}

#[cfg(test)]
mod test {
    use crate::{FilterSource, Link, SpanView, Token};

    #[test]
    fn list_tokens() {
        let source = FilterSource {
            label: "".into(),
            source:
                r#"(doggo.name = Intel AND doggo.age > 12) OR "with spaces" != "some \"  \\ value" "#
                    .into(),
            previous_link: Link::And,
            next_down: 0,
        };
        let view = SpanView::from_entire_source(0, &source);

        for token in Token::iter_tokens(view) {
            let token = token.unwrap();
            dbg!(token);
        }
    }

    #[test]
    fn fail_tokens() {
        let source = FilterSource {
            label: "".into(),
            source: "illegal {}".into(),
            previous_link: Link::And,
            next_down: 0,
        };
        let view = SpanView::from_entire_source(0, &source);

        for token in Token::iter_tokens(view).take(10) {
            dbg!(&token);
            let token = token.unwrap();
        }
    }
}
