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
//! - Parsing starts with tokenization, then tokens are used to built [`Terminal`]s separated by their [`Link`]s (AND, OR), and
//! finally a semantics ties the knot.
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

use crate::token::{ParseOutput, Token, TokenKind};

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

            match terminal {
                Terminal::VectorExists { embedder, filter } => semantics.vector_exists(
                    embedder.as_ref().map(view),
                    VectorFilterView::from_sources(*filter, &self.sources).unwrap(),
                )?,
                Terminal::GeoLowerThan { point, radius, resolution } => semantics
                    .geo_lower_than(
                        point_view(point),
                        view(radius),
                        resolution.as_ref().map(view),
                    )?,
                Terminal::GeoBoundingBox { top_right_point, bottom_left_point } => semantics
                    .geo_bounding_box(point_view(top_right_point), point_view(bottom_left_point))?,
                Terminal::GeoPolygon { points } => semantics
                    .geo_polygon(points.iter().map(|point| point_view(point)), points.len())?,
                Terminal::GreaterThan { left, right } => {
                    semantics.greater_than(view(left), view(right))?
                }
                Terminal::GreaterThanOrEqual { left, right } => {
                    semantics.greater_than_or_equal(view(left), view(right))?;
                }
                Terminal::Equal { left, right } => {
                    semantics.equal(view(left), view(right))?;
                }
                Terminal::Null { operand } => {
                    semantics.null(view(operand))?;
                }
                Terminal::Empty { operand } => {
                    semantics.empty(view(operand))?;
                }
                Terminal::Exists { operand } => {
                    semantics.exists(view(operand))?;
                }
                Terminal::LowerThan { left, right } => {
                    semantics.lower_than(view(left), view(right))?;
                }
                Terminal::LowerThanOrEqual { left, right } => {
                    semantics.lower_than_or_equal(view(left), view(right))?;
                }
                Terminal::Between { operand, lower, upper } => {
                    semantics.between(view(operand), view(lower), view(upper))?;
                }
                Terminal::Contains { left, right } => {
                    semantics.contains(view(left), view(right))?;
                }
                Terminal::StartsWith { left, right } => {
                    semantics.starts_with(view(left), view(right))?;
                }
                Terminal::ForeignGroup { foreign_field, foreign_source } => {
                    semantics.foreign(view(foreign_field), view(foreign_source))?;
                }
                Terminal::In {
                    operand,
                    values,
                } => {
                    semantics.in_range(view(operand), values.iter().map(|value| view(value)))?;
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

    fn in_range<'a, I: Iterator<Item=SpanView<'a>>>(&mut self, operand: SpanView<'a>, values: I) -> Result<(), Self::Error>;
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

struct OpenParen<'a> {
    paren: SpanView<'a>,
    polarity: bool,
    is_foreign: bool,
    has_associative_priority: bool,
}

struct ParsingContext<'a> {
    open_parens: Vec<OpenParen<'a>>,
    open_brackets: Vec<SpanView<'a>>,
    previous_link: Link,
    has_associative_priority: bool,
    allows_empty_terminal: bool,
    input: SpanView<'a>,
}

struct SourceParser<'a> {
    source: &'a FilterSource,
    source_handle: SourceHandle,
    current: ParsingContext<'a>,
    nested: Vec<ParsingContext<'a>>,
}

impl<'a> ParsingContext<'a> {
    fn new(parse: SpanView<'a>) -> Self {
        Self { previous_link: Link::And, input: parse, ..wip::wip!() }
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

    fn advance_to_next_token(&mut self) -> Token<'a> {
        let ParseOutput { parsed_token, remaining_input } = Token::parse_next(self.current.input);
        self.current.input = remaining_input;
        parsed_token
    }

    fn parse_next_instruction_or_eof(
        &mut self,
    ) -> Result<Option<Instruction>, ParseInstructionError> {
        let Some(terminal) = self.parse_next_terminal_or_eof()? else {
            // eof
            if !self.current.allows_empty_terminal {
                return Err(ParseInstructionError::DanglingLink);
            }
            self.check_eof()?;

            return Ok(None);
        };

        let Some(forward_link) = self.parse_next_link_or_eof()? else {
            self.check_eof()?;
            return Ok(None);
        };

        let previous_link = std::mem::replace(&mut self.current.previous_link, forward_link);

        let next_instruction =
            Instruction { origin: wip::wip!(), terminal, previous_link, next_down: wip::wip!() };
        wip::fixme!("push and save state as necessary");
        Ok(Some(next_instruction))
    }

    fn check_eof(&self) -> Result<(), ParseInstructionError> {
        if !self.current.open_parens.is_empty() {
            return Err(ParseInstructionError::UnmatchedParens);
        }
        if !self.current.open_brackets.is_empty() {
            return Err(ParseInstructionError::UnmatchedBracket);
        }
        wip::fixme!("what about nested?");
        wip::fixme!("add token data of the unmatched parens/bracket to the error variant");
        Ok(())
    }

    fn parse_next_terminal_or_eof(&mut self) -> Result<Option<PolarizedTerminal>, ParseInstructionError> {
        let mut polarity = true;
        let terminal = loop {
            let next_token = self.advance_to_next_token();
            match next_token.kind {
            TokenKind::Value | TokenKind::FloatValue => {
                break self.parse_next_operator_or_second_value(next_token, polarity)?
            },
            TokenKind::IllegalSingleQuoted => {
                wip::wip!("illegal single quoted")
            },
            TokenKind::IllegalDoubleQuoted => wip::wip!("illegal double quoted"),
            TokenKind::IllegalCharacter => wip::wip!("illegal character"),
            TokenKind::LeftParens => {
                wip::fixme!("push state");
                continue;
            }
            TokenKind::RightParens => wip::wip!("legal or not? would imagine not... if legal, pop state"),
            TokenKind::LeftSquareBracket => wip::wip!("illegal bracket, dym ("),
            TokenKind::RightSquareBracket => wip::wip!("illegal bracket, dym )"),
            TokenKind::Not => {self.current.allows_empty_terminal = false;
                polarity = !polarity ;
        continue;},
            TokenKind::Or |
            TokenKind::And |
            TokenKind::In |
            TokenKind::Exists |
            TokenKind::Is |
            TokenKind::Null |
            TokenKind::Equal |
            TokenKind::Different |
            TokenKind::GreaterThan |
            TokenKind::GreaterOrEqual |
            TokenKind::LowerThan |
            TokenKind::LowerOrEqual |
            TokenKind::To => wip::wip!("illegal operator/link, put a value first! if your intended value is a keyword, quote it"),
            TokenKind::GeoRadius => break self.parse_next_geo_radius(next_token, polarity)?,
            TokenKind::GeoBoundingBox => break self.parse_next_geo_bounding_box(next_token, polarity)?,
            TokenKind::GeoPolygon => break self.parse_next_geo_polygon(next_token, polarity)?,
            TokenKind::Vectors => break self.parse_next_vectors(next_token, polarity)?,
            TokenKind::Foreign => break self.parse_next_foreign(next_token, polarity)?,
            TokenKind::Comma => wip::wip!("illegal comma"),
            TokenKind::Eof => return Ok(None),
        }
        };
        Ok(Some(terminal))
    }

    fn parse_next_link_or_eof(&mut self) -> Result<Option<Link>, ParseInstructionError> {
        wip::wip!()
    }

    fn parse_next_operator_or_second_value(
        &mut self,
        first_value: Token<'a>,
        polarity: bool,
    ) -> Result<PolarizedTerminal, ParseInstructionError> {
        wip::fixme!("missing token kind: CONTAINS, STARTS, WITH");
        let next_token = self.advance_to_next_token();
        Ok(match next_token.kind {
            TokenKind::Value | TokenKind::FloatValue => {
                self.parse_next_to(first_value, next_token, polarity)?
            }
            TokenKind::IllegalSingleQuoted => wip::wip!("illegal"),
            TokenKind::IllegalDoubleQuoted => wip::wip!("illegal"),
            TokenKind::IllegalCharacter => wip::wip!("illegal"),
            TokenKind::LeftParens => wip::wip!("illegal"),
            TokenKind::RightParens => {
                wip::wip!("illegal, either stray paren or missing an operator etc")
            }
            TokenKind::LeftSquareBracket => wip::wip!("illegal"),
            TokenKind::RightSquareBracket => wip::wip!("illegal, either stray or plain illegal"),
            TokenKind::Not => self.parse_not_operator(first_value, next_token, polarity)?,
            TokenKind::In => self.parse_next_in(first_value, next_token, polarity)?,
            TokenKind::Exists => PolarizedTerminal { polarity, terminal: Terminal::Exists { operand: first_value.span_view.span() } },
            TokenKind::Or | TokenKind::And => wip::wip!("illegal, missing thing"),
            TokenKind::Is => self.parse_next_is(first_value, next_token, polarity)?,
            TokenKind::Null => wip::wip!("illegal, missing IS"),
            TokenKind::To => wip::wip!("illegal, missing second value"),
            TokenKind::GeoRadius |
            TokenKind::GeoBoundingBox |
            TokenKind::GeoPolygon |
            TokenKind::Vectors |
            TokenKind::Foreign => wip::wip!("illegal, if second value use quotes"),,
            TokenKind::Comma => wip::wip!("illegal"),
            TokenKind::Equal |
            TokenKind::Different |
            TokenKind::GreaterThan |
            TokenKind::GreaterOrEqual |
            TokenKind::LowerThan |
            TokenKind::LowerOrEqual => self.parse_next_right_hand(first_value, next_token, polarity)?,
            TokenKind::Eof => wip::wip!("illegal, missing operator"),
        })
    }

    fn parse_next_geo_radius(
        &mut self,
        reserved_field: Token<'a>,
        polarity: bool
    ) -> Result<PolarizedTerminal, ParseInstructionError> {
        wip::wip!()
    }

    fn parse_next_geo_bounding_box(
        &mut self,
        reserved_field: Token<'a>,
        polarity: bool
    ) -> Result<PolarizedTerminal, ParseInstructionError> {
        wip::wip!()
    }

    fn parse_next_geo_polygon(
        &mut self,
        reserved_field: Token<'a>,
        polarity: bool
    ) -> Result<PolarizedTerminal, ParseInstructionError> {
        wip::wip!()
    }

    fn parse_next_vectors(
        &mut self,
        reserved_field: Token<'a>,
        polarity: bool
    ) -> Result<PolarizedTerminal, ParseInstructionError> {
        wip::wip!()
    }

    fn parse_next_foreign(
        &mut self,
        reserved_field: Token<'a>,
        polarity: bool
    ) -> Result<PolarizedTerminal, ParseInstructionError> {
        wip::wip!()
    }

    fn parse_next_to(&mut self, first_value: Token<'a>, from_value: Token<'a>,
polarity: bool) -> Result<PolarizedTerminal, ParseInstructionError> {
        let to_token = self.advance_to_next_token();
        if let TokenKind::To = to_token.kind {
            let to_value = self.advance_to_next_token();
            if !to_value.kind.is_value() {
                wip::wip!("explain value value TO value syntax")
            }
            Ok(PolarizedTerminal { polarity, terminal: Terminal::Between { operand: first_value.span(), lower: from_value.span(), upper: to_value.span() } })
        } else {
            wip::wip!("check first_value == from_value, if so might be accidental repetition. Otherwise explains the value value TO value syntax")
        }
    }

    fn parse_not_operator(&mut self, first_value: Token<'a>, not_keyword: Token<'a>, polarity: bool) -> Result<PolarizedTerminal, ParseInstructionError> {
        let operator = self.advance_to_next_token();
        Ok(match operator.kind {
            TokenKind::Value |
            TokenKind::FloatValue => wip::wip!("illegal value"),
            TokenKind::Not => wip::wip!("duplicate not"),
            TokenKind::Or |
            TokenKind::And => wip::wip!("duplicate and"),
            TokenKind::In => self.parse_next_in(first_value, operator, !polarity)?,
            TokenKind::Exists => PolarizedTerminal { polarity: !polarity, terminal: Terminal::Exists { operand: first_value.span() } },
            TokenKind::Is => wip::wip!("incorrect syntax: dym value IS NOT _"),
            TokenKind::Null => wip::wip!("correct syntax: IS NOT NULL, missing IS"),
            TokenKind::To => wip::wip!("incorrect syntax: NOT value value TO value"),
            TokenKind::Equal => wip::wip!("incorrect: dym value != _"),
            TokenKind::Different => wip::wip!("incorrect: dym value = _?"),
            TokenKind::GreaterThan |
            TokenKind::GreaterOrEqual |
            TokenKind::LowerThan |
            TokenKind::LowerOrEqual => wip::wip!("incorrect: dym NOT value <OP> _?"),
            TokenKind::LeftParens => wip::wip!("illegal parens here"),
            TokenKind::RightParens => wip::wip!("illegal parens here"),
            TokenKind::LeftSquareBracket => wip::wip!("illegal bracket here"),
            TokenKind::RightSquareBracket => wip::wip!("illegal bracket here"),
            TokenKind::Comma => wip::wip!("illegal comma here"),
            TokenKind::GeoRadius |
            TokenKind::GeoBoundingBox |
            TokenKind::GeoPolygon |
            TokenKind::Vectors |
            TokenKind::Foreign => wip::wip!("illegal reserved field. extraneous value or did you mean to use it as a value?"),
            TokenKind::IllegalSingleQuoted |
            TokenKind::IllegalDoubleQuoted |
            TokenKind::IllegalCharacter => wip::wip!("illegal character"),
            TokenKind::Eof => wip::wip!("truncated input"),
        })
    }

    fn parse_next_in(&mut self, first_value: Token<'a>, in_keyword: Token<'a>, polarity: bool) -> Result<PolarizedTerminal, ParseInstructionError> {
        let left_bracket = self.advance_to_next_token();
        if left_bracket.kind != TokenKind::LeftSquareBracket {
            wip::wip!("expected left bracket: _ IN [ _, _, _ ]");
        }
        let mut values = vec![];
        loop {
            let value_or_bracket = self.advance_to_next_token();
            if value_or_bracket.kind == TokenKind::RightSquareBracket {
                break;
            }
            if value_or_bracket.kind == TokenKind::Comma {
                wip::wip!("missing value")
            }
            if !value_or_bracket.kind.is_value() {
                wip::wip!("expected value")
            }
            values.push(value_or_bracket.span());
            let comma_or_bracket = self.advance_to_next_token();
            if comma_or_bracket.kind == TokenKind::RightSquareBracket {
                break;
            }
            if comma_or_bracket.kind.is_value() {
                wip::wip!("missing comma")
            }
            if comma_or_bracket.kind != TokenKind::Comma {
                wip::wip!("expected comma")
            }
        }

        Ok(PolarizedTerminal { terminal: Terminal::In { operand: first_value.span(), values }, polarity })
    }

    fn parse_next_is(&mut self, first_value: Token<'a>, is_keyword: Token<'a>, polarity: bool) -> Result<PolarizedTerminal, ParseInstructionError>  {
        let next_token = self.advance_to_next_token();
        Ok(match next_token.kind {
            TokenKind::Value => _,
            TokenKind::FloatValue => _,
            TokenKind::Not => _,
            TokenKind::Or => _,
            TokenKind::And => _,
            TokenKind::In => _,
            TokenKind::Exists => _,
            TokenKind::Is => _,
            TokenKind::Null => _,
            TokenKind::To => _,
            TokenKind::Equal => _,
            TokenKind::Different => _,
            TokenKind::GreaterThan => _,
            TokenKind::GreaterOrEqual => _,
            TokenKind::LowerThan => _,
            TokenKind::LowerOrEqual => _,
            TokenKind::LeftParens => _,
            TokenKind::RightParens => _,
            TokenKind::LeftSquareBracket => _,
            TokenKind::RightSquareBracket => _,
            TokenKind::Comma => _,
            TokenKind::GeoRadius => _,
            TokenKind::GeoBoundingBox => _,
            TokenKind::GeoPolygon => _,
            TokenKind::Vectors => _,
            TokenKind::Foreign => _,
            TokenKind::IllegalSingleQuoted => _,
            TokenKind::IllegalDoubleQuoted => _,
            TokenKind::IllegalCharacter => _,
            TokenKind::Eof => _,
        })
    }
}

impl<'a> Iterator for SourceParser<'a> {
    type Item = Result<Instruction, ParseInstructionError>;

    /// Parses the next instruction
    fn next(&mut self) -> Option<Self::Item> {
        wip::fixme!(
            "review 'switch polarity' verbiage when the link can indicate absolute polarity"
        );
        wip::fixme!("address distributivy, associativity and de morgan's law: NOT (a AND b) <=> NOT a OR NOT b");
        self.parse_next_instruction_or_eof().transpose()
        // 1. parse terminal
        // 2. parse forward link or eof
        // 3. forcefully push state if:
        //    1. previous link is a non push AND
        //    2. next link is an OR
        // 4. save state:
        //    1. remaining input
        //    2. whether we forcefully pushed
        //    3. polarity
        //    4. unclosed parens
        //    5. unclosed brackets
        // 5. allow eof if:
        //    1. no unclosed stuff
        //    2. in link context or instead of an empty source

        // NOT (a AND b OR c) <=> NOT (a AND (b OR c)) <=> NOT a OR NOT (b OR c) <=> NOT a OR (NOT b AND NOT c)
        // => it seems to work as follow: 1. de morgan's still replace semantics of AND to OR, but not associations
        // in terms of implementation, just need to know about the parens' polarity, and can proceed as usual
        //
        // double polarity: cancels as expected
        //
        // NOT (a AND NOT (b OR c)) <=> NOT a OR NOT NOT (b OR c) <=> NOT a OR NOT (NOT b AND NOT c) <=> NOT a OR (NOT NOT b OR NOT NOT c) <=> NOT a OR (b OR c)

    }
}

#[derive(Debug, Clone)]
pub enum ParseInstructionError {
    DanglingLink,
    UnmatchedParens,
    UnmatchedBracket,
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

#[derive(Debug, Clone)]
struct PolarizedTerminal {
    terminal: Terminal,
    polarity: bool,
}

/// A terminal typically represents the leaf objects of a filter
///
/// In Meilisearch's case, it generally resolves to roaring bitmaps representing lists of docids.
#[derive(Debug, Clone)]
enum Terminal {
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
    In {operand: Span, values: Vec<Span> },
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
