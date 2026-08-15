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

use crate::{FilterSource, FilterSources, SourceHandle};

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
    pub(crate) fn from_entire_source(source_handle: SourceHandle, source: &FilterSource) -> Self {
        let end = source.source.len().try_into().unwrap_wip();
        Self { source: source_handle, start: 0, end }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// A view of a span that contains the text of that span.
pub struct SpanView<'a> {
    span: Span,
    text: &'a str,
}

impl<'a> SpanView<'a> {
    /// Build a view from a source and its handle, spanning the entire source
    pub(crate) fn from_entire_source(
        source_handle: SourceHandle,
        source: &'a FilterSource,
    ) -> Self {
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

    /// Given two spans, returns a new span
    /// whose end is the minimum of the two spans
    ///
    /// This method assumes and **does not check** the following:
    ///
    /// 1. The two spans refer to the same source
    /// 2. One of the two spans is included in the other
    pub(crate) fn earliest_end(larger: Self, smaller: Self) -> Self {
        let span =
            Span { source: larger.span.source, start: larger.span.start, end: smaller.span.end };
        SpanView { span, text: &larger.text[..(span.end - span.start) as usize] }
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
    pub fn raw_possibly_escaped_text(&self) -> &'a str {
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
