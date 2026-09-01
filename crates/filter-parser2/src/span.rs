use std::borrow::Cow;

use nom::Input;
use wip::WipResultExt;

use crate::{FilterSource, FilterSources, SourceHandle};

/// A reference inside of a source.
///
/// Consists of a handle to the source and the span inside the source.
/// The span must be smaller than 4GB.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
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
        unquote(self.text)
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

fn unquote<'a>(text: &'a str) -> Cow<'a, str> {
    if text.len() < 2 {
        return Cow::Borrowed(text);
    }
    if text.starts_with('\'') && text.ends_with('\'') {
        let inner = &text[1..(text.len() - 1)];
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
    } else if text.starts_with('"') && text.ends_with('"') {
        let inner = &text[1..(text.len() - 1)];
        if !inner.contains('\\') {
            return Cow::Borrowed(inner);
        }
        let inner: String = serde_json::from_str(text).unwrap();
        Cow::Owned(inner)
    } else {
        Cow::Borrowed(text)
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

#[cfg(test)]
mod tests {
    use crate::span::unquote;

    enum Cow {
        Borrowed,
        Owned,
    }

    fn check_unquote(text: &str, expected: &str, cow: Cow) {
        let unquoted = unquote(text);
        assert_eq!(unquoted, expected);
        match cow {
            Cow::Borrowed => assert!(matches!(unquoted, std::borrow::Cow::Borrowed(_))),
            Cow::Owned => assert!(matches!(unquoted, std::borrow::Cow::Owned(_))),
        }
    }

    #[test]
    fn unquote_works() {
        // short text always borrowed
        check_unquote("t", "t", Cow::Borrowed);
        check_unquote("\"", "\"", Cow::Borrowed);

        // no quote: unchanged and borrowed
        check_unquote("toto", "toto", Cow::Borrowed);
        // unbalanced quotes: unchanged and borrowed
        check_unquote("\"toto", "\"toto", Cow::Borrowed);
        check_unquote("toto\"", "toto\"", Cow::Borrowed);
        check_unquote("'toto", "'toto", Cow::Borrowed);
        check_unquote("toto'", "toto'", Cow::Borrowed);

        // quotes with no inner escape: unquoted and borrowed
        check_unquote("\"toto\"", "toto", Cow::Borrowed);
        check_unquote("'toto'", "toto", Cow::Borrowed);

        // quotes with inner escape: replaced and owned
        check_unquote("\"to\\\\to\"", "to\\to", Cow::Owned);
        check_unquote("'to\\\\to'", "to\\to", Cow::Owned);
        check_unquote("\"to\\\"to\"", "to\"to", Cow::Owned);
        check_unquote("'to\\\"to'", "to\"to", Cow::Owned);

        // should not parse in actual filter as it would contain a char illegal outside of quotes
        // unchanged because not inside quotes
        check_unquote("to\"to", "to\"to", Cow::Borrowed);
        check_unquote("to'to", "to'to", Cow::Borrowed);
        check_unquote("to\\\\to", "to\\\\to", Cow::Borrowed);
    }
}
