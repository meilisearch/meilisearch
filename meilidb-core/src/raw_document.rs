use std::sync::Arc;
use std::fmt;
use sdset::SetBuf;
use slice_group_by::GroupBy;
use crate::{TmpMatch, DocumentId, Highlight};

#[derive(Clone)]
pub struct RawDocument {
    pub id: DocumentId,
    pub matches: SharedMatches,
    pub highlights: Vec<Highlight>,
}

impl RawDocument {
    fn new(id: DocumentId, matches: SharedMatches, highlights: Vec<Highlight>) -> RawDocument {
        RawDocument { id, matches, highlights }
    }

    pub fn query_index(&self) -> &[u32] {
        let r = self.matches.range;
        // it is safe because construction/modifications
        // can only be done in this module
        unsafe { &self.matches.matches.query_index.get_unchecked(r.start..r.end) }
    }

    pub fn distance(&self) -> &[u8] {
        let r = self.matches.range;
        // it is safe because construction/modifications
        // can only be done in this module
        unsafe { &self.matches.matches.distance.get_unchecked(r.start..r.end) }
    }

    pub fn attribute(&self) -> &[u16] {
        let r = self.matches.range;
        // it is safe because construction/modifications
        // can only be done in this module
        unsafe { &self.matches.matches.attribute.get_unchecked(r.start..r.end) }
    }

    pub fn word_index(&self) -> &[u16] {
        let r = self.matches.range;
        // it is safe because construction/modifications
        // can only be done in this module
        unsafe { &self.matches.matches.word_index.get_unchecked(r.start..r.end) }
    }

    pub fn is_exact(&self) -> &[bool] {
        let r = self.matches.range;
        // it is safe because construction/modifications
        // can only be done in this module
        unsafe { &self.matches.matches.is_exact.get_unchecked(r.start..r.end) }
    }
}

impl fmt::Debug for RawDocument {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("RawDocument {\r\n")?;
        f.write_fmt(format_args!("{:>15}: {:?},\r\n",    "id",          self.id))?;
        f.write_fmt(format_args!("{:>15}: {:^5?},\r\n",  "query_index", self.query_index()))?;
        f.write_fmt(format_args!("{:>15}: {:^5?},\r\n",  "distance",    self.distance()))?;
        f.write_fmt(format_args!("{:>15}: {:^5?},\r\n",  "attribute",   self.attribute()))?;
        f.write_fmt(format_args!("{:>15}: {:^5?},\r\n",  "word_index",  self.word_index()))?;
        f.write_fmt(format_args!("{:>15}: {:^5?},\r\n", "is_exact",    self.is_exact()))?;
        f.write_str("}")?;
        Ok(())
    }
}

pub fn raw_documents_from_matches(matches: SetBuf<(DocumentId, TmpMatch, Highlight)>) -> Vec<RawDocument> {
    let mut docs_ranges: Vec<(_, Range, _)> = Vec::new();
    let mut matches2 = Matches::with_capacity(matches.len());

    for group in matches.linear_group_by(|(a, _, _), (b, _, _)| a == b) {
        let document_id = group[0].0;
        let start = docs_ranges.last().map(|(_, r, _)| r.end).unwrap_or(0);
        let end = start + group.len();

        let highlights = group.iter().map(|(_, _, h)| *h).collect();
        docs_ranges.push((document_id, Range { start, end }, highlights));

        matches2.extend_from_slice(group);
    }

    let matches = Arc::new(matches2);
    docs_ranges.into_iter().map(|(i, range, highlights)| {
        let matches = SharedMatches { range, matches: matches.clone() };
        RawDocument::new(i, matches, highlights)
    }).collect()
}

#[derive(Debug, Copy, Clone)]
struct Range {
    start: usize,
    end: usize,
}

#[derive(Clone)]
pub struct SharedMatches {
    range: Range,
    matches: Arc<Matches>,
}

#[derive(Clone)]
struct Matches {
    query_index: Vec<u32>,
    distance: Vec<u8>,
    attribute: Vec<u16>,
    word_index: Vec<u16>,
    is_exact: Vec<bool>,
}

impl Matches {
    fn with_capacity(cap: usize) -> Matches {
        Matches {
            query_index: Vec::with_capacity(cap),
            distance: Vec::with_capacity(cap),
            attribute: Vec::with_capacity(cap),
            word_index: Vec::with_capacity(cap),
            is_exact: Vec::with_capacity(cap),
        }
    }

    fn extend_from_slice(&mut self, matches: &[(DocumentId, TmpMatch, Highlight)]) {
        for (_, match_, _) in matches {
            self.query_index.push(match_.query_index);
            self.distance.push(match_.distance);
            self.attribute.push(match_.attribute);
            self.word_index.push(match_.word_index);
            self.is_exact.push(match_.is_exact);
        }
    }
}
