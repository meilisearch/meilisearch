use std::fmt;
use std::sync::Arc;

use sdset::SetBuf;
use slice_group_by::GroupBy;
use log::debug;

use crate::{DocumentId, Highlight, TmpMatch, AttrCount};

#[derive(Clone)]
pub struct RawDocument {
    pub id: DocumentId,
    pub matches: SharedMatches,
    pub highlights: Vec<Highlight>,
    pub fields_counts: Option<SetBuf<AttrCount>>,
}

impl RawDocument {
    pub fn query_index(&self) -> &[u32] {
        let r = self.matches.range;
        // it is safe because construction/modifications
        // can only be done in this module
        unsafe {
            &self
                .matches
                .matches
                .query_index
                .get_unchecked(r.start..r.end)
        }
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
        unsafe {
            &self
                .matches
                .matches
                .word_index
                .get_unchecked(r.start..r.end)
        }
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
        f.write_fmt(format_args!("{:>15}: {:?},\r\n", "id", self.id))?;
        f.write_fmt(format_args!(
            "{:>15}: {:^5?},\r\n",
            "query_index",
            self.query_index()
        ))?;
        f.write_fmt(format_args!(
            "{:>15}: {:^5?},\r\n",
            "distance",
            self.distance()
        ))?;
        f.write_fmt(format_args!(
            "{:>15}: {:^5?},\r\n",
            "attribute",
            self.attribute()
        ))?;
        f.write_fmt(format_args!(
            "{:>15}: {:^5?},\r\n",
            "word_index",
            self.word_index()
        ))?;
        f.write_fmt(format_args!(
            "{:>15}: {:^5?},\r\n",
            "is_exact",
            self.is_exact()
        ))?;
        f.write_str("}")?;
        Ok(())
    }
}

pub fn raw_documents_from(
    matches: SetBuf<(DocumentId, TmpMatch)>,
    highlights: SetBuf<(DocumentId, Highlight)>
) -> Vec<RawDocument> {
    let mut docs_ranges: Vec<(_, Range, _, _)> = Vec::new();
    let mut matches2 = Matches::with_capacity(matches.len());

    let matches = matches.linear_group_by_key(|(id, _)| *id);
    let highlights = highlights.linear_group_by_key(|(id, _)| *id);

    let mut loops_count = 0;

    for (mgroup, hgroup) in matches.zip(highlights) {
        loops_count += 1;
        assert_eq!(mgroup[0].0, hgroup[0].0);

        let document_id = mgroup[0].0;
        let start = docs_ranges.last().map(|(_, r, _, _)| r.end).unwrap_or(0);
        let end = start + mgroup.len();
        let highlights = hgroup.iter().map(|(_, h)| *h).collect();
        let fields_counts = None;

        docs_ranges.push((document_id, Range { start, end }, highlights, fields_counts));
        // TODO we could try to keep both data
        //  - the data oriented one and the raw one,
        //  - the one that comes from the arguments of this function
        // This way we would be able to only produce data oriented lazily.
        //
        // For example the default first criterion is `SumOfTypos`
        // and just needs the `query_index` and the `distance` fields.
        // It would probably be good to avoid wasting time sorting other fields of documents
        // that will never ever reach the second criterion.
        matches2.extend_from_slice(mgroup);
    }

    debug!("loops_counts number is {}", loops_count);

    let matches = Arc::new(matches2);
    docs_ranges
        .into_iter()
        .map(|(id, range, highlights, fields_counts)| {
            let matches = SharedMatches { range, matches: matches.clone() };
            RawDocument { id, matches, highlights, fields_counts }
        })
        .collect()
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

    fn extend_from_slice(&mut self, matches: &[(DocumentId, TmpMatch)]) {
        for (_, match_) in matches {
            self.query_index.push(match_.query_index);
            self.distance.push(match_.distance);
            self.attribute.push(match_.attribute);
            self.word_index.push(match_.word_index);
            self.is_exact.push(match_.is_exact);
        }
    }
}
