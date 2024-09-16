mod extract_facets;
mod facet_document;

pub use extract_facets::FacetedDocidsExtractor;

#[repr(u8)]
pub enum FacetKind {
    Number = 0,
    String = 1,
    Null = 2,
    Empty = 3,
    Exists,
}

impl From<u8> for FacetKind {
    fn from(value: u8) -> Self {
        match value {
            0 => Self::Number,
            1 => Self::String,
            2 => Self::Null,
            3 => Self::Empty,
            4 => Self::Exists,
            _ => unreachable!(),
        }
    }
}
