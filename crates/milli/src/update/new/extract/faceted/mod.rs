mod extract_facets;
mod facet_document;
mod field_facet_status;

pub use extract_facets::FacetedDocidsExtractor;
pub use field_facet_status::FieldFacetStatus;

#[repr(u8)]
#[derive(Debug, Clone, Copy)]
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

impl FacetKind {
    pub fn extract_from_key(key: &[u8]) -> (FacetKind, &[u8]) {
        (FacetKind::from(key[0]), &key[1..])
    }
}
