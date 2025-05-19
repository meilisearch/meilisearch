pub const VERSION_MAJOR: u32 = parse_u32(env!("CARGO_PKG_VERSION_MAJOR"));
pub const VERSION_MINOR: u32 = parse_u32(env!("CARGO_PKG_VERSION_MINOR"));
pub const VERSION_PATCH: u32 = parse_u32(env!("CARGO_PKG_VERSION_PATCH"));

const fn parse_u32(s: &str) -> u32 {
    match u32::from_str_radix(s, 10) {
        Ok(version) => version,
        Err(_) => panic!("could not parse as u32"),
    }
}

pub const RESERVED_VECTORS_FIELD_NAME: &str = "_vectors";
pub const RESERVED_GEO_FIELD_NAME: &str = "_geo";
