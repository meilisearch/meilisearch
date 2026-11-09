#![allow(clippy::disallowed_macros)]
use build_info::BuildInfo;

fn main() {
    let info = BuildInfo::from_build();
    dbg!(info);
}
