use build_info::BuildInfo;

fn main() {
    let info = BuildInfo::from_build();
    dbg!(info);
}
