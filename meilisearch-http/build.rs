use vergen::{generate_cargo_keys, ConstantsFlags};

fn main() {
    // Setup the flags, toggling off the 'SEMVER_FROM_CARGO_PKG' flag
    let mut flags = ConstantsFlags::all();
    flags.toggle(ConstantsFlags::SEMVER_FROM_CARGO_PKG);

    // Generate the 'cargo:' key output
    generate_cargo_keys(ConstantsFlags::all()).expect("Unable to generate the cargo keys!");

    // gRPC codegen
    tonic_build::configure()
        .build_client(true)
        .build_server(true)
        .compile(&["proto/raft_service.proto"], &["proto/"])
        .expect("error compiling proto");
}
