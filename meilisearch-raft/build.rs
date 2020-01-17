fn main() {
    tonic_build::compile_protos("proto/indexpb.proto").unwrap();
}
