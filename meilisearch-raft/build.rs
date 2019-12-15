fn main() {
    tonic_build::compile_protos("proto/raft.proto").unwrap();
}
