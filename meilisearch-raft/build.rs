fn main() {
    prost_build::compile_protos(&["proto/indexpb.proto"]).unwrap();
}
