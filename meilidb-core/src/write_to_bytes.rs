pub trait WriteToBytes {
    fn write_to_bytes(&self, bytes: &mut Vec<u8>);

    fn into_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        self.write_to_bytes(&mut bytes);
        bytes
    }
}
