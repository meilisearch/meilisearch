use protobuf::{self, Message};

pub fn parse_data<T: Message>(data: &[u8]) -> T {
    protobuf::parse_from_bytes::<T>(data).unwrap_or_else(|e| {
        panic!("data is corrupted: {:?}", e);
    })
}
