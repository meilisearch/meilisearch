use obkv::Key;

pub type KvWriterDelAdd<W> = obkv::KvWriter<W, DelAdd>;
pub type KvReaderDelAdd<'a> = obkv::KvReader<'a, DelAdd>;

/// DelAdd defines the new value to add in the database and old value to delete from the database.
///
/// Its used in an OBKV to be serialized in grenad files.
#[repr(u8)]
#[derive(Clone, Copy, PartialOrd, PartialEq, Debug)]
pub enum DelAdd {
    Deletion = 0,
    Addition = 1,
}

impl Key for DelAdd {
    const BYTES_SIZE: usize = std::mem::size_of::<DelAdd>();
    type BYTES = [u8; Self::BYTES_SIZE];

    fn to_be_bytes(&self) -> Self::BYTES {
        u8::to_be_bytes(*self as u8)
    }

    fn from_be_bytes(array: Self::BYTES) -> Self {
        match u8::from_be_bytes(array) {
            0 => Self::Deletion,
            1 => Self::Addition,
            otherwise => unreachable!("DelAdd has only 2 variants, unknown variant: {}", otherwise),
        }
    }
}

/// Creates a Kv<K, Kv<DelAdd, value>> from Kv<K, value>
///
/// if deletion is `true`, the value will be inserted behind a DelAdd::Deletion key.
/// if addition is `true`, the value will be inserted behind a DelAdd::Addition key.
/// if both deletion and addition are `true, the value will be inserted in both keys.
pub fn into_del_add_obkv<K: obkv::Key + PartialOrd>(
    reader: obkv::KvReader<K>,
    deletion: bool,
    addition: bool,
    buffer: &mut Vec<u8>,
) -> Result<(), std::io::Error> {
    let mut writer = obkv::KvWriter::new(buffer);
    let mut value_buffer = Vec::new();
    for (key, value) in reader.iter() {
        value_buffer.clear();
        let mut value_writer = KvWriterDelAdd::new(&mut value_buffer);
        if deletion {
            value_writer.insert(DelAdd::Deletion, value)?;
        }
        if addition {
            value_writer.insert(DelAdd::Addition, value)?;
        }
        value_writer.finish()?;
        writer.insert(key, &value_buffer)?;
    }

    writer.finish()
}
