use obkv::Key;

pub type KvWriterDelAdd<W> = obkv::KvWriter<W, DelAdd>;
pub type KvReaderDelAdd = obkv::KvReader<DelAdd>;

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
/// Deletion: put all the values under DelAdd::Deletion
/// Addition: put all the values under DelAdd::Addition,
/// DeletionAndAddition: put all the values under DelAdd::Deletion and DelAdd::Addition,
pub fn into_del_add_obkv<K: obkv::Key + PartialOrd>(
    reader: &obkv::KvReader<K>,
    operation: DelAddOperation,
    buffer: &mut Vec<u8>,
) -> Result<(), std::io::Error> {
    into_del_add_obkv_conditional_operation(reader, buffer, |_| operation)
}

/// Akin to the [into_del_add_obkv] function but lets you
/// conditionally define the `DelAdd` variant based on the obkv key.
pub fn into_del_add_obkv_conditional_operation<K, F>(
    reader: &obkv::KvReader<K>,
    buffer: &mut Vec<u8>,
    operation: F,
) -> std::io::Result<()>
where
    K: obkv::Key + PartialOrd,
    F: Fn(K) -> DelAddOperation,
{
    let mut writer = obkv::KvWriter::new(buffer);
    let mut value_buffer = Vec::new();
    for (key, value) in reader.iter() {
        value_buffer.clear();
        let mut value_writer = KvWriterDelAdd::new(&mut value_buffer);
        let operation = operation(key);
        if matches!(operation, DelAddOperation::Deletion | DelAddOperation::DeletionAndAddition) {
            value_writer.insert(DelAdd::Deletion, value)?;
        }
        if matches!(operation, DelAddOperation::Addition | DelAddOperation::DeletionAndAddition) {
            value_writer.insert(DelAdd::Addition, value)?;
        }
        value_writer.finish()?;
        writer.insert(key, &value_buffer)?;
    }

    writer.finish()
}

/// Enum controlling the side of the DelAdd obkv in which the provided value will be written.
#[derive(Debug, Clone, Copy)]
pub enum DelAddOperation {
    Deletion,
    Addition,
    DeletionAndAddition,
}

/// Creates a Kv<K, Kv<DelAdd, value>> from two Kv<K, value>
///
/// putting each deletion obkv's keys under an DelAdd::Deletion
/// and putting each addition obkv's keys under an DelAdd::Addition
pub fn del_add_from_two_obkvs<K: obkv::Key + PartialOrd + Ord>(
    deletion: &obkv::KvReader<K>,
    addition: &obkv::KvReader<K>,
    buffer: &mut Vec<u8>,
) -> Result<(), std::io::Error> {
    use itertools::merge_join_by;
    use itertools::EitherOrBoth::{Both, Left, Right};

    let mut writer = obkv::KvWriter::new(buffer);
    let mut value_buffer = Vec::new();

    for eob in merge_join_by(deletion.iter(), addition.iter(), |(b, _), (u, _)| b.cmp(u)) {
        value_buffer.clear();
        match eob {
            Left((k, v)) => {
                let mut value_writer = KvWriterDelAdd::new(&mut value_buffer);
                value_writer.insert(DelAdd::Deletion, v).unwrap();
                writer.insert(k, value_writer.into_inner()?).unwrap();
            }
            Right((k, v)) => {
                let mut value_writer = KvWriterDelAdd::new(&mut value_buffer);
                value_writer.insert(DelAdd::Addition, v).unwrap();
                writer.insert(k, value_writer.into_inner()?).unwrap();
            }
            Both((k, deletion), (_, addition)) => {
                let mut value_writer = KvWriterDelAdd::new(&mut value_buffer);
                value_writer.insert(DelAdd::Deletion, deletion).unwrap();
                value_writer.insert(DelAdd::Addition, addition).unwrap();
                writer.insert(k, value_writer.into_inner()?).unwrap();
            }
        }
    }

    writer.finish()
}

pub fn is_noop_del_add_obkv(del_add: &KvReaderDelAdd) -> bool {
    del_add.get(DelAdd::Deletion) == del_add.get(DelAdd::Addition)
}

/// A function that extracts and returns the Add side of a DelAdd obkv.
/// This is useful when there are no previous value in the database and
/// therefore we don't need to do a diff with what's already there.
///
/// If there is no Add side we currently write an empty buffer
/// which is a valid CboRoaringBitmap.
#[allow(clippy::ptr_arg)] // required to avoid signature mismatch
pub fn deladd_serialize_add_side<'a>(
    obkv: &'a [u8],
    _buffer: &mut Vec<u8>,
) -> crate::Result<&'a [u8]> {
    Ok(KvReaderDelAdd::from_slice(obkv).get(DelAdd::Addition).unwrap_or_default())
}
