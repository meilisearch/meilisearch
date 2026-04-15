use std::{
    fs::File,
    io::{self, BufReader, BufWriter, ErrorKind, Read as _, Seek as _, Write as _},
};

use roaring::RoaringBitmap;

use crate::DeCboRoaringBitmapCodec;

/// A data-structure that offloads prefixes and their serialized lengths to a file.
pub struct PrefixIntegersOffloader<E, D> {
    file: BufWriter<File>,
    tmp_buffer: Vec<u8>,
    _marker: std::marker::PhantomData<(E, D)>,
}

impl<E, D> PrefixIntegersOffloader<E, D> {
    pub fn new(file: File) -> Self {
        Self {
            file: BufWriter::new(file),
            tmp_buffer: Vec::new(),
            _marker: std::marker::PhantomData,
        }
    }

    pub fn push(&mut self, entry: E) -> io::Result<()>
    where
        E: Encode,
    {
        self.tmp_buffer.clear();
        entry.encode(&mut self.tmp_buffer, &mut self.file)
    }

    pub fn finish(self) -> io::Result<PrefixIntegersReader<D>> {
        self.file.into_inner().map_err(|e| e.into_error()).and_then(|mut file| {
            file.rewind()?;
            Ok(PrefixIntegersReader {
                file: BufReader::new(file),
                _marker: std::marker::PhantomData,
            })
        })
    }
}

pub struct PrefixIntegersReader<D> {
    file: BufReader<File>,
    _marker: std::marker::PhantomData<D>,
}

impl<D> PrefixIntegersReader<D> {
    pub fn next_entry(
        &mut self,
        first_tmp_buffer: &mut Vec<u8>,
        second_tmp_buffer: &mut Vec<u8>,
    ) -> io::Result<Option<D>>
    where
        D: Decode,
    {
        first_tmp_buffer.clear();
        second_tmp_buffer.clear();
        D::decode(first_tmp_buffer, second_tmp_buffer, &mut self.file)
    }
}

pub trait Encode {
    fn encode<W: io::Write>(self, tmp_buffer: &mut Vec<u8>, writer: &mut W) -> io::Result<()>;
}

pub trait Decode: Sized {
    fn decode<'a, R: io::Read>(
        first_tmp_buffer: &'a mut Vec<u8>,
        second_tmp_buffer: &'a mut Vec<u8>,
        reader: &mut R,
    ) -> io::Result<Option<Self>>
    where
        Self: 'a;
}

/// Represents a prefix, its position in the field and the length the bitmap takes on disk.
pub struct InPrefixIntegerEntry<'a> {
    pub prefix: &'a str,
    pub pos: u16,
    pub bitmap: Option<RoaringBitmap>,
}

impl Encode for InPrefixIntegerEntry<'_> {
    fn encode<W: io::Write>(self, tmp_buffer: &mut Vec<u8>, writer: &mut W) -> io::Result<()> {
        let InPrefixIntegerEntry { prefix, pos, bitmap } = self;

        // prefix length and prefix
        let prefix_length: u8 =
            prefix.len().try_into().map_err(|_| io::Error::other("prefix length too long"))?;
        writer.write_all(bytemuck::bytes_of(&prefix_length))?;
        writer.write_all(prefix.as_bytes())?;

        // pos
        writer.write_all(bytemuck::bytes_of(&pos))?;

        // bitmap length and bitmap
        let serialized_bytes = match bitmap {
            Some(bitmap) => {
                tmp_buffer.clear();
                CboRoaringBitmapCodec::serialize_into_vec(&bitmap, tmp_buffer);
                &tmp_buffer[..]
            }
            None => &[][..],
        };
        let serialized_bitmap_length: u32 = serialized_bytes
            .len()
            .try_into()
            .map_err(|_| io::Error::other("serialized bitmap length too long"))?;
        writer.write_all(bytemuck::bytes_of(&serialized_bitmap_length))?;
        writer.write_all(serialized_bytes)?;

        Ok(())
    }
}

/// Represents a prefix, its position in the field and the length the bitmap takes on disk.
pub struct OutPrefixIntegerEntry<'b> {
    pub prefix: &'b str,
    pub pos: u16,
    pub bitmap: Option<&'b [u8]>,
}

impl<'b> Decode for OutPrefixIntegerEntry<'b> {
    fn decode<'a, R: io::Read>(
        first_tmp_buffer: &'a mut Vec<u8>,
        second_tmp_buffer: &'a mut Vec<u8>,
        reader: &mut R,
    ) -> io::Result<Option<Self>>
    where
        Self: 'a,
    {
        // prefix length and prefix
        let mut prefix_length: u16 = 0;
        match reader.read_exact(bytemuck::bytes_of_mut(&mut prefix_length)) {
            Ok(()) => (),
            Err(e) if e.kind() == ErrorKind::UnexpectedEof => return Ok(None),
            Err(e) => return Err(e),
        }
        first_tmp_buffer.resize(prefix_length as usize, 0);
        reader.read_exact(first_tmp_buffer)?;
        let prefix = std::str::from_utf8(first_tmp_buffer)
            .map_err(|e| io::Error::new(ErrorKind::InvalidData, e))?;

        // pos
        let mut pos: u16 = 0;
        reader.read_exact(bytemuck::bytes_of_mut(&mut pos))?;

        // bitmap length and bitmap (bytes)
        let mut bitmap_length: u16 = 0;
        reader.read_exact(bytemuck::bytes_of_mut(&mut bitmap_length))?;
        let bitmap = if bitmap_length == 0 {
            None
        } else {
            second_tmp_buffer.resize(bitmap_length as usize, 0);
            reader.read_exact(second_tmp_buffer)?;
            Some(second_tmp_buffer.as_slice())
        };

        Ok(Some(Self { prefix, pos, bitmap }))
    }
}
