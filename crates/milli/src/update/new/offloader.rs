use std::fs::File;
use std::io::{self, BufReader, BufWriter, Seek as _};

/// A data-structure that offloads prefixes and their serialized lengths to a file.
pub struct Offloader<E, D> {
    file: BufWriter<File>,
    tmp_buffer: Vec<u8>,
    _marker: std::marker::PhantomData<(E, D)>,
}

impl<E, D> Offloader<E, D> {
    pub fn new(file: File) -> Self {
        Self {
            file: BufWriter::new(file),
            tmp_buffer: Vec::new(),
            _marker: std::marker::PhantomData,
        }
    }

    pub fn push(&mut self, entry: E) -> io::Result<()>
    where
        E: Encoder,
    {
        self.tmp_buffer.clear();
        entry.encode(&mut self.tmp_buffer, &mut self.file)
    }

    pub fn finish(self) -> io::Result<OffloadedReader<D>> {
        self.file.into_inner().map_err(|e| e.into_error()).and_then(|mut file| {
            file.rewind()?;
            Ok(OffloadedReader {
                file: BufReader::new(file),
                first_tmp_buffer: Default::default(),
                second_tmp_buffer: Default::default(),
                _marker: std::marker::PhantomData,
            })
        })
    }
}

pub struct OffloadedReader<D> {
    file: BufReader<File>,
    first_tmp_buffer: Vec<u8>,
    second_tmp_buffer: Vec<u8>,
    _marker: std::marker::PhantomData<D>,
}

impl<D> OffloadedReader<D> {
    pub fn next_entry<'a>(&'a mut self) -> io::Result<Option<D::Decoded>>
    where
        D: Decoder<'a>,
    {
        self.first_tmp_buffer.clear();
        self.second_tmp_buffer.clear();
        D::decode(&mut self.first_tmp_buffer, &mut self.second_tmp_buffer, &mut self.file)
    }
}

pub trait Encoder {
    fn encode<W: io::Write>(self, tmp_buffer: &mut Vec<u8>, writer: &mut W) -> io::Result<()>;
}

pub trait Decoder<'b>: Sized {
    type Decoded: 'b;

    fn decode<R: io::Read>(
        first_tmp_buffer: &'b mut Vec<u8>,
        second_tmp_buffer: &'b mut Vec<u8>,
        reader: &mut R,
    ) -> io::Result<Option<Self::Decoded>>;
}
