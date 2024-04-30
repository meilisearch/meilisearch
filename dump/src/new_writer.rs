use std::fs::File;
use std::io::{Read, Seek, Write};
use std::path::Path;
use std::result::Result as StdResult;

use flate2::write::GzEncoder;
use flate2::Compression;
use meilisearch_types::milli::documents::{
    obkv_to_object, DocumentsBatchCursor, DocumentsBatchIndex, DocumentsBatchReader,
};
use tar::{Builder as TarBuilder, Header};
use time::OffsetDateTime;
use uuid::Uuid;

use crate::{Key, Metadata, Result, TaskId, CURRENT_DUMP_VERSION};

pub struct DumpWriter<W: Write> {
    tar: TarBuilder<GzEncoder<W>>,
}

impl<W: Write> DumpWriter<W> {
    pub fn new(instance_uuid: Option<Uuid>, writer: W) -> Result<Self> {
        /// TODO: should we use a BuffWriter?
        let gz_encoder = GzEncoder::new(writer, Compression::default());
        let mut tar = TarBuilder::new(gz_encoder);

        let mut header = Header::new_gnu();

        // Append metadata into metadata.json.
        let metadata = Metadata {
            dump_version: CURRENT_DUMP_VERSION,
            db_version: env!("CARGO_PKG_VERSION").to_string(),
            dump_date: OffsetDateTime::now_utc(),
        };
        let data = serde_json::to_string(&metadata).unwrap();
        header.set_size(data.len() as u64);
        tar.append_data(&mut header, "metadata.json", data.as_bytes()).unwrap();

        // Append instance uid into instance_uid.uuid.
        if let Some(instance_uuid) = instance_uuid {
            let data = instance_uuid.as_hyphenated().to_string();
            header.set_size(data.len() as u64);
            tar.append_data(&mut header, "instance_uid.uuid", data.as_bytes()).unwrap();
        }

        Ok(Self { tar })
    }

    pub fn dump_keys(&mut self, keys: &[Key]) -> Result<()> {
        let mut buffer = Vec::new();
        for key in keys {
            serde_json::to_writer(&mut buffer, key)?;
            buffer.push(b'\n');
        }
        let mut header = Header::new_gnu();
        header.set_path("keys.jsonl");
        header.set_size(buffer.len() as u64);

        self.tar.append(&mut header, buffer.as_slice())?;
        Ok(())
    }

    pub fn create_tasks(&mut self) -> Result<FileWriter<W>> {
        FileWriter::new(&mut self.tar, "tasks/queue.jsonl")
    }

    pub fn dump_update_file<R: Read + Seek>(
        &mut self,
        task_uid: TaskId,
        update_file: DocumentsBatchReader<R>,
    ) -> Result<()> {
        let path = format!("tasks/update_files/{}.jsonl", task_uid);
        let mut fw = FileWriter::new(&mut self.tar, path)?;
        let mut serializer = UpdateFileSerializer::new(update_file);
        fw.calculate_len(SerializerIteratorReader::new(&mut serializer))?;
        serializer.reset();
        fw.write_data(SerializerIteratorReader::new(&mut serializer))
    }
}

trait SerializerIterator {
    fn next_serialize_into(&mut self, buffer: &mut Vec<u8>) -> StdResult<bool, std::io::Error>;
}

struct SerializerIteratorReader<'i, I: SerializerIterator> {
    iterator: &'i mut I,
    buffer: Vec<u8>,
}

impl<I: SerializerIterator> Read for SerializerIteratorReader<'_, I> {
    fn read(&mut self, buf: &mut [u8]) -> StdResult<usize, std::io::Error> {
        let mut size = 0;
        loop {
            // if the inner buffer is empty, fill it with a new document.
            if self.buffer.is_empty() {
                if !self.iterator.next_serialize_into(&mut self.buffer)? {
                    // nothing more to write, return the written size.
                    return Ok(size);
                }
            }

            let doc_size = self.buffer.len();
            let remaining_size = buf[size..].len();
            if remaining_size < doc_size {
                // if the serialized document size exceed the buf size,
                // drain the inner buffer filling the remaining space.
                buf[size..].copy_from_slice(&self.buffer[..remaining_size]);
                self.buffer.drain(..remaining_size);

                // then return.
                return Ok(buf.len());
            } else {
                // otherwise write the whole inner buffer into the buf, clear it and continue.
                buf[size..][..doc_size].copy_from_slice(&self.buffer);
                size += doc_size;
                self.buffer.clear();
            }
        }
    }
}

impl<'i, I: SerializerIterator> SerializerIteratorReader<'i, I> {
    fn new(iterator: &'i mut I) -> Self {
        Self { iterator, buffer: Vec::new() }
    }
}

struct UpdateFileSerializer<R: Read> {
    cursor: DocumentsBatchCursor<R>,
    documents_batch_index: DocumentsBatchIndex,
}

impl<R: Read + Seek> SerializerIterator for UpdateFileSerializer<R> {
    fn next_serialize_into(&mut self, buffer: &mut Vec<u8>) -> StdResult<bool, std::io::Error> {
        /// TODO: don't unwrap, original version: `cursor.next_document().map_err(milli::Error::from)?`
        match self.cursor.next_document().unwrap() {
            Some(doc) => {
                /// TODO: don't unwrap
                let json_value = obkv_to_object(&doc, &self.documents_batch_index).unwrap();
                serde_json::to_writer(&mut *buffer, &json_value)?;
                buffer.push(b'\n');
                Ok(true)
            }
            None => Ok(false),
        }
    }
}

impl<R: Read + Seek> UpdateFileSerializer<R> {
    fn new(reader: DocumentsBatchReader<R>) -> Self {
        let (cursor, documents_batch_index) = reader.into_cursor_and_fields_index();

        Self { cursor, documents_batch_index }
    }

    /// Resets the cursor to be able to read from the start again.
    pub fn reset(&mut self) {
        self.cursor.reset();
    }
}

pub struct FileWriter<'a, W: Write> {
    header: Header,
    tar: &'a mut TarBuilder<GzEncoder<W>>,
    size: Option<u64>,
}

impl<'a, W: Write> FileWriter<'a, W> {
    pub(crate) fn new<P: AsRef<Path>>(
        tar: &'a mut TarBuilder<GzEncoder<W>>,
        path: P,
    ) -> Result<Self> {
        let mut header = Header::new_gnu();
        header.set_path(path);
        Ok(Self { header, tar, size: None })
    }

    pub fn calculate_len<R: Read>(&mut self, mut reader: R) -> Result<u64> {
        let mut calculator = SizeCalculatorWriter::new();
        std::io::copy(&mut reader, &mut calculator)?;
        let size = calculator.into_inner();
        self.size = Some(size);

        Ok(size)
    }

    pub fn write_data<R: Read>(mut self, reader: R) -> Result<()> {
        let expected_size =
            self.size.expect("calculate_len must be called before writing the data.");
        self.header.set_size(expected_size);

        let mut scr = SizeCalculatorReader::new(reader);
        self.tar.append(&mut self.header, &mut scr)?;
        assert_eq!(
            expected_size,
            scr.into_inner(),
            "Provided data size is different from the pre-calculated size."
        );

        Ok(())
    }
}

struct SizeCalculatorWriter {
    size: usize,
}

impl SizeCalculatorWriter {
    fn new() -> Self {
        Self { size: 0 }
    }

    fn into_inner(self) -> u64 {
        self.size as u64
    }
}

impl Write for SizeCalculatorWriter {
    fn write(&mut self, buf: &[u8]) -> StdResult<usize, std::io::Error> {
        self.size += buf.len();
        Ok(self.size)
    }

    fn flush(&mut self) -> std::result::Result<(), std::io::Error> {
        Ok(())
    }
}

struct SizeCalculatorReader<R: Read> {
    size: usize,
    reader: R,
}

impl<R: Read> SizeCalculatorReader<R> {
    fn new(reader: R) -> Self {
        Self { size: 0, reader }
    }

    fn into_inner(self) -> u64 {
        self.size as u64
    }
}

impl<R: Read> Read for SizeCalculatorReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> StdResult<usize, std::io::Error> {
        let size = self.reader.read(buf)?;
        self.size += size;

        Ok(size)
    }
}
