//! Read vectors from a `vectors` directory for each index.
//!
//! The `vectors` directory is architected in the following way:
//! - `commands/` directory containing binary files that indicate which vectors should go into which embedder and fragment for which document
//! - `data/` directory containing the vector data.
//! - `status/` directory containing embedding metadata (`EmbeddingStatus`)

use std::fs::File;
use std::io::{BufReader, ErrorKind, Read};
use std::path::PathBuf;

use meilisearch_types::heed::byteorder::{BigEndian, ReadBytesExt};
use meilisearch_types::heed::RoTxn;
use meilisearch_types::milli::vector::RuntimeEmbedders;
use meilisearch_types::milli::DocumentId;
use meilisearch_types::Index;
use memmap2::Mmap;

use crate::Result;

pub struct VectorReader {
    dir: PathBuf,
    file_count: usize,
}

impl VectorReader {
    pub fn new(dir: PathBuf) -> Result<Self> {
        let commands = dir.join("commands");
        let file_count = commands.read_dir()?.count();
        Ok(Self { dir, file_count })
    }

    pub fn visit<V: Visitor>(
        &self,
        mut v: V,
        index: usize,
    ) -> Result<std::result::Result<(), V::Error>> {
        let filename = format!("{:04}.bin", index);
        let commands = self.dir.join("commands").join(&filename);
        let data = self.dir.join("data").join(&filename);
        let mut commands = BufReader::new(File::open(commands)?);
        let data = File::open(data)?;
        let data = unsafe { Mmap::map(&data)? };
        let mut buf = Vec::new();
        let mut dimensions = None;
        while let Some(command) = read_next_command(&mut buf, &mut commands)? {
            let res = match command {
                Command::ChangeCurrentEmbedder { name } => v
                    .on_current_embedder_change(name)
                    .map(|new_dimensions| dimensions = Some(new_dimensions)),
                Command::ChangeCurrentStore { name } => v.on_current_store_change(name),
                Command::ChangeDocid { external_docid } => {
                    v.on_current_docid_change(external_docid)
                }
                Command::SetVector { offset } => {
                    let dimensions = dimensions.unwrap();
                    let vec = &data[(offset as usize)
                        ..(offset as usize + (dimensions * std::mem::size_of::<f32>()))];

                    v.on_set_vector(bytemuck::cast_slice(vec))
                }
            };
            if let Err(err) = res {
                return Ok(Err(err));
            }
        }
        Ok(Ok(()))
    }
}

fn read_next_command(buf: &mut Vec<u8>, mut commands: impl Read) -> Result<Option<Command>> {
    let kind = match commands.read_u8() {
        Ok(kind) => kind,
        Err(err) if err.kind() == ErrorKind::UnexpectedEof => return Ok(None),
        Err(err) => return Err(err.into()),
    };
    let s = if Command::has_len(kind) {
        let len = commands.read_u32::<BigEndian>()?;
        buf.resize(len as usize, 0);
        if len != 0 {
            commands.read_exact(buf)?;
            std::str::from_utf8(buf).unwrap()
        } else {
            ""
        }
    } else {
        ""
    };
    let offset = if Command::has_offset(kind) { commands.read_u64::<BigEndian>()? } else { 0 };
    Ok(Some(Command::from_raw(kind, s, offset)))
}

#[repr(u8)]
pub enum Command<'pl> {
    /// Tell the importer that the next embeddings are to be added in the context of the specified embedder.
    ///
    /// Replaces the embedder specified by the previous such command.
    ///
    /// Embedder is specified by its name.
    ChangeCurrentEmbedder { name: &'pl str },
    /// Tell the importer that the next embeddings are to be added in the context of the specified store.
    ///
    /// Replaces the store specified by the previous such command.
    ///
    /// The store is specified by an optional fragment name
    ChangeCurrentStore { name: Option<&'pl str> },
    /// Tell the importer that the next embeddings are to be added in the context of the specified document.
    ///
    /// Replaces the store specified by the previous such command.
    ///
    /// The document is specified by the external docid of the document.
    ChangeDocid { external_docid: &'pl str },
    /// Tell the importer where to find the next vector in the current data file.
    SetVector { offset: u64 },
}

impl Command<'_> {
    const CHANGE_CURRENT_EMBEDDER: Self = Self::ChangeCurrentEmbedder { name: "" };
    const CHANGE_CURRENT_STORE: Self = Self::ChangeCurrentStore { name: Some("") };
    const CHANGE_DOCID: Self = Self::ChangeDocid { external_docid: "" };
    const SET_VECTOR: Self = Self::SetVector { offset: 0 };

    fn has_len(kind: u8) -> bool {
        kind == Self::CHANGE_CURRENT_EMBEDDER.discriminant()
            || kind == Self::CHANGE_CURRENT_STORE.discriminant()
            || kind == Self::CHANGE_DOCID.discriminant()
    }

    fn has_offset(kind: u8) -> bool {
        kind == Self::SET_VECTOR.discriminant()
    }

    /// See <https://doc.rust-lang.org/std/mem/fn.discriminant.html#accessing-the-numeric-value-of-the-discriminant>
    fn discriminant(&self) -> u8 {
        // SAFETY: Because `Self` is marked `repr(u8)`, its layout is a `repr(C)` `union`
        // between `repr(C)` structs, each of which has the `u8` discriminant as its first
        // field, so we can read the discriminant without offsetting the pointer.
        unsafe { *<*const _>::from(self).cast::<u8>() }
    }

    fn from_raw(kind: u8, s: &str, offset: u64) -> Command {
        if kind == Self::CHANGE_CURRENT_EMBEDDER.discriminant() {
            Command::ChangeCurrentEmbedder { name: s }
        } else if kind == Self::CHANGE_CURRENT_STORE.discriminant() {
            Command::ChangeCurrentStore { name: (!s.is_empty()).then_some(s) }
        } else if kind == Self::CHANGE_DOCID.discriminant() {
            Command::ChangeDocid { external_docid: s }
        } else if kind == Self::SET_VECTOR.discriminant() {
            Command::SetVector { offset }
        } else {
            panic!("unknown command")
        }
    }
}
