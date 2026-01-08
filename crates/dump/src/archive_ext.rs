use std::fs;
use std::io;
use std::path::Path;

use tar::Archive;

pub trait ArchiveExt {
    fn safe_unpack(&mut self, dst: impl AsRef<Path>) -> io::Result<()>;
}

impl<R: io::Read> ArchiveExt for Archive<R> {
    fn safe_unpack(&mut self, dst: impl AsRef<Path>) -> io::Result<()> {
        // Note that I should create a subfunction with non-generic types
        // but the `Archive::entries` method doesn't work on non-sized types...
        let dst = dst.as_ref();

        if dst.symlink_metadata().is_err() {
            fs::create_dir_all(&dst).map_err(|e| {
                io::Error::new(e.kind(), format!("failed to create `{}`", dst.display()))
            })?;
        }

        // Canonicalizing the dst directory will prepend the path with '\\?\'
        // on windows which will allow windows APIs to treat the path as an
        // extended-length path with a 32,767 character limit. Otherwise all
        // unpacked paths over 260 characters will fail on creation with a
        // NotFound exception.
        let dst = &dst.canonicalize().unwrap_or(dst.to_path_buf());

        // Delay any directory entries until the end (they will be created if needed by
        // descendants), to ensure that directory permissions do not interfer with descendant
        // extraction.
        let mut directories = Vec::new();
        for entry in self.entries()? {
            let mut entry =
                entry.map_err(|e| io::Error::new(e.kind(), "failed to iterate over archive"))?;
            match entry.header().entry_type() {
                tar::EntryType::Directory => directories.push(entry),
                _ => {
                    if !entry.header().path()?.starts_with(dst) {
                        return Err(io::Error::new(
                            io::ErrorKind::Other,
                            "links and symlinks must link within the dump directory",
                        ));
                    }
                    entry.unpack_in(dst)?;
                }
            }
        }

        // Apply the directories.
        //
        // Note: the order of application is important to permissions. That is, we must traverse
        // the filesystem graph in topological ordering or else we risk not being able to create
        // child directories within those of more restrictive permissions. See [0] for details.
        //
        // [0]: <https://github.com/alexcrichton/tar-rs/issues/242>
        directories.sort_by(|a, b| b.path_bytes().cmp(&a.path_bytes()));
        for mut dir in directories {
            dir.unpack_in(dst)?;
        }

        Ok(())
    }
}
