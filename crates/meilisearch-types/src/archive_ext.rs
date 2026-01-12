use std::fs::DirEntry;
use std::path::{self, Path};
use std::{fs, io};

use tar::Archive;

pub trait ArchiveExt {
    /// Unpacks the archive into the specified directory in a safer way,
    /// specifically around symlinks.
    fn safe_unpack(&mut self, dst: impl AsRef<Path>) -> io::Result<()>;
}

impl<R: io::Read> ArchiveExt for Archive<R> {
    /// Most of the dcode comes from the `tar` crate. The destination path must be absolute.
    ///
    /// <https://github.com/alexcrichton/tar-rs/blob/20a650970793e56238b58ac2f51773d343b02117/src/archive.rs#L217-L257>
    fn safe_unpack(&mut self, dst: impl AsRef<Path>) -> io::Result<()> {
        // Note that I should create a subfunction with non-generic types
        // but the `Archive::entries` method doesn't work on non-sized types...
        let dst = dst.as_ref();

        if dst.symlink_metadata().is_err() {
            fs::create_dir_all(dst).map_err(|e| {
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
                    if let Some(link_name) = entry.header().link_name()? {
                        // The destination path must be absolute.
                        let absolute_link_name = path::absolute(dst.join(&link_name))?;
                        if !absolute_link_name.starts_with(dst) {
                            return Err(io::Error::other(
                                "links and symlinks must link within the dump directory",
                            ));
                        }
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

        // Finally check the unpacked files and directories
        // to check if symlinks are pointing inside the dst folder.
        check_symlinks(dst)?;

        Ok(())
    }
}

/// Makes sure no symlink points outside the dst folder.
fn check_symlinks(dir: &Path) -> io::Result<()> {
    /// Walk a directory only visiting files.
    /// <https://doc.rust-lang.org/stable/std/fs/fn.read_dir.html>
    fn visit_dirs(
        dir: &Path,
        max_depth: u32,
        cb: &dyn Fn(&DirEntry) -> io::Result<()>,
    ) -> io::Result<()> {
        if !dir.is_dir() {
            return Ok(());
        }

        for entry in fs::read_dir(dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_dir() {
                match max_depth.checked_sub(1) {
                    Some(new_max_depth) => visit_dirs(&path, new_max_depth, cb)?,
                    None => {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "maximum depth exceeded",
                        ))
                    }
                }
            } else {
                cb(&entry)?;
            }
        }
        Ok(())
    }

    let max_depth = 10;
    visit_dirs(dir, max_depth, &|entry| {
        if entry.file_type()?.is_symlink() && !entry.path().canonicalize()?.starts_with(dir) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "links and symlinks must link within the dump directory",
            ));
        }
        Ok(())
    })
}
