use std::fs::DirEntry;
use std::path::Path;
use std::{fs, io};

use tar::Archive;

pub trait ArchiveExt {
    /// Unpacks the archive into the specified directory in a safer way,
    /// specifically around symlinks.
    fn safe_unpack(&mut self, dst: impl AsRef<Path>) -> io::Result<()>;
}

impl<R: io::Read> ArchiveExt for Archive<R> {
    /// Most of the dcode comes from the `tar` crate.
    ///
    /// <https://github.com/alexcrichton/tar-rs/blob/20a650970793e56238b58ac2f51773d343b02117/src/archive.rs#L217-L257>
    fn safe_unpack(&mut self, dst: impl AsRef<Path>) -> io::Result<()> {
        let dst = dst.as_ref();

        // This is the only place where we use `unpack`
        // directly and we do the verification just after.
        #[allow(clippy::disallowed_methods)]
        self.unpack(dst)?;

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

    let max_depth = 100;
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
