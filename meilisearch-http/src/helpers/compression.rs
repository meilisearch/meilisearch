use std::fs::{create_dir_all, File};
use std::io::Write;
use std::path::Path;

use flate2::{Compression, write::GzEncoder, read::GzDecoder};
use tar::{Archive, Builder};

use crate::error::Error;

pub fn to_tar_gz(src: impl AsRef<Path>, dest: impl AsRef<Path>) -> Result<(), Error> {
    let mut f = File::create(dest)?;
    let gz_encoder = GzEncoder::new(&mut f, Compression::default());
    let mut tar_encoder = Builder::new(gz_encoder);
    tar_encoder.append_dir_all(".", src)?;
    let gz_encoder = tar_encoder.into_inner()?;
    gz_encoder.finish()?;
    f.flush()?;
    Ok(())
}

pub fn from_tar_gz(src: impl AsRef<Path>, dest: impl AsRef<Path>) -> Result<(), Error> {
    let f = File::open(&src)?;
    let gz = GzDecoder::new(f);
    let mut ar = Archive::new(gz);
    create_dir_all(&dest)?;
    ar.unpack(&dest)?;
    Ok(())
}
