use flate2::Compression;
use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use std::fs::{create_dir_all, File};
use std::path::Path;
use tar::{Builder, Archive};

use crate::error::Error;

pub fn to_tar_gz(src: &Path, dest: &Path) -> Result<(), Error> {
    let f = File::create(dest)?;
    let gz_encoder = GzEncoder::new(f, Compression::default());
    let mut tar_encoder = Builder::new(gz_encoder);
    tar_encoder.append_dir_all(".", src)?;
    let gz_encoder = tar_encoder.into_inner()?;
    gz_encoder.finish()?;
    Ok(())
}

pub fn from_tar_gz(src: &Path, dest: &Path) -> Result<(), Error> {
    let f = File::open(src)?;
    let gz = GzDecoder::new(f);
    let mut ar = Archive::new(gz);
    create_dir_all(dest)?;
    ar.unpack(dest)?;
    Ok(())
}
