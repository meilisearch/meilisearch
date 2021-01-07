use flate2::Compression;
use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use std::fs::{create_dir_all, rename, File};
use std::path::Path;
use tar::{Builder, Archive};
use uuid::Uuid;

use crate::error::Error;

pub fn to_tar_gz(src: &Path, dest: &Path) -> Result<(), Error> {
    let file_name = format!(".{}", Uuid::new_v4().to_urn());
    let p = dest.with_file_name(file_name);
    let tmp_dest = p.as_path();

    let f = File::create(tmp_dest)?;
    let gz_encoder = GzEncoder::new(f, Compression::default());
    let mut tar_encoder = Builder::new(gz_encoder);
    tar_encoder.append_dir_all(".", src)?;
    let gz_encoder = tar_encoder.into_inner()?;
    gz_encoder.finish()?;

    rename(tmp_dest, dest)?;

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
