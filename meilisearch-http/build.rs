use std::env;
use std::fs::create_dir_all;
use std::io::Cursor;
use std::path::PathBuf;

use anyhow::Context;
use sha1::{Sha1, Digest};
use reqwest::blocking::get;
use actix_web_static_files::resource_dir;

use vergen::{generate_cargo_keys, ConstantsFlags};
use cargo_toml::Manifest;

fn main() {
    // Setup the flags, toggling off the 'SEMVER_FROM_CARGO_PKG' flag
    let mut flags = ConstantsFlags::all();
    flags.toggle(ConstantsFlags::SEMVER_FROM_CARGO_PKG);

    // Generate the 'cargo:' key output
    generate_cargo_keys(ConstantsFlags::all()).expect("Unable to generate the cargo keys!");

    if let Ok(_) = env::var("CARGO_FEATURE_MINI_DASHBOARD") {
        setup_mini_dashboard().expect("Could not load mini-dashboard assets");
    }
}

fn setup_mini_dashboard() -> anyhow::Result<()> {
    let cargo_manifest_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR").unwrap());
    let cargo_toml = cargo_manifest_dir.join("Cargo.toml");

    let manifest = Manifest::from_path(cargo_toml).unwrap();

    let meta = &manifest
        .package
        .as_ref()
        .context("package not specified in Cargo.toml")?
        .metadata
        .as_ref()
        .context("no metadata specified in Cargo.toml")?
        ["mini-dashboard"];

    let url = meta["assets-url"].as_str().unwrap();

    let dashboard_assets_bytes = get(url)?
        .bytes()?;

    let mut hasher = Sha1::new();
    hasher.update(&dashboard_assets_bytes);
    let sha1_dashboard = hex::encode(hasher.finalize());

    assert_eq!(meta["sha1"].as_str().unwrap(), sha1_dashboard);

    let dashboard_dir = cargo_manifest_dir.join("mini-dashboard");
    create_dir_all(&dashboard_dir)?;
    let cursor = Cursor::new(&dashboard_assets_bytes);
    let mut zip = zip::read::ZipArchive::new(cursor)?;
    zip.extract(&dashboard_dir)?;
    resource_dir(&dashboard_dir).build()?;
    Ok(())
}
