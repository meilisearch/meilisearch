use vergen::{vergen, Config, SemverKind};

fn main() {
    let mut config = Config::default();
    // allow using non-annotated tags
    *config.git_mut().semver_kind_mut() = SemverKind::Lightweight;

    if let Err(e) = vergen(config) {
        println!("cargo:warning=vergen: {}", e);
    }

    #[cfg(feature = "mini-dashboard")]
    mini_dashboard::setup_mini_dashboard().expect("Could not load the mini-dashboard assets");
}

#[cfg(feature = "mini-dashboard")]
mod mini_dashboard {
    use std::env;
    use std::fs::{create_dir_all, File, OpenOptions};
    use std::io::{Cursor, Read, Write};
    use std::path::PathBuf;

    use anyhow::Context;
    use cargo_toml::Manifest;
    use reqwest::blocking::get;
    use sha1::{Digest, Sha1};
    use static_files::resource_dir;

    pub fn setup_mini_dashboard() -> anyhow::Result<()> {
        let cargo_manifest_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR").unwrap());
        let cargo_toml = cargo_manifest_dir.join("Cargo.toml");
        let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());

        let sha1_path = out_dir.join(".mini-dashboard.sha1");
        let dashboard_dir = out_dir.join("mini-dashboard");

        let manifest = Manifest::from_path(cargo_toml).unwrap();

        let meta = &manifest
            .package
            .as_ref()
            .context("package not specified in Cargo.toml")?
            .metadata
            .as_ref()
            .context("no metadata specified in Cargo.toml")?["mini-dashboard"];

        // Check if there already is a dashboard built, and if it is up to date.
        if sha1_path.exists() && dashboard_dir.exists() {
            let mut sha1_file = File::open(&sha1_path)?;
            let mut sha1 = String::new();
            sha1_file.read_to_string(&mut sha1)?;
            if sha1 == meta["sha1"].as_str().unwrap() {
                // Nothing to do.
                return Ok(());
            }
        }

        let url = meta["assets-url"].as_str().unwrap();

        let dashboard_assets_bytes = get(url)?.bytes()?;

        let mut hasher = Sha1::new();
        hasher.update(&dashboard_assets_bytes);
        let sha1 = hex::encode(hasher.finalize());

        assert_eq!(
            meta["sha1"].as_str().unwrap(),
            sha1,
            "Downloaded mini-dashboard shasum differs from the one specified in the Cargo.toml"
        );

        create_dir_all(&dashboard_dir)?;
        let cursor = Cursor::new(&dashboard_assets_bytes);
        let mut zip = zip::read::ZipArchive::new(cursor)?;
        zip.extract(&dashboard_dir)?;
        resource_dir(&dashboard_dir).build()?;

        // Write the sha1 for the dashboard back to file.
        let mut file =
            OpenOptions::new().write(true).create(true).truncate(true).open(sha1_path)?;

        file.write_all(sha1.as_bytes())?;
        file.flush()?;

        Ok(())
    }
}
