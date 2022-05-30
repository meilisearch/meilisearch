use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

use serde_json::{Deserializer, Value};
use tempfile::NamedTempFile;

use crate::dump::compat::{self, v2, v3};
use crate::dump::Metadata;
use crate::options::IndexerOpts;

/// The dump v2 reads the dump folder and patches all the needed file to make it compatible with a
/// dump v3, then calls the dump v3 to actually handle the dump.
pub fn load_dump(
    meta: Metadata,
    src: impl AsRef<Path>,
    dst: impl AsRef<Path>,
    index_db_size: usize,
    update_db_size: usize,
    indexing_options: &IndexerOpts,
) -> anyhow::Result<()> {
    log::info!("Patching dump V2 to dump V3...");
    let indexes_path = src.as_ref().join("indexes");

    let dir_entries = std::fs::read_dir(indexes_path)?;
    for entry in dir_entries {
        let entry = entry?;

        // rename the index folder
        let path = entry.path();
        let new_path = patch_index_uuid_path(&path).expect("invalid index folder.");

        std::fs::rename(path, &new_path)?;

        let settings_path = new_path.join("meta.json");

        patch_settings(settings_path)?;
    }

    let update_dir = src.as_ref().join("updates");
    let update_path = update_dir.join("data.jsonl");
    patch_updates(update_dir, update_path)?;

    super::v3::load_dump(
        meta,
        src,
        dst,
        index_db_size,
        update_db_size,
        indexing_options,
    )
}

fn patch_index_uuid_path(path: &Path) -> Option<PathBuf> {
    let uuid = path.file_name()?.to_str()?.trim_start_matches("index-");
    let new_path = path.parent()?.join(uuid);
    Some(new_path)
}

fn patch_settings(path: impl AsRef<Path>) -> anyhow::Result<()> {
    let mut meta_file = File::open(&path)?;
    let mut meta: Value = serde_json::from_reader(&mut meta_file)?;

    // We first deserialize the dump meta into a serde_json::Value and change
    // the custom ranking rules settings from the old format to the new format.
    if let Some(ranking_rules) = meta.pointer_mut("/settings/rankingRules") {
        patch_custom_ranking_rules(ranking_rules);
    }

    let mut meta_file = OpenOptions::new().truncate(true).write(true).open(path)?;

    serde_json::to_writer(&mut meta_file, &meta)?;

    Ok(())
}

fn patch_updates(dir: impl AsRef<Path>, path: impl AsRef<Path>) -> anyhow::Result<()> {
    let mut output_update_file = NamedTempFile::new_in(&dir)?;
    let update_file = File::open(&path)?;

    let stream = Deserializer::from_reader(update_file).into_iter::<v2::UpdateEntry>();

    for update in stream {
        let update_entry = update?;

        let update_entry = v3::UpdateEntry::from(update_entry);

        serde_json::to_writer(&mut output_update_file, &update_entry)?;
        output_update_file.write_all(b"\n")?;
    }

    output_update_file.flush()?;
    output_update_file.persist(path)?;

    Ok(())
}

/// Converts the ranking rules from the format `asc(_)`, `desc(_)` to the format `_:asc`, `_:desc`.
///
/// This is done for compatibility reasons, and to avoid a new dump version,
/// since the new syntax was introduced soon after the new dump version.
fn patch_custom_ranking_rules(ranking_rules: &mut Value) {
    *ranking_rules = match ranking_rules.take() {
        Value::Array(values) => values
            .into_iter()
            .filter_map(|value| match value {
                Value::String(s) if s.starts_with("asc") => compat::asc_ranking_rule(&s)
                    .map(|f| format!("{}:asc", f))
                    .map(Value::String),
                Value::String(s) if s.starts_with("desc") => compat::desc_ranking_rule(&s)
                    .map(|f| format!("{}:desc", f))
                    .map(Value::String),
                otherwise => Some(otherwise),
            })
            .collect(),
        otherwise => otherwise,
    }
}

impl From<v2::UpdateEntry> for v3::UpdateEntry {
    fn from(v2::UpdateEntry { uuid, update }: v2::UpdateEntry) -> Self {
        let update = match update {
            v2::UpdateStatus::Processing(meta) => v3::UpdateStatus::Processing(meta.into()),
            v2::UpdateStatus::Enqueued(meta) => v3::UpdateStatus::Enqueued(meta.into()),
            v2::UpdateStatus::Processed(meta) => v3::UpdateStatus::Processed(meta.into()),
            v2::UpdateStatus::Aborted(_) => unreachable!("Updates could never be aborted."),
            v2::UpdateStatus::Failed(meta) => v3::UpdateStatus::Failed(meta.into()),
        };

        Self { uuid, update }
    }
}

impl From<v2::Failed> for v3::Failed {
    fn from(other: v2::Failed) -> Self {
        let v2::Failed {
            from,
            error,
            failed_at,
        } = other;

        Self {
            from: from.into(),
            msg: error.message,
            code: v2::error_code_from_str(&error.error_code)
                .expect("Invalid update: Invalid error code"),
            failed_at,
        }
    }
}

impl From<v2::Processing> for v3::Processing {
    fn from(other: v2::Processing) -> Self {
        let v2::Processing {
            from,
            started_processing_at,
        } = other;

        Self {
            from: from.into(),
            started_processing_at,
        }
    }
}

impl From<v2::Enqueued> for v3::Enqueued {
    fn from(other: v2::Enqueued) -> Self {
        let v2::Enqueued {
            update_id,
            meta,
            enqueued_at,
            content,
        } = other;

        let meta = match meta {
            v2::UpdateMeta::DocumentsAddition {
                method,
                primary_key,
                ..
            } => {
                v3::Update::DocumentAddition {
                    primary_key,
                    method,
                    // Just ignore if the uuid is no present. If it is needed later, an error will
                    // be thrown.
                    content_uuid: content.unwrap_or_default(),
                }
            }
            v2::UpdateMeta::ClearDocuments => v3::Update::ClearDocuments,
            v2::UpdateMeta::DeleteDocuments { ids } => v3::Update::DeleteDocuments(ids),
            v2::UpdateMeta::Settings(settings) => v3::Update::Settings(settings),
        };

        Self {
            update_id,
            meta,
            enqueued_at,
        }
    }
}

impl From<v2::Processed> for v3::Processed {
    fn from(other: v2::Processed) -> Self {
        let v2::Processed {
            from,
            success,
            processed_at,
        } = other;

        Self {
            success,
            processed_at,
            from: from.into(),
        }
    }
}
