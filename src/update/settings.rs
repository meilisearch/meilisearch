use anyhow::Context;
use crate::Index;

pub struct Settings<'t, 'u, 'i> {
    wtxn: &'t mut heed::RwTxn<'i, 'u>,
    index: &'i Index,
    // If the field is set to `None` it means that it hasn't been set by the user,
    // however if it is `Some(None)` it means that the user forced a reset of the setting.
    displayed_fields: Option<Option<Vec<String>>>,
}

impl<'t, 'u, 'i> Settings<'t, 'u, 'i> {
    pub fn new(wtxn: &'t mut heed::RwTxn<'i, 'u>, index: &'i Index) -> Settings<'t, 'u, 'i> {
        Settings { wtxn, index, displayed_fields: None }
    }

    pub fn reset_displayed_fields(&mut self) {
        self.displayed_fields = Some(None);
    }

    pub fn set_displayed_fields(&mut self, names: Vec<String>) {
        self.displayed_fields = Some(Some(names));
    }

    pub fn execute(self) -> anyhow::Result<()> {
        // Check that the displayed attributes parameters has been specified.
        if let Some(value) = self.displayed_fields {
            match value {
                // If it has been set, and it was a list of fields names, we create
                // or generate the fields ids corresponds to those names and store them
                // in the database in the order they were specified.
                Some(fields_names) => {
                    let mut fields_ids_map = self.index.fields_ids_map(self.wtxn)?;

                    // We create or generate the fields ids corresponding to those names.
                    let mut fields_ids = Vec::new();
                    for name in fields_names {
                        let id = fields_ids_map.insert(&name).context("field id limit reached")?;
                        fields_ids.push(id);
                    }

                    self.index.put_displayed_fields(self.wtxn, &fields_ids)?;
                },
                // If it was set to `null` it means that the user wants to get the default behavior
                // which is displaying all the attributes in no specific order (FieldsIdsMap order),
                // we just have to delete the displayed fields.
                None => {
                    self.index.delete_displayed_fields(self.wtxn)?;
                },
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::update::{IndexDocuments, UpdateFormat};
    use heed::EnvOpenOptions;

    #[test]
    fn default_displayed_fields() {
        let path = tempfile::tempdir().unwrap();
        let mut options = EnvOpenOptions::new();
        options.map_size(10 * 1024 * 1024); // 10 MB
        let index = Index::new(options, &path).unwrap();

        // First we send 3 documents with ids from 1 to 3.
        let mut wtxn = index.write_txn().unwrap();
        let content = &b"name,age\nkevin,23\nkevina,21\nbenoit,34\n"[..];
        let mut builder = IndexDocuments::new(&mut wtxn, &index);
        builder.update_format(UpdateFormat::Csv);
        builder.execute(content, |_, _| ()).unwrap();
        wtxn.commit().unwrap();

        // Check that the displayed fields are correctly set to `None` (default value).
        let rtxn = index.read_txn().unwrap();
        let fields_ids = index.displayed_fields(&rtxn).unwrap();
        assert_eq!(fields_ids, None);
        drop(rtxn);
    }

    #[test]
    fn set_and_reset_displayed_field() {
        let path = tempfile::tempdir().unwrap();
        let mut options = EnvOpenOptions::new();
        options.map_size(10 * 1024 * 1024); // 10 MB
        let index = Index::new(options, &path).unwrap();

        // First we send 3 documents with ids from 1 to 3.
        let mut wtxn = index.write_txn().unwrap();
        let content = &b"name,age\nkevin,23\nkevina,21\nbenoit,34\n"[..];
        let mut builder = IndexDocuments::new(&mut wtxn, &index);
        builder.update_format(UpdateFormat::Csv);
        builder.execute(content, |_, _| ()).unwrap();

        // In the same transaction we change the displayed fields to be only the age.
        let mut builder = Settings::new(&mut wtxn, &index);
        builder.set_displayed_fields(vec!["age".into()]);
        builder.execute().unwrap();
        wtxn.commit().unwrap();

        // Check that the displayed fields are correctly set to only the "age" field.
        let rtxn = index.read_txn().unwrap();
        let fields_ids_map = index.fields_ids_map(&rtxn).unwrap();
        let age_field_id = fields_ids_map.id("age").unwrap();
        let fields_ids = index.displayed_fields(&rtxn).unwrap();
        assert_eq!(fields_ids.unwrap(), &[age_field_id][..]);
        drop(rtxn);

        // We reset the fields ids to become `None`, the default value.
        let mut wtxn = index.write_txn().unwrap();
        let mut builder = Settings::new(&mut wtxn, &index);
        builder.reset_displayed_fields();
        builder.execute().unwrap();
        wtxn.commit().unwrap();

        // Check that the displayed fields are correctly set to `None` (default value).
        let rtxn = index.read_txn().unwrap();
        let fields_ids = index.displayed_fields(&rtxn).unwrap();
        assert_eq!(fields_ids, None);
        drop(rtxn);
    }
}
