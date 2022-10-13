use std::io::Read;
use std::{fs::File, io::BufReader};

use flate2::bufread::GzDecoder;

use serde::Deserialize;

use tempfile::TempDir;

use crate::{Result, Version};

use self::compat::Compat;

mod compat;

// pub(self) mod v1;
pub(self) mod v2;
pub(self) mod v3;
pub(self) mod v4;
pub(self) mod v5;
pub(self) mod v6;

pub type Document = serde_json::Map<String, serde_json::Value>;
pub type UpdateFile = dyn Iterator<Item = Result<Document>>;

pub fn open(dump: impl Read) -> Result<Compat> {
    let path = TempDir::new()?;
    let mut dump = BufReader::new(dump);
    let gz = GzDecoder::new(&mut dump);
    let mut archive = tar::Archive::new(gz);
    archive.unpack(path.path())?;

    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct MetadataVersion {
        pub dump_version: Version,
    }
    let mut meta_file = File::open(path.path().join("metadata.json"))?;
    let MetadataVersion { dump_version } = serde_json::from_reader(&mut meta_file)?;

    match dump_version {
        // Version::V1 => Ok(Box::new(v1::Reader::open(path)?)),
        Version::V1 => todo!(),
        Version::V2 => Ok(v2::V2Reader::open(path)?
            .to_v3()
            .to_v4()
            .to_v5()
            .to_v6()
            .into()),
        Version::V3 => Ok(v3::V3Reader::open(path)?.to_v4().to_v5().to_v6().into()),
        Version::V4 => Ok(v4::V4Reader::open(path)?.to_v5().to_v6().into()),
        Version::V5 => Ok(v5::V5Reader::open(path)?.to_v6().into()),
        Version::V6 => Ok(v6::V6Reader::open(path)?.into()),
    }
}

#[cfg(test)]
pub(crate) mod test {
    use std::fs::File;

    use super::*;

    #[test]
    fn import_dump_v5() {
        let dump = File::open("tests/assets/v5.dump").unwrap();
        let mut dump = open(dump).unwrap();

        // top level infos
        insta::assert_display_snapshot!(dump.date().unwrap(), @"2022-10-04 15:55:10.344982459 +00:00:00");
        insta::assert_display_snapshot!(dump.instance_uid().unwrap().unwrap(), @"9e15e977-f2ae-4761-943f-1eaf75fd736d");

        // tasks
        let tasks = dump.tasks().collect::<Result<Vec<_>>>().unwrap();
        let (tasks, update_files): (Vec<_>, Vec<_>) = tasks.into_iter().unzip();
        meili_snap::snapshot_hash!(meili_snap::json_string!(tasks), @"0fff3c32487e3d3058d51ed951c1057f");
        assert_eq!(update_files.len(), 22);
        assert!(update_files[0].is_none()); // the dump creation
        assert!(update_files[1].is_some()); // the enqueued document addition
        assert!(update_files[2..].iter().all(|u| u.is_none())); // everything already processed

        // keys
        let keys = dump.keys().collect::<Result<Vec<_>>>().unwrap();
        meili_snap::snapshot_hash!(meili_snap::json_string!(keys), @"c9d2b467fe2fca0b35580d8a999808fb");

        // indexes
        let mut indexes = dump.indexes().unwrap().collect::<Result<Vec<_>>>().unwrap();
        // the index are not ordered in any way by default
        indexes.sort_by_key(|index| index.metadata().uid.to_string());

        let mut products = indexes.pop().unwrap();
        let mut movies = indexes.pop().unwrap();
        let mut spells = indexes.pop().unwrap();
        assert!(indexes.is_empty());

        // products
        insta::assert_json_snapshot!(products.metadata(), { ".createdAt" => "[now]", ".updatedAt" => "[now]" }, @r###"
        {
          "uid": "products",
          "primaryKey": "sku",
          "createdAt": "[now]",
          "updatedAt": "[now]"
        }
        "###);

        meili_snap::snapshot_hash!(format!("{:#?}", products.settings()), @"8e5cadabf74aebe1160bf51c3d489efe");
        let documents = products
            .documents()
            .unwrap()
            .collect::<Result<Vec<_>>>()
            .unwrap();
        assert_eq!(documents.len(), 10);
        meili_snap::snapshot_hash!(format!("{:#?}", documents), @"b01c8371aea4c7171af0d4d846a2bdca");

        // movies
        insta::assert_json_snapshot!(movies.metadata(), { ".createdAt" => "[now]", ".updatedAt" => "[now]" }, @r###"
        {
          "uid": "movies",
          "primaryKey": "id",
          "createdAt": "[now]",
          "updatedAt": "[now]"
        }
        "###);

        meili_snap::snapshot_hash!(format!("{:#?}", movies.settings()), @"4894ac1e74b9e1069ed5ee262b7a1aca");
        let documents = movies
            .documents()
            .unwrap()
            .collect::<Result<Vec<_>>>()
            .unwrap();
        assert_eq!(documents.len(), 200);
        meili_snap::snapshot_hash!(format!("{:#?}", documents), @"e962baafd2fbae4cdd14e876053b0c5a");

        // spells
        insta::assert_json_snapshot!(spells.metadata(), { ".createdAt" => "[now]", ".updatedAt" => "[now]" }, @r###"
        {
          "uid": "dnd_spells",
          "primaryKey": "index",
          "createdAt": "[now]",
          "updatedAt": "[now]"
        }
        "###);

        meili_snap::snapshot_hash!(format!("{:#?}", spells.settings()), @"054dbf08a79e08bb9becba6f5d090f13");
        let documents = spells
            .documents()
            .unwrap()
            .collect::<Result<Vec<_>>>()
            .unwrap();
        assert_eq!(documents.len(), 10);
        meili_snap::snapshot_hash!(format!("{:#?}", documents), @"235016433dd04262c7f2da01d1e808ce");
    }

    #[test]
    fn import_dump_v4() {
        let dump = File::open("tests/assets/v4.dump").unwrap();
        let mut dump = open(dump).unwrap();

        // top level infos
        insta::assert_display_snapshot!(dump.date().unwrap(), @"2022-10-06 12:53:49.131989609 +00:00:00");
        insta::assert_display_snapshot!(dump.instance_uid().unwrap().unwrap(), @"9e15e977-f2ae-4761-943f-1eaf75fd736d");

        // tasks
        let tasks = dump.tasks().collect::<Result<Vec<_>>>().unwrap();
        let (tasks, update_files): (Vec<_>, Vec<_>) = tasks.into_iter().unzip();
        meili_snap::snapshot_hash!(meili_snap::json_string!(tasks), @"0903b293c6ff8dc0819cbd3406848ef2");
        assert_eq!(update_files.len(), 10);
        assert!(update_files[0].is_some()); // the enqueued document addition
        assert!(update_files[1..].iter().all(|u| u.is_none())); // everything already processed

        // keys
        let keys = dump.keys().collect::<Result<Vec<_>>>().unwrap();
        meili_snap::snapshot_hash!(meili_snap::json_string!(keys, { "[].uid" => "[uuid]" }), @"23afab5753c5a99d8c530075bf0ebd9c");

        // indexes
        let mut indexes = dump.indexes().unwrap().collect::<Result<Vec<_>>>().unwrap();
        // the index are not ordered in any way by default
        indexes.sort_by_key(|index| index.metadata().uid.to_string());

        let mut products = indexes.pop().unwrap();
        let mut movies = indexes.pop().unwrap();
        let mut spells = indexes.pop().unwrap();
        assert!(indexes.is_empty());

        // products
        insta::assert_json_snapshot!(products.metadata(), { ".createdAt" => "[now]", ".updatedAt" => "[now]" }, @r###"
        {
          "uid": "products",
          "primaryKey": "sku",
          "createdAt": "[now]",
          "updatedAt": "[now]"
        }
        "###);

        meili_snap::snapshot_hash!(format!("{:#?}", products.settings()), @"1f9da51a4518166fb440def5437eafdb");
        let documents = products
            .documents()
            .unwrap()
            .collect::<Result<Vec<_>>>()
            .unwrap();
        assert_eq!(documents.len(), 10);
        meili_snap::snapshot_hash!(format!("{:#?}", documents), @"b01c8371aea4c7171af0d4d846a2bdca");

        // movies
        insta::assert_json_snapshot!(movies.metadata(), { ".createdAt" => "[now]", ".updatedAt" => "[now]" }, @r###"
        {
          "uid": "movies",
          "primaryKey": "id",
          "createdAt": "[now]",
          "updatedAt": "[now]"
        }
        "###);

        meili_snap::snapshot_hash!(format!("{:#?}", movies.settings()), @"488816aba82c1bd65f1609630055c611");
        let documents = movies
            .documents()
            .unwrap()
            .collect::<Result<Vec<_>>>()
            .unwrap();
        assert_eq!(documents.len(), 110);
        meili_snap::snapshot_hash!(format!("{:#?}", documents), @"786022a66ecb992c8a2a60fee070a5ab");

        // spells
        insta::assert_json_snapshot!(spells.metadata(), { ".createdAt" => "[now]", ".updatedAt" => "[now]" }, @r###"
        {
          "uid": "dnd_spells",
          "primaryKey": "index",
          "createdAt": "[now]",
          "updatedAt": "[now]"
        }
        "###);

        meili_snap::snapshot_hash!(format!("{:#?}", spells.settings()), @"7b4f66dad597dc651650f35fe34be27f");
        let documents = spells
            .documents()
            .unwrap()
            .collect::<Result<Vec<_>>>()
            .unwrap();
        assert_eq!(documents.len(), 10);
        meili_snap::snapshot_hash!(format!("{:#?}", documents), @"235016433dd04262c7f2da01d1e808ce");
    }

    #[test]
    fn import_dump_v3() {
        let dump = File::open("tests/assets/v3.dump").unwrap();
        let mut dump = open(dump).unwrap();

        // top level infos
        insta::assert_display_snapshot!(dump.date().unwrap(), @"2022-10-07 11:39:03.709153554 +00:00:00");
        assert_eq!(dump.instance_uid().unwrap(), None);

        // tasks
        let tasks = dump.tasks().collect::<Result<Vec<_>>>().unwrap();
        let (tasks, update_files): (Vec<_>, Vec<_>) = tasks.into_iter().unzip();
        meili_snap::snapshot_hash!(meili_snap::json_string!(tasks), @"891538c6fe0ba5187853a4f04890f9b5");
        assert_eq!(update_files.len(), 10);
        assert!(update_files[0].is_some()); // the enqueued document addition
        assert!(update_files[1..].iter().all(|u| u.is_none())); // everything already processed

        // keys
        let keys = dump.keys().collect::<Result<Vec<_>>>().unwrap();
        meili_snap::snapshot_hash!(meili_snap::json_string!(keys), @"d751713988987e9331980363e24189ce");

        // indexes
        let mut indexes = dump.indexes().unwrap().collect::<Result<Vec<_>>>().unwrap();
        // the index are not ordered in any way by default
        indexes.sort_by_key(|index| index.metadata().uid.to_string());

        let mut products = indexes.pop().unwrap();
        let mut movies2 = indexes.pop().unwrap();
        let mut movies = indexes.pop().unwrap();
        let mut spells = indexes.pop().unwrap();
        assert!(indexes.is_empty());

        // products
        insta::assert_json_snapshot!(products.metadata(), { ".createdAt" => "[now]", ".updatedAt" => "[now]" }, @r###"
        {
          "uid": "products",
          "primaryKey": "sku",
          "createdAt": "[now]",
          "updatedAt": "[now]"
        }
        "###);

        meili_snap::snapshot_hash!(format!("{:#?}", products.settings()), @"855f3165dec609b919171ff83f82b364");
        let documents = products
            .documents()
            .unwrap()
            .collect::<Result<Vec<_>>>()
            .unwrap();
        assert_eq!(documents.len(), 10);
        meili_snap::snapshot_hash!(format!("{:#?}", documents), @"548284a84de510f71e88e6cdea495cf5");

        // movies
        insta::assert_json_snapshot!(movies.metadata(), { ".createdAt" => "[now]", ".updatedAt" => "[now]" }, @r###"
        {
          "uid": "movies",
          "primaryKey": "id",
          "createdAt": "[now]",
          "updatedAt": "[now]"
        }
        "###);

        meili_snap::snapshot_hash!(format!("{:#?}", movies.settings()), @"43e0bf1746c3ea1d64c1e10ea544c190");
        let documents = movies
            .documents()
            .unwrap()
            .collect::<Result<Vec<_>>>()
            .unwrap();
        assert_eq!(documents.len(), 110);
        meili_snap::snapshot_hash!(format!("{:#?}", documents), @"d153b5a81d8b3cdcbe1dec270b574022");

        // movies2
        insta::assert_json_snapshot!(movies2.metadata(), { ".createdAt" => "[now]", ".updatedAt" => "[now]" }, @r###"
        {
          "uid": "movies_2",
          "primaryKey": null,
          "createdAt": "[now]",
          "updatedAt": "[now]"
        }
        "###);

        meili_snap::snapshot_hash!(format!("{:#?}", movies2.settings()), @"5fd06a5038f49311600379d43412b655");
        let documents = movies2
            .documents()
            .unwrap()
            .collect::<Result<Vec<_>>>()
            .unwrap();
        assert_eq!(documents.len(), 0);
        meili_snap::snapshot_hash!(format!("{:#?}", documents), @"d751713988987e9331980363e24189ce");

        // spells
        insta::assert_json_snapshot!(spells.metadata(), { ".createdAt" => "[now]", ".updatedAt" => "[now]" }, @r###"
        {
          "uid": "dnd_spells",
          "primaryKey": "index",
          "createdAt": "[now]",
          "updatedAt": "[now]"
        }
        "###);

        meili_snap::snapshot_hash!(format!("{:#?}", spells.settings()), @"5fd06a5038f49311600379d43412b655");
        let documents = spells
            .documents()
            .unwrap()
            .collect::<Result<Vec<_>>>()
            .unwrap();
        assert_eq!(documents.len(), 10);
        meili_snap::snapshot_hash!(format!("{:#?}", documents), @"235016433dd04262c7f2da01d1e808ce");
    }

    #[test]
    fn import_dump_v2() {
        let dump = File::open("tests/assets/v2.dump").unwrap();
        let mut dump = open(dump).unwrap();

        // top level infos
        insta::assert_display_snapshot!(dump.date().unwrap(), @"2022-10-09 20:27:59.904096267 +00:00:00");
        assert_eq!(dump.instance_uid().unwrap(), None);

        // tasks
        let tasks = dump.tasks().collect::<Result<Vec<_>>>().unwrap();
        let (tasks, update_files): (Vec<_>, Vec<_>) = tasks.into_iter().unzip();
        meili_snap::snapshot_hash!(meili_snap::json_string!(tasks), @"c52c07e1b356cce6982e2aeea7d0bf5e");
        assert_eq!(update_files.len(), 9);
        assert!(update_files[0].is_some()); // the enqueued document addition
        assert!(update_files[1..].iter().all(|u| u.is_none())); // everything already processed

        // keys
        let keys = dump.keys().collect::<Result<Vec<_>>>().unwrap();
        meili_snap::snapshot_hash!(meili_snap::json_string!(keys), @"d751713988987e9331980363e24189ce");

        // indexes
        let mut indexes = dump.indexes().unwrap().collect::<Result<Vec<_>>>().unwrap();
        // the index are not ordered in any way by default
        indexes.sort_by_key(|index| index.metadata().uid.to_string());

        let mut products = indexes.pop().unwrap();
        let mut movies2 = indexes.pop().unwrap();
        let mut movies = indexes.pop().unwrap();
        let mut spells = indexes.pop().unwrap();
        assert!(indexes.is_empty());

        // products
        insta::assert_json_snapshot!(products.metadata(), { ".createdAt" => "[now]", ".updatedAt" => "[now]" }, @r###"
        {
          "uid": "products",
          "primaryKey": "sku",
          "createdAt": "[now]",
          "updatedAt": "[now]"
        }
        "###);

        meili_snap::snapshot_hash!(format!("{:#?}", products.settings()), @"b15b71f56dd082d8e8ec5182e688bf36");
        let documents = products
            .documents()
            .unwrap()
            .collect::<Result<Vec<_>>>()
            .unwrap();
        assert_eq!(documents.len(), 10);
        meili_snap::snapshot_hash!(format!("{:#?}", documents), @"548284a84de510f71e88e6cdea495cf5");

        // movies
        insta::assert_json_snapshot!(movies.metadata(), { ".createdAt" => "[now]", ".updatedAt" => "[now]" }, @r###"
        {
          "uid": "movies",
          "primaryKey": "id",
          "createdAt": "[now]",
          "updatedAt": "[now]"
        }
        "###);

        meili_snap::snapshot_hash!(format!("{:#?}", movies.settings()), @"1e51f7fdc322176408f471a6d90d7698");
        let documents = movies
            .documents()
            .unwrap()
            .collect::<Result<Vec<_>>>()
            .unwrap();
        assert_eq!(documents.len(), 110);
        meili_snap::snapshot_hash!(format!("{:#?}", documents), @"d153b5a81d8b3cdcbe1dec270b574022");

        // movies2
        insta::assert_json_snapshot!(movies2.metadata(), { ".createdAt" => "[now]", ".updatedAt" => "[now]" }, @r###"
        {
          "uid": "movies_2",
          "primaryKey": null,
          "createdAt": "[now]",
          "updatedAt": "[now]"
        }
        "###);

        meili_snap::snapshot_hash!(format!("{:#?}", movies2.settings()), @"8aebab01301d266acf3e18dd449c008f");
        let documents = movies2
            .documents()
            .unwrap()
            .collect::<Result<Vec<_>>>()
            .unwrap();
        assert_eq!(documents.len(), 0);
        meili_snap::snapshot_hash!(format!("{:#?}", documents), @"d751713988987e9331980363e24189ce");

        // spells
        insta::assert_json_snapshot!(spells.metadata(), { ".createdAt" => "[now]", ".updatedAt" => "[now]" }, @r###"
        {
          "uid": "dnd_spells",
          "primaryKey": "index",
          "createdAt": "[now]",
          "updatedAt": "[now]"
        }
        "###);

        meili_snap::snapshot_hash!(format!("{:#?}", spells.settings()), @"8aebab01301d266acf3e18dd449c008f");
        let documents = spells
            .documents()
            .unwrap()
            .collect::<Result<Vec<_>>>()
            .unwrap();
        assert_eq!(documents.len(), 10);
        meili_snap::snapshot_hash!(format!("{:#?}", documents), @"235016433dd04262c7f2da01d1e808ce");
    }
}
