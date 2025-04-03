use meilisearch_types::milli::update::IndexDocumentsMethod::{
    self, ReplaceDocuments, UpdateDocuments,
};
use meilisearch_types::tasks::{IndexSwap, KindWithContent};
use uuid::Uuid;

use self::autobatcher::{autobatch, BatchKind};
use super::*;
use crate::TaskId;

#[macro_export]
macro_rules! debug_snapshot {
        ($value:expr, @$snapshot:literal) => {{
            let value = format!("{:?}", $value);
            meili_snap::snapshot!(value, @$snapshot);
        }};
    }

fn autobatch_from(
    index_already_exists: bool,
    primary_key: Option<&str>,
    input: impl IntoIterator<Item = KindWithContent>,
) -> Option<(BatchKind, bool)> {
    autobatch(
        input.into_iter().enumerate().map(|(id, kind)| (id as TaskId, kind)).collect(),
        index_already_exists,
        primary_key,
    )
}

fn doc_imp(
    method: IndexDocumentsMethod,
    allow_index_creation: bool,
    primary_key: Option<&str>,
) -> KindWithContent {
    KindWithContent::DocumentAdditionOrUpdate {
        index_uid: String::from("doggo"),
        primary_key: primary_key.map(|pk| pk.to_string()),
        method,
        content_file: Uuid::new_v4(),
        documents_count: 0,
        allow_index_creation,
    }
}

fn doc_del() -> KindWithContent {
    KindWithContent::DocumentDeletion {
        index_uid: String::from("doggo"),
        documents_ids: Vec::new(),
    }
}

fn doc_del_fil() -> KindWithContent {
    KindWithContent::DocumentDeletionByFilter {
        index_uid: String::from("doggo"),
        filter_expr: serde_json::json!("cuteness > 100"),
    }
}

fn doc_clr() -> KindWithContent {
    KindWithContent::DocumentClear { index_uid: String::from("doggo") }
}

fn settings(allow_index_creation: bool) -> KindWithContent {
    KindWithContent::SettingsUpdate {
        index_uid: String::from("doggo"),
        new_settings: Default::default(),
        is_deletion: false,
        allow_index_creation,
    }
}

fn idx_create() -> KindWithContent {
    KindWithContent::IndexCreation { index_uid: String::from("doggo"), primary_key: None }
}

fn idx_update() -> KindWithContent {
    KindWithContent::IndexUpdate { index_uid: String::from("doggo"), primary_key: None }
}

fn idx_del() -> KindWithContent {
    KindWithContent::IndexDeletion { index_uid: String::from("doggo") }
}

fn idx_swap() -> KindWithContent {
    KindWithContent::IndexSwap {
        swaps: vec![IndexSwap { indexes: (String::from("doggo"), String::from("catto")) }],
    }
}

#[test]
fn autobatch_simple_operation_together() {
    // we can autobatch one or multiple `ReplaceDocuments` together.
    // if the index exists.
    debug_snapshot!(autobatch_from(true, None, [doc_imp(ReplaceDocuments, true, None)]), @"Some((DocumentOperation { allow_index_creation: true, primary_key: None, operation_ids: [0] }, true))");
    debug_snapshot!(autobatch_from(true, None, [doc_imp(ReplaceDocuments, false, None)]), @"Some((DocumentOperation { allow_index_creation: false, primary_key: None, operation_ids: [0] }, false))");
    debug_snapshot!(autobatch_from(true, None, [doc_imp(ReplaceDocuments, true, None), doc_imp( ReplaceDocuments, true , None), doc_imp(ReplaceDocuments, true , None)]), @"Some((DocumentOperation { allow_index_creation: true, primary_key: None, operation_ids: [0, 1, 2] }, true))");
    debug_snapshot!(autobatch_from(true, None, [doc_imp(ReplaceDocuments, false, None), doc_imp( ReplaceDocuments, false , None), doc_imp(ReplaceDocuments, false , None)]), @"Some((DocumentOperation { allow_index_creation: false, primary_key: None, operation_ids: [0, 1, 2] }, false))");

    // if it doesn't exists.
    debug_snapshot!(autobatch_from(false,None,  [doc_imp(ReplaceDocuments, true, None)]), @"Some((DocumentOperation { allow_index_creation: true, primary_key: None, operation_ids: [0] }, true))");
    debug_snapshot!(autobatch_from(false,None,  [doc_imp(ReplaceDocuments, false, None)]), @"Some((DocumentOperation { allow_index_creation: false, primary_key: None, operation_ids: [0] }, false))");
    debug_snapshot!(autobatch_from(false,None,  [doc_imp(ReplaceDocuments, true, None), doc_imp( ReplaceDocuments, true , None), doc_imp(ReplaceDocuments, true , None)]), @"Some((DocumentOperation { allow_index_creation: true, primary_key: None, operation_ids: [0, 1, 2] }, true))");
    debug_snapshot!(autobatch_from(false,None,  [doc_imp(ReplaceDocuments, false, None), doc_imp( ReplaceDocuments, true , None), doc_imp(ReplaceDocuments, true , None)]), @"Some((DocumentOperation { allow_index_creation: false, primary_key: None, operation_ids: [0] }, false))");

    // we can autobatch one or multiple `UpdateDocuments` together.
    // if the index exists.
    debug_snapshot!(autobatch_from(true, None, [doc_imp(UpdateDocuments, true, None)]), @"Some((DocumentOperation { allow_index_creation: true, primary_key: None, operation_ids: [0] }, true))");
    debug_snapshot!(autobatch_from(true, None, [doc_imp(UpdateDocuments, true, None), doc_imp(UpdateDocuments, true, None), doc_imp(UpdateDocuments, true, None)]), @"Some((DocumentOperation { allow_index_creation: true, primary_key: None, operation_ids: [0, 1, 2] }, true))");
    debug_snapshot!(autobatch_from(true, None, [doc_imp(UpdateDocuments, false, None)]), @"Some((DocumentOperation { allow_index_creation: false, primary_key: None, operation_ids: [0] }, false))");
    debug_snapshot!(autobatch_from(true, None, [doc_imp(UpdateDocuments, false, None), doc_imp(UpdateDocuments, false, None), doc_imp(UpdateDocuments, false, None)]), @"Some((DocumentOperation { allow_index_creation: false, primary_key: None, operation_ids: [0, 1, 2] }, false))");

    // if it doesn't exists.
    debug_snapshot!(autobatch_from(false,None,  [doc_imp(UpdateDocuments, true, None)]), @"Some((DocumentOperation { allow_index_creation: true, primary_key: None, operation_ids: [0] }, true))");
    debug_snapshot!(autobatch_from(false,None,  [doc_imp(UpdateDocuments, true, None), doc_imp(UpdateDocuments, true, None), doc_imp(UpdateDocuments, true, None)]), @"Some((DocumentOperation { allow_index_creation: true, primary_key: None, operation_ids: [0, 1, 2] }, true))");
    debug_snapshot!(autobatch_from(false,None,  [doc_imp(UpdateDocuments, false, None)]), @"Some((DocumentOperation { allow_index_creation: false, primary_key: None, operation_ids: [0] }, false))");
    debug_snapshot!(autobatch_from(false,None,  [doc_imp(UpdateDocuments, false, None), doc_imp(UpdateDocuments, false, None), doc_imp(UpdateDocuments, false, None)]), @"Some((DocumentOperation { allow_index_creation: false, primary_key: None, operation_ids: [0, 1, 2] }, false))");

    // we can autobatch one or multiple DocumentDeletion together
    debug_snapshot!(autobatch_from(true, None, [doc_del()]), @"Some((DocumentDeletion { deletion_ids: [0], includes_by_filter: false }, false))");
    debug_snapshot!(autobatch_from(true, None, [doc_del(), doc_del(), doc_del()]), @"Some((DocumentDeletion { deletion_ids: [0, 1, 2], includes_by_filter: false }, false))");
    debug_snapshot!(autobatch_from(false,None,  [doc_del()]), @"Some((DocumentDeletion { deletion_ids: [0], includes_by_filter: false }, false))");
    debug_snapshot!(autobatch_from(false,None,  [doc_del(), doc_del(), doc_del()]), @"Some((DocumentDeletion { deletion_ids: [0, 1, 2], includes_by_filter: false }, false))");

    // we can autobatch one or multiple DocumentDeletionByFilter together
    debug_snapshot!(autobatch_from(true, None, [doc_del_fil()]), @"Some((DocumentDeletion { deletion_ids: [0], includes_by_filter: true }, false))");
    debug_snapshot!(autobatch_from(true, None, [doc_del_fil(), doc_del_fil(), doc_del_fil()]), @"Some((DocumentDeletion { deletion_ids: [0, 1, 2], includes_by_filter: true }, false))");
    debug_snapshot!(autobatch_from(false,None,  [doc_del_fil()]), @"Some((DocumentDeletion { deletion_ids: [0], includes_by_filter: true }, false))");
    debug_snapshot!(autobatch_from(false,None,  [doc_del_fil(), doc_del_fil(), doc_del_fil()]), @"Some((DocumentDeletion { deletion_ids: [0, 1, 2], includes_by_filter: true }, false))");

    // we can autobatch one or multiple Settings together
    debug_snapshot!(autobatch_from(true, None, [settings(true)]), @"Some((Settings { allow_index_creation: true, settings_ids: [0] }, true))");
    debug_snapshot!(autobatch_from(true, None, [settings(true), settings(true), settings(true)]), @"Some((Settings { allow_index_creation: true, settings_ids: [0, 1, 2] }, true))");
    debug_snapshot!(autobatch_from(true, None, [settings(false)]), @"Some((Settings { allow_index_creation: false, settings_ids: [0] }, false))");
    debug_snapshot!(autobatch_from(true, None, [settings(false), settings(false), settings(false)]), @"Some((Settings { allow_index_creation: false, settings_ids: [0, 1, 2] }, false))");

    debug_snapshot!(autobatch_from(false,None,  [settings(true)]), @"Some((Settings { allow_index_creation: true, settings_ids: [0] }, true))");
    debug_snapshot!(autobatch_from(false,None,  [settings(true), settings(true), settings(true)]), @"Some((Settings { allow_index_creation: true, settings_ids: [0, 1, 2] }, true))");
    debug_snapshot!(autobatch_from(false,None,  [settings(false)]), @"Some((Settings { allow_index_creation: false, settings_ids: [0] }, false))");
    debug_snapshot!(autobatch_from(false,None,  [settings(false), settings(false), settings(false)]), @"Some((Settings { allow_index_creation: false, settings_ids: [0, 1, 2] }, false))");

    // We can autobatch document addition with document deletion
    debug_snapshot!(autobatch_from(true, None, [doc_imp(ReplaceDocuments, true, None), doc_del()]), @"Some((DocumentOperation { allow_index_creation: true, primary_key: None, operation_ids: [0, 1] }, true))");
    debug_snapshot!(autobatch_from(true, None, [doc_imp(UpdateDocuments, true, None), doc_del()]), @"Some((DocumentOperation { allow_index_creation: true, primary_key: None, operation_ids: [0, 1] }, true))");
    debug_snapshot!(autobatch_from(true, None, [doc_imp(ReplaceDocuments, false, None), doc_del()]), @"Some((DocumentOperation { allow_index_creation: false, primary_key: None, operation_ids: [0, 1] }, false))");
    debug_snapshot!(autobatch_from(true, None, [doc_imp(UpdateDocuments, false, None), doc_del()]), @"Some((DocumentOperation { allow_index_creation: false, primary_key: None, operation_ids: [0, 1] }, false))");
    debug_snapshot!(autobatch_from(true, None, [doc_imp(ReplaceDocuments, true, Some("catto")), doc_del()]), @r###"Some((DocumentOperation { allow_index_creation: true, primary_key: Some("catto"), operation_ids: [0, 1] }, true))"###);
    debug_snapshot!(autobatch_from(true, None, [doc_imp(UpdateDocuments, true, Some("catto")), doc_del()]), @r###"Some((DocumentOperation { allow_index_creation: true, primary_key: Some("catto"), operation_ids: [0, 1] }, true))"###);
    debug_snapshot!(autobatch_from(true, None, [doc_imp(ReplaceDocuments, false, Some("catto")), doc_del()]), @r###"Some((DocumentOperation { allow_index_creation: false, primary_key: Some("catto"), operation_ids: [0, 1] }, false))"###);
    debug_snapshot!(autobatch_from(true, None, [doc_imp(UpdateDocuments, false, Some("catto")), doc_del()]), @r###"Some((DocumentOperation { allow_index_creation: false, primary_key: Some("catto"), operation_ids: [0, 1] }, false))"###);
    debug_snapshot!(autobatch_from(false, None, [doc_imp(ReplaceDocuments, true, None), doc_del()]), @"Some((DocumentOperation { allow_index_creation: true, primary_key: None, operation_ids: [0, 1] }, true))");
    debug_snapshot!(autobatch_from(false, None, [doc_imp(UpdateDocuments, true, None), doc_del()]), @"Some((DocumentOperation { allow_index_creation: true, primary_key: None, operation_ids: [0, 1] }, true))");
    debug_snapshot!(autobatch_from(false, None, [doc_imp(ReplaceDocuments, false, None), doc_del()]), @"Some((DocumentOperation { allow_index_creation: false, primary_key: None, operation_ids: [0, 1] }, false))");
    debug_snapshot!(autobatch_from(false, None, [doc_imp(UpdateDocuments, false, None), doc_del()]), @"Some((DocumentOperation { allow_index_creation: false, primary_key: None, operation_ids: [0, 1] }, false))");
    debug_snapshot!(autobatch_from(false, None, [doc_imp(ReplaceDocuments, true, Some("catto")), doc_del()]), @r###"Some((DocumentOperation { allow_index_creation: true, primary_key: Some("catto"), operation_ids: [0, 1] }, true))"###);
    debug_snapshot!(autobatch_from(false, None, [doc_imp(UpdateDocuments, true, Some("catto")), doc_del()]), @r###"Some((DocumentOperation { allow_index_creation: true, primary_key: Some("catto"), operation_ids: [0, 1] }, true))"###);
    debug_snapshot!(autobatch_from(false, None, [doc_imp(ReplaceDocuments, false, Some("catto")), doc_del()]), @r###"Some((DocumentOperation { allow_index_creation: false, primary_key: Some("catto"), operation_ids: [0, 1] }, false))"###);
    debug_snapshot!(autobatch_from(false, None, [doc_imp(UpdateDocuments, false, Some("catto")), doc_del()]), @r###"Some((DocumentOperation { allow_index_creation: false, primary_key: Some("catto"), operation_ids: [0, 1] }, false))"###);
    // And the other way around
    debug_snapshot!(autobatch_from(true, None, [doc_del(), doc_imp(ReplaceDocuments, true, None)]), @"Some((DocumentOperation { allow_index_creation: true, primary_key: None, operation_ids: [0, 1] }, false))");
    debug_snapshot!(autobatch_from(true, None, [doc_del(), doc_imp(UpdateDocuments, true, None)]), @"Some((DocumentOperation { allow_index_creation: true, primary_key: None, operation_ids: [0, 1] }, false))");
    debug_snapshot!(autobatch_from(true, None, [doc_del(), doc_imp(ReplaceDocuments, false, None)]), @"Some((DocumentOperation { allow_index_creation: false, primary_key: None, operation_ids: [0, 1] }, false))");
    debug_snapshot!(autobatch_from(true, None, [doc_del(), doc_imp(UpdateDocuments, false, None)]), @"Some((DocumentOperation { allow_index_creation: false, primary_key: None, operation_ids: [0, 1] }, false))");
    debug_snapshot!(autobatch_from(true, None, [doc_del(), doc_imp(ReplaceDocuments, true, Some("catto"))]), @r###"Some((DocumentOperation { allow_index_creation: true, primary_key: Some("catto"), operation_ids: [0, 1] }, false))"###);
    debug_snapshot!(autobatch_from(true, None, [doc_del(), doc_imp(UpdateDocuments, true, Some("catto"))]), @r###"Some((DocumentOperation { allow_index_creation: true, primary_key: Some("catto"), operation_ids: [0, 1] }, false))"###);
    debug_snapshot!(autobatch_from(true, None, [doc_del(), doc_imp(ReplaceDocuments, false, Some("catto"))]), @r###"Some((DocumentOperation { allow_index_creation: false, primary_key: Some("catto"), operation_ids: [0, 1] }, false))"###);
    debug_snapshot!(autobatch_from(true, None, [doc_del(), doc_imp(UpdateDocuments, false, Some("catto"))]), @r###"Some((DocumentOperation { allow_index_creation: false, primary_key: Some("catto"), operation_ids: [0, 1] }, false))"###);
    debug_snapshot!(autobatch_from(false, None, [doc_del(), doc_imp(ReplaceDocuments, false, None)]), @"Some((DocumentOperation { allow_index_creation: false, primary_key: None, operation_ids: [0, 1] }, false))");
    debug_snapshot!(autobatch_from(false, None, [doc_del(), doc_imp(UpdateDocuments, false, None)]), @"Some((DocumentOperation { allow_index_creation: false, primary_key: None, operation_ids: [0, 1] }, false))");
    debug_snapshot!(autobatch_from(false, None, [doc_del(), doc_imp(ReplaceDocuments, false, Some("catto"))]), @r###"Some((DocumentOperation { allow_index_creation: false, primary_key: Some("catto"), operation_ids: [0, 1] }, false))"###);
    debug_snapshot!(autobatch_from(false, None, [doc_del(), doc_imp(UpdateDocuments, false, Some("catto"))]), @r###"Some((DocumentOperation { allow_index_creation: false, primary_key: Some("catto"), operation_ids: [0, 1] }, false))"###);

    // But we can't autobatch document addition with document deletion by filter
    debug_snapshot!(autobatch_from(true, None, [doc_imp(ReplaceDocuments, true, None), doc_del_fil()]), @"Some((DocumentOperation { allow_index_creation: true, primary_key: None, operation_ids: [0] }, true))");
    debug_snapshot!(autobatch_from(true, None, [doc_imp(UpdateDocuments, true, None), doc_del_fil()]), @"Some((DocumentOperation { allow_index_creation: true, primary_key: None, operation_ids: [0] }, true))");
    debug_snapshot!(autobatch_from(true, None, [doc_imp(ReplaceDocuments, false, None), doc_del_fil()]), @"Some((DocumentOperation { allow_index_creation: false, primary_key: None, operation_ids: [0] }, false))");
    debug_snapshot!(autobatch_from(true, None, [doc_imp(UpdateDocuments, false, None), doc_del_fil()]), @"Some((DocumentOperation { allow_index_creation: false, primary_key: None, operation_ids: [0] }, false))");
    debug_snapshot!(autobatch_from(true, None, [doc_imp(ReplaceDocuments, true, Some("catto")), doc_del_fil()]), @r###"Some((DocumentOperation { allow_index_creation: true, primary_key: Some("catto"), operation_ids: [0] }, true))"###);
    debug_snapshot!(autobatch_from(true, None, [doc_imp(UpdateDocuments, true, Some("catto")), doc_del_fil()]), @r###"Some((DocumentOperation { allow_index_creation: true, primary_key: Some("catto"), operation_ids: [0] }, true))"###);
    debug_snapshot!(autobatch_from(true, None, [doc_imp(ReplaceDocuments, false, Some("catto")), doc_del_fil()]), @r###"Some((DocumentOperation { allow_index_creation: false, primary_key: Some("catto"), operation_ids: [0] }, false))"###);
    debug_snapshot!(autobatch_from(true, None, [doc_imp(UpdateDocuments, false, Some("catto")), doc_del_fil()]), @r###"Some((DocumentOperation { allow_index_creation: false, primary_key: Some("catto"), operation_ids: [0] }, false))"###);
    debug_snapshot!(autobatch_from(false, None, [doc_imp(ReplaceDocuments, true, None), doc_del_fil()]), @"Some((DocumentOperation { allow_index_creation: true, primary_key: None, operation_ids: [0] }, true))");
    debug_snapshot!(autobatch_from(false, None, [doc_imp(UpdateDocuments, true, None), doc_del_fil()]), @"Some((DocumentOperation { allow_index_creation: true, primary_key: None, operation_ids: [0] }, true))");
    debug_snapshot!(autobatch_from(false, None, [doc_imp(ReplaceDocuments, false, None), doc_del_fil()]), @"Some((DocumentOperation { allow_index_creation: false, primary_key: None, operation_ids: [0] }, false))");
    debug_snapshot!(autobatch_from(false, None, [doc_imp(UpdateDocuments, false, None), doc_del_fil()]), @"Some((DocumentOperation { allow_index_creation: false, primary_key: None, operation_ids: [0] }, false))");
    debug_snapshot!(autobatch_from(false, None, [doc_imp(ReplaceDocuments, true, Some("catto")), doc_del_fil()]), @r###"Some((DocumentOperation { allow_index_creation: true, primary_key: Some("catto"), operation_ids: [0] }, true))"###);
    debug_snapshot!(autobatch_from(false, None, [doc_imp(UpdateDocuments, true, Some("catto")), doc_del_fil()]), @r###"Some((DocumentOperation { allow_index_creation: true, primary_key: Some("catto"), operation_ids: [0] }, true))"###);
    debug_snapshot!(autobatch_from(false, None, [doc_imp(ReplaceDocuments, false, Some("catto")), doc_del_fil()]), @r###"Some((DocumentOperation { allow_index_creation: false, primary_key: Some("catto"), operation_ids: [0] }, false))"###);
    debug_snapshot!(autobatch_from(false, None, [doc_imp(UpdateDocuments, false, Some("catto")), doc_del_fil()]), @r###"Some((DocumentOperation { allow_index_creation: false, primary_key: Some("catto"), operation_ids: [0] }, false))"###);
    // And the other way around
    debug_snapshot!(autobatch_from(true, None, [doc_del_fil(), doc_imp(ReplaceDocuments, true, None)]), @"Some((DocumentDeletion { deletion_ids: [0], includes_by_filter: true }, false))");
    debug_snapshot!(autobatch_from(true, None, [doc_del_fil(), doc_imp(UpdateDocuments, true, None)]), @"Some((DocumentDeletion { deletion_ids: [0], includes_by_filter: true }, false))");
    debug_snapshot!(autobatch_from(true, None, [doc_del_fil(), doc_imp(ReplaceDocuments, false, None)]), @"Some((DocumentDeletion { deletion_ids: [0], includes_by_filter: true }, false))");
    debug_snapshot!(autobatch_from(true, None, [doc_del_fil(), doc_imp(UpdateDocuments, false, None)]), @"Some((DocumentDeletion { deletion_ids: [0], includes_by_filter: true }, false))");
    debug_snapshot!(autobatch_from(true, None, [doc_del_fil(), doc_imp(ReplaceDocuments, true, Some("catto"))]), @"Some((DocumentDeletion { deletion_ids: [0], includes_by_filter: true }, false))");
    debug_snapshot!(autobatch_from(true, None, [doc_del_fil(), doc_imp(UpdateDocuments, true, Some("catto"))]), @"Some((DocumentDeletion { deletion_ids: [0], includes_by_filter: true }, false))");
    debug_snapshot!(autobatch_from(true, None, [doc_del_fil(), doc_imp(ReplaceDocuments, false, Some("catto"))]), @"Some((DocumentDeletion { deletion_ids: [0], includes_by_filter: true }, false))");
    debug_snapshot!(autobatch_from(true, None, [doc_del_fil(), doc_imp(UpdateDocuments, false, Some("catto"))]), @"Some((DocumentDeletion { deletion_ids: [0], includes_by_filter: true }, false))");
    debug_snapshot!(autobatch_from(false, None, [doc_del_fil(), doc_imp(ReplaceDocuments, false, None)]), @"Some((DocumentDeletion { deletion_ids: [0], includes_by_filter: true }, false))");
    debug_snapshot!(autobatch_from(false, None, [doc_del_fil(), doc_imp(UpdateDocuments, false, None)]), @"Some((DocumentDeletion { deletion_ids: [0], includes_by_filter: true }, false))");
    debug_snapshot!(autobatch_from(false, None, [doc_del_fil(), doc_imp(ReplaceDocuments, false, Some("catto"))]), @"Some((DocumentDeletion { deletion_ids: [0], includes_by_filter: true }, false))");
    debug_snapshot!(autobatch_from(false, None, [doc_del_fil(), doc_imp(UpdateDocuments, false, Some("catto"))]), @"Some((DocumentDeletion { deletion_ids: [0], includes_by_filter: true }, false))");
}

#[test]
fn simple_different_document_operations_autobatch_together() {
    // addition and updates with deletion by filter can't batch together
    debug_snapshot!(autobatch_from(true, None, [doc_imp(ReplaceDocuments, true, None), doc_imp(UpdateDocuments, true, None)]), @"Some((DocumentOperation { allow_index_creation: true, primary_key: None, operation_ids: [0, 1] }, true))");
    debug_snapshot!(autobatch_from(true, None, [doc_imp(UpdateDocuments, true, None), doc_imp(ReplaceDocuments, true, None)]), @"Some((DocumentOperation { allow_index_creation: true, primary_key: None, operation_ids: [0, 1] }, true))");
    debug_snapshot!(autobatch_from(true, None, [doc_imp(UpdateDocuments, true, None), doc_del_fil()]), @"Some((DocumentOperation { allow_index_creation: true, primary_key: None, operation_ids: [0] }, true))");
    debug_snapshot!(autobatch_from(true, None, [doc_imp(ReplaceDocuments, true, None), doc_del_fil()]), @"Some((DocumentOperation { allow_index_creation: true, primary_key: None, operation_ids: [0] }, true))");
    debug_snapshot!(autobatch_from(true, None, [doc_del_fil(), doc_imp(UpdateDocuments, true, None)]), @"Some((DocumentDeletion { deletion_ids: [0], includes_by_filter: true }, false))");
    debug_snapshot!(autobatch_from(true, None, [doc_del_fil(), doc_imp(ReplaceDocuments, true, None)]), @"Some((DocumentDeletion { deletion_ids: [0], includes_by_filter: true }, false))");

    debug_snapshot!(autobatch_from(true, None, [doc_imp(ReplaceDocuments, true, None), idx_create()]), @"Some((DocumentOperation { allow_index_creation: true, primary_key: None, operation_ids: [0] }, true))");
    debug_snapshot!(autobatch_from(true, None, [doc_imp(UpdateDocuments, true, None), idx_create()]), @"Some((DocumentOperation { allow_index_creation: true, primary_key: None, operation_ids: [0] }, true))");
    debug_snapshot!(autobatch_from(true, None, [doc_del(), idx_create()]), @"Some((DocumentDeletion { deletion_ids: [0], includes_by_filter: false }, false))");
    debug_snapshot!(autobatch_from(true, None, [doc_del_fil(), idx_create()]), @"Some((DocumentDeletion { deletion_ids: [0], includes_by_filter: true }, false))");

    debug_snapshot!(autobatch_from(true, None, [doc_imp(ReplaceDocuments, true, None), idx_update()]), @"Some((DocumentOperation { allow_index_creation: true, primary_key: None, operation_ids: [0] }, true))");
    debug_snapshot!(autobatch_from(true, None, [doc_imp(UpdateDocuments, true, None), idx_update()]), @"Some((DocumentOperation { allow_index_creation: true, primary_key: None, operation_ids: [0] }, true))");
    debug_snapshot!(autobatch_from(true, None, [doc_del(), idx_update()]), @"Some((DocumentDeletion { deletion_ids: [0], includes_by_filter: false }, false))");
    debug_snapshot!(autobatch_from(true, None, [doc_del_fil(), idx_update()]), @"Some((DocumentDeletion { deletion_ids: [0], includes_by_filter: true }, false))");

    debug_snapshot!(autobatch_from(true, None, [doc_imp(ReplaceDocuments, true, None), idx_swap()]), @"Some((DocumentOperation { allow_index_creation: true, primary_key: None, operation_ids: [0] }, true))");
    debug_snapshot!(autobatch_from(true, None, [doc_imp(UpdateDocuments, true, None), idx_swap()]), @"Some((DocumentOperation { allow_index_creation: true, primary_key: None, operation_ids: [0] }, true))");
    debug_snapshot!(autobatch_from(true, None, [doc_del(), idx_swap()]), @"Some((DocumentDeletion { deletion_ids: [0], includes_by_filter: false }, false))");
    debug_snapshot!(autobatch_from(true, None, [doc_del_fil(), idx_swap()]), @"Some((DocumentDeletion { deletion_ids: [0], includes_by_filter: true }, false))");
}

#[test]
fn document_addition_doesnt_batch_with_settings() {
    // simple case
    debug_snapshot!(autobatch_from(true, None, [doc_imp(ReplaceDocuments, true, None), settings(true)]), @"Some((DocumentOperation { allow_index_creation: true, primary_key: None, operation_ids: [0] }, true))");
    debug_snapshot!(autobatch_from(true, None, [doc_imp(UpdateDocuments, true, None), settings(true)]), @"Some((DocumentOperation { allow_index_creation: true, primary_key: None, operation_ids: [0] }, true))");

    // multiple settings and doc addition
    debug_snapshot!(autobatch_from(true, None, [doc_imp(ReplaceDocuments, true, None), doc_imp(ReplaceDocuments, true, None), settings(true), settings(true)]), @"Some((DocumentOperation { allow_index_creation: true, primary_key: None, operation_ids: [0, 1] }, true))");
    debug_snapshot!(autobatch_from(true, None, [doc_imp(ReplaceDocuments, true, None), doc_imp(ReplaceDocuments, true, None), settings(true), settings(true)]), @"Some((DocumentOperation { allow_index_creation: true, primary_key: None, operation_ids: [0, 1] }, true))");

    // addition and setting unordered
    debug_snapshot!(autobatch_from(true, None, [doc_imp(ReplaceDocuments, true, None), settings(true), doc_imp(ReplaceDocuments, true, None), settings(true)]), @"Some((DocumentOperation { allow_index_creation: true, primary_key: None, operation_ids: [0] }, true))");
    debug_snapshot!(autobatch_from(true, None, [doc_imp(UpdateDocuments, true, None), settings(true), doc_imp(UpdateDocuments, true, None), settings(true)]), @"Some((DocumentOperation { allow_index_creation: true, primary_key: None, operation_ids: [0] }, true))");

    // Doesn't batch with other forbidden operations
    debug_snapshot!(autobatch_from(true, None, [doc_imp(ReplaceDocuments, true, None), settings(true), doc_imp(UpdateDocuments, true, None)]), @"Some((DocumentOperation { allow_index_creation: true, primary_key: None, operation_ids: [0] }, true))");
    debug_snapshot!(autobatch_from(true, None, [doc_imp(UpdateDocuments, true, None), settings(true), doc_imp(ReplaceDocuments, true, None)]), @"Some((DocumentOperation { allow_index_creation: true, primary_key: None, operation_ids: [0] }, true))");
    debug_snapshot!(autobatch_from(true, None, [doc_imp(ReplaceDocuments, true, None), settings(true), doc_del()]), @"Some((DocumentOperation { allow_index_creation: true, primary_key: None, operation_ids: [0] }, true))");
    debug_snapshot!(autobatch_from(true, None, [doc_imp(UpdateDocuments, true, None), settings(true), doc_del()]), @"Some((DocumentOperation { allow_index_creation: true, primary_key: None, operation_ids: [0] }, true))");
    debug_snapshot!(autobatch_from(true, None, [doc_imp(ReplaceDocuments, true, None), settings(true), idx_create()]), @"Some((DocumentOperation { allow_index_creation: true, primary_key: None, operation_ids: [0] }, true))");
    debug_snapshot!(autobatch_from(true, None, [doc_imp(UpdateDocuments, true, None), settings(true), idx_create()]), @"Some((DocumentOperation { allow_index_creation: true, primary_key: None, operation_ids: [0] }, true))");
    debug_snapshot!(autobatch_from(true, None, [doc_imp(ReplaceDocuments, true, None), settings(true), idx_update()]), @"Some((DocumentOperation { allow_index_creation: true, primary_key: None, operation_ids: [0] }, true))");
    debug_snapshot!(autobatch_from(true, None, [doc_imp(UpdateDocuments, true, None), settings(true), idx_update()]), @"Some((DocumentOperation { allow_index_creation: true, primary_key: None, operation_ids: [0] }, true))");
    debug_snapshot!(autobatch_from(true, None, [doc_imp(ReplaceDocuments, true, None), settings(true), idx_swap()]), @"Some((DocumentOperation { allow_index_creation: true, primary_key: None, operation_ids: [0] }, true))");
    debug_snapshot!(autobatch_from(true, None, [doc_imp(UpdateDocuments, true, None), settings(true), idx_swap()]), @"Some((DocumentOperation { allow_index_creation: true, primary_key: None, operation_ids: [0] }, true))");
}

#[test]
fn clear_and_additions() {
    // these two doesn't need to batch
    debug_snapshot!(autobatch_from(true, None, [doc_clr(), doc_imp(ReplaceDocuments, true, None)]), @"Some((DocumentClear { ids: [0] }, false))");
    debug_snapshot!(autobatch_from(true, None, [doc_clr(), doc_imp(UpdateDocuments, true, None)]), @"Some((DocumentClear { ids: [0] }, false))");

    // Basic use case
    debug_snapshot!(autobatch_from(true, None, [doc_imp(ReplaceDocuments, true, None), doc_imp(ReplaceDocuments, true, None), doc_clr()]), @"Some((DocumentClear { ids: [0, 1, 2] }, true))");
    debug_snapshot!(autobatch_from(true, None, [doc_imp(UpdateDocuments, true, None), doc_imp(UpdateDocuments, true, None), doc_clr()]), @"Some((DocumentClear { ids: [0, 1, 2] }, true))");

    // This batch kind doesn't mix with other document addition
    debug_snapshot!(autobatch_from(true, None, [doc_imp(ReplaceDocuments, true, None), doc_imp(ReplaceDocuments, true, None), doc_clr(), doc_imp(ReplaceDocuments, true, None)]), @"Some((DocumentClear { ids: [0, 1, 2] }, true))");
    debug_snapshot!(autobatch_from(true, None, [doc_imp(UpdateDocuments, true, None), doc_imp(UpdateDocuments, true, None), doc_clr(), doc_imp(UpdateDocuments, true, None)]), @"Some((DocumentClear { ids: [0, 1, 2] }, true))");

    // But you can batch multiple clear together
    debug_snapshot!(autobatch_from(true, None, [doc_imp(ReplaceDocuments, true, None), doc_imp(ReplaceDocuments, true, None), doc_clr(), doc_clr(), doc_clr()]), @"Some((DocumentClear { ids: [0, 1, 2, 3, 4] }, true))");
    debug_snapshot!(autobatch_from(true, None, [doc_imp(UpdateDocuments, true, None), doc_imp(UpdateDocuments, true, None), doc_clr(), doc_clr(), doc_clr()]), @"Some((DocumentClear { ids: [0, 1, 2, 3, 4] }, true))");
}

#[test]
fn clear_and_additions_and_settings() {
    // A clear don't need to autobatch the settings that happens AFTER there is no documents
    debug_snapshot!(autobatch_from(true, None, [doc_clr(), settings(true)]), @"Some((DocumentClear { ids: [0] }, false))");

    debug_snapshot!(autobatch_from(true, None, [settings(true), doc_clr(), settings(true)]), @"Some((ClearAndSettings { other: [1], allow_index_creation: true, settings_ids: [0, 2] }, true))");
    debug_snapshot!(autobatch_from(true, None, [doc_imp(ReplaceDocuments, true, None), settings(true), doc_clr()]), @"Some((DocumentOperation { allow_index_creation: true, primary_key: None, operation_ids: [0] }, true))");
    debug_snapshot!(autobatch_from(true, None, [doc_imp(UpdateDocuments, true, None), settings(true), doc_clr()]), @"Some((DocumentOperation { allow_index_creation: true, primary_key: None, operation_ids: [0] }, true))");
}

#[test]
fn anything_and_index_deletion() {
    // The `IndexDeletion` doesn't batch with anything that happens AFTER.
    debug_snapshot!(autobatch_from(true, None, [idx_del(), doc_imp(ReplaceDocuments, true, None)]), @"Some((IndexDeletion { ids: [0] }, false))");
    debug_snapshot!(autobatch_from(true, None, [idx_del(), doc_imp(UpdateDocuments, true, None)]), @"Some((IndexDeletion { ids: [0] }, false))");
    debug_snapshot!(autobatch_from(true, None, [idx_del(), doc_imp(ReplaceDocuments, false, None)]), @"Some((IndexDeletion { ids: [0] }, false))");
    debug_snapshot!(autobatch_from(true, None, [idx_del(), doc_imp(UpdateDocuments, false, None)]), @"Some((IndexDeletion { ids: [0] }, false))");
    debug_snapshot!(autobatch_from(true, None, [idx_del(), doc_del()]), @"Some((IndexDeletion { ids: [0] }, false))");
    debug_snapshot!(autobatch_from(true, None, [idx_del(), doc_del_fil()]), @"Some((IndexDeletion { ids: [0] }, false))");
    debug_snapshot!(autobatch_from(true, None, [idx_del(), doc_clr()]), @"Some((IndexDeletion { ids: [0] }, false))");
    debug_snapshot!(autobatch_from(true, None, [idx_del(), settings(true)]), @"Some((IndexDeletion { ids: [0] }, false))");
    debug_snapshot!(autobatch_from(true, None, [idx_del(), settings(false)]), @"Some((IndexDeletion { ids: [0] }, false))");

    debug_snapshot!(autobatch_from(false,None,  [idx_del(), doc_imp(ReplaceDocuments, true, None)]), @"Some((IndexDeletion { ids: [0] }, false))");
    debug_snapshot!(autobatch_from(false,None,  [idx_del(), doc_imp(UpdateDocuments, true, None)]), @"Some((IndexDeletion { ids: [0] }, false))");
    debug_snapshot!(autobatch_from(false,None,  [idx_del(), doc_imp(ReplaceDocuments, false, None)]), @"Some((IndexDeletion { ids: [0] }, false))");
    debug_snapshot!(autobatch_from(false,None,  [idx_del(), doc_imp(UpdateDocuments, false, None)]), @"Some((IndexDeletion { ids: [0] }, false))");
    debug_snapshot!(autobatch_from(false,None,  [idx_del(), doc_del()]), @"Some((IndexDeletion { ids: [0] }, false))");
    debug_snapshot!(autobatch_from(false,None,  [idx_del(), doc_del_fil()]), @"Some((IndexDeletion { ids: [0] }, false))");
    debug_snapshot!(autobatch_from(false,None,  [idx_del(), doc_clr()]), @"Some((IndexDeletion { ids: [0] }, false))");
    debug_snapshot!(autobatch_from(false,None,  [idx_del(), settings(true)]), @"Some((IndexDeletion { ids: [0] }, false))");
    debug_snapshot!(autobatch_from(false,None,  [idx_del(), settings(false)]), @"Some((IndexDeletion { ids: [0] }, false))");

    // The index deletion can accept almost any type of `BatchKind` and transform it to an `IndexDeletion`.
    // First, the basic cases
    debug_snapshot!(autobatch_from(true, None, [doc_imp(ReplaceDocuments, true, None), idx_del()]), @"Some((IndexDeletion { ids: [0, 1] }, true))");
    debug_snapshot!(autobatch_from(true, None, [doc_imp(UpdateDocuments, true, None), idx_del()]), @"Some((IndexDeletion { ids: [0, 1] }, true))");
    debug_snapshot!(autobatch_from(true, None, [doc_imp(ReplaceDocuments, false, None), idx_del()]), @"Some((IndexDeletion { ids: [0, 1] }, false))");
    debug_snapshot!(autobatch_from(true, None, [doc_imp(UpdateDocuments, false, None), idx_del()]), @"Some((IndexDeletion { ids: [0, 1] }, false))");
    debug_snapshot!(autobatch_from(true, None, [doc_del(), idx_del()]), @"Some((IndexDeletion { ids: [0, 1] }, false))");
    debug_snapshot!(autobatch_from(true, None, [doc_del_fil(), idx_del()]), @"Some((IndexDeletion { ids: [0, 1] }, false))");
    debug_snapshot!(autobatch_from(true, None, [doc_clr(), idx_del()]), @"Some((IndexDeletion { ids: [0, 1] }, false))");
    debug_snapshot!(autobatch_from(true, None, [settings(true), idx_del()]), @"Some((IndexDeletion { ids: [0, 1] }, true))");
    debug_snapshot!(autobatch_from(true, None, [settings(false), idx_del()]), @"Some((IndexDeletion { ids: [0, 1] }, false))");

    debug_snapshot!(autobatch_from(false,None,  [doc_imp(ReplaceDocuments, true, None), idx_del()]), @"Some((IndexDeletion { ids: [0, 1] }, true))");
    debug_snapshot!(autobatch_from(false,None,  [doc_imp(UpdateDocuments, true, None), idx_del()]), @"Some((IndexDeletion { ids: [0, 1] }, true))");
    debug_snapshot!(autobatch_from(false,None,  [doc_imp(ReplaceDocuments, false, None), idx_del()]), @"Some((IndexDeletion { ids: [0, 1] }, false))");
    debug_snapshot!(autobatch_from(false,None,  [doc_imp(UpdateDocuments, false, None), idx_del()]), @"Some((IndexDeletion { ids: [0, 1] }, false))");
    debug_snapshot!(autobatch_from(false,None,  [doc_del(), idx_del()]), @"Some((IndexDeletion { ids: [0, 1] }, false))");
    debug_snapshot!(autobatch_from(false,None,  [doc_del_fil(), idx_del()]), @"Some((IndexDeletion { ids: [0, 1] }, false))");
    debug_snapshot!(autobatch_from(false,None,  [doc_clr(), idx_del()]), @"Some((IndexDeletion { ids: [0, 1] }, false))");
    debug_snapshot!(autobatch_from(false,None,  [settings(true), idx_del()]), @"Some((IndexDeletion { ids: [0, 1] }, true))");
    debug_snapshot!(autobatch_from(false,None,  [settings(false), idx_del()]), @"Some((IndexDeletion { ids: [0, 1] }, false))");
}

#[test]
fn allowed_and_disallowed_index_creation() {
    // `DocumentImport` can't be mixed with those disallowed to do so except if the index already exists.
    debug_snapshot!(autobatch_from(true, None, [doc_imp(ReplaceDocuments, false, None), doc_imp(ReplaceDocuments, true, None)]), @"Some((DocumentOperation { allow_index_creation: false, primary_key: None, operation_ids: [0, 1] }, false))");
    debug_snapshot!(autobatch_from(true, None, [doc_imp(ReplaceDocuments, true, None), doc_imp(ReplaceDocuments, true, None)]), @"Some((DocumentOperation { allow_index_creation: true, primary_key: None, operation_ids: [0, 1] }, true))");
    debug_snapshot!(autobatch_from(true, None, [doc_imp(ReplaceDocuments, false, None), doc_imp(ReplaceDocuments, false, None)]), @"Some((DocumentOperation { allow_index_creation: false, primary_key: None, operation_ids: [0, 1] }, false))");
    debug_snapshot!(autobatch_from(true, None, [doc_imp(ReplaceDocuments, true, None), settings(true)]), @"Some((DocumentOperation { allow_index_creation: true, primary_key: None, operation_ids: [0] }, true))");
    debug_snapshot!(autobatch_from(true, None, [doc_imp(ReplaceDocuments, false, None), settings(true)]), @"Some((DocumentOperation { allow_index_creation: false, primary_key: None, operation_ids: [0] }, false))");

    debug_snapshot!(autobatch_from(false,None,  [doc_imp(ReplaceDocuments, false, None), doc_imp(ReplaceDocuments, true, None)]), @"Some((DocumentOperation { allow_index_creation: false, primary_key: None, operation_ids: [0] }, false))");
    debug_snapshot!(autobatch_from(false,None,  [doc_imp(ReplaceDocuments, true, None), doc_imp(ReplaceDocuments, true, None)]), @"Some((DocumentOperation { allow_index_creation: true, primary_key: None, operation_ids: [0, 1] }, true))");
    debug_snapshot!(autobatch_from(false,None,  [doc_imp(ReplaceDocuments, false, None), doc_imp(ReplaceDocuments, false, None)]), @"Some((DocumentOperation { allow_index_creation: false, primary_key: None, operation_ids: [0, 1] }, false))");
    debug_snapshot!(autobatch_from(false,None,  [doc_imp(ReplaceDocuments, true, None), settings(true)]), @"Some((DocumentOperation { allow_index_creation: true, primary_key: None, operation_ids: [0] }, true))");
    debug_snapshot!(autobatch_from(false,None,  [doc_imp(ReplaceDocuments, false, None), settings(true)]), @"Some((DocumentOperation { allow_index_creation: false, primary_key: None, operation_ids: [0] }, false))");

    // batch deletion and addition
    debug_snapshot!(autobatch_from(false, None, [doc_del(), doc_imp(ReplaceDocuments, true, Some("catto"))]), @"Some((DocumentDeletion { deletion_ids: [0], includes_by_filter: false }, false))");
    debug_snapshot!(autobatch_from(false, None, [doc_del(), doc_imp(UpdateDocuments, true, Some("catto"))]), @"Some((DocumentDeletion { deletion_ids: [0], includes_by_filter: false }, false))");
    debug_snapshot!(autobatch_from(false, None, [doc_del(), doc_imp(ReplaceDocuments, true, None)]), @"Some((DocumentDeletion { deletion_ids: [0], includes_by_filter: false }, false))");
    debug_snapshot!(autobatch_from(false, None, [doc_del(), doc_imp(UpdateDocuments, true, None)]), @"Some((DocumentDeletion { deletion_ids: [0], includes_by_filter: false }, false))");
}

#[test]
fn autobatch_primary_key() {
    // ==> If I have a pk
    // With a single update
    debug_snapshot!(autobatch_from(true, Some("id"), [doc_imp(ReplaceDocuments, true, None)]), @"Some((DocumentOperation { allow_index_creation: true, primary_key: None, operation_ids: [0] }, true))");
    debug_snapshot!(autobatch_from(true, Some("id"), [doc_imp(ReplaceDocuments, true, Some("id"))]), @r###"Some((DocumentOperation { allow_index_creation: true, primary_key: Some("id"), operation_ids: [0] }, true))"###);
    debug_snapshot!(autobatch_from(true, Some("id"), [doc_imp(ReplaceDocuments, true, Some("other"))]), @r###"Some((DocumentOperation { allow_index_creation: true, primary_key: Some("other"), operation_ids: [0] }, true))"###);

    // With a multiple updates
    debug_snapshot!(autobatch_from(true, Some("id"), [doc_imp(ReplaceDocuments, true, None), doc_imp(ReplaceDocuments, true, None)]), @"Some((DocumentOperation { allow_index_creation: true, primary_key: None, operation_ids: [0, 1] }, true))");
    debug_snapshot!(autobatch_from(true, Some("id"), [doc_imp(ReplaceDocuments, true, None), doc_imp(ReplaceDocuments, true, Some("id"))]), @r###"Some((DocumentOperation { allow_index_creation: true, primary_key: Some("id"), operation_ids: [0, 1] }, true))"###);
    debug_snapshot!(autobatch_from(true, Some("id"), [doc_imp(ReplaceDocuments, true, None), doc_imp(ReplaceDocuments, true, Some("id")), doc_imp(ReplaceDocuments, true, None)]), @r###"Some((DocumentOperation { allow_index_creation: true, primary_key: Some("id"), operation_ids: [0, 1] }, true))"###);
    debug_snapshot!(autobatch_from(true, Some("id"), [doc_imp(ReplaceDocuments, true, None), doc_imp(ReplaceDocuments, true, Some("other"))]), @"Some((DocumentOperation { allow_index_creation: true, primary_key: None, operation_ids: [0] }, true))");
    debug_snapshot!(autobatch_from(true, Some("id"), [doc_imp(ReplaceDocuments, true, None), doc_imp(ReplaceDocuments, true, Some("other")), doc_imp(ReplaceDocuments, true, None)]), @"Some((DocumentOperation { allow_index_creation: true, primary_key: None, operation_ids: [0] }, true))");
    debug_snapshot!(autobatch_from(true, Some("id"), [doc_imp(ReplaceDocuments, true, None), doc_imp(ReplaceDocuments, true, Some("other")), doc_imp(ReplaceDocuments, true, Some("id"))]), @"Some((DocumentOperation { allow_index_creation: true, primary_key: None, operation_ids: [0] }, true))");

    debug_snapshot!(autobatch_from(true, Some("id"), [doc_imp(ReplaceDocuments, true, Some("id")), doc_imp(ReplaceDocuments, true, None)]), @r###"Some((DocumentOperation { allow_index_creation: true, primary_key: Some("id"), operation_ids: [0] }, true))"###);
    debug_snapshot!(autobatch_from(true, Some("id"), [doc_imp(ReplaceDocuments, true, Some("id")), doc_imp(ReplaceDocuments, true, Some("id"))]), @r###"Some((DocumentOperation { allow_index_creation: true, primary_key: Some("id"), operation_ids: [0, 1] }, true))"###);
    debug_snapshot!(autobatch_from(true, Some("id"), [doc_imp(ReplaceDocuments, true, Some("id")), doc_imp(ReplaceDocuments, true, Some("id")), doc_imp(ReplaceDocuments, true, None)]), @r###"Some((DocumentOperation { allow_index_creation: true, primary_key: Some("id"), operation_ids: [0, 1] }, true))"###);
    debug_snapshot!(autobatch_from(true, Some("id"), [doc_imp(ReplaceDocuments, true, Some("id")), doc_imp(ReplaceDocuments, true, Some("other"))]), @r###"Some((DocumentOperation { allow_index_creation: true, primary_key: Some("id"), operation_ids: [0] }, true))"###);
    debug_snapshot!(autobatch_from(true, Some("id"), [doc_imp(ReplaceDocuments, true, Some("id")), doc_imp(ReplaceDocuments, true, Some("other")), doc_imp(ReplaceDocuments, true, None)]), @r###"Some((DocumentOperation { allow_index_creation: true, primary_key: Some("id"), operation_ids: [0] }, true))"###);
    debug_snapshot!(autobatch_from(true, Some("id"), [doc_imp(ReplaceDocuments, true, Some("id")), doc_imp(ReplaceDocuments, true, Some("other")), doc_imp(ReplaceDocuments, true, Some("id"))]), @r###"Some((DocumentOperation { allow_index_creation: true, primary_key: Some("id"), operation_ids: [0] }, true))"###);

    debug_snapshot!(autobatch_from(true, Some("id"), [doc_imp(ReplaceDocuments, true, Some("other")), doc_imp(ReplaceDocuments, true, None)]), @r###"Some((DocumentOperation { allow_index_creation: true, primary_key: Some("other"), operation_ids: [0] }, true))"###);
    debug_snapshot!(autobatch_from(true, Some("id"), [doc_imp(ReplaceDocuments, true, Some("other")), doc_imp(ReplaceDocuments, true, Some("id"))]), @r###"Some((DocumentOperation { allow_index_creation: true, primary_key: Some("other"), operation_ids: [0] }, true))"###);
    debug_snapshot!(autobatch_from(true, Some("id"), [doc_imp(ReplaceDocuments, true, Some("other")), doc_imp(ReplaceDocuments, true, Some("id")), doc_imp(ReplaceDocuments, true, None)]), @r###"Some((DocumentOperation { allow_index_creation: true, primary_key: Some("other"), operation_ids: [0] }, true))"###);
    debug_snapshot!(autobatch_from(true, Some("id"), [doc_imp(ReplaceDocuments, true, Some("other")), doc_imp(ReplaceDocuments, true, Some("other"))]), @r###"Some((DocumentOperation { allow_index_creation: true, primary_key: Some("other"), operation_ids: [0] }, true))"###);
    debug_snapshot!(autobatch_from(true, Some("id"), [doc_imp(ReplaceDocuments, true, Some("other")), doc_imp(ReplaceDocuments, true, Some("other")), doc_imp(ReplaceDocuments, true, None)]), @r###"Some((DocumentOperation { allow_index_creation: true, primary_key: Some("other"), operation_ids: [0] }, true))"###);
    debug_snapshot!(autobatch_from(true, Some("id"), [doc_imp(ReplaceDocuments, true, Some("other")), doc_imp(ReplaceDocuments, true, Some("other")), doc_imp(ReplaceDocuments, true, Some("id"))]), @r###"Some((DocumentOperation { allow_index_creation: true, primary_key: Some("other"), operation_ids: [0] }, true))"###);

    // ==> If I don't have a pk
    // With a single update
    debug_snapshot!(autobatch_from(true, None, [doc_imp(ReplaceDocuments, true, None)]), @"Some((DocumentOperation { allow_index_creation: true, primary_key: None, operation_ids: [0] }, true))");
    debug_snapshot!(autobatch_from(true, None, [doc_imp(ReplaceDocuments, true, Some("id"))]), @r###"Some((DocumentOperation { allow_index_creation: true, primary_key: Some("id"), operation_ids: [0] }, true))"###);
    debug_snapshot!(autobatch_from(true, None, [doc_imp(ReplaceDocuments, true, Some("other"))]), @r###"Some((DocumentOperation { allow_index_creation: true, primary_key: Some("other"), operation_ids: [0] }, true))"###);

    // With a multiple updates
    debug_snapshot!(autobatch_from(true, None, [doc_imp(ReplaceDocuments, true, None), doc_imp(ReplaceDocuments, true, None)]), @"Some((DocumentOperation { allow_index_creation: true, primary_key: None, operation_ids: [0, 1] }, true))");
    debug_snapshot!(autobatch_from(true, None, [doc_imp(ReplaceDocuments, true, None), doc_imp(ReplaceDocuments, true, Some("id"))]), @"Some((DocumentOperation { allow_index_creation: true, primary_key: None, operation_ids: [0] }, true))");
    debug_snapshot!(autobatch_from(true, None, [doc_imp(ReplaceDocuments, true, Some("id")), doc_imp(ReplaceDocuments, true, None)]), @r###"Some((DocumentOperation { allow_index_creation: true, primary_key: Some("id"), operation_ids: [0] }, true))"###);
}
