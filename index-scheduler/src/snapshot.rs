use meilisearch_types::milli::{RoaringBitmapCodec, BEU32};
use meilisearch_types::tasks::Details;
use meilisearch_types::{
    heed::{
        types::{OwnedType, SerdeBincode, SerdeJson, Str},
        Database, RoTxn,
    },
    tasks::Task,
};
use roaring::RoaringBitmap;

use crate::{index_mapper::IndexMapper, IndexScheduler, Kind, Status};

pub fn snapshot_index_scheduler(scheduler: &IndexScheduler) -> String {
    let IndexScheduler {
        autobatching_enabled,
        processing_tasks,
        file_store,
        env,
        all_tasks,
        status,
        kind,
        index_tasks,
        index_mapper,
        wake_up: _,
        dumps_path: _,
        test_breakpoint_sdr: _,
    } = scheduler;

    let rtxn = env.read_txn().unwrap();

    let mut snap = String::new();

    let (_time, processing_tasks) = processing_tasks.read().unwrap().clone();
    snap.push_str(&format!(
        "### Autobatching Enabled = {autobatching_enabled}\n"
    ));
    snap.push_str("### Processing Tasks:\n");
    snap.push_str(&snapshot_bitmap(&processing_tasks));
    snap.push_str("\n----------------------------------------------------------------------\n");

    snap.push_str("### All Tasks:\n");
    snap.push_str(&snapshot_all_tasks(&rtxn, *all_tasks));
    snap.push_str("----------------------------------------------------------------------\n");

    snap.push_str("### Status:\n");
    snap.push_str(&snapshot_status(&rtxn, *status));
    snap.push_str("----------------------------------------------------------------------\n");

    snap.push_str("### Kind:\n");
    snap.push_str(&snapshot_kind(&rtxn, *kind));
    snap.push_str("----------------------------------------------------------------------\n");

    snap.push_str("### Index Tasks:\n");
    snap.push_str(&snapshot_index_tasks(&rtxn, *index_tasks));
    snap.push_str("----------------------------------------------------------------------\n");

    snap.push_str("### Index Mapper:\n");
    snap.push_str(&snapshot_index_mapper(&rtxn, index_mapper));
    snap.push_str("\n----------------------------------------------------------------------\n");

    snap.push_str("### File Store:\n");
    snap.push_str(&snapshot_file_store(file_store));
    snap.push_str("\n----------------------------------------------------------------------\n");

    rtxn.commit().unwrap();

    snap
}

fn snapshot_file_store(file_store: &file_store::FileStore) -> String {
    let mut snap = String::new();
    for uuid in file_store.__all_uuids() {
        snap.push_str(&format!("{uuid}\n"));
    }
    snap
}

fn snapshot_bitmap(r: &RoaringBitmap) -> String {
    let mut snap = String::new();
    snap.push('[');
    for x in r {
        snap.push_str(&format!("{x},"));
    }
    snap.push(']');
    snap
}

fn snapshot_all_tasks(rtxn: &RoTxn, db: Database<OwnedType<BEU32>, SerdeJson<Task>>) -> String {
    let mut snap = String::new();
    let mut iter = db.iter(rtxn).unwrap();
    while let Some(next) = iter.next() {
        let (task_id, task) = next.unwrap();
        snap.push_str(&format!("{task_id} {}\n", snapshot_task(&task)));
    }
    snap
}

fn snapshot_task(task: &Task) -> String {
    let mut snap = String::new();
    let Task {
        uid,
        enqueued_at: _,
        started_at: _,
        finished_at: _,
        error,
        details,
        status,
        kind,
    } = task;
    snap.push('{');
    snap.push_str(&format!("uid: {uid}, "));
    snap.push_str(&format!("status: {status}, "));
    if let Some(error) = error {
        snap.push_str(&format!("error: {error:?}, "));
    }
    if let Some(details) = details {
        snap.push_str(&format!("details: {}, ", &snaphsot_details(details)));
    }
    snap.push_str(&format!("kind: {kind:?}"));

    snap.push('}');
    snap
}
fn snaphsot_details(d: &Details) -> String {
    match d {
        Details::DocumentAddition {
            received_documents,
            indexed_documents,
        } => {
            format!("{{ received_documents: {received_documents}, indexed_documents: {indexed_documents:?} }}")
        }
        Details::Settings { settings } => {
            format!("{{ settings: {settings:?} }}")
        }
        Details::IndexInfo { primary_key } => {
            format!("{{ primary_key: {primary_key:?} }}")
        }
        Details::DocumentDeletion {
            received_document_ids,
            deleted_documents,
        } => format!("{{ received_document_ids: {received_document_ids}, deleted_documents: {deleted_documents:?} }}"),
        Details::ClearAll { deleted_documents } => {
            format!("{{ deleted_documents: {deleted_documents:?} }}")
        },
        Details::TaskDeletion {
            matched_tasks,
            deleted_tasks,
            original_query,
        } => {
            format!("{{ matched_tasks: {matched_tasks:?}, deleted_tasks: {deleted_tasks:?}, original_query: {original_query:?} }}")
        },
        Details::Dump { dump_uid } => {
            format!("{{ dump_uid: {dump_uid:?} }}")
        },
    }
}

fn snapshot_status(rtxn: &RoTxn, db: Database<SerdeBincode<Status>, RoaringBitmapCodec>) -> String {
    let mut snap = String::new();
    let mut iter = db.iter(rtxn).unwrap();
    while let Some(next) = iter.next() {
        let (status, task_ids) = next.unwrap();
        snap.push_str(&format!("{status} {}\n", snapshot_bitmap(&task_ids)));
    }
    snap
}
fn snapshot_kind(rtxn: &RoTxn, db: Database<SerdeBincode<Kind>, RoaringBitmapCodec>) -> String {
    let mut snap = String::new();
    let mut iter = db.iter(rtxn).unwrap();
    while let Some(next) = iter.next() {
        let (kind, task_ids) = next.unwrap();
        let kind = serde_json::to_string(&kind).unwrap();
        snap.push_str(&format!("{kind} {}\n", snapshot_bitmap(&task_ids)));
    }
    snap
}

fn snapshot_index_tasks(rtxn: &RoTxn, db: Database<Str, RoaringBitmapCodec>) -> String {
    let mut snap = String::new();
    let mut iter = db.iter(rtxn).unwrap();
    while let Some(next) = iter.next() {
        let (index, task_ids) = next.unwrap();
        snap.push_str(&format!("{index} {}\n", snapshot_bitmap(&task_ids)));
    }
    snap
}

fn snapshot_index_mapper(rtxn: &RoTxn, mapper: &IndexMapper) -> String {
    let names = mapper
        .indexes(rtxn)
        .unwrap()
        .into_iter()
        .map(|(n, _)| n)
        .collect::<Vec<_>>();
    format!("{names:?}")
}
