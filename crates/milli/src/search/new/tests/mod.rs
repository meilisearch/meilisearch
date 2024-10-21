pub mod attribute_fid;
pub mod attribute_position;
pub mod cutoff;
pub mod distinct;
pub mod exactness;
pub mod geo_sort;
pub mod integration;
#[cfg(feature = "all-tokenizations")]
#[cfg(not(feature = "chinese-pinyin"))]
pub mod language;
pub mod ngram_split_words;
pub mod proximity;
pub mod proximity_typo;
pub mod sort;
pub mod stop_words;
pub mod typo;
pub mod typo_proximity;
pub mod words_tms;

fn collect_field_values(
    index: &crate::Index,
    txn: &heed::RoTxn<'_>,
    fid: &str,
    docids: &[u32],
) -> Vec<String> {
    let mut values = vec![];
    let fid = index.fields_ids_map(txn).unwrap().id(fid).unwrap();
    for doc in index.documents(txn, docids.iter().copied()).unwrap() {
        if let Some(v) = doc.1.get(fid) {
            let v: serde_json::Value = serde_json::from_slice(v).unwrap();
            let v = v.to_string();
            values.push(v);
        } else {
            values.push("__does_not_exist__".to_owned());
        }
    }
    values
}
