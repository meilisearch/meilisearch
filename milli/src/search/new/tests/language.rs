use crate::index::tests::TempIndex;
use crate::{Search, SearchResult};

#[test]
fn test_kanji_language_detection() {
    let index = TempIndex::new();

    index
        .add_documents(documents!([
            { "id": 0, "title": "The quick (\"brown\") fox can't jump 32.3 feet, right? Brr, it's 29.3°F!" },
            { "id": 1, "title": "東京のお寿司。" },
            { "id": 2, "title": "הַשּׁוּעָל הַמָּהִיר (״הַחוּם״) לֹא יָכוֹל לִקְפֹּץ 9.94 מֶטְרִים, נָכוֹן? ברר, 1.5°C- בַּחוּץ!" }
        ]))
        .unwrap();

    let txn = index.write_txn().unwrap();
    let mut search = Search::new(&txn, &index);

    search.query("東京");
    let SearchResult { documents_ids, .. } = search.execute().unwrap();

    insta::assert_snapshot!(format!("{documents_ids:?}"), @"[1]");
}
