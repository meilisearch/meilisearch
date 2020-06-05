use std::io;
use std::str::FromStr;

use anyhow::Context;
use cow_utils::CowUtils;
use roaring::RoaringBitmap;
use slice_group_by::StrGroupBy;

use mega_mini_indexer::{FastMap4, DocumentId, SmallString32};

const MAX_POSITION: usize = 1000;
const MAX_ATTRIBUTES: usize = u32::max_value() as usize / MAX_POSITION;

fn simple_alphanumeric_tokens(string: &str) -> impl Iterator<Item = &str> {
    let is_alphanumeric = |s: &&str| s.chars().next().map_or(false, char::is_alphanumeric);
    string.linear_group_by_key(|c| c.is_alphanumeric()).filter(is_alphanumeric)
}

fn main() -> anyhow::Result<()> {
    let mut rdr = csv::Reader::from_reader(io::stdin());

    let mut document = csv::StringRecord::new();
    let mut postings_positions = FastMap4::default();

    let headers = rdr.headers()?;
    let mut writer = csv::WriterBuilder::new().has_headers(false).from_writer(Vec::new());
    writer.write_byte_record(headers.as_byte_record())?;
    let id_pos = headers.iter().position(|h| h == "id").context("missing 'id' header")?;

    while rdr.read_record(&mut document)? {
        let document_id = document.get(id_pos).unwrap();
        let document_id = DocumentId::from_str(document_id).context("invalid document id")?;

        for (attr, content) in document.iter().enumerate().take(MAX_ATTRIBUTES) {
            if attr == id_pos { continue }

            for (pos, word) in simple_alphanumeric_tokens(&content).enumerate().take(MAX_POSITION) {
                if !word.is_empty() && word.len() < 500 { // LMDB limits
                    let word = word.cow_to_lowercase();
                    let position = (attr * 1000 + pos) as u32;

                    postings_positions.entry(SmallString32::from(word.as_ref()))
                        .or_insert_with(FastMap4::default).entry(position)
                        .or_insert_with(RoaringBitmap::new).insert(document_id);
                }
            }
        }
    }

    // Write the stats to stdout
    let mut wrt = csv::Writer::from_writer(io::stdout());
    wrt.write_record(&["word", "position", "count"])?;

    for (word, positions) in postings_positions {
        let word = word.as_str();
        for (pos, ids) in positions {
            let pos = pos.to_string();
            let count = ids.len().to_string();
            wrt.write_record(&[word, &pos, &count])?;
        }
    }

    Ok(())
}
