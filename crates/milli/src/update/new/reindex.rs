use heed::RwTxn;
use zstd::dict::DecoderDictionary;

use super::document::{Document, DocumentFromDb};
use crate::progress::{self, AtomicSubStep, Progress};
use crate::{FieldDistribution, Index, Result};

pub fn field_distribution(index: &Index, wtxn: &mut RwTxn<'_>, progress: &Progress) -> Result<()> {
    let mut distribution = FieldDistribution::new();

    let document_count = index.number_of_documents(wtxn)?;
    let field_id_map = index.fields_ids_map(wtxn)?;

    let (update_document_count, sub_step) =
        AtomicSubStep::<progress::Document>::new(document_count as u32);
    progress.update_progress(sub_step);

    let docids = index.documents_ids(wtxn)?;
    let mut doc_alloc = bumpalo::Bump::new();

    let db_document_decompression_dictionary =
        index.document_compression_raw_dictionary(wtxn)?.map(|raw| DecoderDictionary::copy(raw));

    for docid in docids {
        update_document_count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        let Some(document) = DocumentFromDb::new(
            docid,
            wtxn,
            index,
            &field_id_map,
            db_document_decompression_dictionary.as_ref(),
            &doc_alloc,
        )?
        else {
            continue;
        };
        let geo_iter = document.geo_field().transpose().map(|res| res.map(|rv| ("_geo", rv)));
        for res in document.iter_top_level_fields().chain(geo_iter) {
            let (field_name, _) = res?;
            if let Some(count) = distribution.get_mut(field_name) {
                *count += 1;
            } else {
                distribution.insert(field_name.to_owned(), 1);
            }
        }

        doc_alloc.reset();
    }

    index.put_field_distribution(wtxn, &distribution)?;

    Ok(())
}
