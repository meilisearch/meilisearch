use bumpalo::collections::CollectIn;
use bumpalo::Bump;
use rayon::iter::IndexedParallelIterator;
use rayon::slice::ParallelSlice as _;
use roaring::RoaringBitmap;

use super::document_changes::{DocumentChangeContext, DocumentChanges, MostlySend};
use crate::documents::PrimaryKey;
use crate::update::new::{Deletion, DocumentChange};
use crate::{DocumentId, Result};

#[derive(Default)]
pub struct DocumentDeletion {
    pub to_delete: RoaringBitmap,
}

impl DocumentDeletion {
    pub fn new() -> Self {
        Self { to_delete: Default::default() }
    }

    pub fn delete_documents_by_docids(&mut self, docids: RoaringBitmap) {
        self.to_delete |= docids;
    }

    pub fn into_changes<'indexer>(
        self,
        indexer: &'indexer Bump,
        primary_key: PrimaryKey<'indexer>,
    ) -> DocumentDeletionChanges<'indexer> {
        let to_delete: bumpalo::collections::Vec<_> =
            self.to_delete.into_iter().collect_in(indexer);

        let to_delete = to_delete.into_bump_slice();

        DocumentDeletionChanges { to_delete, primary_key }
    }
}

pub struct DocumentDeletionChanges<'indexer> {
    to_delete: &'indexer [DocumentId],
    primary_key: PrimaryKey<'indexer>,
}

impl<'pl> DocumentChanges<'pl> for DocumentDeletionChanges<'pl> {
    type Item = DocumentId;

    fn iter(
        &self,
        chunk_size: usize,
    ) -> impl IndexedParallelIterator<Item = impl AsRef<[Self::Item]>> {
        self.to_delete.par_chunks(chunk_size)
    }

    fn item_to_document_change<
        'doc, // lifetime of a single `process` call
        T: MostlySend,
    >(
        &'doc self,
        context: &'doc DocumentChangeContext<T>,
        docid: &'doc Self::Item,
    ) -> Result<Option<DocumentChange<'doc>>>
    where
        'pl: 'doc, // the payload must survive the process calls
    {
        let current = context.index.document(&context.txn, *docid)?;

        let external_document_id = self.primary_key.extract_docid_from_db(
            current,
            &context.db_fields_ids_map,
            &context.doc_alloc,
        )?;

        let external_document_id = external_document_id.to_bump(&context.doc_alloc);

        Ok(Some(DocumentChange::Deletion(Deletion::create(*docid, external_document_id))))
    }
}

#[cfg(test)]
mod test {
    use std::cell::RefCell;
    use std::marker::PhantomData;
    use std::sync::RwLock;

    use bumpalo::Bump;
    use raw_collections::alloc::RefBump;

    use crate::fields_ids_map::metadata::{FieldIdMapWithMetadata, MetadataBuilder};
    use crate::index::tests::TempIndex;
    use crate::update::new::indexer::document_changes::{
        for_each_document_change, DocumentChangeContext, Extractor, IndexingContext, MostlySend,
        ThreadLocal,
    };
    use crate::update::new::indexer::DocumentDeletion;
    use crate::update::new::DocumentChange;
    use crate::DocumentId;

    #[test]
    fn test_deletions() {
        struct DeletionWithData<'extractor> {
            deleted: RefCell<
                hashbrown::HashSet<
                    DocumentId,
                    hashbrown::hash_map::DefaultHashBuilder,
                    RefBump<'extractor>,
                >,
            >,
        }

        unsafe impl<'extractor> MostlySend for DeletionWithData<'extractor> {}

        struct TrackDeletion<'extractor>(PhantomData<&'extractor ()>);

        impl<'extractor> Extractor<'extractor> for TrackDeletion<'extractor> {
            type Data = DeletionWithData<'extractor>;

            fn init_data(
                &self,
                extractor_alloc: raw_collections::alloc::RefBump<'extractor>,
            ) -> crate::Result<Self::Data> {
                let deleted = RefCell::new(hashbrown::HashSet::new_in(extractor_alloc));
                Ok(DeletionWithData { deleted })
            }

            fn process<'doc>(
                &self,
                changes: impl Iterator<Item = crate::Result<DocumentChange<'doc>>>,
                context: &DocumentChangeContext<Self::Data>,
            ) -> crate::Result<()> {
                for change in changes {
                    let change = change?;
                    context.data.deleted.borrow_mut().insert(change.docid());
                }
                Ok(())
            }
        }

        let mut deletions = DocumentDeletion::new();
        deletions.delete_documents_by_docids(vec![0, 2, 42].into_iter().collect());
        let indexer = Bump::new();

        let index = TempIndex::new();

        let rtxn = index.read_txn().unwrap();

        let db_fields_ids_map = index.fields_ids_map(&rtxn).unwrap();
        let metadata_builder = MetadataBuilder::from_index(&index, &rtxn).unwrap();
        let fields_ids_map =
            RwLock::new(FieldIdMapWithMetadata::new(db_fields_ids_map.clone(), metadata_builder));

        let fields_ids_map_store = ThreadLocal::new();

        let mut extractor_allocs = ThreadLocal::new();
        let doc_allocs = ThreadLocal::new();

        let deletion_tracker = TrackDeletion(PhantomData);

        let changes = deletions
            .into_changes(&indexer, crate::documents::PrimaryKey::Flat { name: "id", field_id: 0 });

        let context = IndexingContext {
            index: &index,
            db_fields_ids_map: &db_fields_ids_map,
            new_fields_ids_map: &fields_ids_map,
            doc_allocs: &doc_allocs,
            fields_ids_map_store: &fields_ids_map_store,
        };

        for _ in 0..3 {
            let datastore = ThreadLocal::new();

            for_each_document_change(
                &changes,
                &deletion_tracker,
                context,
                &mut extractor_allocs,
                &datastore,
            )
            .unwrap();

            for (index, data) in datastore.into_iter().enumerate() {
                println!("deleted by {index}: {:?}", data.deleted.borrow());
            }
            for alloc in extractor_allocs.iter_mut() {
                let alloc = &mut alloc.0;
                alloc.get_mut().reset();
            }
        }
    }
}
