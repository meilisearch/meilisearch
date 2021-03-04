use serde_json::{Map, Value};

use crate::index::{SearchQuery, SearchResult};
use super::Data;

impl Data {
    pub async fn search<S: AsRef<str>>(
        &self,
        index: S,
        search_query: SearchQuery,
    ) -> anyhow::Result<SearchResult> {
        self.index_controller.search(index.as_ref().to_string(), search_query).await
    }

    pub async fn retrieve_documents(
        &self,
        index: String,
        offset: usize,
        limit: usize,
        attributes_to_retrieve: Option<Vec<String>>,
    ) -> anyhow::Result<Vec<Map<String, Value>>> {
        self.index_controller.documents(index, offset, limit, attributes_to_retrieve).await
    }

    pub async fn retrieve_document<S>(
        &self,
        _index: impl AsRef<str> + Sync + Send + 'static,
        _document_id: impl AsRef<str> + Sync + Send + 'static,
        _attributes_to_retrieve: Option<Vec<String>>,
    ) -> anyhow::Result<Map<String, Value>>
    {
        todo!()
        //let index_controller = self.index_controller.clone();
        //let document: anyhow::Result<_> = tokio::task::spawn_blocking(move || {
            //let index = index_controller
                //.index(&index)?
                //.with_context(|| format!("Index {:?} doesn't exist", index.as_ref()))?;
            //let txn = index.read_txn()?;

            //let fields_ids_map = index.fields_ids_map(&txn)?;

            //let attributes_to_retrieve_ids = match attributes_to_retrieve {
                //Some(attrs) => attrs
                    //.iter()
                    //.filter_map(|f| fields_ids_map.id(f.as_ref()))
                    //.collect::<Vec<_>>(),
                //None => fields_ids_map.iter().map(|(id, _)| id).collect(),
            //};

            //let internal_id = index
                //.external_documents_ids(&txn)?
                //.get(document_id.as_ref().as_bytes())
                //.with_context(|| format!("Document with id {} not found", document_id.as_ref()))?;

            //let document = index
                //.documents(&txn, std::iter::once(internal_id))?
                //.into_iter()
                //.next()
                //.map(|(_, d)| d);

            //match document {
                //Some(document) => Ok(obkv_to_json(
                    //&attributes_to_retrieve_ids,
                    //&fields_ids_map,
                    //document,
                //)?),
                //None => bail!("Document with id {} not found", document_id.as_ref()),
            //}
        //})
        //.await?;
        //document
    }
}
