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

    pub async fn retrieve_documents<S>(
        &self,
        _index: String,
        _offset: usize,
        _limit: usize,
        _attributes_to_retrieve: Option<Vec<S>>,
    ) -> anyhow::Result<Vec<Map<String, Value>>>
    where
        S: AsRef<str> + Send + Sync + 'static,
    {
         todo!()
        //let index_controller = self.index_controller.clone();
        //let documents: anyhow::Result<_> = tokio::task::spawn_blocking(move || {
            //let index = index_controller
                //.index(index.clone())?
                //.with_context(|| format!("Index {:?} doesn't exist", index))?;

            //let txn = index.read_txn()?;

            //let fields_ids_map = index.fields_ids_map(&txn)?;

            //let attributes_to_retrieve_ids = match attributes_to_retrieve {
                //Some(attrs) => attrs
                    //.iter()
                    //.filter_map(|f| fields_ids_map.id(f.as_ref()))
                    //.collect::<Vec<_>>(),
                //None => fields_ids_map.iter().map(|(id, _)| id).collect(),
            //};

            //let iter = index.documents.range(&txn, &(..))?.skip(offset).take(limit);

            //let mut documents = Vec::new();

            //for entry in iter {
                //let (_id, obkv) = entry?;
                //let object = obkv_to_json(&attributes_to_retrieve_ids, &fields_ids_map, obkv)?;
                //documents.push(object);
            //}

            //Ok(documents)
        //})
        //.await?;
        //documents
    }

    pub async fn retrieve_document<S>(
        &self,
        _index: impl AsRef<str> + Sync + Send + 'static,
        _document_id: impl AsRef<str> + Sync + Send + 'static,
        _attributes_to_retrieve: Option<Vec<S>>,
    ) -> anyhow::Result<Map<String, Value>>
    where
        S: AsRef<str> + Sync + Send + 'static,
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
