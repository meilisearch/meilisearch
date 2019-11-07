# Getting Started

Download the [movies dataset](#lien_vers_movie_dataset) to try our example.

## Installation 

```
cargo build --release
```

## Run 
```bash
cargo run --release
Server is listening on: http://127.0.0.1:8080
```

## Indexation

**Create an [index](#index_doc)** whitout defining the [document](#link_to_documents_doc) [schema](#link_to_schema_doc).
```bash
curl --request POST 'http://127.0.0.1:8080/indexes/myindex'
```

**Add documents**. [Learn how to format your documents here](#link)


```bash
curl --request POST 'http://127.0.0.1:8080/indexes/myindex/documents' \
  --data @movies.json \
  --header 'content-type: application/json'
```

You can track [updates](#link) with the provided update id's .

When no [schema](#link_to_schema_doc) is defined MeiliDB will try to infer it based upon the first document you sent.

## Search 
Now that our movie dataset has been indexed, you can try out the search engine :
```bash
curl 'http://127.0.0.1:8080/indexes/myindex/search?q=kun&limit=2&attributesToRetrieve=title,overview'
```

```json
{
  "hits": [
    {
      "title": "Kung Fu Panda: Secrets of the Masters",
      "overview": "Po and the Furious Five uncover the legend of three of kung fu's greatest heroes: Master Thundering Rhino, Master Storming Ox, and Master Croc."
    },
    {
      "title": "Kung Fu Panda: Secrets of the Scroll",
      "overview": "As Po looks for his lost action figures, the story of how the panda inadvertently helped create the Furious Five is told."
    }
  ],
  "offset": 0,
  "limit": 2,
  "processingTimeMs": 1,
  "query": "kun"
}
```
