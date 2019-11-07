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
curl 'http://127.0.0.1:8080/indexes/myindex/search?q=botman&limit=2&attributesToRetrieve=title,overview'
```

```json
{
  "hits": [
    {
      "title": "Batman Unmasked: The Psychology of the Dark Knight",
      "overview": "Delve into the world of Batman and the vigilante justice that he brought to the city of Gotham. Batman is a man who, after experiencing great tragedy, devotes his life to an ideal--but what happens when one man takes on the evil underworld alone? Examine why Batman is who he is--and explore how a boy scarred by tragedy becomes a symbol of hope to everyone else."
    },
    {
      "title": "Batman: Gotham by Gaslight",
      "overview": "In an alternative Victorian Age Gotham City, Batman begins his war on crime while he investigates a new series of murders by Jack the Ripper."
    }
  ],
  "offset": 0,
  "limit": 2,
  "processingTimeMs": 1,
  "query": "botman"
}
```
