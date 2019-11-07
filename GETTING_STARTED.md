## Getting Started

Download the [movies dataset](#lien_vers_movie_dataset) to try our example.

### Install & Run

```bash
cargo run --release
Server is listening on: http://127.0.0.1:8080
```

### Index documents

Create an [index](#index_doc) without defining the [document](#link_to_documents_doc) [schema](#link_to_schema_doc).
```bash
curl --request POST 'http://127.0.0.1:8080/indexes/myindex'
```

Add documents and [learn how to format your documents](#link).


```bash
curl --request POST 'http://127.0.0.1:8080/indexes/myindex/documents' \
  --header 'content-type: application/json' \
  --data @movies.json
```

You [can track updates](#link) with the provided update id's .

### Search 
Now that our movie dataset has been indexed, you can try out the search engine with, for example, `botman` as a query.
```bash
curl 'http://127.0.0.1:8080/indexes/myindex/search?q=botman'
```

```
{
  "hits": [
    {
      "id": "29751",
      "title": "Batman Unmasked: The Psychology of the Dark Knight",
      "overview": "Delve into the world of Batman and the vigilante justice that he brought to the city of Gotham. Batman is a man who, after experiencing great tragedy, devotes his life to an ideal--but what happens when one man takes on the evil underworld alone? Examine why Batman is who he is--and explore how a boy scarred by tragedy becomes a symbol of hope to everyone else.",
    },
    {
      "id": "471474",
      "title": "Batman: Gotham by Gaslight",
      "overview": "In an alternative Victorian Age Gotham City, Batman begins his war on crime while he investigates a new series of murders by Jack the Ripper.",
    },
    ...
  ],
  "offset": 0,
  "limit": 20,
  "processingTimeMs": 1,
  "query": "botman"
}
```
