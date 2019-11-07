# Getting Started

MeiliDB is a full-text search database based on the fast [LMDB key-value store](https://en.wikipedia.org/wiki/Lightning_Memory-Mapped_Database) and written in Rust. 

MeiliDB provides an http interface.

It offers an easy to use and deploy solution to search inside your documents. No configuration is needed but customization of search and indexation is possible.

You can find more about the MeiliDB [engine and features here](#link_to_engine_and_features).

## Quick Start

## Installation 

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

Download the [movies dataset](#lien_vers_movie_dataset) to try our example.

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
curl --request GET 'http://127.0.0.1:8080/indexes/myindex/search?q=kun&limit=5'
```

```json
{
  "hits": [
    {
      "id": "81003",
      "title": "Kung Fu Panda: Secrets of the Masters",
      "poster": "https://image.tmdb.org/t/p/w1280/kU8szr8xUWdMkVXifAw9r5tsuOT.jpg",
      "overview": "Po and the Furious Five uncover the legend of three of kung fu's greatest heroes: Master Thundering Rhino, Master Storming Ox, and Master Croc.",
      "release_date": "2011-12-12"
    },
    {
      "id": "381693",
      "title": "Kung Fu Panda: Secrets of the Scroll",
      "poster": "https://image.tmdb.org/t/p/w1280/8UvKl3SZhE6McLK4Yv5w7fRIg9Y.jpg",
      "overview": "As Po looks for his lost action figures, the story of how the panda inadvertently helped create the Furious Five is told.",
      "release_date": "2016-01-05"
    },
    {
      "id": "17108",
      "title": "Kung Fu Dunk",
      "poster": "https://image.tmdb.org/t/p/w1280/2xFGlI4MXH9rJunia0l2VmK3Mw1.jpg",
      "overview": "Shi-Jie is a brilliant martial artist from the Kung Fu School. One day, he encounters a group of youths playing basketball and shows off how easy it is for him, with his martial arts training, to do a Slam Dunk. Watching him was Chen-Li, a shrewd businessman, who recruits him to play varsity basketball at the local university.",
      "release_date": "2008-02-07"
    },
    {
      "id": "383785",
      "title": "Kung Fu Yoga",
      "poster": "https://image.tmdb.org/t/p/w1280/rL6XM4fsr1cM5mN2flEhQ8jQter.jpg",
      "overview": "Chinese archeology professor Jack teams up with beautiful Indian professor Ashmita and assistant Kyra to locate lost Magadha treasure. In a Tibetan ice cave, they find the remains of the royal army that had vanished together with the treasure, only to be ambushed by Randall, the descendent of a rebel army leader. When they free themselves, their next stop is Dubai where a diamond from the ice cave is to be auctioned. After a series of double-crosses and revelations about their past, Jack and his team travel to a mountain temple in India, using the diamond as a key to unlock the real treasure.",
      "release_date": "2017-01-27"
    },
    {
      "id": "560246",
      "title": "Kung Paano Siya Nawala",
      "poster": "https://image.tmdb.org/t/p/w1280/gj543vQE2eFTZ20H1osenT3308S.jpg",
      "overview": "A young man suffering from face blindness and a young woman with a troubled past fall in love.",
      "release_date": "2018-11-14"
    }
  ],
  "offset": 0,
  "limit": 5,
  "processingTimeMs": 0,
  "query": "kun"
}

```
