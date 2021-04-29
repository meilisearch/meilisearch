<p align="center">
  <img alt="the milli logo" src="http-ui/public/logo-black.svg">
</p>

<p align="center">a concurrent indexer combined with fast and relevant search algorithms</p>

## Introduction

This engine is a prototype, do not use it in production.
This is one of the most advanced search engine I have worked on.
It currently only supports the proximity criterion.

### Compile and Run the server

You can specify the number of threads to use to index documents and many other settings too.

```bash
cd http-ui
cargo run --release -- --db my-database.mdb -vvv --indexing-jobs 8
```

### Index your documents

It can index a massive amount of documents in not much time, I already achieved to index:
 - 115m songs (song and artist name) in ~1h and take 107GB on disk.
 - 12m cities (name, timezone and country ID) in 15min and take 10GB on disk.

All of that on a 39$/month machine with 4cores.

You can feed the engine with your CSV (comma-seperated, yes) data like this:

```bash
printf "name,age\nhello,32\nkiki,24\n" | http POST 127.0.0.1:9700/documents content-type:text/csv
```

Here ids will be automatically generated as UUID v4 if they doesn't exist in some or every documents.

Note that it also support JSON and JSON streaming, you can send them to the engine by using
the `content-type:application/json` and `content-type:application/x-ndjson` headers respectively.

### Querying the engine via the website

You can query the engine by going to [the HTML page itself](http://127.0.0.1:9700).
