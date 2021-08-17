<p align="center">
  <img alt="the milli logo" src="http-ui/public/logo-black.svg">
</p>

<p align="center">a concurrent indexer combined with fast and relevant search algorithms</p>

## Introduction

This repository contains the core engine used in [MeiliSearch].

It contains a library that can manage one and only one index. MeiliSearch
manages the multi-index itself. Milli is unable to store updates in a store:
it is the job of something else above and this is why it is only able
to process one update at a time.

This repository contains crates to quickly debug the engine:
 - There are benchmarks located in the `benchmarks` crate.
 - The `http-ui` crate is a simple HTTP dashboard to tests the features like for real!
 - The `infos` crate is used to dump the internal data-structure and ensure correctness.
 - The `search` crate is a simple command-line that helps run [flamegraph] on top of it.
 - The `helpers` crate is only used to modify the database inplace, sometimes.

### Compile and run the HTTP debug server

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

## Contributing

You can setup a `git-hook` to stop you from making a commit too fast. It'll stop you if:
- Any of the workspaces does not build
- Your code is not well-formatted

These two things are also checked in the CI, so ignoring the hook won't help you merge your code.
But if you need to, you can still add `--no-verify` when creating your commit to ignore the hook.

To enable the hook, run the following command from the root of the project:
```
cp script/pre-commit .git/hooks/pre-commit
```

[MeiliSearch]: https://github.com/MeiliSearch/MeiliSearch
[flamegraph]: https://github.com/flamegraph-rs/flamegraph
