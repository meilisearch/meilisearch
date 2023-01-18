Benchmarks
==========

## TOC

- [Run the benchmarks](#run-the-benchmarks)
- [Comparison between benchmarks](#comparison-between-benchmarks)
- [Datasets](#datasets)

## Run the benchmarks

### On our private server

The Meili team has self-hosted his own GitHub runner to run benchmarks on our dedicated bare metal server.

To trigger the benchmark workflow:
- Go to the `Actions` tab of this repository.
- Select the `Benchmarks` workflow on the left.
- Click on `Run workflow` in the blue banner.
- Select the branch on which you want to run the benchmarks and select the dataset you want (default: `songs`).
- Finally, click on `Run workflow`.

This GitHub workflow will run the benchmarks and push the `critcmp` report to a DigitalOcean Space (= S3).

The name of the uploaded file is displayed in the workflow.

_[More about critcmp](https://github.com/BurntSushi/critcmp)._

💡 To compare the just-uploaded benchmark with another one, check out the [next section](#comparison-between-benchmarks).

### On your machine

To run all the benchmarks (~5h):

```bash
cargo bench
```

To run only the `search_songs` (~1h), `search_wiki` (~3h), `search_geo` (~20m) or `indexing` (~2h) benchmark:

```bash
cargo bench --bench <dataset name>
```

By default, the benchmarks will be downloaded and uncompressed automatically in the target directory.<br>
If you don't want to download the datasets every time you update something on the code, you can specify a custom directory with the environment variable `MILLI_BENCH_DATASETS_PATH`:

```bash
mkdir ~/datasets
MILLI_BENCH_DATASETS_PATH=~/datasets cargo bench --bench search_songs # the four datasets are downloaded
touch build.rs
MILLI_BENCH_DATASETS_PATH=~/datasets cargo bench --bench songs # the code is compiled again but the datasets are not downloaded
```

## Comparison between benchmarks

The benchmark reports we push are generated with `critcmp`. Thus, we use `critcmp` to show the result of a benchmark, or compare results between multiple benchmarks.

We provide a script to download and display the comparison report.

Requirements:
- `grep`
- `curl`
- [`critcmp`](https://github.com/BurntSushi/critcmp)

List the available file in the DO Space:

```bash
./benchmarks/script/list.sh
```
```bash
songs_main_09a4321.json
songs_geosearch_24ec456.json
search_songs_main_cb45a10b.json
```

Run the comparison script:

```bash
# we get the result of ONE benchmark, this give you an idea of how much time an operation took
./benchmarks/scripts/compare.sh son songs_geosearch_24ec456.json
# we compare two benchmarks
./benchmarks/scripts/compare.sh songs_main_09a4321.json songs_geosearch_24ec456.json
# we compare three benchmarks
./benchmarks/scripts/compare.sh songs_main_09a4321.json songs_geosearch_24ec456.json search_songs_main_cb45a10b.json
```

## Datasets

The benchmarks uses the following datasets:
- `smol-songs`
- `smol-wiki`
- `movies`
- `smol-all-countries`

### Songs

`smol-songs` is a subset of the [`songs.csv` dataset](https://milli-benchmarks.fra1.digitaloceanspaces.com/datasets/songs.csv.gz).

It was generated with this command:

```bash
xsv sample --seed 42 1000000 songs.csv -o smol-songs.csv
```

_[Download the generated `smol-songs` dataset](https://milli-benchmarks.fra1.digitaloceanspaces.com/datasets/smol-songs.csv.gz)._

### Wiki

`smol-wiki` is a subset of the [`wikipedia-articles.csv` dataset](https://milli-benchmarks.fra1.digitaloceanspaces.com/datasets/wiki-articles.csv.gz).

It was generated with the following command:

```bash
xsv sample --seed 42 500000 wiki-articles.csv -o smol-wiki-articles.csv
```

_[Download the `smol-wiki` dataset](https://milli-benchmarks.fra1.digitaloceanspaces.com/datasets/smol-wiki-articles.csv.gz)._

### Movies

`movies` is a really small dataset we uses as our example in the [getting started](https://docs.meilisearch.com/learn/getting_started/)

_[Download the `movies` dataset](https://docs.meilisearch.com/movies.json)._


### All Countries

`smol-all-countries` is a subset of the [`all-countries.csv` dataset](https://milli-benchmarks.fra1.digitaloceanspaces.com/datasets/all-countries.csv.gz)
It has been converted to jsonlines and then edited so it matches our format for the `_geo` field.

It was generated with the following command:
```bash
bat all-countries.csv.gz | gunzip | xsv sample --seed 42 1000000 | csv2json-lite | sd '"latitude":"(.*?)","longitude":"(.*?)"' '"_geo": { "lat": $1, "lng": $2 }' | sd '\[|\]|,$' '' | gzip > smol-all-countries.jsonl.gz
```

_[Download the `smol-all-countries` dataset](https://milli-benchmarks.fra1.digitaloceanspaces.com/datasets/smol-all-countries.jsonl.gz)._

