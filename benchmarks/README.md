Benchmarks
==========

## TOC

- [Datasets](#datasets)
- [Run the benchmarks](#run-the-benchmarks)
- [Comparison between benchmarks](#comparison-between-benchmarks)

## Datasets

The benchmarks are available for the following datasets:
- `songs`
- `wiki`

### Songs

`songs` is a subset of the [`songs.csv` dataset](https://meili-datasets.s3.fr-par.scw.cloud/songs.csv.gz).

It was generated with this command:

```bash
xsv sample --seed 42 1000000 songs.csv -o smol-songs.csv
```

_[Download the generated `songs` dataset](https://meili-datasets.s3.fr-par.scw.cloud/benchmarks/smol-songs.csv.gz)._

### Wiki

`wiki` is a subset of the [`wikipedia-articles.csv` dataset](https://meili-datasets.s3.fr-par.scw.cloud/wikipedia-articles.csv.gz).

It was generated with the following command:

```bash
xsv sample --seed 42 500000 wikipedia-articles.csv -o smol-wikipedia-articles.csv
```

_[Download the generated `wiki` dataset](https://meili-datasets.s3.fr-par.scw.cloud/benchmarks/smol-wikipedia-articles.csv.gz)._

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

To run all the benchmarks (~4h):

```bash
cargo bench
```

To run only the `songs` (~1h) or `wiki` (~3h) benchmark:

```bash
cargo bench --bench <dataset name>
```

By default, the benchmarks will be downloaded and uncompressed automatically in the target directory.<br>
If you don't want to download the datasets every time you update something on the code, you can specify a custom directory with the environment variable `MILLI_BENCH_DATASETS_PATH`:

```bash
mkdir ~/datasets
MILLI_BENCH_DATASETS_PATH=~/datasets cargo bench --bench songs # the two datasets are downloaded
touch build.rs
MILLI_BENCH_DATASETS_PATH=~/datasets cargo bench --bench songs # the code is compiled again but the datasets are not downloaded
```

## Comparison between benchmarks

The benchmark reports we push are generated with `critcmp`. Thus, we use `critcmp` to generate comparison results between 2 benchmarks.

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
2021-05-31 14:40       279890  s3://milli-benchmarks/critcmp_results/songs_main_09a4321.json
2021-05-31 13:49       279576  s3://milli-benchmarks/critcmp_results/songs_geosearch_24ec456.json
```

Run the comparison script:

```bash
./benchmarks/scripts/compare.sh songs_main_09a4321.json songs_geosearch_24ec456.json
```
