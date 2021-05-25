Benchmarks
==========

For our benchmark we are using a small subset of the dataset `songs.csv`. It was generated with this command:
```
xsv sample --seed 42 1000000 songs.csv -o smol-songs.csv
```
You can download it [here](https://meili-datasets.s3.fr-par.scw.cloud/benchmarks/smol-songs.csv.gz)
And the original `songs.csv` dataset is available [here](https://meili-datasets.s3.fr-par.scw.cloud/songs.csv.gz).

We also use a subset of `wikipedia-articles.csv` that was generated with the following command:
```
xsv sample --seed 42 500000 wikipedia-articles.csv -o smol-wikipedia-articles.csv
```
You can download the original [here](https://meili-datasets.s3.fr-par.scw.cloud/wikipedia-articles.csv.gz) and the subset [here](https://meili-datasets.s3.fr-par.scw.cloud/benchmarks/smol-wikipedia-articles.csv.gz).

-----

- To run all the benchmarks we recommand using `cargo bench`, this should takes around ~4h
- You can also run the benchmarks on the `songs` dataset with `cargo bench --bench songs`, it should takes around 1h
- And on the `wiki` dataset with `cargo bench --bench wiki`, it should takes around 3h

By default the benchmarks will be downloaded and uncompressed automatically in the target directory.
If you don't want to download the datasets everytime you updates something on the code you can specify a custom directory with the env variable `MILLI_BENCH_DATASETS_PATH`:
```
mkdir ~/datasets
MILLI_BENCH_DATASETS_PATH=~/datasets cargo bench --bench songs # the two datasets are downloaded
touch build.rs
MILLI_BENCH_DATASETS_PATH=~/datasets cargo bench --bench songs # the code is compiled again but the datasets are not downloaded
```
