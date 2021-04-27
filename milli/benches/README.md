Benchmarks
==========

For our benchmark we are using a small subset of the dataset songs.csv. It was generated with this command:
```
xsv sample --seed 42 song.csv -o smol-songs.csv
```
You can download it [here](https://meili-datasets.s3.fr-par.scw.cloud/benchmarks/smol-songs.csv.gz)
And the original `songs.csv` dataset is available [here](https://meili-datasets.s3.fr-par.scw.cloud/songs.csv.gz).

You need to put this file in the current directory: `milli/milli/benches/smol-songs.csv.gz`
You can run the following command from the root of this git repository
```
wget https://meili-datasets.s3.fr-par.scw.cloud/benchmarks/smol-songs.csv.gz -O milli/benches/smol-songs.csv.gz
```

- To run all the benchmarks we recommand using `cargo bench`, this should takes around ~4h
- You can also run the benchmarks on the `songs` dataset with `cargo bench --bench songs`, it should takes around 1h
- And on the `wiki` dataset with `cargo bench --bench wiki`, it should takes around 3h

By default the benchmarks expect the datasets to be uncompressed and present in `milli/milli/benches`, but you can also specify your own path with the environment variable `MILLI_BENCH_DATASETS_PATH` like that:
```
MILLI_BENCH_DATASETS_PATH=~/Downloads/datasets cargo bench --bench songs
```

Our benchmarking suite uses criterion which allow you to do a lot of configuration, see the documentation [here](https://bheisler.github.io/criterion.rs/book/user_guide/user_guide.html)

