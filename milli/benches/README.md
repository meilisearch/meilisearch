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
