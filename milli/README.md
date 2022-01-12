# Milli

## Fuzzing milli

Currently you can only fuzz the indexation.
To execute the fuzzer run:
```
cargo +nightly fuzz run indexing
```

To execute the fuzzer on multiple thread you can also run:
```
cargo +nightly fuzz run -j4 indexing
```

Since the fuzzer is going to create a lot of temporary file to let milli index its documents
I would also recommand to execute it on a ramdisk.
Here is how to setup a ramdisk on linux:
```
sudo mount -t tmpfs none path/to/your/ramdisk
```
And then set the [TMPDIR](https://doc.rust-lang.org/std/env/fn.temp_dir.html) environment variable
to make the fuzzer create its file in it:
```
export TMPDIR=path/to/your/ramdisk
```
