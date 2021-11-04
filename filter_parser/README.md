# Filter parser

This workspace is dedicated to the parsing of the MeiliSearch filters.

Most of the code and explanation are in the [`lib.rs`](./src/lib.rs). Especially, the BNF of the filters at the top of this file.

The parser use [nom](https://docs.rs/nom/) to do most of its work and [nom-locate](https://docs.rs/nom_locate/) to keep track of what we were doing when we encountered an error.

## Cli
A simple main is provided to quick-test if a filter can be parsed or not without bringing milli.
It takes one argument and try to parse it.
```
cargo run -- 'field = value' # success
cargo run -- 'field = "doggo' # error => missing closing delimiter "
```

## Fuzz
The workspace have been fuzzed with [cargo-fuzz](https://rust-fuzz.github.io/book/cargo-fuzz.html).

### Setup
You'll need rust-nightly to execute the fuzzer.

```
cargo install cargo-fuzz
```

### Run
```
cargo fuzz run parse
```

## What to do if you find a bug in the parser

- Write a test at the end of the [`lib.rs`](./src/lib.rs) to ensure it never happens again.
- Add a file in [the corpus directory](./fuzz/corpus/parse/) with your filter to help the fuzzer finding new bug. Since this directory is going to be heavily polluted by the execution of the fuzzer it's in the gitignore and you'll need to force push your new test.
  Since this directory is going to be heavily polluted by the execution of the fuzzer it's in the gitignore and you'll need to force add your new test.
