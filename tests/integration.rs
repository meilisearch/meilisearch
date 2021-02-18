mod common;
mod index;
mod search;
mod settings;
mod documents;

// Tests are isolated by features in different modules to allow better readability, test
// targetability, and improved incremental compilation times.
//
// All the integration tests live in the same root module so only one test executable is generated,
// thus improving linking time.
