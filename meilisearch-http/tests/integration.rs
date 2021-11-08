mod auth;
mod common;
mod dashboard;
mod documents;
mod index;
mod search;
mod settings;
mod snapshot;
mod stats;
mod tasks;

// Tests are isolated by features in different modules to allow better readability, test
// targetability, and improved incremental compilation times.
//
// All the integration tests live in the same root module so only one test executable is generated,
// thus improving linking time.
