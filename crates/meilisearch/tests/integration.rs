mod auth;
mod batches;
mod common;
mod dashboard;
mod documents;
mod dumps;
mod features;
mod index;
mod logs;
mod network;
mod search;
mod settings;
mod similar;
mod snapshot;
mod stats;
mod swap_indexes;
mod tasks;
mod upgrade;
mod vector;

// Tests are isolated by features in different modules to allow better readability, test
// targetability, and improved incremental compilation times.
//
// All the integration tests live in the same root module so only one test executable is generated,
// thus improving linking time.
