<div align="center">
  <a href="https://docs.rs/async-openai">
  <img width="50px" src="https://raw.githubusercontent.com/64bit/async-openai/assets/create-image-b64-json/img-1.png" />
  </a>
</div>
<h1 align="center"> async-openai </h1>
<p align="center"> Async Rust library for OpenAI </p>
<div align="center">
    <a href="https://crates.io/crates/async-openai">
    <img src="https://img.shields.io/crates/v/async-openai.svg" />
    </a>
    <a href="https://docs.rs/async-openai">
    <img src="https://docs.rs/async-openai/badge.svg" />
    </a>
</div>
<div align="center">
<sub>Logo created by this <a href="https://github.com/64bit/async-openai/tree/main/examples/create-image-b64-json">repo itself</a></sub>
</div>

## Overview

`async-openai` is an unofficial Rust library for OpenAI.

- It's based on [OpenAI OpenAPI spec](https://github.com/openai/openai-openapi)
- Current features:
  - [x] Assistants (v2)
  - [x] Audio
  - [x] Batch
  - [x] Chat
  - [x] Completions (Legacy)
  - [x] Embeddings
  - [x] Files
  - [x] Fine-Tuning
  - [x] Images
  - [x] Models
  - [x] Moderations
  - [x] Organizations | Administration (partially implemented)
  - [x] Realtime (Beta) (partially implemented)
  - [x] Uploads
- Bring your own custom types for Request or Response objects.
- SSE streaming on available APIs
- Requests (except SSE streaming) including form submissions are retried with exponential backoff when [rate limited](https://platform.openai.com/docs/guides/rate-limits).
- Ergonomic builder pattern for all request objects.
- Microsoft Azure OpenAI Service (only for APIs matching OpenAI spec)

## Usage

The library reads [API key](https://platform.openai.com/account/api-keys) from the environment variable `OPENAI_API_KEY`.

```bash
# On macOS/Linux
export OPENAI_API_KEY='sk-...'
```

```powershell
# On Windows Powershell
$Env:OPENAI_API_KEY='sk-...'
```

- Visit [examples](https://github.com/64bit/async-openai/tree/main/examples) directory on how to use `async-openai`.
- Visit [docs.rs/async-openai](https://docs.rs/async-openai) for docs.

## Realtime API

Only types for Realtime API are implemented, and can be enabled with feature flag `realtime`.
These types were written before OpenAI released official specs.

## Image Generation Example

```rust
use async_openai::{
    types::{CreateImageRequestArgs, ImageSize, ImageResponseFormat},
    Client,
};
use std::error::Error;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // create client, reads OPENAI_API_KEY environment variable for API key.
    let client = Client::new();

    let request = CreateImageRequestArgs::default()
        .prompt("cats on sofa and carpet in living room")
        .n(2)
        .response_format(ImageResponseFormat::Url)
        .size(ImageSize::S256x256)
        .user("async-openai")
        .build()?;

    let response = client.images().create(request).await?;

    // Download and save images to ./data directory.
    // Each url is downloaded and saved in dedicated Tokio task.
    // Directory is created if it doesn't exist.
    let paths = response.save("./data").await?;

    paths
        .iter()
        .for_each(|path| println!("Image file path: {}", path.display()));

    Ok(())
}
```

<div align="center">
  <img width="315" src="https://raw.githubusercontent.com/64bit/async-openai/assets/create-image/img-1.png" />
  <img width="315" src="https://raw.githubusercontent.com/64bit/async-openai/assets/create-image/img-2.png" />
  <br/>
  <sub>Scaled up for README, actual size 256x256</sub>
</div>

## Bring Your Own Types

Enable methods whose input and outputs are generics with `byot` feature. It creates a new method with same name and `_byot` suffix.

For example, to use `serde_json::Value` as request and response type:
```rust
let response: Value = client
        .chat()
        .create_byot(json!({
            "messages": [
                {
                    "role": "developer",
                    "content": "You are a helpful assistant"
                },
                {
                    "role": "user",
                    "content": "What do you think about life?"
                }
            ],
            "model": "gpt-4o",
            "store": false
        }))
        .await?;
```

This can be useful in many scenarios:
- To use this library with other OpenAI compatible APIs whose types don't exactly match OpenAI. 
- Extend existing types in this crate with new fields with `serde`.
- To avoid verbose types.
- To escape deserialization errors.

Visit [examples/bring-your-own-type](https://github.com/64bit/async-openai/tree/main/examples/bring-your-own-type) directory to learn more.

## Contributing

Thank you for taking the time to contribute and improve the project. I'd be happy to have you!

All forms of contributions, such as new features requests, bug fixes, issues, documentation, testing, comments, [examples](https://github.com/64bit/async-openai/tree/main/examples) etc. are welcome.

A good starting point would be to look at existing [open issues](https://github.com/64bit/async-openai/issues).

To maintain quality of the project, a minimum of the following is a must for code contribution:

- **Names & Documentation**: All struct names, field names and doc comments are from OpenAPI spec. Nested objects in spec without names leaves room for making appropriate name.
- **Tested**: For changes supporting test(s) and/or example is required. Existing examples, doc tests, unit tests, and integration tests should be made to work with the changes if applicable.
- **Scope**: Keep scope limited to APIs available in official documents such as [API Reference](https://platform.openai.com/docs/api-reference) or [OpenAPI spec](https://github.com/openai/openai-openapi/). Other LLMs or AI Providers offer OpenAI-compatible APIs, yet they may not always have full parity. In such cases, the OpenAI spec takes precedence.
- **Consistency**: Keep code style consistent across all the "APIs" that library exposes; it creates a great developer experience.

This project adheres to [Rust Code of Conduct](https://www.rust-lang.org/policies/code-of-conduct)

## Complimentary Crates

- [openai-func-enums](https://github.com/frankfralick/openai-func-enums) provides procedural macros that make it easier to use this library with OpenAI API's tool calling feature. It also provides derive macros you can add to existing [clap](https://github.com/clap-rs/clap) application subcommands for natural language use of command line tools. It also supports openai's [parallel tool calls](https://platform.openai.com/docs/guides/function-calling/parallel-function-calling) and allows you to choose between running multiple tool calls concurrently or own their own OS threads.
- [async-openai-wasm](https://github.com/ifsheldon/async-openai-wasm) provides WASM support.

## License

This project is licensed under [MIT license](https://github.com/64bit/async-openai/blob/main/LICENSE).
