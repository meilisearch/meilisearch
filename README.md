<p align="center">
  <img src="assets/logo.svg" alt="Meilisearch" width="200" height="200" />
</p>

<h1 align="center">Meilisearch</h1>

<h4 align="center">
  <a href="https://www.meilisearch.com">Website</a> |
  <a href="https://roadmap.meilisearch.com/tabs/1-under-consideration">Roadmap</a> |
  <a href="https://blog.meilisearch.com">Blog</a> |
  <a href="https://fr.linkedin.com/company/meilisearch">LinkedIn</a> |
  <a href="https://twitter.com/meilisearch">Twitter</a> |
  <a href="https://docs.meilisearch.com">Documentation</a> |
  <a href="https://docs.meilisearch.com/faq/">FAQ</a>
</h4>

<p align="center">
  <a href="https://github.com/meilisearch/meilisearch/actions"><img src="https://github.com/meilisearch/meilisearch/workflows/Cargo%20test/badge.svg" alt="Build Status"></a>
  <a href="https://deps.rs/repo/github/meilisearch/meilisearch"><img src="https://deps.rs/repo/github/meilisearch/meilisearch/status.svg" alt="Dependency status"></a>
  <a href="https://github.com/meilisearch/meilisearch/blob/main/LICENSE"><img src="https://img.shields.io/badge/license-MIT-informational" alt="License"></a>
  <a href="https://slack.meilisearch.com"><img src="https://img.shields.io/badge/slack-meilisearch-blue.svg?logo=slack" alt="Slack"></a>
  <a href="https://github.com/meilisearch/meilisearch/discussions" alt="Discussions"><img src="https://img.shields.io/badge/github-discussions-red" /></a>
  <a href="https://app.bors.tech/repositories/26457"><img src="https://bors.tech/images/badge_small.svg" alt="Bors enabled"></a>
</p>

<p align="center">⚡ Lightning Fast, Ultra Relevant, and Typo-Tolerant Search Engine 🔍</p>

**Meilisearch** is a powerful, fast, open-source, easy to use and deploy search engine. Both searching and indexing are highly customizable. Features such as typo-tolerance, filters, and synonyms are provided out-of-the-box.
For more information about features go to [our documentation](https://docs.meilisearch.com/).

<p align="center">
  <img src="assets/trumen-fast.gif" alt="Web interface gif" />
</p>

## ✨ Features
* Search-as-you-type experience (answers < 50 milliseconds)
* Full-text search
* Typo tolerant (understands typos and misspelling)
* Faceted search and filters
* Supports hanzi (Chinese characters)
* Supports synonyms
* Easy to install, deploy, and maintain
* Whole documents are returned
* Highly customizable
* RESTful API

## Getting started

### Deploy the Server

#### Homebrew (Mac OS)

```bash
brew update && brew install meilisearch
meilisearch
```

#### Docker

```bash
docker run -p 7700:7700 -v "$(pwd)/meili_data:/meili_data" getmeili/meilisearch
```

#### Announcing a cloud-hosted Meilisearch

Join the closed beta by filling out this [form](https://meilisearch.typeform.com/to/FtnzvZfh).

#### Try Meilisearch in our Sandbox

Create a Meilisearch instance in [Meilisearch Sandbox](https://sandbox.meilisearch.com/). This instance is free, and will be active for 48 hours.

#### Run on Digital Ocean

[![DigitalOcean Marketplace](assets/do-btn-blue.svg)](https://marketplace.digitalocean.com/apps/meilisearch?action=deploy&refcode=7c67bd97e101)

#### Deploy on Platform.sh

<a href="https://console.platform.sh/projects/create-project?template=https://raw.githubusercontent.com/platformsh/template-builder/master/templates/meilisearch/.platform.template.yaml&utm_content=meilisearch&utm_source=github&utm_medium=button&utm_campaign=deploy_on_platform">
    <img src="https://platform.sh/images/deploy/lg-blue.svg" alt="Deploy on Platform.sh" width="180px" />
</a>

#### APT (Debian & Ubuntu)

```bash
echo "deb [trusted=yes] https://apt.fury.io/meilisearch/ /" > /etc/apt/sources.list.d/fury.list
apt update && apt install meilisearch-http
meilisearch
```

#### Download the binary (Linux & Mac OS)

```bash
curl -L https://install.meilisearch.com | sh
./meilisearch
```

#### Compile and run it from sources

If you have the latest stable Rust toolchain installed on your local system, clone the repository and change it to your working directory.

```bash
git clone https://github.com/meilisearch/meilisearch.git
cd meilisearch
cargo run --release
```

### Create an Index and Upload Some Documents

Let's create an index! If you need a sample dataset, use [this movie database](https://www.notion.so/meilisearch/A-movies-dataset-to-test-Meili-1cbf7c9cfa4247249c40edfa22d7ca87#b5ae399b81834705ba5420ac70358a65). You can also find it in the `datasets/` directory.

```bash
curl -L https://docs.meilisearch.com/movies.json -o movies.json
```

Now, you're ready to index some data.

```bash
curl -i -X POST 'http://127.0.0.1:7700/indexes/movies/documents' \
  --header 'content-type: application/json' \
  --data-binary @movies.json
```

### Search for Documents

#### In command line

The search engine is now aware of your documents and can serve those via an HTTP server.

The [`jq` command-line tool](https://stedolan.github.io/jq/) can greatly help you read the server responses.

```bash
curl 'http://127.0.0.1:7700/indexes/movies/search?q=botman+robin&limit=2' | jq
```

```json
{
  "hits": [
    {
      "id": "415",
      "title": "Batman & Robin",
      "poster": "https://image.tmdb.org/t/p/w1280/79AYCcxw3kSKbhGpx1LiqaCAbwo.jpg",
      "overview": "Along with crime-fighting partner Robin and new recruit Batgirl, Batman battles the dual threat of frosty genius Mr. Freeze and homicidal horticulturalist Poison Ivy. Freeze plans to put Gotham City on ice, while Ivy tries to drive a wedge between the dynamic duo.",
      "release_date": 866768400
    },
    {
      "id": "411736",
      "title": "Batman: Return of the Caped Crusaders",
      "poster": "https://image.tmdb.org/t/p/w1280/GW3IyMW5Xgl0cgCN8wu96IlNpD.jpg",
      "overview": "Adam West and Burt Ward returns to their iconic roles of Batman and Robin. Featuring the voices of Adam West, Burt Ward, and Julie Newmar, the film sees the superheroes going up against classic villains like The Joker, The Riddler, The Penguin and Catwoman, both in Gotham City… and in space.",
      "release_date": 1475888400
    }
  ],
  "nbHits": 8,
  "exhaustiveNbHits": false,
  "query": "botman robin",
  "limit": 2,
  "offset": 0,
  "processingTimeMs": 2
}
```

#### Use the Web Interface

We also deliver an **out-of-the-box [web interface](https://github.com/meilisearch/mini-dashboard)** in which you can test Meilisearch interactively.

You can access the web interface in your web browser at the root of the server. The default URL is [http://127.0.0.1:7700](http://127.0.0.1:7700). All you need to do is open your web browser and enter Meilisearch’s address to visit it. This will lead you to a web page with a search bar that will allow you to search in the selected index.

| [See the gif above](#demo)

## Documentation

Now that your Meilisearch server is up and running, you can learn more about how to tune your search engine in [the documentation](https://docs.meilisearch.com).

## Contributing

Hey! We're glad you're thinking about contributing to Meilisearch! Feel free to pick an [issue labeled as `good first issue`](https://github.com/meilisearch/meilisearch/issues?q=is%3Aissue+is%3Aopen+label%3A%22good+first+issue%22), and to ask any question you need. Some points might not be clear and we are available to help you!

Also, we recommend following the [CONTRIBUTING](./CONTRIBUTING.md) to create your PR.

## Core engine and tokenizer

The code in this repository is only concerned with managing multiple indexes, handling the update store, and exposing an HTTP API.

Search and indexation are the domain of our core engine, [`milli`](https://github.com/meilisearch/milli), while tokenization is handled by [our `tokenizer` library](https://github.com/meilisearch/tokenizer/).
## Telemetry

Meilisearch collects anonymous data regarding general usage.
This helps us better understand developers' usage of Meilisearch features.

To find out more on what information we're retrieving, please see our documentation on [Telemetry](https://docs.meilisearch.com/learn/what_is_meilisearch/telemetry.html).

This program is optional, you can disable these analytics by using the `MEILI_NO_ANALYTICS` env variable.

## Feature request

The feature requests are not managed in this repository. Please visit our [dedicated repository](https://github.com/meilisearch/product) to see our work about the Meilisearch product.

If you have a feature request or any feedback about an existing feature, please open [a discussion](https://github.com/meilisearch/product/discussions).
Also, feel free to participate in the current discussions, we are looking forward to reading your comments.

## 💌 Contact

Please visit [this page](https://docs.meilisearch.com/learn/what_is_meilisearch/contact.html#contact-us).

Meilisearch is developed by [Meili](https://www.meilisearch.com), a young company. To know more about us, you can [read our blog](https://blog.meilisearch.com). Any suggestion or feedback is highly appreciated. Thank you for your support!
