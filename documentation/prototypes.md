# Prototype process

## What is a prototype?

A prototype is an alternative version of Meilisearch (provided in a Docker image) containing a new feature or an improvement the engine team provides to the users.

## Why providing a prototype?

For some features or improvements we want to introduce in Meilisearch, we also have to make the users test them first before releasing them for many reasons:
- to ensure we solve the first use case defined during the discovery
- to ensure the API does not have major issues of usages
- identify/remove concrete technical roadblocks by working on an implementation as soon as possible, like performance issues
- to get any other feedback from the users regarding their usage

These make us iterate fast before stabilizing it for the current release.

> ⚠️ Prototypes are NOT [experimental features](./experimental-features.md). All experimental features are thoroughly tested before release and follow the same quality standards as other features. This is not the case with prototypes which are the equivalent of a first draft of a new feature.

## How to publish a prototype?

### Release steps

The prototype name must [follow this convention](https://semver.org/#spec-item-11): `prototype-v<version>-<name>.<iteration>` where
- `version` is the version of Meilisearch on which the prototype is based.
- `name` is the feature name formatted in `kebab-case`.
- `iteration` is the iteration of the prototype, starting from `0`.

✅ Example: `prototype-v1.23.0-search-personalization.1`. </br>
❌ Bad example: `prototype-v1.23.0-search-personalization-0`: a dash separates the name and version. </br>
❌ Bad example: `prototype-v1.23.0.search-personalization.0`: a dot separates the version and name. </br>
❌ Bad example: `prototype-search-personalization-0`: version is missing.</br>
❌ Bad example: `v1.23.0-auto-resize-0`: lacks the `prototype-` prefix. </br>
❌ Bad example: `prototype-v1.23.0-auto-resize`: lacks the version suffix. </br>
❌ Bad example: `prototype-v1.23.0-auto-resize.0-0`: feature name ends with something else than a number.

Steps to create a prototype:

1. In your terminal, go to the last commit of your branch (the one you want to provide as a prototype).
2. Use the `cargo xtask generate-prototype` command to generate the prototype name.
3. Create the tag using the `git tag` command.
4. Checkout the tag, run Meilisearch and check that it launches summary features a line: `Prototype: prototype-v<version>-<name>.<iteration>`.
5. Checkout back to your branch: `git checkout -`.
6. Push the tag: `git push origin prototype-v<version>-<name>.<iteration>`
7. Check that the [Docker CI](https://github.com/meilisearch/meilisearch/actions/workflows/publish-docker-images.yml) is now running.

🐳 Once the CI has finished to run, a Docker image named `prototype-v<version>-<name>.<iteration>` will be available on [DockerHub](https://hub.docker.com/repository/docker/getmeili/meilisearch/general). People can use it with the following command: `docker run -p 7700:7700 -v $(pwd)/meili_data:/meili_data getmeili/meilisearch:prototype-v<version>-<name>.<iteration>`. <br>
More information about [how to run Meilisearch with Docker](https://docs.meilisearch.com/learn/cookbooks/docker.html#download-meilisearch-with-docker).

⚠️ However, no binaries will be created. If the users do not use Docker, they can go to the `prototype-v<version>-<name>.<iteration>` tag in the Meilisearch repository and compile it from the source code.

### Communication

When sharing a prototype with users, it's important to
- remind them not to use it in production. Prototypes are solely for test purposes.
- explain how to run the prototype
- explain how to use the new feature
- encourage users to let their feedback

The prototype should be shared at least in the related issue and/or the related product discussion. It's the developer and the PM to decide to add more communication, like sharing it on Discord or Twitter.

Here is an example of messages to share on GitHub:

> Hello everyone,
>
> Here is the current prototype you can use to test the new XXX feature:
>
> How to run the prototype?
> You need to start from a fresh new database (remove the previous used `data.ms`) and use the following Docker image:
> ```bash
> docker run -it --rm -p 7700:7700 -v $(pwd)/meili_data:/meili_data getmeili/meilisearch:prototype-v<version>-<name>.<iteration>
> ```
>
> You can use the feature this way:
> ```bash
> ...
> ```
>
> ⚠️ We do NOT recommend using this prototype in production. This is only for test purposes.
>
> Everyone is more than welcome to give feedback and to report any issue or bug you might encounter when using this prototype. Thanks in advance for your involvement. It means a lot to us ❤️
