# Versioning policy

This page describes the versioning rules Meilisearch will follow once v1.0.0 is released and how/when we should increase the MAJOR, MINOR, and PATCH of the versions.

## 🤖 Basic rules

Meilisearch engine releases follow the [SemVer rules](https://semver.org/), including the following basic ones:

> 🔥 Given a version number MAJOR.MINOR.PATCH, increment the:
>
> 1. MAJOR version when you make incompatible API changes
> 2. MINOR version when you add functionality in a backwards compatible
> manner
> 3. PATCH version when you make backwards compatible bug fixes

**Changes that MAY lead the Meilisearch users (developers) to change their code are considered API incompatibility and will make us increase the MAJOR version of Meilisearch.**

**In other terms, if the users MAY have to do more steps than just downloading the new Meilisearch instance and running it, a new MAJOR is needed.**

Examples of changes making the code break and then, involving increasing the MAJOR:

- Name change of a route or a field in the request/response body
- Change a default value of a parameter or a setting.
- Any API behavior change: the users expect in their code the engine to behave this way, but it does not.
Examples:
    - Make a synchronous error asynchronous or the contrary
    - `displayableAttributes` impact now the `/documents` route: the users expect to retrieve all the fields, so specific fields, in their code but cannot.
- Change a final value type.
Ex: `/stats` now return floats instead of integers. This can impact strongly typed languages.

⚠️ This guide only applies to the Meilisearch binary. Additional tools like SDKs and Docker images are out of the scope of this guide. However, we will ensure the changelogs are clear enough to inform users of the changes and their impacts.

## ✋ Exceptions related to Meilisearch’s specificities

Meilisearch is a search engine working with an internal database. It means some parts of the project can be really problematic to consider as breaking (and then leading to an increase of the MAJOR) without slowing down innovation.

Here is the list of the following exceptions of changes that will not lead to an increase in the MAJOR in Meilisearch release.

### DB incompatibilities: force using a dump

A DB breaking leads to a failure when starting Meilisearch: you need to use a dump.

We know this kind of failure requiring an additional step is the definition of “breaking” on the user side, but it’s really complicated to consider increasing a MAJOR for this. Indeed, since we don’t want to release a major version every two months and we also want to keep innovating simultaneously, increasing the MINOR is the best solution.

People would need to use dump sometimes between two MAJOR versions; for instance, this is something [PostgreSQL does](https://www.postgresql.org/support/versioning/) by asking their users to perform some manual actions between two MINOR releases.

### Search relevancy and algorithm improvements

Relevancy is the engine team job; we need to improve it every day, like performance. It will be really hard to improve the engine without allowing the team to change the relevancy algorithm. Same as for DB breaking, considering relevancy changes as breaking can really slow down innovation.

This way, changing the search relevancy, not the API behavior or fields, but the final relevancy result (like cropping algorithm, search algorithm, placeholder behavior, highlight behavior…) is not considered as a breaking change. Indeed, changing the relevancy behavior is not supposed to make the code fail since the final results of Meilisearch are only displayed, no matter the matched documents.

This kind of change will lead us to increase the MINOR to let the people know about the change and avoid non-expected changes when pulling the latest patched version of Meilisearch. Indeed, increasing the MINOR (instead of the PATCH) will prevent users from downloading the new patched version without noticing the changes.

🚨 Any change about the relevancy that is related to API usage, and thus, that may impact users to change their code (for instance changing the default `matchingStrategy` value) is not related to this specific section and would lead us to increase the MAJOR.

### New "variant" type addition

We don't consider breaking to add a new type to an already existing list of variant. For example, adding a new type of `task`, or a new type of error `code`.

We are aware some strongly typed language code bases could be impacted, and our recommendation is to handle the possibility of having an unknown type when deserializing Meilisearch's response.

### Human-readability purposes

- Changing the value of `message` or `link` in error object will only increase the PATCH. The users should not refer to this field in their code since `code` and `type` exist in the same object.
- Any error message sent to the terminal that changed will increase the PATCH. People should not rely on them since these messages are for human debugging.
- Updating the logs format will increase the MINOR: this is supposed to be used by humans for debugging, but we are aware some people can plug some tools at the top of them. But since it’s not the main purpose of our logs, we don’t want to increase the MAJOR for a log format change. However, we will increase the MINOR to let the people know better about the change and avoid bad surprises when pulling the latest patched version of Meilisearch.

### Integrated web-interface

Any changes done to the integrated web interface are not considered breaking. The interface is considered an additional tool for test purposes, not for production.

## 📝 About the Meilisearch changelogs

All the changes, no matter if they are considered as breaking or not, if they are related to an algorithm change or not, will be announced in the changelogs.

The details of the change will depend on the impact on the users. For instance, giving too many details on really deep tech improvements can lead to some confusion on the user side.

## 👀 Some precisions

- Updating a dependence requirement of Meilisearch is NOT considered as breaking by SemVer guide and will lead, in our case, to increasing the MINOR. Indeed, increasing the MINOR (instead of the PATCH) will prevent users from downloading the new patched version without noticing the changes.
See the [related rule](https://semver.org/#what-should-i-do-if-i-update-my-own-dependencies-without-changing-the-public-api).
- Fixing a CVE (Common Vulnerabilities and Exposures) will not increase the MAJOR; depending on the CVE, it will be a PATCH or a MINOR upgrade.
