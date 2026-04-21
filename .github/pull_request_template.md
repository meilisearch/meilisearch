## Related issue

Fixes #...

## Generative AI tools

- [ ] This PR does not use generative AI tooling
- [ ] This PR uses generative AI tooling and respect the [related policies](https://github.com/meilisearch/meilisearch/blob/main/CONTRIBUTING.md#use-of-generative-ai-tools)
    - *list of used tools and what they were used for*

## Requirements

⚠️ Ensure the following requirements before merging ⚠️
- [ ] Automated tests have been added.
- [ ] If some tests cannot be automated, manual rigorous tests should be applied.
- [ ] ⚠️ If there is any change in the DB:
    - [ ] Test that any impacted DB still works as expected after using `--experimental-dumpless-upgrade` on a DB created with the last released Meilisearch
    - [ ] Test that during the upgrade, **search is still available** (artificially make the upgrade longer if needed)
    - [ ] Set the `db change` label.
- [ ] If necessary, the feature have been tested in the Cloud production environment (with [prototypes](./documentation/prototypes.md)) and the Cloud UI is ready.
- [ ] If necessary, the [documentation](https://github.com/meilisearch/documentation) related to the implemented feature in the PR is ready.
- [ ] If necessary, the [integrations](https://github.com/meilisearch/integration-guides) related to the implemented feature in the PR are ready.
