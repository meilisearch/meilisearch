## Related issue

Fixes #...

## Use of Generative AI Tools

In accordance with our [contributing policy](https://github.com/meilisearch/meilisearch/blob/main/CONTRIBUTING.md#use-of-generative-ai-tools), using generative AI tooling is not allowed in external contributions meant for Meilisearch. As a result, this PR will not be reviewed unless its description contains the following checked statement:

- [ ] I certify that this contribution (code, PR description, and PR comments) is not using any generative AI tooling

## Requirements

Ensure the following requirements before merging:
- [ ] Automated tests have been added.
- [ ] If some tests cannot be automated, manual rigorous tests should be applied.
- [ ] If there is any change in the DB:
    - [ ] Test that any impacted DB still works as expected after using `--upgrade-db` on a DB created with the last released Meilisearch
    - [ ] Test that during the upgrade, **search is still available** (artificially make the upgrade longer if needed)
    - [ ] Inform the reviewer so they can set the `db change` label accordingly
