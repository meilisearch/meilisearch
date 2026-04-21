# Meilisearch release process

This guide is to describe how to make releases for the current repository.

## 📅 Weekly Meilisearch release

1. A weekly meeting is held every Thursday afternoon to define the release and to ensure minimal checks before the release.
<details>
<summary>Check out the TODO 👇👇👇</summary>
- [ ] Define the version of the release (`vX.Y.Z`) based on our <a href="https://github.com/meilisearch/meilisearch/blob/main/documentation/versioning-policy.md">Versioning Policy</a></br>.
- [ ] Define the commit that will reference the tag release. Every PR merged after this commit will not be taken into account in the future release
- [ ] Manually test `--experimental-dumpless-upgrade` on a DB of the previous Meilisearch minor version</br>
- [ ] Check recent <a href="https://github.com/meilisearch/meilisearch/actions">automated tests</a> on `main`</br>
    - [ ] Scheduled test suite</br>
    - [ ] Scheduled SDK tests</br>
    - [ ] Scheduled flaky tests</br>
    - [ ] Scheduled fuzzer tests</br>
    - [ ] Scheduled Docker CI (dry run)</br>
    - [ ] Scheduled GitHub binary release (dry run)</br>
- [ ] <a href="https://github.com/meilisearch/meilisearch/actions/workflows/update-cargo-toml-version.yml">Create the PR updating the version</a>and merge it.
</details>

2. Go to the GitHub interface, in the [`Release` section](https://github.com/meilisearch/meilisearch/releases).

3. Select the already drafted release or click on the `Draft a new release` button if you want to start a blank one, and fill the form with the appropriate information.
⚠️ Publish on a specific commit defined by the team. Or publish on `main`, but ensure you do want all the PRs merged in your release.

⚙️ The CIs will be triggered to:
- [Upload binaries](https://github.com/meilisearch/meilisearch/actions/workflows/publish-binaries.yml) to the associated GitHub release.
- [Publish the Docker images](https://github.com/meilisearch/meilisearch/actions/workflows/publish-docker-images.yml) (`latest`, `vX`, `vX.Y` and `vX.Y.Z`) to DockerHub -> check the "Docker meta" steps in the CI to check the right tags are created
- [Publish binaries for Homebrew and APT](https://github.com/meilisearch/meilisearch/actions/workflows/publish-apt-brew-pkg.yml)
- [Move the `latest` git tag to the release commit](https://github.com/meilisearch/meilisearch/actions/workflows/latest-git-tag.yml).


### 🔥 How to do a patch release for a hotfix

It happens some releases come with impactful bugs in production (e.g. indexation or search issues): we obviously don't wait for the next cycle to fix them and we release a patched version of Meilisearch.

1. Create a new release branch starting from the latest stable Meilisearch release (`latest` git tag or the corresponding `vX.Y.Z` tag).

```bash
# Ensure you get all the current tags of the repository
git fetch origin --tags --force

# Create the branch
git checkout vX.Y.Z # The latest release you want to patch
git checkout -b release-vX.Y.Z+1 # Increase the Z here
git push -u origin release-vX.Y.Z+1
```

2. Add the newly created branch `release-vX.Y.Z+1` to "Target Branches" of [this GitHub Ruleset](https://github.com/meilisearch/meilisearch/settings/rules/4253297).
Why? GitHub Merge Queue does not work with branch patterns yet, so we have to add the new created branch to the GitHub Ruleset to be able to use GitHub Merge Queue.

3. Change the [version in `Cargo.toml` file](https://github.com/meilisearch/meilisearch/blob/e9b62aacb38f2c7a777adfda55293d407e0d6254/Cargo.toml#L21). You can use [our automation](https://github.com/meilisearch/meilisearch/actions/workflows/update-cargo-toml-version.yml) -> click on `Run workflow` -> Fill the appropriate version and run it on the newly created branch `release-vX.Y.Z` -> Click on "Run workflow". A PR updating the version in the `Cargo.toml` and `Cargo.lock` files will be created.

4. Open and merge the PRs (fixing your bugs): they should point to `release-vX.Y.Z+1` branch.

5. Go to the GitHub interface, in the [`Release` section](https://github.com/meilisearch/meilisearch/releases) and click on `Draft a new release`
   ⚠️⚠️⚠️ Publish on `release-vX.Y.Z+1` branch, not on `main`!

📝 <ins>About the changelogs</s>
- Use the "Generate release notes" button in the GitHub interface to get the exhaustive list of PRs.
- Separate the PRs into different categories: Enhancement/Features, Bug fixes, Maintenance.
- Ensure each line makes sense for external people reading the changelogs. Add more details of usage if needed.
- Thank the external contributors at the end of the changelogs.

⚠️ <ins>If doing a patch release that should NOT be the `latest` release</s>:

- Do NOT check `Set as the latest release` when creating the GitHub release. If you did, quickly interrupt all CIs and delete the GitHub release!
- Once the release is created, you don't have to care about Homebrew, APT and Docker CIs: they will not consider this new release as the latest; the CIs are already adapted for this situation.
- However, the [CI updating the `latest` git tag](https://github.com/meilisearch/meilisearch/actions/workflows/latest-git-tag.yml) is not working for this situation currently and will attach the `latest` git tag to the just-created release, which is something we don't want! If you don't succeed in stopping the CI on time, don't worry, you just have to re-run the [old CI](https://github.com/meilisearch/meilisearch/actions/workflows/latest-git-tag.yml) corresponding to the real latest release, and the `latest` git tag will be attached back to the right commit.

6. Bring the new commits back from `release-vX.Y.Z+1` to `main` by merging a PR originating `release-vX.Y.Z+1` and pointing to `main`.

⚠️ If you encounter any merge conflicts, please do NOT fix the git conflicts directly on the `release-vX.Y.Z` branch. It would bring the changes present in `main` into `release-vX.Y.Z`, which would break a potential future patched release.

![GitHub interface showing merge conflicts](../assets/merge-conflicts.png)

Instead:
- Create a new branch originating `release-vX.Y.Z+1`, like `tmp-release-vX.Y.Z+1`
- Create a PR from the `tmp-release-vX.Y.Z+1` branch and pointing to `main`
- Fix the git conflicts on this new branch
    - By either fixing the git conflict via the GitHub interface
    - By pulling the `main` branch into `tmp-release-vX.Y.Z+1` and fixing them on your machine.
- Merge this new PR into `main`
