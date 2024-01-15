---
name: New sprint issue
about: ⚠️ Should only be used by the engine team ⚠️
title: ''
labels: ''
assignees: ''

---

Related product team resources: [PRD]() (_internal only_)
Related product discussion:
Related spec: WIP

## Motivation

<!---Copy/paste the information in PRD or briefly detail the product motivation. Ask product team if any hesitation.-->

## Usage

<!---Link to the public part of the PRD, or to the related product discussion for experimental features-->

## TODO

<!---Feel free to adapt this list with more technical/product steps-->

- [ ] Release a prototype
- [ ] If prototype validated, merge changes into `main`
- [ ] Update the spec

### Reminders when modifying the Setting API

<!--- Special steps to remind when adding a new index setting -->

- [ ] Ensure the new setting route is at least tested by the [`test_setting_routes` macro](https://github.com/meilisearch/meilisearch/blob/5204c0b60b384cbc79621b6b2176fca086069e8e/meilisearch/tests/settings/get_settings.rs#L276)
- [ ] Ensure Analytics are fully implemented
  - [ ] `/settings/my-new-setting` configurated in the [`make_setting_routes` macro](https://github.com/meilisearch/meilisearch/blob/5204c0b60b384cbc79621b6b2176fca086069e8e/meilisearch/src/routes/indexes/settings.rs#L141-L165)
  - [ ] global `/settings` route configurated in the [`update_all` function](https://github.com/meilisearch/meilisearch/blob/5204c0b60b384cbc79621b6b2176fca086069e8e/meilisearch/src/routes/indexes/settings.rs#L655-L751)
- [ ] Ensure the dump serializing is consistent with the `/settings` route serializing, e.g., enums case can be different (`camelCase` in route and `PascalCase` in the dump)

<!---Ping the related teams. Ask for the engine manager if any hesitation-->
