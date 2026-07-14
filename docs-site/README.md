# docs-site/

Sourcey-generated static documentation site for Meilisearch v1.49.0.

This is a candidate deliverable for Frantic bounty #46 — a "second ecosystem" Sourcey docs proof. The site is generated from the official `meilisearch-openapi.json` (pinned to commit `17758dce3fcc21d8e2415176c29d1106e905de00`, OpenAPI 3.1.0, 69 paths, 139 operations).

## Build

```bash
cd docs-site
npm install
npx sourcey build
```

The output goes to `docs-site/dist/` and is a fully static HTML site.

## Why this exists

Frantic bounty #46 ("Publish Sourcey docs for a second ecosystem", $16) requires the docs to live on the target project's real site/repo or a credible durable home the project owns or would adopt. A personal `<handle>.github.io` page is explicitly disqualified by the bounty.

If Meilisearch maintainers want to adopt this, the natural path is to enable GitHub Pages from `/docs-site/dist` on a `gh-pages` branch (or merge this PR and add a Pages workflow under `.github/workflows/`). Until then, the docs-site/ directory is the durable, project-owned candidate home — it lives inside this repo at a known commit.

## Sourcey version

`sourcey 3.6.5` (Node 20+) — see `package.json`.
