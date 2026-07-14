import { defineConfig, markdown, openapi } from "sourcey";

export default defineConfig({
  name: "Meilisearch",
  description: "Lightning-fast, ultra-relevant search engine (Sourcey-generated docs, second ecosystem proof for runx bounty #46).",
  siteUrl: "https://example.org",
  theme: {
    colors: {
      primary: "#ff5c5c",
      light: "#ff7575",
      dark: "#cc0000",
    },
  },
  navigation: {
    tabs: [
      {
        tab: "Getting Started",
        source: markdown({
          groups: [
            {
              group: "Overview",
              pages: ["introduction", "quickstart", "concepts"],
            },
            {
              group: "Reference",
              pages: ["adapter-notes"],
            },
          ],
        }),
      },
      {
        tab: "API Reference",
        slug: "api",
        source: openapi("/tmp/meili_openapi.json"),
      },
    ],
  },
});
