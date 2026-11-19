# Vendored Swagger UI

Files from the npm package `swagger-ui-dist` **5.34.1** (Apache-2.0, Copyright SmartBear
Software Inc.), unmodified, embedded into the binary with `include_bytes!` by
`src/docs.rs`. Nothing is fetched at runtime.

- Source: `npm pack swagger-ui-dist@5.34.1`
- Tarball sha512 (npm integrity):
  `sha512-VSr8fXfI93obYaQicLJbLUKGRxDojj/SfRJDDLCioGTUBj4BK/Nj4eE9jCeCAnJxVLi7iS+08PbiZLyrQKBkJQ==`
- Kept: `swagger-ui-bundle.js`, `swagger-ui-standalone-preset.js`, `swagger-ui.css`,
  `favicon-32x32.png`, `LICENSE`, `NOTICE` and the two `*.js.LICENSE.txt` files that list the
  licences of the bundled libraries. Source maps and the unused bundles are not vendored.
- `swagger-initializer.js` and `index.html` are ours (`src/docs.rs`).

To upgrade: pack the new version, replace these files, update the version here, in
`THIRD_PARTY_NOTICES` and in the `bundled_version` test of `src/docs.rs`.
