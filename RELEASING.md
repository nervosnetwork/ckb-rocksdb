# Releasing

This repository publishes two crates:

- `ckb-rocksdb` — tagged `v<version>` (for example `v0.23.0`).
- `ckb-librocksdb-sys` — tagged `ckb-librocksdb-sys-v<version>`.

Publishing runs from `.github/workflows/publish.yml` when a tag is pushed and
uses crates.io Trusted Publishing, so no long-lived registry token is stored in
the repository.

## One-time setup

1. On crates.io, open each crate's **Settings → Trusted Publishers** and add a
   GitHub publisher with:
   - repository: `nervosnetwork/ckb-rocksdb`
   - workflow: `publish.yml`
   - environment: `release`
2. In the GitHub repository settings, create an environment named `release`.
   Adding required reviewers is recommended so a tag cannot publish without
   approval.
3. After the first successful trusted publish, enable **Trusted Publishing
   Only** on crates.io and remove any old `CARGO_REGISTRY_TOKEN` secret.

Trusted Publishing cannot create a brand-new crate. Both crate names already
exist, so only new versions need this setup; a future new crate must be
published once with a token before it can be configured.

## Release checklist

1. Bump the version in `Cargo.toml`, add the release notes, and merge the
   change.
2. Make sure the target branch contains this workflow. Tag-triggered workflows
   run the copy of `publish.yml` from the tagged commit, so maintenance branches
   need a cherry-pick before their first release.
3. Pre-flight on a clean checkout:

   ```bash
   cargo package --list
   cargo publish --dry-run
   ```

4. Tag the merge commit and push the tag:

   ```bash
   git tag -a v0.23.0 -m "ckb-rocksdb 0.23.0"
   git push origin v0.23.0
   ```

5. Create a GitHub Release from the tag with the release notes. Include the
   RocksDB version in the notes when it changes.

## Publish order

`ckb-rocksdb` pins `ckb-librocksdb-sys` exactly, so publish the sys crate first:

```bash
git tag -a ckb-librocksdb-sys-v10.7.5 -m "ckb-librocksdb-sys 10.7.5"
git push origin ckb-librocksdb-sys-v10.7.5
# wait until the new version is visible on crates.io, then tag ckb-rocksdb
```

The sys crate package contains the vendored `rocksdb/` sources from the
submodule. The workflow checks for them before publishing, so the checkout must
have submodules initialized.

## If a release is broken

Published versions cannot be deleted or overwritten. Use `cargo yank` to stop
new resolutions, fix the issue, and publish a new patch version:

```bash
cargo yank --version 0.23.0 ckb-rocksdb
```
