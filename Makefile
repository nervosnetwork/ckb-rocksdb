.PHONY: clippy
clippy:
	cargo clippy -p ckb-rocksdb --tests -- -A clippy::upper-case-acronyms -A clippy::missing_safety_doc -A clippy::redundant-static-lifetimes -D warnings
