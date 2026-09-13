SHELL := /bin/bash

include rocksdb/src.mk

rocksdb_lib_sources.txt: rocksdb/src.mk
	@printf '%s\n' $(LIB_SOURCES) > $@

gen_lib_sources: rocksdb_lib_sources.txt

.PHONY: gen_lib_sources test
test:
	@set -euo pipefail; \
	output=$$(cargo build -p ckb-librocksdb-sys --locked --message-format=json | \
		jq -r 'select(.reason == "build-script-executed" and (.package_id | contains("ckb-librocksdb-sys"))) | .out_dir'); \
	test -n "$$output"; \
	trap 'rm -f "$$output/memory-test"' EXIT; \
	$(CC) -g -I . tests/memory/rocksdb_options_load_from_file.c \
		"$$output/librocksdb.a" -lpthread -ldl -lm -lstdc++ -o "$$output/memory-test"; \
	valgrind --leak-check=full --errors-for-leak-kinds=definite,indirect \
		--error-exitcode=1 "$$output/memory-test"
