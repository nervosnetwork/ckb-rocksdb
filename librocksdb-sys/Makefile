SHELL := /bin/bash

include rocksdb/src.mk

rocksdb_lib_sources.txt: rocksdb/src.mk
	@printf '%s\n' $(LIB_SOURCES) > $@

gen_lib_sources: rocksdb_lib_sources.txt

.PHONY: gen_lib_sources test
# The allocation test overrides operator new; Valgrind must retain its injection.
test:
	@set -euo pipefail; \
	output=$$(cargo build -p ckb-librocksdb-sys --locked --message-format=json | \
		jq -r 'select(.reason == "build-script-executed" and (.package_id | contains("ckb-librocksdb-sys"))) | .out_dir'); \
	test -n "$$output"; \
	trap 'rm -f "$$output/memory-test" "$$output/allocation-test"' EXIT; \
	$(CC) -g -I . tests/memory/rocksdb_options_load_from_file.c \
		"$$output/librocksdb.a" -lpthread -ldl -lm -lstdc++ -o "$$output/memory-test"; \
	valgrind --leak-check=full --errors-for-leak-kinds=definite,indirect \
		--error-exitcode=1 "$$output/memory-test"; \
	$(CXX) -g -std=c++17 -I . tests/memory/options_allocation_failure.cc \
		"$$output/librocksdb.a" -lpthread -ldl -lm -lstdc++ -o "$$output/allocation-test"; \
	valgrind --soname-synonyms=somalloc=nouserintercepts \
		--leak-check=full --errors-for-leak-kinds=definite,indirect \
		--error-exitcode=1 "$$output/allocation-test" tests/memory/OPTIONS
