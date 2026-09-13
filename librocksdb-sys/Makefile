include rocksdb/src.mk

rocksdb_lib_sources.txt: rocksdb/src.mk
	@printf '%s\n' $(LIB_SOURCES) > $@

gen_lib_sources: rocksdb_lib_sources.txt

test:
	cargo build
	rm -rf tmp-test-memory-leak
	gcc -g -I rocksdb/include/ -I . \
			-L $$(find . -name "librocksdb.a" -exec dirname {} \; | head -1) \
			-o tmp-test-memory-leak \
			tests/memory/rocksdb_options_load_from_file.c \
			-lrocksdb -lpthread -ldl -lm -lstdc++ \
		&& valgrind --tool=memcheck --leak-check=full ./tmp-test-memory-leak \
	    && rm tmp-test-memory-leak
