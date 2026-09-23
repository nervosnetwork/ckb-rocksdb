#include "patches/rocksdb.h"
#include <stdio.h>

int main(void) {
    rocksdb_env_t* env = rocksdb_create_default_env();
    rocksdb_cache_t* cache = rocksdb_cache_create_lru(1000);
    char* error = NULL;
    rocksdb_fulloptions_t options = rocksdb_options_load_from_file(
        "tests/memory/OPTIONS", env, false, cache, &error);

    int result = 0;
    if (error != NULL) {
        fprintf(stderr, "%s\n", error);
        rocksdb_free(error);
        result = 1;
    } else {
        rocksdb_column_family_descriptors_destroy(options.cf_descs);
        rocksdb_options_destroy(options.db_opts);
    }
    rocksdb_cache_destroy(cache);
    rocksdb_env_destroy(env);
    return result;
}
