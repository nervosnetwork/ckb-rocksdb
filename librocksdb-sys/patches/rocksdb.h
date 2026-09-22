#include <stdbool.h>

#include "../rocksdb/include/rocksdb/c.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct rocksdb_column_family_descriptor_t   rocksdb_column_family_descriptor_t;
typedef struct rocksdb_column_family_descriptors_t  rocksdb_column_family_descriptors_t;
typedef struct {
    rocksdb_options_t* db_opts;
    rocksdb_column_family_descriptors_t* cf_descs;
} rocksdb_fulloptions_t;

extern ROCKSDB_LIBRARY_API
    rocksdb_cache_t* rocksdb_null_cache();

// Return NULL on allocation or construction failure; no C++ exception escapes.
extern ROCKSDB_LIBRARY_API
    rocksdb_env_t* rocksdb_create_default_env_checked();

extern ROCKSDB_LIBRARY_API
    rocksdb_cache_t* rocksdb_cache_create_lru_checked(size_t capacity);

extern ROCKSDB_LIBRARY_API
    rocksdb_options_t* rocksdb_options_clone(rocksdb_options_t* options);

extern ROCKSDB_LIBRARY_API
    rocksdb_column_family_descriptors_t* rocksdb_column_family_descriptors_create();

extern ROCKSDB_LIBRARY_API
    void rocksdb_column_family_descriptors_destroy(rocksdb_column_family_descriptors_t* cf_descs);

extern ROCKSDB_LIBRARY_API
    int rocksdb_column_family_descriptors_count(const rocksdb_column_family_descriptors_t* cf_descs);

extern ROCKSDB_LIBRARY_API
    char* rocksdb_column_family_descriptors_name(const rocksdb_column_family_descriptors_t* cf_descs, int index);

extern ROCKSDB_LIBRARY_API
    rocksdb_options_t* rocksdb_column_family_descriptors_options(const rocksdb_column_family_descriptors_t* cf_descs, int index);

// On failure both payload pointers are NULL, even if allocating *errptr fails.
// A successful load leaves an existing *errptr unchanged, as required by c.h.
extern ROCKSDB_LIBRARY_API
    rocksdb_fulloptions_t rocksdb_options_load_from_file(
        const char* config_file,
        rocksdb_env_t* env,
        bool ignore_unknown_options,
        rocksdb_cache_t* cache,
        char** errptr);

#ifdef __cplusplus
}  /* end extern "C" */
#endif
