#include "patches/rocksdb.h"

#include <climits>
#include <memory>
#include <new>
#include <string>
#include "rocksdb/utilities/options_util.h"

using rocksdb::Cache;
using rocksdb::ColumnFamilyDescriptor;
using rocksdb::Env;
using rocksdb::Options;
using rocksdb::Status;

extern "C" {
    // Match the opaque C API wrappers in rocksdb/db/c.cc.
    struct rocksdb_cache_t {
        std::shared_ptr<Cache> rep;
    };
    struct rocksdb_env_t {
        Env* rep;
        bool is_default;
    };
    struct rocksdb_options_t {
        Options rep;
    };

    struct rocksdb_column_family_descriptor_t {
        std::string name;
        Options options;
    };
    struct rocksdb_column_family_descriptors_t {
        std::vector<rocksdb_column_family_descriptor_t> rep;
    };

    rocksdb_env_t* rocksdb_create_default_env_checked() {
        try {
            return new rocksdb_env_t{Env::Default(), true};
        } catch (...) {
            return nullptr;
        }
    }

    rocksdb_cache_t* rocksdb_cache_create_lru_checked(size_t capacity) {
        try {
            return new rocksdb_cache_t{rocksdb::NewLRUCache(capacity)};
        } catch (...) {
            return nullptr;
        }
    }

    rocksdb_cache_t* rocksdb_null_cache() {
        return new (std::nothrow) rocksdb_cache_t;
    }

    rocksdb_options_t* rocksdb_options_clone(rocksdb_options_t* options) {
        try {
            return new rocksdb_options_t{options->rep};
        } catch (...) {
            return nullptr;
        }
    }

    rocksdb_column_family_descriptors_t* rocksdb_column_family_descriptors_create() {
        return new (std::nothrow) rocksdb_column_family_descriptors_t;
    }

    void rocksdb_column_family_descriptors_destroy(rocksdb_column_family_descriptors_t* cf_descs) {
        delete cf_descs;
    }

    int rocksdb_column_family_descriptors_count(const rocksdb_column_family_descriptors_t* cf_descs) {
        return static_cast<int>(cf_descs->rep.size());
    }

    char* rocksdb_column_family_descriptors_name(const rocksdb_column_family_descriptors_t* cf_descs, int index) {
        // Borrowed until the descriptor collection is destroyed, as before.
        return const_cast<char*>(cf_descs->rep[index].name.c_str());
    }

    rocksdb_options_t* rocksdb_column_family_descriptors_options(const rocksdb_column_family_descriptors_t* cf_descs, int index) {
        try {
            return new rocksdb_options_t{cf_descs->rep[index].options};
        } catch (...) {
            return nullptr;
        }
    }

    static void SaveLoadError(char** errptr, const char* message) {
        // The C API permits replacing a previously allocated error. Allocation
        // failure may leave this null; the null result payload still means failure.
        free(*errptr);
        *errptr = strdup(message);
    }

    rocksdb_fulloptions_t rocksdb_options_load_from_file(
        const char* config_file,
        rocksdb_env_t* env,
        bool ignore_unknown_options,
        rocksdb_cache_t* cache,
        char** errptr) {
        try {
            auto db_opts = std::make_unique<rocksdb_options_t>();
            std::vector<ColumnFamilyDescriptor> loaded;
            rocksdb::ConfigOptions config;
            config.ignore_unknown_options = ignore_unknown_options;
            config.input_strings_escaped = true;
            config.env = env->rep;
            Status status = rocksdb::LoadOptionsFromFile(
                config, std::string(config_file), &db_opts->rep, &loaded, &cache->rep);
            if (!status.ok()) {
                SaveLoadError(errptr, status.ToString().c_str());
                return {};
            }
            if (loaded.size() > INT_MAX) {
                SaveLoadError(errptr, "Too many RocksDB column families.");
                return {};
            }
            auto cf_descs = std::make_unique<rocksdb_column_family_descriptors_t>();
            cf_descs->rep.reserve(loaded.size());
            for (const auto& cf : loaded) {
                cf_descs->rep.push_back({cf.name, Options(db_opts->rep, cf.options)});
            }
            return {db_opts.release(), cf_descs.release()};
        } catch (...) {
            // Error reporting must not construct another throwing C++ object.
            SaveLoadError(errptr, "Could not allocate or construct RocksDB options.");
            return {};
        }
    }
}
