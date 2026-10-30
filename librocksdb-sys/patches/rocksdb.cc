#include "patches/rocksdb.h"

#include "rocksdb/utilities/options_util.h"

using rocksdb::Cache;
using rocksdb::ColumnFamilyDescriptor;
using rocksdb::Env;
using rocksdb::Options;
using rocksdb::ReadOptions;
using rocksdb::Slice;
using rocksdb::Status;

extern "C" {
    // Copy structs from librocksdb-sys/rocksdb/db/c.cc
    struct rocksdb_cache_t {
        std::shared_ptr<Cache>  rep;
    };
    struct rocksdb_env_t {
        Env* rep;
        bool is_default;
    };
    struct rocksdb_options_t {
        Options rep;
    };
    struct rocksdb_readoptions_t {
        ReadOptions rep;
        Slice upper_bound;
        Slice lower_bound;
        Slice timestamp;
        Slice iter_start_ts;
    };

    // New structs
    struct rocksdb_column_family_descriptor_t {
        char *name;
        Options options;
    };
    struct rocksdb_column_family_descriptors_t {
        std::vector<rocksdb_column_family_descriptor_t> rep;
    };

    rocksdb_cache_t* rocksdb_null_cache() {
        rocksdb_cache_t* c = new rocksdb_cache_t;
        c->rep = nullptr;
        return c;
    }

    rocksdb_options_t* rocksdb_options_clone(rocksdb_options_t* options) {
        rocksdb_options_t* o = new rocksdb_options_t;
        o->rep = Options(options->rep);
        return o;
    }

    rocksdb_readoptions_t* rocksdb_readoptions_clone(const rocksdb_readoptions_t* options) {
        rocksdb_readoptions_t* o = new rocksdb_readoptions_t(*options);
        // ReadOptions points at the Slices stored next to it, so the copy has
        // to point at its own Slices instead of the source's.
        if (options->rep.iterate_upper_bound != nullptr) {
            o->rep.iterate_upper_bound = &o->upper_bound;
        }
        if (options->rep.iterate_lower_bound != nullptr) {
            o->rep.iterate_lower_bound = &o->lower_bound;
        }
        if (options->rep.timestamp != nullptr) {
            o->rep.timestamp = &o->timestamp;
        }
        if (options->rep.iter_start_ts != nullptr) {
            o->rep.iter_start_ts = &o->iter_start_ts;
        }
        return o;
    }

    rocksdb_column_family_descriptors_t* rocksdb_column_family_descriptors_create() {
        return new rocksdb_column_family_descriptors_t;
    }

    void rocksdb_column_family_descriptors_destroy(rocksdb_column_family_descriptors_t* cf_descs) {
        int size = static_cast<int>(cf_descs->rep.size());
        for (int i = 0; i < size; i++) {
            free(cf_descs->rep[i].name);
        }
        delete cf_descs;
    }

    int rocksdb_column_family_descriptors_count(const rocksdb_column_family_descriptors_t* cf_descs) {
        return static_cast<int>(cf_descs->rep.size());
    }

    char* rocksdb_column_family_descriptors_name(const rocksdb_column_family_descriptors_t* cf_descs, int index) {
        return cf_descs->rep[index].name;
    }

    rocksdb_options_t* rocksdb_column_family_descriptors_options(const rocksdb_column_family_descriptors_t* cf_descs, int index) {
        rocksdb_options_t* options = new rocksdb_options_t;
        options->rep = cf_descs->rep[index].options;
        return options;
    }

    rocksdb_fulloptions_t rocksdb_options_load_from_file(
        const char* config_file,
        rocksdb_env_t* env,
        bool ignore_unknown_options,
        rocksdb_cache_t* cache,
        char** errptr) {

        rocksdb_fulloptions_t full_opts;
        full_opts.db_opts = nullptr;
        full_opts.cf_descs = nullptr;

        rocksdb_options_t* db_opts = new rocksdb_options_t;
        std::vector<ColumnFamilyDescriptor> cf_descs_tmp;

        rocksdb::ConfigOptions config_opt;
        config_opt.ignore_unknown_options = ignore_unknown_options;
        config_opt.input_strings_escaped = true;
        config_opt.env = env->rep;

        Status status = rocksdb::LoadOptionsFromFile(
            config_opt,
            std::string(config_file),
            &db_opts->rep,
            &cf_descs_tmp,
            &cache->rep);
        if (status.ok()) {
            rocksdb_column_family_descriptors_t* cf_descs = new rocksdb_column_family_descriptors_t;
            full_opts.db_opts = db_opts;
            int cf_descs_tmp_size = static_cast<int>(cf_descs_tmp.size());
            for (int i = 0; i < cf_descs_tmp_size; i++) {
                rocksdb_column_family_descriptor_t cf_desc;
                cf_desc.name = strdup(cf_descs_tmp[i].name.c_str());
                cf_desc.options = Options(db_opts->rep, cf_descs_tmp[i].options);
                cf_descs->rep.push_back(cf_desc);
            }
            full_opts.cf_descs = cf_descs;
            return full_opts;
        } else {
            delete db_opts;
        }
        if (*errptr != nullptr) {
            free(*errptr);
        }
        *errptr = strdup(status.ToString().c_str());
        return full_opts;
    }
}
