// Exercise allocation failures without letting a C++ exception cross the C API.
#include "patches/rocksdb.h"
#include <cassert>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <new>

static thread_local long fail_after = -1;
static thread_local bool allocation_failed = false;
static thread_local bool fail_error_string = false;

void* operator new(std::size_t size) {
    if (fail_after == 0) {
        fail_after = -1;
        allocation_failed = true;
        throw std::bad_alloc();
    }
    if (fail_after > 0) --fail_after;
    if (void* p = std::malloc(size ? size : 1)) return p;
    throw std::bad_alloc();
}
void* operator new[](std::size_t size) { return ::operator new(size); }
// Define nothrow overloads here too: Valgrind replaces the standard-library
// versions, which would otherwise bypass the failure injection above.
void* operator new(std::size_t size, const std::nothrow_t&) noexcept {
    try {
        return ::operator new(size);
    } catch (const std::bad_alloc&) {
        return nullptr;
    }
}
void* operator new[](std::size_t size, const std::nothrow_t& tag) noexcept {
    return ::operator new(size, tag);
}
void operator delete(void* p) noexcept { std::free(p); }
void operator delete[](void* p) noexcept { std::free(p); }
void operator delete(void* p, std::size_t) noexcept { std::free(p); }
void operator delete[](void* p, std::size_t) noexcept { std::free(p); }
void operator delete(void* p, const std::nothrow_t&) noexcept { std::free(p); }
void operator delete[](void* p, const std::nothrow_t&) noexcept { std::free(p); }

extern "C" char* strdup(const char* value) {
    if (fail_error_string) return nullptr;
    auto size = std::strlen(value) + 1;
    auto* copy = static_cast<char*>(std::malloc(size));
    if (copy) std::memcpy(copy, value, size);
    return copy;
}

extern "C" void bz_internal_error(int) { std::abort(); }

static void destroy(rocksdb_fulloptions_t options) {
    rocksdb_column_family_descriptors_destroy(options.cf_descs);
    if (options.db_opts) rocksdb_options_destroy(options.db_opts);
}

int main(int argc, char** argv) {
    assert(argc == 2);
    auto* env = rocksdb_create_default_env_checked();
    auto* cache = rocksdb_null_cache();
    assert(env && cache);
    char* error = nullptr;
    auto control = rocksdb_options_load_from_file(argv[1], env, false, cache, &error);
    assert(!error && control.db_opts && control.cf_descs);
    assert(rocksdb_column_family_descriptors_count(control.cf_descs) > 0);

    // Guard every allocation in a successful parse, including descriptor names
    // and options copies, with a normal successful load between failures.
    unsigned failures = 0;
    for (long nth = 0;; ++nth) {
        allocation_failed = false;
        fail_after = nth;
        auto result = rocksdb_options_load_from_file(argv[1], env, false, cache, &error);
        fail_after = -1;
        if (!allocation_failed) {
            assert(!error && result.db_opts && result.cf_descs);
            destroy(result);
            break;
        }
        ++failures;
        // Some native parser allocations may be handled internally. Either a
        // complete usable result or a completely empty failure is acceptable.
        if (result.db_opts) {
            assert(!error && result.cf_descs);
            int count = rocksdb_column_family_descriptors_count(result.cf_descs);
            for (int i = 0; i < count; ++i) {
                assert(rocksdb_column_family_descriptors_name(result.cf_descs, i));
            }
        } else {
            assert(!result.cf_descs);
        }
        destroy(result);
        rocksdb_free(error);
        error = nullptr;
        assert(nth < 100000);
    }
    assert(failures > 10);

    // Error-string allocation failure must not turn failure into partial success.
    fail_error_string = true;
    auto missing = rocksdb_options_load_from_file("/missing/ckb-options", env, false, cache, &error);
    assert(!missing.db_opts && !missing.cf_descs && !error);
    fail_after = 0;
    auto failed = rocksdb_options_load_from_file(argv[1], env, false, cache, &error);
    fail_after = -1;
    fail_error_string = false;
    assert(!failed.db_opts && !failed.cf_descs && !error);

    char* previous = strdup("previous error");
    error = previous;
    auto valid = rocksdb_options_load_from_file(argv[1], env, false, cache, &error);
    assert(error == previous && valid.db_opts && valid.cf_descs);
    destroy(valid);
    auto invalid = rocksdb_options_load_from_file("/missing/ckb-options", env, false, cache, &error);
    assert(error && std::strcmp(error, "previous error") != 0);
    assert(!invalid.db_opts && !invalid.cf_descs);
    rocksdb_free(error);

    fail_after = 0;
    assert(!rocksdb_options_clone(control.db_opts));
    fail_after = 0;
    assert(!rocksdb_column_family_descriptors_options(control.cf_descs, 0));
    fail_after = 0;
    assert(!rocksdb_column_family_descriptors_create());
    fail_after = 0;
    assert(!rocksdb_null_cache());
    fail_after = 0;
    assert(!rocksdb_create_default_env_checked());
    fail_after = 0;
    assert(!rocksdb_cache_create_lru_checked(1024));
    fail_after = -1;
    destroy(control);
    rocksdb_cache_destroy(cache);
    rocksdb_env_destroy(env);
    std::printf("Passed %u allocation failures, normal loading and C error ownership.\n", failures);
}
