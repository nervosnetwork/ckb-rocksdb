#include <rocksdb/c.h>
#include <rocksdb/file_system.h>

#include <linux/filter.h>
#include <linux/seccomp.h>
#include <sys/prctl.h>
#include <sys/syscall.h>
#include <unistd.h>

#include <cassert>
#include <cerrno>
#include <cstddef>
#include <cstdio>
#include <cstdlib>
#include <memory>

// The Rust crate supplies this callback when linking bzip2 with BZ_NO_STDIO.
extern "C" void bz_internal_error(int code) {
  std::fprintf(stderr, "bzip2 internal error: %d\n", code);
  std::abort();
}

// Exercise the same failure an older kernel or a restricted container returns,
// without changing the host's io-uring policy.
static void deny_io_uring() {
  sock_filter instructions[] = {
      BPF_STMT(BPF_LD | BPF_W | BPF_ABS, offsetof(seccomp_data, nr)),
      BPF_JUMP(BPF_JMP | BPF_JEQ | BPF_K, __NR_io_uring_setup, 0, 1),
      BPF_STMT(BPF_RET | BPF_K, SECCOMP_RET_ERRNO | EPERM),
      BPF_STMT(BPF_RET | BPF_K, SECCOMP_RET_ALLOW),
  };
  sock_fprog filter = {4, instructions};
  assert(prctl(PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0) == 0);
  assert(prctl(PR_SET_SECCOMP, SECCOMP_MODE_FILTER, &filter) == 0);
}

int main(int argc, char**) {
  if (argc > 1) deny_io_uring();

  char* error = nullptr;
  auto* allocator = rocksdb_jemalloc_nodump_allocator_create(&error);
  if (error != nullptr) {
    std::fprintf(stderr, "%s\n", error);
    rocksdb_free(error);
    return 1;
  }
  assert(allocator != nullptr);
  rocksdb_memory_allocator_destroy(allocator);

  char path[] = "/tmp/rocksdb-native-features-XXXXXX";
  int fd = mkstemp(path);
  assert(fd >= 0);
  const char data[] = "first----second";
  assert(write(fd, data, sizeof(data)) == sizeof(data));
  assert(close(fd) == 0);

  std::unique_ptr<rocksdb::FSRandomAccessFile> file;
  assert(rocksdb::FileSystem::Default()
             ->NewRandomAccessFile(path, rocksdb::FileOptions(), &file, nullptr)
             .ok());
  assert(unlink(path) == 0);

  char first[5], second[6];
  rocksdb::FSReadRequest requests[2];
  requests[0].offset = 0;
  requests[0].len = sizeof(first);
  requests[0].scratch = first;
  requests[1].offset = 9;
  requests[1].len = sizeof(second);
  requests[1].scratch = second;
  assert(file->MultiRead(requests, 2, rocksdb::IOOptions(), nullptr).ok());
  assert(requests[0].status.ok() && requests[0].result == "first");
  assert(requests[1].status.ok() && requests[1].result == "second");
  std::puts("jemalloc and MultiRead passed");
}
