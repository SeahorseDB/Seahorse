#include <cstddef>
#include <memory>
#include <iostream>

#include "vdb/common/memory_allocator.hh"
#include "vdb/common/system_log.hh"

#ifdef __cplusplus
extern "C" {
#include "zmalloc.h"
#include "server.h"
}
#endif

namespace vdb {

BaseAllocator base_allocator;
ArrowMemoryPool arrow_pool;

std::atomic_uint64_t vdb_allocated_size = 0;

uint64_t GetVdbAllocatedSize() { return vdb_allocated_size.load(); }

/* if allocated size is managed by higher layer, don't need to allocate memory
 * with prefix(size) */
void *BaseAllocator::AllocateNoPrefixInternal(uint64_t n, const char *file,
                                              const int line) {
  auto ptr = malloc(n);
  if (ptr != nullptr) {
    zmalloc_increase_used_memory(n);
    vdb_allocated_size += n;
  } else {
    OomHandler(n);
  }
  SYSTEM_LOG_WITH_PATH(
      LogLevel::kLogDebug, file, line,
      "AllocateNoPrefix Done: ptr=%p size=%lld (redis=%ld, vdb=%lld)", ptr, n,
      zmalloc_used_memory(), GetVdbAllocatedSize());
  return ptr;
}

void BaseAllocator::DeallocateNoPrefixInternal(void *p, uint64_t n,
                                               const char *file,
                                               const int line) {
  zmalloc_decrease_used_memory(n);
  vdb_allocated_size -= n;
  free(p);
  SYSTEM_LOG_WITH_PATH(
      LogLevel::kLogDebug, file, line,
      "DeallocateNoPrefix Done: ptr=%p size=%lld (redis=%ld, vdb=%lld)", p, n,
      zmalloc_used_memory(), GetVdbAllocatedSize());
}

/* length of allocated size is stored in prefix portion */
void *BaseAllocator::AllocateWithPrefixInternal(uint64_t n, const char *file,
                                                const int line) {
  auto ptr = zmalloc(n);
  if (ptr != nullptr) {
    vdb_allocated_size += zmalloc_size(ptr);
  } else {
    OomHandler(n);
  }
  SYSTEM_LOG_WITH_PATH(
      LogLevel::kLogDebug, file, line,
      "AllocateWithPrefix Done: ptr=%p size=%ld (redis=%ld, vdb=%lld)", ptr,
      zmalloc_size(ptr), zmalloc_used_memory(), GetVdbAllocatedSize());
  return ptr;
}

void BaseAllocator::DeallocateWithPrefixInternal(void *p, const char *file,
                                                 const int line) {
  size_t chunk_size = zmalloc_size(p);
  vdb_allocated_size -= chunk_size;
  zfree(p);
  SYSTEM_LOG_WITH_PATH(
      LogLevel::kLogDebug, file, line,
      "DeallocateWithPrefix Done: ptr=%p size=%ld (redis=%ld, vdb=%lld)", p,
      chunk_size, zmalloc_used_memory(), GetVdbAllocatedSize());
}

void *BaseAllocator::AllocateAlignedInternal(uint64_t alignment, uint64_t n,
                                             const char *file, const int line) {
  void *ptr = nullptr;
  if (posix_memalign(&ptr, alignment, n) == 0) {
    zmalloc_increase_used_memory(n);
    vdb_allocated_size += n;
  } else {
    OomHandler(n);
    ptr = nullptr;
  }
  SYSTEM_LOG_WITH_PATH(LogLevel::kLogDebug, file, line,
                       "AllocateAligned Done: ptr=%p align=%llu, size=%llu "
                       "(redis=%ld, vdb=%lld)",
                       ptr, alignment, n, zmalloc_used_memory(),
                       GetVdbAllocatedSize());
  return ptr;
}

void BaseAllocator::DeallocateAlignedInternal(void *p, uint64_t n,
                                              const char *file,
                                              const int line) {
  zmalloc_decrease_used_memory(n);
  vdb_allocated_size -= n;
  free(p);
  SYSTEM_LOG_WITH_PATH(
      LogLevel::kLogDebug, file, line,
      "DeallocateAligned Done: ptr=%p size=%lld (redis=%ld, vdb=%lld)", p, n,
      zmalloc_used_memory(), GetVdbAllocatedSize());
}

void BaseAllocator::OomHandler(uint64_t size) {
  /* TODO system log */
  SYSTEM_LOG(LogLevel::kLogAlways,
             "VdbBaseAllocator: Out of memory trying to allocate %llu bytes",
             size);
  abort();
}
ArrowMemoryPool::ArrowMemoryPool() : pool_{arrow::default_memory_pool()} {}

arrow::Status ArrowMemoryPool::Allocate(int64_t size, int64_t alignment,
                                        uint8_t **out) {
  arrow::Status s = pool_->Allocate(size, alignment, out);
  if (s.ok()) {
    zmalloc_increase_used_memory(size);
    vdb_allocated_size += size;
  }
  SYSTEM_LOG(
      LogLevel::kLogDebug,
      "ArrowMemoryPool(Allocate) align=%lld size=%lld (redis=%ld, vdb=%lld)",
      alignment, size, zmalloc_used_memory(), GetVdbAllocatedSize());
  return s;
}

arrow::Status ArrowMemoryPool::Reallocate(int64_t old_size, int64_t new_size,
                                          int64_t alignment, uint8_t **ptr) {
  arrow::Status s = pool_->Reallocate(old_size, new_size, alignment, ptr);
  if (s.ok()) {
    if (new_size > old_size) {
      size_t size_change = static_cast<size_t>(new_size - old_size);
      zmalloc_increase_used_memory(size_change);
      vdb_allocated_size += size_change;
    } else {
      size_t size_change = static_cast<size_t>(old_size - new_size);
      zmalloc_decrease_used_memory(size_change);
      vdb_allocated_size -= size_change;
    }
  }
  SYSTEM_LOG(LogLevel::kLogDebug,
             "ArrowMemoryPool(Reallocate) align=%lld size=(%lld -> %lld) "
             "(redis=%ld, vdb=%lld)",
             alignment, old_size, new_size, zmalloc_used_memory(),
             GetVdbAllocatedSize());
  return s;
}

void ArrowMemoryPool::Free(uint8_t *buffer, int64_t size, int64_t alignment) {
  pool_->Free(buffer, size, alignment);
  zmalloc_decrease_used_memory(size);
  vdb_allocated_size -= size;
  SYSTEM_LOG(LogLevel::kLogDebug,
             "ArrowMemoryPool(Free) align=%lld size=%lld (redis=%ld, vdb=%lld)",
             alignment, size, zmalloc_used_memory(), GetVdbAllocatedSize());
}

int64_t ArrowMemoryPool::bytes_allocated() const {
  int64_t nb_bytes = pool_->bytes_allocated();
  return nb_bytes;
}

int64_t ArrowMemoryPool::max_memory() const {
  int64_t mem = pool_->max_memory();
  return mem;
}

int64_t ArrowMemoryPool::total_bytes_allocated() const {
  int64_t mem = pool_->total_bytes_allocated();
  return mem;
}

int64_t ArrowMemoryPool::num_allocations() const {
  int64_t mem = pool_->num_allocations();
  return mem;
}

std::string ArrowMemoryPool::backend_name() const {
  return pool_->backend_name();
}
}  // namespace vdb
