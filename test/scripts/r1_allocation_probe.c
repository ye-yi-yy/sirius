/*
 * Copyright 2026, Sirius Contributors.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * See the LICENSE file at the repo root for the full text.
 *
 * Linux/glibc-only, opt-in T6 measurement helper. Build as a shared library and
 * preload ONLY for allocation measurements, never latency or ordinary tests.
 * Allocations are attributed to R1 by their exported capture/registry stack
 * frames. Free/realloc are tracked even after a measurement window ends, so
 * the peak is simultaneous live storage, not cumulative allocation.
 */
#define _GNU_SOURCE
#include <dlfcn.h>
#include <execinfo.h>
#include <malloc.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>

extern void* __libc_malloc(size_t);
extern void* __libc_calloc(size_t, size_t);
extern void* __libc_realloc(void*, size_t);
extern void __libc_free(void*);

#define SLOT_COUNT (1u << 20)
typedef struct {
  void* pointer;
  size_t bytes;
  int r1;
} allocation;
typedef struct {
  uint64_t live, peak, cumulative, allocations;
  uint64_t all_live, all_peak, all_cumulative;
  uint64_t overflow;
} measurement;

static allocation* slots;
static measurement stats;
static pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;
static _Atomic int enabled;
static _Thread_local int inside;

// Repeated allocations visit the same instruction addresses. Cache symbol classification per
// thread so dladdr's symbol-table walk does not dominate the measurement itself.
typedef struct {
  void* address;
  int r1;
} frame_classification;
static _Thread_local frame_classification frame_cache[4096];

static int r1_frame(void* address)
{
  size_t index                 = ((uintptr_t)address ^ ((uintptr_t)address >> 17)) & 4095;
  frame_classification* cached = NULL;
  for (size_t probes = 0; probes < 4096; ++probes) {
    frame_classification* entry = &frame_cache[index];
    if (entry->address == address) return entry->r1;
    if (!entry->address) {
      cached = entry;
      break;
    }
    index = (index + 1) & 4095;
  }
  Dl_info info;
  int r1 = 0;
  if (dladdr(address, &info) && info.dli_sname) {
    char const* symbol = info.dli_sname;
    r1 = strstr(symbol, "capture_bound_read") || strstr(symbol, "make_bound_read_identity") ||
         strstr(symbol, "read_view_registry") || strstr(symbol, "compare_read_views") ||
         strstr(symbol, "share_equal_read_view") || strstr(symbol, "allocate_scan_contract") ||
         strstr(symbol, "make_read_view_evidence_index");
  }
  if (cached) *cached = (frame_classification){address, r1};
  return r1;
}

static int r1_stack(void)
{
  void* frames[64];
  int count = backtrace(frames, 64);
  for (int i = 2; i < count; ++i) {
    if (r1_frame(frames[i])) return 1;
  }
  return 0;
}

static size_t index_for(void* pointer)
{
  return (((uintptr_t)pointer >> 4) * UINT64_C(11400714819323198485)) & (SLOT_COUNT - 1);
}

// The caller holds mutex. Returning the old record preserves attribution across realloc.
static allocation forget_locked(void* pointer)
{
  if (!slots || !pointer) return (allocation){0};
  size_t index = index_for(pointer);
  for (size_t probes = 0; probes < SLOT_COUNT; ++probes) {
    allocation* slot = &slots[index];
    if (!slot->pointer) break;
    if (slot->pointer == pointer) {
      allocation previous = *slot;
      stats.all_live -= slot->bytes;
      if (slot->r1) stats.live -= slot->bytes;
      slot->pointer = (void*)1;
      return previous;
    }
    index = (index + 1) & (SLOT_COUNT - 1);
  }
  return (allocation){0};
}

static void forget(void* pointer)
{
  pthread_mutex_lock(&mutex);
  forget_locked(pointer);
  pthread_mutex_unlock(&mutex);
}

// Lifecycle updates always maintain live bytes; only an active window records activity.
static void remember_locked(void* pointer, int r1, int measure)
{
  size_t bytes = malloc_usable_size(pointer);
  size_t index = index_for(pointer);
  size_t probes;
  for (probes = 0; probes < SLOT_COUNT; ++probes) {
    allocation* slot = &slots[index];
    if (!slot->pointer || slot->pointer == (void*)1) {
      *slot = (allocation){pointer, bytes, r1};
      stats.all_live += bytes;
      if (r1) stats.live += bytes;
      if (measure) {
        stats.all_cumulative += bytes;
        if (stats.all_live > stats.all_peak) stats.all_peak = stats.all_live;
        if (r1) {
          stats.cumulative += bytes;
          ++stats.allocations;
          if (stats.live > stats.peak) stats.peak = stats.live;
        }
      }
      break;
    }
    index = (index + 1) & (SLOT_COUNT - 1);
  }
  if (probes == SLOT_COUNT) ++stats.overflow;
}

static void remember(void* pointer)
{
  if (!pointer || !atomic_load(&enabled)) return;
  int r1 = r1_stack();
  pthread_mutex_lock(&mutex);
  remember_locked(pointer, r1, 1);
  pthread_mutex_unlock(&mutex);
}

void* malloc(size_t size)
{
  void* result = __libc_malloc(size);
  if (!inside) {
    inside = 1;
    remember(result);
    inside = 0;
  }
  return result;
}

void* calloc(size_t count, size_t size)
{
  void* result = __libc_calloc(count, size);
  if (!inside) {
    inside = 1;
    remember(result);
    inside = 0;
  }
  return result;
}

void free(void* pointer)
{
  if (!inside) {
    inside = 1;
    forget(pointer);
    inside = 0;
  }
  __libc_free(pointer);
}

void* realloc(void* pointer, size_t size)
{
  if (inside) return __libc_realloc(pointer, size);
  inside      = 1;
  int measure = atomic_load(&enabled);
  int r1      = measure ? r1_stack() : 0;
  // Serialize pointer migration with other tracking updates: realloc may release the old
  // address for another thread to reuse before the replacement record is installed.
  pthread_mutex_lock(&mutex);
  void* result = __libc_realloc(pointer, size);
  if (result || size == 0) {
    allocation previous = forget_locked(pointer);
    // Existing records survive closed windows and keep their original attribution.
    // Failed realloc leaves the original block and its record intact.
    if (result && (previous.pointer || measure))
      remember_locked(result, previous.pointer ? previous.r1 : r1, measure);
  }
  pthread_mutex_unlock(&mutex);
  inside = 0;
  return result;
}

int sirius_t6_allocation_begin(void)
{
  inside = 1;
  if (!slots) {
    slots = mmap(NULL,
                 SLOT_COUNT * sizeof(allocation),
                 PROT_READ | PROT_WRITE,
                 MAP_PRIVATE | MAP_ANONYMOUS,
                 -1,
                 0);
    if (slots == MAP_FAILED) {
      slots  = NULL;
      inside = 0;
      return 0;
    }
    void* warmup[8];
    (void)backtrace(warmup, 8);
  }
  pthread_mutex_lock(&mutex);
  stats.peak       = stats.live;
  stats.all_peak   = stats.all_live;
  stats.cumulative = stats.all_cumulative = stats.allocations = 0;
  pthread_mutex_unlock(&mutex);
  atomic_store(&enabled, 1);
  inside = 0;
  return 1;
}

measurement sirius_t6_allocation_end(void)
{
  atomic_store(&enabled, 0);
  pthread_mutex_lock(&mutex);
  measurement result = stats;
  pthread_mutex_unlock(&mutex);
  return result;
}
