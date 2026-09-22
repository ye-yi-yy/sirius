#!/usr/bin/env python3
# Copyright 2026, Sirius Contributors.
# Licensed under the Apache License, Version 2.0 (the "License");
# See the LICENSE file at the repo root for the full text.
"""CPU-only Linux/glibc regression test for the actual LD_PRELOAD T6 probe.

Run from the repository root: pixi run -e default python test/scripts/test_r1_allocation_probe.py
"""
import os
from pathlib import Path
import subprocess
import tempfile
import unittest

DRIVER = r"""
#define _GNU_SOURCE
#include <assert.h>
#include <dlfcn.h>
#include <malloc.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

typedef struct {
  uint64_t live, peak, cumulative, allocations;
  uint64_t all_live, all_peak, all_cumulative;
  uint64_t overflow;
} measurement;

__attribute__((noinline, visibility("default")))
void* capture_bound_read_probe(size_t bytes) { return malloc(bytes); }

__attribute__((noinline, visibility("default")))
void* capture_bound_read_resize_probe(void* pointer, size_t bytes) {
  return realloc(pointer, bytes);
}

int main(int argc, char** argv) {
  assert(argc == 2);
  int (*begin)(void) = dlsym(RTLD_DEFAULT, "sirius_t6_allocation_begin");
  measurement (*end)(void) = dlsym(RTLD_DEFAULT, "sirius_t6_allocation_end");
  assert(begin && end);
  assert(begin());
  int unowned = strcmp(argv[1], "attribution_plain") == 0;
  void* p = unowned ? malloc(1024) : capture_bound_read_probe(1024);
  assert(p);
  size_t bytes = malloc_usable_size(p);
  measurement first = end();
  assert(first.live == (unowned ? 0 : bytes));
  assert(first.all_live == bytes);
  assert(first.overflow == 0);
  int during = strncmp(argv[1], "attribution_", 12) == 0;
  if (during) assert(begin());

  if (strcmp(argv[1], "failure") == 0) {
    volatile size_t impossible = SIZE_MAX;
    void* failed = realloc(p, impossible);
    assert(!failed);
  } else if (strcmp(argv[1], "zero") == 0) {
    assert(realloc(p, 0) == NULL);  /* Linux/glibc frees on size zero. */
    p = NULL;
  } else if (strcmp(argv[1], "free") == 0) {
    free(p);
    p = NULL;
  } else {
    size_t size = strcmp(argv[1], "shrink") == 0 ? 512 : 4096;
    int moved = strcmp(argv[1], "moved") == 0;
    if (moved) size = 4 * 1024 * 1024;
    uintptr_t old_address = (uintptr_t)p;
    void* next = unowned ? capture_bound_read_resize_probe(p, size) : realloc(p, size);
    assert(next);
    if (moved) assert((uintptr_t)next != old_address);
    if (size == 512) assert((uintptr_t)next == old_address);
    p = next;
  }
  bytes = p ? malloc_usable_size(p) : 0;
  if (!during) assert(begin());
  measurement second = end();
  fprintf(stderr, "%s: live=%llu all_live=%llu usable=%zu\n", argv[1],
          (unsigned long long)second.live, (unsigned long long)second.all_live, bytes);
  assert(second.live == (unowned ? 0 : bytes));
  assert(second.all_live == bytes);
  assert(second.peak >= second.live && second.all_peak >= bytes);
  assert(second.overflow == 0);
  if (!during) {
    assert(second.peak == second.live && second.all_peak == bytes);
    assert(second.cumulative == 0 && second.all_cumulative == 0 && second.allocations == 0);
  }
  free(p);  /* Outside the window, the migrated record must still be removed. */
  /* A new, previously untracked allocation outside a window must stay untracked,
     including realloc(NULL, n) and growth of that allocation. */
  p = realloc(NULL, 256);
  assert(p);
  void* next = realloc(p, 8192);
  assert(next);
  p = next;
  assert(begin());
  measurement last = end();
  assert(last.live == 0 && last.all_live == 0);
  assert(last.overflow == 0);
  free(p);
  return 0;
}
"""


class AllocationProbeTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temp = tempfile.TemporaryDirectory(prefix="sirius-allocation-probe-")
        cls.addClassCleanup(cls.temp.cleanup)
        directory = Path(cls.temp.name)
        source = directory / "driver.c"
        source.write_text(DRIVER)
        cls.probe = directory / "probe.so"
        cls.driver = directory / "driver"
        probe_source = Path(__file__).with_name("r1_allocation_probe.c")
        subprocess.run(
            [
                "cc",
                "-shared",
                "-fPIC",
                "-O2",
                "-std=c11",
                str(probe_source),
                "-ldl",
                "-pthread",
                "-o",
                str(cls.probe),
            ],
            check=True,
        )
        subprocess.run(
            [
                "cc",
                "-O0",
                "-fno-builtin",
                "-rdynamic",
                str(source),
                "-ldl",
                "-o",
                str(cls.driver),
            ],
            check=True,
        )

    def test_allocation_lifetimes(self):
        for scenario in (
            "grow",
            "shrink",
            "moved",
            "failure",
            "zero",
            "free",
            "attribution_r1",
            "attribution_plain",
        ):
            with self.subTest(scenario=scenario):
                result = subprocess.run(
                    [str(self.driver), scenario],
                    env={**os.environ, "LD_PRELOAD": str(self.probe)},
                    capture_output=True,
                    text=True,
                    timeout=20,
                    check=False,
                )
                self.assertEqual(result.returncode, 0, result.stdout + result.stderr)


if __name__ == "__main__":
    unittest.main()
