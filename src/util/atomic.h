// Copyright (c) 2013 The CascaDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef CASCADB_UTIL_ATOMIC_H_
#define CASCADB_UTIL_ATOMIC_H_

/********************************
          atomic
********************************/
template <typename T, typename U> __attribute__((always_inline))
static inline T ATOMIC_ADD(T *addr, U diff) {
        return __sync_fetch_and_add(addr, diff);
}

#endif
