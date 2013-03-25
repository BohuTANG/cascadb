// Copyright (c) 2013 The CascaDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef CASCADB_UTIL_BITS_H_
#define CASCADB_UTIL_BITS_H_

#define ROUND_UP(x, n)  ((((x)+ (n) - 1) / (n)) * (n))
#define ROUND_DOWN(x, n)  (((x) / (n)) * (n))

#define PAGE_SIZE 4096
#define PAGE_ROUND_UP(x) (((x) + PAGE_SIZE-1)  & (~(PAGE_SIZE-1)))
#define PAGE_ROUND_DOWN(x) ((x) & (~(PAGE_SIZE-1)))
#define PAGE_ROUNDED(x) ((x) == PAGE_ROUND_DOWN(x) )

#endif
