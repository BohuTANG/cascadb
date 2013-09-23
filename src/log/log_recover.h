// Copyright (c) 2013 The CascaDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef CASCADB_LOG_LOG_RECOVER_H_
#define CASCADB_LOG_LOG_RECOVER_H_

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <map>

#include "cache/cache.h"

namespace cascadb {
class LogRecover {
public: LogRecover(const Options& options,
               Cache *cache)
    : options_(options),
      cache_(cache)
    {
        recover_cnt_ = 0UL;
    }
    ~LogRecover();

    bool recover(uint64_t from_lsn);

    uint64_t cnt() { return recover_cnt_;}

private:
    Options options_;
    Cache *cache_;
    uint64_t recover_cnt_;
}; // class

} // namespace

#endif
