// Copyright (c) 2013 The CascaDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef CASCADB_UTIL_LOG_READER_H_
#define CASCADB_UTIL_LOG_READER_H_

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <set>
#include <deque>

#include "sys/sys.h"
#include "cascadb/file.h"
#include "cascadb/options.h"
#include "cache/cache.h"

namespace cascadb {

class LogReader {
public:
    LogReader(const std::string& filename,
              const Options& options,
              Cache *cache)
      : filename_(filename),
        options_(options),
        cache_(cache)
    {
        file_size_ = 0;
        reads_ = 0;
        entry_buffer_size_ = 4096;
        last_checkpoint_lsn_ = 0UL;
        log_init_lsn_ = 0UL;
        reader_start_location_ = 0UL;
    }
    ~LogReader();

    bool init(uint64_t min_checkpoint_lsn);
    bool recovery();
    bool close_and_remove();

    uint64_t reads() const { return reads_; }

private:
    Mutex mtx_;
    uint32_t file_size_;
    uint64_t reads_;
    uint64_t last_checkpoint_lsn_;
    uint64_t log_init_lsn_;
    uint64_t reader_start_location_;
    uint32_t entry_buffer_size_;
    char *entry_buffer_;

    // log file name
    std::string filename_;
    Options options_;
    Cache *cache_;

    // read file
    SequenceFileReader *sfreader_;

    bool recover_put(BlockReader& reader, uint32_t tbn, uint64_t lsn);
    bool recover_del(BlockReader& reader, uint32_t tbn, uint64_t lsn);
    bool read_header();
}; // class

} // namespace cascadb

#endif
