// Copyright (c) 2013 The CascaDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef CASCADB_LOG_LOG_WRITER_H_
#define CASCADB_LOG_LOG_WRITER_H_

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <set>

#include "sys/sys.h"
#include "tree/msg.h"
#include "cascadb/file.h"
#include "cascadb/options.h"

namespace cascadb {

class LogWriter {
public:
    LogWriter(Options& options,
              const std::string& filename,
              const uint64_t init_lsn);

    ~LogWriter();

    bool init();
    void flush();
    void fsync();
    bool write(const MsgType& type,
            const Slice& key,
            const Slice& val,
            uint32_t tbn,
            bool issync);

    uint64_t last_lsn() const { return last_lsn_; }
    uint64_t last_fsync_lsn() const { return last_fsync_lsn_; }
    uint64_t last_written_lsn() const { return last_written_lsn_; }

    void close();
    void close_and_del_file();
    bool oversize();
    uint64_t filesize() const { return offset_ ; }
    uint64_t ref() const  { return refcnt_;}
    void inc_ref() { refcnt_++; }
    void dec_ref() { refcnt_--; assert(refcnt_ >= 0); }


private:
    // options
    Options& options_;

    // log file name
    std::string filename_;

    uint64_t init_lsn_;
    uint64_t last_lsn_;
    uint64_t offset_;
    uint64_t last_written_lsn_;
    uint64_t last_fsync_lsn_;
    uint64_t refcnt_;

    Mutex inlock_;
    Mutex outlock_;

    struct LogBuf {
        uint64_t used;
        uint64_t size;
        char *buf;
    };

    LogBuf in_;
    LogBuf out_;

    // write file
    SequenceFileWriter *sfwriter_;

    // write log buffer to disk
    void write_outbuf();

    // check write buffer
    void check_space(const uint32_t bytes_need);

    // write header at the log beginning
    bool write_header();
}; // class

} //namespace

#endif
