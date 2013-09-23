// Copyright (c) 2013 The CascaDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef CASCADB_LOG_LOG_MGR_H_
#define CASCADB_LOG_LOG_MGR_H_

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <map>

#include "cascadb/options.h"
#include "cascadb/slice.h"
#include "tree/msg.h"
#include "log_writer.h"

namespace cascadb {
class LogMgr {
public: LogMgr(const Options& options)
    : options_(options)
    {
        last_lsn_ = 0;
        last_checkpoint_lsn_ = 0;
        log_num_ = 0;
        log_flush_cron_ = NULL;
        log_fsync_cron_ = NULL;
        log_clean_cron_ = NULL;
    }

    ~LogMgr();

    bool init();
    void flush();
    void fsync();

    void enq_put(const Slice& key,
                 const Slice& val,
                 uint32_t tbn,
                 bool issync);

    void enq_del(const Slice& key,
                 uint32_t tbn,
                 bool issync);


    // the max lsn(write to cascadb log buffer) of all logs
    uint64_t last_lsn();

    // the max written(write to buffered io) lsn of all logs
    uint64_t last_written_lsn();

    // the max fsync(fsync to disk) lsn of all logs
    // this lsn is for checkpoint
    uint64_t make_checkpoint_begin();

    // update lsn after a checkpoint
    // to decide an old log is reserve or delete
    void make_checkpoint_end(uint64_t lsn);

    // the clean func
    void clean();

    uint32_t logs_cnt() { return logs_.size(); }

private:
    // max log lsn
    uint64_t last_lsn_;

    // last cp lsn
    uint64_t last_checkpoint_lsn_;

    // log sequence number
    long long log_num_;

    // a chain of logs
    std::map<long long, LogWriter*> logs_;

    Options options_;

    // active logwriter lock
    RWLock active_lw_lock_;

    // egg new log
    bool egglog();
    
    // get the active logwriter
    LogWriter *get_writer();


    Cron *log_flush_cron_;
    Cron *log_fsync_cron_;
    Cron *log_clean_cron_;

    bool change_flush_period(unsigned int period_ms);
    bool change_fsync_period(unsigned int period_ms);

    uint64_t last_log_num();

}; // class

} // namespace

#endif
