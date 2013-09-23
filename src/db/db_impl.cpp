// Copyright (c) 2013 The CascaDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "util/logger.h"
#include "store/ram_directory.h"
#include "sys/linux/linux_fs_directory.h"
#include "log/log_recover.h"
#include "db_impl.h"

using namespace std;
using namespace cascadb;
    
#define DAT_FILE_SUFFIX "cdb"

DBImpl::~DBImpl()
{
    delete tree_;
    delete cache_;
    delete layout_;
    delete file_;

    if (lmgr_)
        delete lmgr_;
}

bool DBImpl::init()
{
    Directory *dir = options_.dir;
    if (!dir) {
        LOG_ERROR("dir must be set in options");
        return false;
    }

    string filename = name_ + "." + DAT_FILE_SUFFIX;
    size_t length = 0;
    bool create = true;
    if (dir->file_exists(filename)) {
        length = dir->file_length(filename);
        if (length > 0) {
            create = false;
        }
    }
    LOG_INFO("init db , data file length " << length << ", create " << create);

    // file
    file_ = dir->open_aio_file(filename);
    layout_ = new Layout(file_, length, options_, &status_);
    if (!layout_->init(create)) {
        LOG_ERROR("init layout error");
        return false;
    }

    // log
    lmgr_ = new LogMgr(options_);
    if (!lmgr_->init()) {
        LOG_ERROR("lmgr init error");
        return false;
    }


    // cache
    cache_ = new Cache(options_, &status_, lmgr_);
    if (!cache_->init()) {
        LOG_ERROR("init cache error");
        return false;
    }

    // tree
    tree_ = new Tree(tbn_, options_, &status_, cache_, layout_);
    if (!tree_->init()) {
        LOG_ERROR("tree init error");
        return false;
    }

    // recovery
    cache_->set_in_recovering();
    LogRecover *recvr = new LogRecover(options_, cache_);
    if (!recvr->recover(layout_->checkpoint_lsn())) {
        LOG_ERROR("log recover error");
        return false;
    }
    delete recvr;
    cache_->set_out_recovering();

    return true;
}

bool DBImpl::put(const Slice& key, const Slice& value)
{
    lmgr_->enq_put(key, value, tbn_, false);
    return tree_->put(key, value);
}

bool DBImpl::del(const Slice& key)
{
    lmgr_->enq_del(key, tbn_, false);
    return tree_->del(key);
}

bool DBImpl::get(const Slice& key, Slice& value)
{
    return tree_->get(key, value);
}

void DBImpl::flush()
{
    cache_->flush_table(tbn_);
}

void DBImpl::debug_print(std::ostream& out)
{
    cache_->debug_print(out);
}

DB* cascadb::DB::open(const std::string& name, const Options& options)
{
    DBImpl* db = new DBImpl(name, options);
    if (!db->init()) {
        delete db;
        return NULL;
    }
    return db;
}
