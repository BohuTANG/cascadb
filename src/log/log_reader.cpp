// Copyright (c) 2013 The CascaDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <stdio.h>

#include "util/crc.h"
#include "util/logger.h"
#include "cascadb/slice.h"
#include "tree/tree.h"
#include "serialize/block.h"
#include "sys/linux/linux_fs_directory.h"
#include "log_reader.h"

#define LOG_HEADER_SIZE (8 + CRC_SIZE + 8)
#define LOG_ENTRY_MIN_SIZE (4 + 8 + 4 + 1 + 4 + 4 + CRC_SIZE + 4)

using namespace cascadb;

LogReader::~LogReader()
{
    free(entry_buffer_);
    delete sfreader_;
}

bool LogReader::init(uint64_t min_checkpoint_lsn)
{
    ScopedMutex lock(&mtx_);
    Directory *dir = options_.log_dir;

    if (!dir) {
        LOG_ERROR("dir must be set in options");
        return false;
    }
    sfreader_ = dir->open_sequence_file_reader(filename_);
    file_size_ = dir->file_length(filename_);
    last_checkpoint_lsn_ = min_checkpoint_lsn;
    entry_buffer_ = (char*) malloc(entry_buffer_size_); // default 4KB

    return true;
}


bool LogReader::read_header()
{
    // read log header
    Slice s(entry_buffer_, LOG_HEADER_SIZE);
    if (!sfreader_->read(s)) {
        return false;
    }

    Block blk(s, 0, LOG_HEADER_SIZE);
    BlockReader reader(&blk);
    if (!reader.readUInt64(&log_init_lsn_)) return false;

    uint32_t exp_crc;
    if (!reader.readUInt32(&exp_crc)) return false;

    uint32_t act_crc = cascadb::crc32(blk.start(), 8);
    if (exp_crc != act_crc) {
        LOG_ERROR("log header crc error, " << filename_
                << ", lenght " << file_size_);
        return false;
    }

    return true;
}

bool LogReader::recovery()
{
    ScopedMutex lock(&mtx_);

    uint64_t start_location = 0UL;

    // if file is too small, skip it
    if (file_size_ <= LOG_HEADER_SIZE) return true;

    // read header
    if (!read_header()) {
        LOG_ERROR("read log header error, " << filename_
                << ", lenght " << file_size_);
        return false;
    }

    // no need to read
    if (last_checkpoint_lsn_ >= (log_init_lsn_ + file_size_)) {
        LOG_WARN("this log is outdated "
                << " , log init lsn " << log_init_lsn_
                << " , file size " << file_size_
                << " , last checkpoint lsn " << last_checkpoint_lsn_);
        return true;
    }

    if (last_checkpoint_lsn_ < log_init_lsn_) {
        start_location = LOG_HEADER_SIZE;
    } else
        start_location = (last_checkpoint_lsn_ - log_init_lsn_);

    // new
    if (start_location == 0)
        start_location = LOG_HEADER_SIZE;

    if (!sfreader_->seek(start_location))
        return false;

    Time start = now();
    LOG_WARN(" recover start "
            << filename_
            << " , log init lsn " << log_init_lsn_
            << " , last checkpoint lsn " << last_checkpoint_lsn_
            << " , log size " << file_size_
            << "  [" << file_size_ / 1024 / 1024 <<"MB]"
            << " , start location " << start_location);

    while (start_location < file_size_) {
        Slice size_s(entry_buffer_, 4);

        // *1) read log entry len
        if (!sfreader_->read(size_s)) {
            LOG_ERROR("read entry size buffer error, " << start_location);
            return false;
        }

        uint32_t entry_size = 0U;
        Block size_blk(size_s, 0, 4);
        BlockReader size_reader(&size_blk);
        if (!size_reader.readUInt32(&entry_size)) {
            LOG_ERROR("reader entry size error");
            return false;
        }

        if (entry_size< LOG_ENTRY_MIN_SIZE) return false;

        // *2) read entry buffer
        if (entry_size > entry_buffer_size_) {
            entry_buffer_size_ = entry_size;
            entry_buffer_ = (char*) realloc(entry_buffer_, entry_size);
        }

        // skip length at the beginning
        uint32_t sub_entry_size = entry_size - 4;
        Slice sub_slice(entry_buffer_, sub_entry_size);
        if (!sfreader_->read(sub_slice)) {
            LOG_ERROR("reader sub slice error");
            return false;
        }

        Slice entry_slice(entry_buffer_, sub_entry_size);
        Block entry_blk(entry_slice, 0, sub_entry_size);
        BlockReader entry_reader(&entry_blk);

        uint32_t act_xsum = 0U;
        uint32_t exp_xsum = 0U;

        // *checksum: skip the length at the end
        entry_reader.seek(sub_entry_size - CRC_SIZE - 4);
        if (!entry_reader.readUInt32(&exp_xsum)) {
            LOG_ERROR("reader xsum error");
            return false;
        }
        act_xsum = cascadb::crc32(entry_blk.start(), sub_entry_size - CRC_SIZE - 4);
        if (exp_xsum != act_xsum) {
            LOG_ERROR("log entry xsum error, " << filename_
                      << " , offset " << start_location
                      << " , reads " << reads_
                      << " , entry size " << entry_size
                      << " , exp_xsum " << exp_xsum
                      << " , act_xsum " << act_xsum);
            return false;
        }


        uint64_t entry_lsn = 0UL;
        uint32_t entry_tbn = 0U;
        uint8_t entry_type = 0U;

        // *read from the beginning
        entry_reader.seek(0);
        if (!entry_reader.readUInt64(&entry_lsn)) return false;
        if (!entry_reader.readUInt32(&entry_tbn)) return false;
        if (!entry_reader.readUInt8(&entry_type)) return false;
        switch(entry_type) {
            case Put:
                if (!recover_put(entry_reader, entry_tbn, entry_lsn)) {
                    LOG_ERROR("log insert-recovery  error, " << filename_
                              << " , offset " << start_location);
                    return false;
                }
                break;

            case Del:
                if (!recover_del(entry_reader, entry_tbn, entry_lsn)) {
                    LOG_ERROR("log insert-recovery  error, " << filename_
                              << " , offset " << start_location);
                    return false;
                }
                break;

            default:break;
        };

        start_location += entry_size;
        reads_++;
    }

    Time end = now();
    LOG_WARN(" recover end "
            << filename_
            << " , recover count " << reads_
            << " , cost time(s) " << interval_us(start, end) / 1000 / 1000);

    return true;
}

bool LogReader::recover_put(BlockReader& reader, uint32_t tbn, uint64_t lsn)
{

    TableSettings tbs;
    // maybe the index is deleted
    // so here skip insert to the tree
    if (!cache_->get_table_settings(tbn, tbs)) {
        LOG_ERROR("get table error, " << filename_
                << " , file_size " <<file_size_
                << " , tbn " << tbn);
        return true;
    }

    // some tables which no need recovery this entry
    // this means that, there was a crash between tables checkpoint.
    assert(tbs.layout);
    if (tbs.layout->checkpoint_lsn() > lsn) return true;

    Slice k, v;
    if (!reader.readSlice(k)) return false;
    if (!reader.readSlice(v)) return false;

    Tree *tree = tbs.tree;
    assert(tree);

    tree->put(k, v);

    k.destroy();
    v.destroy();

    return true;
}

bool LogReader::recover_del(BlockReader& reader, uint32_t tbn, uint64_t lsn)
{

    TableSettings tbs;
    // maybe the index is deleted
    // so here skip insert to the tree
    if (!cache_->get_table_settings(tbn, tbs)) {
        LOG_ERROR("get table error, " << filename_
                << " , file_size " <<file_size_
                << " , tbn " << tbn);
        return true;
    }

    // some tables which no need recovery this entry
    // this means that, there was a crash between tables checkpoint.
    assert(tbs.layout);
    if (tbs.layout->checkpoint_lsn() > lsn) return true;

    Slice k;
    if (!reader.readSlice(k)) return false;

    Tree *tree = tbs.tree;
    assert(tree);

    tree->del(k);

    k.destroy();

    return true;
}

bool LogReader::close_and_remove()
{
    Directory *dir = options_.log_dir;
    if (!dir) {
        LOG_ERROR("dir must be set in options");
        return false;
    }

    sfreader_->close();
    dir->delete_file(filename_);

    return true;
}
