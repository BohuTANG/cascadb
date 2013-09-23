// Copyright (c) 2013 The CascaDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <stdio.h>

#include "util/crc.h"
#include "util/logger.h"
#include "cascadb/slice.h"
#include "serialize/block.h"
#include "sys/linux/linux_fs_directory.h"
#include "log_writer.h"

using namespace std;
using namespace cascadb;

LogWriter::LogWriter(
        Options& options,
        const std::string& filename,
        const uint64_t lsn)
        : options_(options),
        filename_(filename),
        init_lsn_(lsn)
{
    offset_= 0;
    refcnt_ = 0;
    last_lsn_ = lsn;
    last_written_lsn_ = lsn;
    last_fsync_lsn_ = lsn;

    in_.used = 0;
    out_.used = 0;
}

LogWriter::~LogWriter()
{
    free(in_.buf);
    free(out_.buf);

    delete sfwriter_;
}

bool LogWriter::init()
{
    Directory *dir = options_.log_dir;
    if (!dir) {
        LOG_ERROR("dir must be set in options");
        return false;
    }

    sfwriter_ = dir->open_sequence_file_writer(filename_);
    in_.size = options_.log_bufsize_byte;
    in_.buf = (char*)malloc(in_.size);

    out_.size = options_.log_bufsize_byte;
    out_.buf = (char*)malloc(out_.size);

    if (!in_.buf || !out_.buf) {
        LOG_ERROR("log in/out buffer is NULL when malloc");
        return false;
    }

    // write log header
    write_header();

    return true;
}

bool LogWriter::write_header()
{
    uint32_t hdr_size = (8 + CRC_SIZE + 8); // last_lsn + crc + reverse
    char buffer[hdr_size];

    Block block(Slice(buffer, hdr_size), 0, 0);
    BlockWriter writer(&block);

    if (!writer.writeUInt64(init_lsn_)) return false;
    if (!writer.writeUInt32(cascadb::crc32(writer.start(), writer.pos()))) return false;
    if (!writer.writeUInt64(0UL)) return false;
    if (!sfwriter_->append(Slice(buffer, hdr_size))) return false;

    last_lsn_ += hdr_size;
    last_fsync_lsn_ += hdr_size;
    last_written_lsn_ += hdr_size;

    return true;
}

// need acquire output lock
void LogWriter::write_outbuf()
{
    if (out_.used > 0) {
        if (sfwriter_->append(Slice(out_.buf, out_.used))) {
            last_written_lsn_ = last_lsn_;
            offset_ += out_.used;
            out_.used = 0;
        } else {
            LOG_ERROR("write logfile, offset " << offset_
                    << ", filename " << filename_
                    << ", size " << out_.used
                    << " error");
        }
    }
}

void LogWriter::flush()
{
    if (last_written_lsn_ == last_lsn_) return;

    // IF: other thread is doing write-back 
    // OR : inbuffer is writing
    // do nothing
    if (outlock_.lock_try()) {
        inlock_.lock();
        LogBuf tmp = out_;
        out_ = in_;
        in_ = tmp;

        // release the inock, so other thread is writeable
        inlock_.unlock();

        write_outbuf();

        outlock_.unlock();
    }
}

void LogWriter::fsync()
{
    if (last_fsync_lsn_ == last_written_lsn_) return;

    if (!sfwriter_->flush()) {
        LOG_ERROR("sync logfile" << filename_ << " error");
        return;
    }
    last_fsync_lsn_ = last_written_lsn_;
}

void LogWriter::close()
{
    if (sfwriter_) {
        flush();
        fsync();
        sfwriter_->close();
    }
}

void LogWriter::close_and_del_file()
{
    close();

    assert(last_fsync_lsn_ == last_written_lsn_);
    options_.log_dir->delete_file(filename_);
}

void LogWriter::check_space(uint32_t bytes_need)
{
    if (in_.size > (in_.used + bytes_need)) return;

    outlock_.lock();
    LogBuf tmp = out_;
    out_ = in_;
    in_ = tmp;

    // other thread can write to swap buffer
    inlock_.unlock();

    write_outbuf();
    outlock_.unlock();

    // acquire the lock again
    inlock_.lock();

    if (bytes_need >= in_.size) {
        int new_size = bytes_need > (2 * in_.size) ? bytes_need : (2 * in_.size);
        in_.buf = (char*)realloc(in_.buf, new_size);
        in_.size = new_size;
    }
}

bool LogWriter::write(const MsgType& type,
        const Slice& key,
        const Slice& value,
        uint32_t tbn,
        bool issync)
{
    const uint32_t len  = (
            + 4                                         // len at the beginning
            + 8                                         // lsn
            + 4                                         // table number
            + 1                                         // cmd
            + 4
            + key.size()
            + 4
            + value.size()
            + CRC_SIZE                                  // crc
            + 4                                         // len at the end
            );

    inlock_.lock();
    check_space(len);

    Block blk(Slice(in_.buf + in_.used, len), 0, 0);
    BlockWriter writer(&blk);

    if (!writer.writeUInt32(len)) {
        inlock_.unlock();
        return false;
    }
    if (!writer.writeUInt64(last_lsn_)) {
        inlock_.unlock();
        return false;
    }
    if (!writer.writeUInt32(tbn)) {
        inlock_.unlock();
        return false;
    }
    if (!writer.writeUInt8(type)) {
        inlock_.unlock();
        return false;
    }

    if (!writer.writeSlice(key)) {
        inlock_.unlock();
        return false;
    }

    // deal with Del ops
    if (type == Del) {
        if (!writer.writeUInt32(0U)) {
            inlock_.unlock();
            return false;
        }
    } else {
        if (!writer.writeSlice(value)) {
            inlock_.unlock();
            return false;
        }
    }

    // not include the length at the beginning and end
    uint32_t crc = cascadb::crc32(writer.start() + 4, len - 4 - 4 - 4);
    if (!writer.writeUInt32(crc)) {
        inlock_.unlock();
        return false;
    }

    if (!writer.writeUInt32(len)) {
        inlock_.unlock();
        return false;
    }

    in_.used += len;
    last_lsn_ += len;
    inlock_.unlock();

    if (issync) {
        flush();
        fsync();
    }

    return  true;
}

bool LogWriter::oversize()
{
	//LOG_WARN("offset " << offset_ << " , filesize " << options_->log_filesize_byte);
	return offset_ > options_.log_filesize_byte;
}
