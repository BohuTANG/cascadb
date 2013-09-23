#include <stdio.h>
#include "sys/sys.h"
#include "util/logger.h"
#include "sys/posix/posix_fs_directory.h"
#include "log_mgr.h"

using namespace std;
using namespace cascadb;

LogMgr::~LogMgr()
{
    if (log_flush_cron_)
        delete log_flush_cron_;
    if (log_fsync_cron_)
        delete log_fsync_cron_;
    if (log_clean_cron_)
        delete log_clean_cron_;

    // it's a normal shutdown, delete redo logs
    for(map<long long, LogWriter*>::iterator it = logs_.begin(); it != logs_.end();) {
        map<long long, LogWriter*>::iterator it_tmp = it;
        it++;

        LogWriter *lw = it_tmp->second;
        lw->close_and_del_file();
        delete lw;

        logs_.erase(it_tmp);
    }
}

uint64_t LogMgr::last_log_num()
{
    uint64_t last_num = 0UL;

    Directory *dir = options_.log_dir;
    if (!dir) {
        return last_num;
    }

    vector<string> filenames;
    if (!dir->get_files(&filenames)) return false;
    for (size_t i = 0; i < filenames.size(); i++) {
        long long log_num = 0;
        int r = sscanf(filenames[i].c_str(), "cdb%lld.redolog", &log_num);
        if (r != 1) continue;

        if ((uint64_t)log_num > last_num)
            last_num = (uint64_t)log_num;
    }

    return ++last_num;
}

bool LogMgr::change_flush_period(unsigned int period_ms)
{
    return log_flush_cron_->change_period(period_ms);
}

static void *flush_job(void *arg)
{
    LogMgr *lmgr = (LogMgr*)arg;
    lmgr->flush();

    return NULL;
}

bool LogMgr::change_fsync_period(unsigned int period_ms)
{
    return log_fsync_cron_->change_period(period_ms);
}

static void *fsync_job(void *arg)
{
    LogMgr *lmgr = (LogMgr*)arg;
    lmgr->fsync();

    return NULL;
}

static void *clean_job(void *arg)
{
    LogMgr *lmgr = (LogMgr*)arg;
    lmgr->clean();

    return NULL;
}

bool LogMgr::init()
{
    if (options_.log_dir == NULL) {
        LOG_WARN("cascadb is in no log mode, option.log_dir = NULL");
        return true;
    }

    assert(logs_.size() == 0);

    // flush cron
    log_flush_cron_ = new Cron();
    if (!log_flush_cron_->init(flush_job, this, options_.log_flush_period_ms)) {
        LOG_ERROR("log flush cron init error");
        return false;
    }
    log_flush_cron_->start();
    LOG_WARN("log flush cron is start,  flush period_ms, " << options_.log_flush_period_ms);

    // fsync cron
    log_fsync_cron_ = new Cron();
    if (!log_fsync_cron_->init(fsync_job, this, options_.log_fsync_period_ms)) {
        LOG_ERROR("log fsync cron init error");
        return false;
    }
    log_fsync_cron_->start();
    LOG_WARN("log fsync cron is start,  fsync period_ms, " << options_.log_fsync_period_ms);

    // clean cron
    log_clean_cron_ = new Cron();
    if (!log_clean_cron_->init(clean_job, this, options_.log_clean_period_ms)) {
        LOG_ERROR("log clean cron init error");
        return false;
    }
    log_clean_cron_->start();
    log_num_ = last_log_num();

    LOG_WARN("log clean cron is start,  clean period_ms, " << options_.log_clean_period_ms);
    LOG_WARN("log option filesize " << options_.log_filesize_byte <<
            " , log buffer size " << options_.log_bufsize_byte <<
            " , log num " << log_num_);

    return true;
}

void LogMgr::fsync()
{
    if (logs_.size() == 0) return;

    for(map<long long, LogWriter*>::iterator it = logs_.begin(); it != logs_.end(); it++ ) {
        LogWriter *lw = it->second;
        if (lw->last_written_lsn() > last_checkpoint_lsn_)
            lw->fsync();
    }
}

void LogMgr::flush()
{

    if (logs_.size() == 0) return;

    for(map<long long, LogWriter*>::iterator it = logs_.begin(); it != logs_.end(); it++ ) {
        LogWriter *lw = it->second;
        if (lw->last_written_lsn() > last_checkpoint_lsn_)
            lw->flush();
    }
}

void LogMgr::clean()
{
    if (logs_.size() < 2) return;

    for(map<long long, LogWriter*>::iterator it = logs_.begin(); it != logs_.end();) {
        map<long long, LogWriter*>::iterator it_tmp = it;
        it++;

        LogWriter *lw = it_tmp->second;
        if ((lw->last_written_lsn() < last_checkpoint_lsn_) && (lw->ref() == 0)) {
            LOG_WARN("delete log "
                    << it_tmp->first
                    << " , last_written lsn " << lw->last_written_lsn()
                    << " , last_checkpoint lsn " << last_checkpoint_lsn_
                    << " , log size " << logs_.size()
                    );

            lw->close_and_del_file();
            delete lw;

            logs_.erase(it_tmp);
        }
    }
}

uint64_t LogMgr::last_lsn()
{
    if (logs_.size() == 0)
        return last_checkpoint_lsn_;

    LogWriter *lw = logs_.rbegin()->second;
    assert(lw != NULL);
    return lw->last_lsn();
}

uint64_t LogMgr::last_written_lsn()
{
    if (logs_.size() == 0) return 0UL;
    LogWriter *lw = logs_.rbegin()->second;
    assert(lw != NULL);
    return lw->last_written_lsn();
}

uint64_t LogMgr::make_checkpoint_begin()
{
    if (logs_.size() == 0) return 0UL;

    flush();
    fsync();

    LogWriter *lw = logs_.rbegin()->second;
    assert(lw != NULL);
    return lw->last_fsync_lsn();
}

void LogMgr::make_checkpoint_end(uint64_t lsn)
{
    last_checkpoint_lsn_ = lsn;
}

// create a new log
// logname as :cdb000006.redolog
bool LogMgr::egglog()
{
    char logname[256];

    snprintf(logname, sizeof(logname), "cdb%06lld.redolog", log_num_);
    LogWriter *lw = new LogWriter(options_, logname, last_lsn());
    if (!lw->init()) {
        LOG_ERROR("logmgr egg log error, pls check");
        return false;
    }
    logs_[log_num_] = lw;
    log_num_++;

    LOG_WARN("log " << logname << " created");
    return true;
}

LogWriter *LogMgr::get_writer()
{
    // no log mode
    if (options_.log_dir == NULL) return NULL;

    active_lw_lock_.write_lock();
    if (logs_.size() == 0) {
        if (!egglog()) {
            assert(false);
        }
    }

    assert(logs_.size() > 0);

    LogWriter *lw = logs_.rbegin()->second;

    // thread condition:
    // if other threads reference the logwriter
    // current thread can't egg a new log!
    if (lw->oversize()) {
        if (lw->ref() == 0)
            egglog();
        lw = logs_.rbegin()->second;
    }

    lw->inc_ref();

    active_lw_lock_.unlock();

    return lw;
}

void LogMgr::enq_put(const Slice& key,
        const Slice& val,
        uint32_t tbn,
        bool issync)
{
    LogWriter *lw = get_writer();

    if (!lw) return;

    if (!lw->write(Put, key, val, tbn, issync))
        LOG_ERROR("enq put log error");

    lw->dec_ref();
}

void LogMgr::enq_del(const Slice& key,
        uint32_t tbn,
        bool issync)
{
    LogWriter *lw = get_writer();

    if (!lw) return;

    Slice val;
    if (!lw->write(Del, key, val, tbn, issync))
        LOG_ERROR("enq del log error");

    lw->dec_ref();
}
