#include "sys/sys.h"
#include "util/logger.h"
#include "log_reader.h"
#include "store/ram_directory.h"
#include "sys/posix/posix_fs_directory.h"
#include "log_recover.h"

using namespace std;
using namespace cascadb;

LogRecover::~LogRecover()
{}

bool LogRecover::recover(uint64_t from_lsn)
{
    Directory *dir = options_.log_dir;
    if (!dir) {
        return true;
    }

    // open all logreaders order by lognum
    map<long long, LogReader*> readers;
    vector<string> filenames;
    if (!dir->get_files(&filenames)) return false;
    for (size_t i = 0; i < filenames.size(); i++) {
        long long log_num = 0;
        int r = sscanf(filenames[i].c_str(), "cdb%lld.redolog", &log_num);

        if (r != 1) continue;

        LogReader *reader = new LogReader(filenames[i], options_, cache_);
        if (!reader->init(from_lsn)) {
            LOG_ERROR("log reader init error, " << filenames[i]);
            return false;
        }
        readers[log_num] = reader;
    }

    // do recovery
    for(map<long long, LogReader*>::iterator it = readers.begin(); it != readers.end(); it++ ) {
        LogReader *reader = it->second;
        if (!reader->recovery()) {
            return false;
        }
        recover_cnt_ += reader->reads();
    }
    LOG_WARN("****all redo-logs recover done! recover counts " << recover_cnt_);

    // if all success, clear all logs
    for(map<long long, LogReader*>::iterator it = readers.begin(); it != readers.end();) {
        map<long long, LogReader*>::iterator it_tmp = it;
        it++;

        LogReader *reader = it_tmp->second;
        reader->close_and_remove();
        readers.erase(it_tmp);
        delete reader;
    }

    filenames.clear();

    return true;
}
