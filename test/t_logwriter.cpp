#include <gtest/gtest.h>
#include "sys/sys.h"
#include "store/fs_directory.h"
#include "store/ram_directory.h"
#include "serialize/layout.h"
#include "tree/tree.h"
#include "util/crc.h"

#include "log/log_writer.h"

using namespace cascadb;
using namespace std;

TEST(LogWriter, write) {
    string filename = "cdb000011.redolog-test";

    Options opts;
    opts.log_dir = create_fs_directory("/tmp");
    opts.log_bufsize_byte = 4 * 1024; //4kb 

    LogWriter *log = new LogWriter(opts, filename, 0);
    EXPECT_TRUE(log->init());

    uint32_t len, all, loop;
    loop = 10000;
    len  = (
            + 4                 // len 
            + 8                 // lsn
            + 4                 // table number 
            + 1                 // cmd
            + 4                 // key len
            + 4
            + 4                 // val len
            + 4
            + 4                 // len
            + 4                 // crc
            );
    all = loop * len;


    Slice k("key1");
    Slice v("val1");
    for (uint32_t i = 0; i < loop; i++) {
        EXPECT_TRUE(log->write(Put, k, v, 0, false));
    }

    uint32_t len2  = (
            + 4                 // len
            + 8                 // lsn
            + 4                 // table number
            + 1                 // cmd
            + 4                 // key len
            + 4                 // key
            + 4                 // value len
            + 4                 // len
            + 4                 // crc
            );


    Slice k_del("key1");
    Slice v_del;
    EXPECT_TRUE(log->write(Del, k_del, v_del, 0, false));

    uint32_t hdr_size = (8 + 4 + 8);
    EXPECT_EQ(all + len2 + hdr_size, log->last_lsn());

    log->close_and_del_file();

    delete log;
    delete opts.log_dir;
}

TEST(LogWriter, write_overflow) {
    string filename = "cdb000012.redolog-test";

    Options opts;
    opts.log_dir = create_fs_directory("/tmp");
    opts.log_bufsize_byte = 1024; // 1024bytes

    LogWriter *log = new LogWriter(opts, filename, 0);
    EXPECT_TRUE(log->init());


    uint32_t len, all, loop;
    loop = 1000;
    
    len = (
            + 4                 // len
            + 8                 // lsn
            + 4                 // table number
            + 1                 // cmd
            + 4                 // key len
            + 512
            + 4                 // val len
            + 512
            + CRC_SIZE          // crc
            + 4                 // len
            );
    all = loop * len;

    char buffer[4096];
    for (uint32_t i = 0; i < loop; i++) {
        memset(buffer, 0, 4096);

        Slice k(buffer, 512);
        Slice v(buffer, 512);
        EXPECT_TRUE(log->write(Put, k, v, 0, false));
    }

    uint32_t hdr_size = (8 + 4 + 8);
    EXPECT_EQ(all + hdr_size, log->last_lsn());

    log->close_and_del_file();

    delete log;
    delete opts.log_dir;
}

void *run_xthreads(void *arg)
{
    char buffer[512];
    LogWriter *log = (LogWriter*)arg;
    for (uint32_t i = 0; i < 1000; i++) {
        memset(buffer, 0, 512);

        Slice k(buffer, 512);
        Slice v(buffer, 512);

        EXPECT_TRUE(log->write(Put, k, v, i, false));

       // LOG_ERROR("log is writing by thread: " << pthread_self());
        int min = 100;
        int max = 1000;
        int s = rand() % (max - min + 1) + min;
        cascadb::usleep(s);
    }

    return NULL;
}

TEST(LogWriter, multi_threads_write) {
    string filename = "cdb000003.redolog-test";

    Options opts;
    opts.log_dir = create_fs_directory("/tmp");
    opts.log_bufsize_byte = 1<<20; // 1MB

    LogWriter *log = new LogWriter(opts, filename, 0);
    EXPECT_TRUE(log->init());

    Thread thr1(run_xthreads);
    Thread thr2(run_xthreads);
    Thread thr3(run_xthreads);
    Thread thr4(run_xthreads);
    Thread thr5(run_xthreads);

    thr1.start(log);
    thr2.start(log);
    thr3.start(log);
    thr4.start(log);
    thr5.start(log);

    thr1.join();
    thr2.join();

    // do fsync
    log->fsync();

    thr3.join();

    // do flush
    log->flush();

    thr4.join();
    thr5.join();

    uint32_t len, all, loop;
    loop = 1000 * 5;
    len  = (
            + 4                 // len
            + 8                 // lsn
            + 4                 // table number
            + 1                 // cmd
            + 4                 // key len
            + 512
            + 4                 // val len
            + 512
            + CRC_SIZE          // crc
            + 4                 // len
            );
    all = loop * len;

    uint32_t hdr_size = (8 + 4 + 8);
    EXPECT_EQ(all + hdr_size, log->last_lsn());

    log->close_and_del_file();

    delete log;
    delete opts.log_dir;
}
