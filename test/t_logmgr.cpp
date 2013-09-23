#include <gtest/gtest.h>
#include "sys/sys.h"
#include "store/fs_directory.h"
#include "store/ram_directory.h"
#include "serialize/layout.h"
#include "tree/tree.h"

#include "log/log_mgr.h"

using namespace cascadb;
using namespace std;


TEST(LogMgr, write) {

    Status status;
    Options opts;
    opts.comparator = new LexicalComparator();
    opts.log_dir = create_fs_directory("/tmp");
    opts.log_bufsize_byte = 4 * 1024;  // 4KB
    opts.log_filesize_byte = 10 << 20; // 10MB


    Directory *dir = new RAMDirectory();
    AIOFile *file = dir->open_aio_file("tree_test");
    Layout *layout = new Layout(file, 0, opts, &status);
    ASSERT_TRUE(layout->init(true));

    LogMgr *lgr = new LogMgr(opts);
    EXPECT_TRUE(lgr->init());

    Cache *cache = new Cache(opts, &status, lgr);
    ASSERT_TRUE(cache->init());

    Tree *tree = new Tree(0, opts, &status, cache, layout);
    ASSERT_TRUE(tree->init());


    uint32_t tbn = 0;
    uint32_t loop = 10000;
    uint32_t all;

    char buffer[512];
    for (uint32_t i = 0; i < loop; i++) {
        memset(buffer, 0, 512);
        Slice sk(buffer, 512);
        Slice sv(buffer, 512);
        lgr->enq_put(sk, sv, tbn, false);
    }

    uint32_t len  = (
            + 4                 // len
            + 8                 // lsn
            + 4                 // table number
            + 1                 // cmd
            + 512               // key len
            + 4
            + 512               // val len
            + 4
            + 4                 // crc
            + 4                 // len
            );
    all = loop * len;

    uint32_t hdr_size = (8 + 4 + 8);
    ASSERT_EQ(all + hdr_size * 2, lgr->make_checkpoint_begin());
    ASSERT_EQ(2, (int)lgr->logs_cnt());

    delete tree;
    delete cache;
    delete lgr;
    delete layout;
    delete file;
    delete dir;
    delete opts.log_dir;
    delete opts.comparator;
}
