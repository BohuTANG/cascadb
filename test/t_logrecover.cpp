#include <gtest/gtest.h>
#include "sys/sys.h"
#include "store/fs_directory.h"
#include "store/ram_directory.h"
#include "serialize/layout.h"
#include "tree/tree.h"
#include "util/crc.h"

#include "log/log_reader.h"
#include "log/log_writer.h"
#include "log/log_recover.h"

using namespace cascadb;
using namespace std;


// ****if test fails
// first to check your '/tmp' log files and delete them
TEST(LogRecover, recover) {
    string filename = "cdb000011.redolog";

    Status status;
    Options opts;
    opts.comparator = new LexicalComparator();
    opts.log_dir = create_fs_directory("/tmp");

    Directory *dir = new RAMDirectory();
    AIOFile *file = dir->open_aio_file("tree_test");

    Layout *layout = new Layout(file, 0, opts, &status);
    ASSERT_TRUE(layout->init(true));

    LogMgr *lmgr = new LogMgr(opts);
    ASSERT_TRUE(lmgr->init());

    Cache *cache = new Cache(opts, &status, lmgr);
    ASSERT_TRUE(cache->init());

    Tree *tree = new Tree(0, opts, &status, cache, layout);
    ASSERT_TRUE(tree->init());

    LogWriter *writer = new LogWriter(opts, filename, 0);
    EXPECT_TRUE(writer->init());

    uint32_t loop = 10;
    char buffer[512];
    for (uint32_t i = 0; i < loop; i++) {
        memset(buffer, 0, 512);
        Slice k(buffer, 512);
        Slice v(buffer, 512);

        EXPECT_TRUE(writer->write(Put, k, v, 0, false));
    }

    Slice k1("key1");
    Slice v1("value1");
    EXPECT_TRUE(writer->write(Put, k1, v1, 0, false));


    Slice k2("key2");
    Slice v2("value2");
    EXPECT_TRUE(writer->write(Put, k2, v2, 0, false));

    Slice k2_del("key2");
    Slice v2_del;
    EXPECT_TRUE(writer->write(Del, k2_del, v2_del, 0, false));
    writer->close();

    LogRecover *recover = new LogRecover(opts, cache);
    EXPECT_TRUE(recover->recover(0UL));
    EXPECT_EQ(loop + 3, recover->cnt());

    // check recover datas
    Slice value;
    ASSERT_TRUE(tree->get("key1", value));
    ASSERT_STREQ("value1", value.data());
    value.destroy();

    ASSERT_FALSE(tree->get("key2", value));

    writer->close_and_del_file();

    delete tree;
    delete cache;
    delete writer;
    delete recover;
    delete lmgr;
    delete file;
    delete dir;
    delete opts.log_dir;
    delete opts.comparator;
}
