#include <gtest/gtest.h>

#include "cascadb/options.h"
#include "cascadb/status.h"
#include "sys/sys.h"
#include "store/ram_directory.h"
#include "serialize/layout.h"
#include "cache/cache.h"

using namespace std;
using namespace cascadb;

class FakeNode : public Node {
public:
    FakeNode(uint32_t tbn, bid_t nid) : Node(tbn, nid), data(0) {}

    bool cascade(MsgBuf *mb, InnerNode* parent) { return false; }

    bool find(Slice key, Slice& value, InnerNode* parent) { return false; }

    void lock_path(Slice key, std::vector<Node*>& path) {}
    
    size_t size()
    {
        return 4096;
    }

    size_t estimated_buffer_size()
    {
        return size();
    }
    
    bool read_from(BlockReader& reader, bool skeleton_only) {
        EXPECT_TRUE(reader.readUInt64(&data));
        Slice s;
        EXPECT_TRUE(reader.readSlice(s));
        return true;
    }

    bool write_to(BlockWriter& writer, size_t& skeleton_size) {
        EXPECT_TRUE(writer.writeUInt64(nid_));
        char buf[4084] = {0};
        Slice s(buf, sizeof(buf));
        EXPECT_TRUE(writer.writeSlice(s));
        skeleton_size = size();
        return true;
    }

    uint64_t data;
};

class FakeNodeFactory : public NodeFactory {
public:
    FakeNodeFactory(uint32_t tbn)
    : tbn_(tbn)
    {
    }

    Node* new_node(bid_t nid) {
        return new FakeNode(tbn_, nid);
    }

    uint32_t tbn_;
};

TEST(Cache, read_and_write) {
    uint32_t tbn = 0;

    Status status;
    Options opts;
    opts.cache_limit = 4096 * 1000;

    Directory *dir = new RAMDirectory();
    AIOFile *file = dir->open_aio_file("cache_test");
    Layout *layout = new Layout(file, 0, opts, &status);
    layout->init(true);

    LogMgr *lmgr = new LogMgr(opts);
    ASSERT_TRUE(lmgr->init());

    Cache *cache = new Cache(opts, &status, lmgr);
    cache->init();

    NodeFactory *factory = new FakeNodeFactory(tbn);
    cache->add_table(tbn, factory, layout, NULL);

    for (int i = 0; i < 1000; i++) {
        Node *node = new FakeNode(tbn, i);
        node->set_dirty(true);
        cache->put(tbn, i, node);
        node->dec_ref();
    }

    // give it time to flush
    cascadb::sleep(5);
    // flush rest and clear nodes
    cache->del_table(tbn);

    cache->add_table(tbn, factory, layout, NULL);
    for (int i = 0; i < 1000; i++) {
        Node *node = cache->get(tbn, i, false);
        EXPECT_TRUE(node != NULL);
        EXPECT_EQ((uint64_t)i, ((FakeNode*)node)->data);
        node->dec_ref();
    }
    cache->del_table(tbn);

    delete cache;
    delete factory;
    delete layout;
    delete file;
    delete dir;
    delete lmgr;
}
