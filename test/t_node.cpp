#include <gtest/gtest.h>

#define private public
#define protected public

#include "store/ram_directory.h"
#include "serialize/layout.h"
#include "tree/tree.h"
#include "helper.h"

using namespace cascadb;
using namespace std;

TEST(Record, serialize)
{
    char buffer[4096];
    Block blk(Slice(buffer, 4096), 0, 0);
    BlockReader reader(&blk);
    BlockWriter writer(&blk);
    
    Record rec1("a", "1");
    rec1.write_to(writer);
    
    EXPECT_EQ(rec1.size(), blk.size());
    
    Record rec2;
    rec2.read_from(reader);
    EXPECT_EQ("a", rec2.key);
    EXPECT_EQ("1", rec2.value);
}

TEST(Tree, bootstrap)
{
    Status status;
    Options opts;
    opts.comparator = new LexicalComparator();
    opts.inner_node_msg_count = 4;
    opts.inner_node_children_number = 2;
    opts.leaf_node_record_count = 4;

    Directory *dir = new RAMDirectory();
    AIOFile *file = dir->open_aio_file("tree_test");
    Layout *layout = new Layout(file, 0, opts, &status);
    ASSERT_TRUE(layout->init(true));
    Cache *cache = new Cache(opts, &status);
    ASSERT_TRUE(cache->init());
    Tree *tree = new Tree("", opts, &status, cache, layout);
    ASSERT_TRUE(tree->init());
    
    InnerNode *n1 = tree->root_;
    EXPECT_EQ(NID_START, n1->nid_);
    
    // the first msgbuf is created
    n1->put("a", "1");
    n1->put("b", "1");
    n1->put("c", "1");
    EXPECT_EQ(NID_NIL, n1->first_child_);
    EXPECT_EQ(3U, n1->first_msgbuf_->count());
    n1->put("d", "1");
    EXPECT_EQ(1U, status.status_leaf_created_num);

    // limit of the first msg reached
    // create leaf#1
    EXPECT_EQ(0U, n1->first_msgbuf_->count());
    EXPECT_TRUE(n1->first_child_ != NID_NIL);
    LeafNode *l1 = (LeafNode*)tree->load_node(n1->first_child_, false);
    EXPECT_EQ(NID_LEAF_START, l1->nid_);
    EXPECT_EQ(4U, l1->records_.size());
    CHK_REC(l1->records_[0], "a", "1");
    CHK_REC(l1->records_[1], "b", "1");
    CHK_REC(l1->records_[2], "c", "1");
    CHK_REC(l1->records_[3], "d", "1");
    
    // go on filling leaf#1's msgbuf
    n1->put("e", "1");
    n1->put("f", "1");
    n1->put("g", "1");
    EXPECT_EQ(3U, n1->first_msgbuf_->count());
    
    n1->put("h", "1");
    EXPECT_EQ(1U, status.status_leaf_split_num);
    EXPECT_EQ(2U, status.status_leaf_created_num);

    // cascading into #leaf1, and split into two leafs
    EXPECT_EQ(0U, n1->first_msgbuf_->count());
    EXPECT_EQ(l1->nid_, n1->first_child_);
    EXPECT_EQ(1U, n1->pivots_.size());
    EXPECT_TRUE(n1->pivots_[0].key == "e");
    EXPECT_TRUE(n1->pivots_[0].child != NID_NIL);
    EXPECT_TRUE(n1->pivots_[0].msgbuf != NULL);
    LeafNode *l2 = (LeafNode*)tree->load_node(n1->pivots_[0].child, false);
    EXPECT_EQ(NID_LEAF_START+1, l2->nid_);
    EXPECT_EQ(4U, l1->records_.size());
    CHK_REC(l1->records_[0], "a", "1");
    CHK_REC(l1->records_[1], "b", "1");
    CHK_REC(l1->records_[2], "c", "1");
    CHK_REC(l1->records_[3], "d", "1");
    EXPECT_EQ(4U, l2->records_.size());
    CHK_REC(l2->records_[0], "e", "1");
    CHK_REC(l2->records_[1], "f", "1");
    CHK_REC(l2->records_[2], "g", "1");
    CHK_REC(l2->records_[3], "h", "1");
    
    n1->put("a", "2");
    n1->put("b", "2");
    n1->put("bb", "1");
    EXPECT_EQ(100U, n1->size());

    n1->put("e", "2");
    
    // cascade into #leaf1 and force leaf#1 split,
    // then propogate to node#1 and generate new root
    
    // node#3(the new root)
    EXPECT_NE(tree->root_, n1);
    InnerNode *n3 = tree->root_;
    EXPECT_EQ(NID_START+2, n3->nid_);
    EXPECT_EQ(n3->first_child_, n1->nid_);
    EXPECT_EQ(n3->first_msgbuf_->count(), 0U);
    
    // node#1
    EXPECT_EQ(0U, n1->first_msgbuf_->count());
    EXPECT_EQ(l1->nid_, n1->first_child_);
    EXPECT_EQ(1U, n1->pivots_.size());
    EXPECT_TRUE(n1->pivots_[0].key == "bb");
    EXPECT_TRUE(n1->pivots_[0].child != NID_NIL);
    EXPECT_TRUE(n1->pivots_[0].child != l2->nid_);
    EXPECT_TRUE(n1->pivots_[0].msgbuf != NULL);
    LeafNode *l3 = (LeafNode*)tree->load_node(n1->pivots_[0].child, false);
    EXPECT_EQ(NID_LEAF_START+2, l3->nid_);
    EXPECT_EQ(2U, l1->records_.size());
    CHK_REC(l1->records_[0], "a", "2");
    CHK_REC(l1->records_[1], "b", "2");
    EXPECT_EQ(3U, l3->records_.size());
    CHK_REC(l3->records_[0], "bb", "1");
    CHK_REC(l3->records_[1], "c", "1");
    CHK_REC(l3->records_[2], "d", "1");
    EXPECT_EQ(67U, n1->size());
    
    // node#2
    EXPECT_EQ(n3->pivots_.size(), 1U);
    EXPECT_TRUE(n3->pivots_[0].key == "e");
    EXPECT_TRUE(n3->pivots_[0].child != NID_NIL);
    InnerNode *n2 = (InnerNode*)tree->load_node(n3->pivots_[0].child, false);
    EXPECT_EQ(NID_START+1, n2->nid_);
    EXPECT_EQ(1U, n2->first_msgbuf_->count());
    CHK_MSG(n2->first_msgbuf_->get(0),  Put, "e", "2");
    EXPECT_EQ(l2->nid_, n2->first_child_);
    EXPECT_EQ(0U, n2->pivots_.size());
    EXPECT_EQ(44U, n2->size());
    
    n3->put("abc", "1");
    n3->put("bb", "2");
    n3->put("ee", "1");
    n3->put("f", "2");

    // cascading down, no split
    EXPECT_EQ(tree->root_, n3);
    EXPECT_EQ(NID_START+2, n3->nid_);
    EXPECT_EQ(n3->first_child_, n1->nid_);
    EXPECT_EQ(0U, n3->first_msgbuf_->count());
    EXPECT_EQ(n3->pivots_[0].child, n2->nid_);
    EXPECT_EQ(2U, n3->pivots_[0].msgbuf->count());
    CHK_MSG(n3->pivots_[0].msgbuf->get(0), Put, "ee", "1");
    CHK_MSG(n3->pivots_[0].msgbuf->get(1), Put, "f", "2");
    
    EXPECT_EQ(1U, n1->first_msgbuf_->count());
    CHK_MSG(n1->first_msgbuf_->get(0), Put, "abc", "1");
    EXPECT_EQ(1U, n1->pivots_[0].msgbuf->count());
    CHK_MSG(n1->pivots_[0].msgbuf->get(0), Put, "bb", "2");

    n3->put("abcd", "1");
    n3->put("g", "2");
    // l2 split
    EXPECT_TRUE(tree->root_ == n3);
    EXPECT_EQ(NID_START+2, n3->nid_);
    EXPECT_TRUE(n3->first_child_ == n1->nid_);
    EXPECT_EQ(1U, n3->first_msgbuf_->count());
    CHK_MSG(n3->first_msgbuf_->get(0), Put, "abcd", "1");
    EXPECT_TRUE(n3->pivots_[0].child == n2->nid_);
    EXPECT_EQ(0U, n3->pivots_[0].msgbuf->count());
    
    EXPECT_EQ(1U, n2->pivots_.size());
    EXPECT_TRUE(n2->first_child_ == l2->nid_);
    EXPECT_EQ(0U, n2->first_msgbuf_->count());
    EXPECT_TRUE(n2->pivots_[0].child != NID_NIL);
    EXPECT_EQ("f", n2->pivots_[0].key);
    EXPECT_EQ(0U, n2->pivots_[0].msgbuf->count());
    LeafNode *l4 = (LeafNode*)tree->load_node(n2->pivots_[0].child, false);
    EXPECT_EQ(NID_LEAF_START+3, l4->nid_);
    EXPECT_EQ(2U, l2->records_.size());
    CHK_REC(l2->records_[0], "e", "2");
    CHK_REC(l2->records_[1], "ee", "1");
    EXPECT_EQ(3U, l4->records_.size());
    CHK_REC(l4->records_[0], "f", "2");
    CHK_REC(l4->records_[1], "g", "2");
    CHK_REC(l4->records_[2], "h", "1");

    delete tree;
    delete cache;
    delete layout;
    delete file;
    delete dir;
    delete opts.comparator;
}

TEST(InnerNode, serialize)
{
    Status status;
    Options opts;
    opts.comparator = new LexicalComparator();
    opts.inner_node_msg_count = 4;
    opts.inner_node_children_number = 2;
    opts.leaf_node_record_count = 4;

    Directory *dir = new RAMDirectory();
    AIOFile *file = dir->open_aio_file("tree_test");
    Layout *layout = new Layout(file, 0, opts, &status);
    ASSERT_TRUE(layout->init(true));
    Cache *cache = new Cache(opts, &status);
    ASSERT_TRUE(cache->init());
    Tree *tree = new Tree("", opts, &status, cache, layout);
    ASSERT_TRUE(tree->init());

    char buffer[40960];
    Block blk(Slice(buffer, 40960), 0, 0);
    BlockReader reader(&blk);
    BlockWriter writer(&blk);

    InnerNode n1("", NID_START, tree);
    n1.bottom_ = true;
    n1.first_child_ = NID_LEAF_START;
    n1.first_msgbuf_ = new MsgBuf(opts.comparator);
    PUT(*n1.first_msgbuf_, "a", "1");
    PUT(*n1.first_msgbuf_, "b", "1");
    PUT(*n1.first_msgbuf_, "c", "1");
    n1.pivots_.resize(1);
    n1.pivots_[0].key = Slice("d").clone();
    n1.pivots_[0].child = NID_LEAF_START + 1;
    n1.pivots_[0].msgbuf = new MsgBuf(opts.comparator);

    size_t skeleton_size;
    EXPECT_TRUE(n1.write_to(writer, skeleton_size) == true);

    InnerNode n2("", NID_START, tree);
    EXPECT_TRUE(n2.read_from(reader, false) == true);

    EXPECT_TRUE(n2.bottom_ == true);
    EXPECT_EQ(NID_LEAF_START, n2.first_child_);
    EXPECT_TRUE(n2.first_msgbuf_ != NULL);
    EXPECT_EQ(3U, n2.first_msgbuf_->count());
    CHK_MSG(n2.first_msgbuf_->get(0), Put, "a", "1");
    CHK_MSG(n2.first_msgbuf_->get(1), Put, "b", "1");
    CHK_MSG(n2.first_msgbuf_->get(2), Put, "c", "1");
    EXPECT_EQ(1U, n2.pivots_.size());
    EXPECT_EQ("d", n2.pivots_[0].key);
    EXPECT_EQ(NID_LEAF_START+1, n2.pivots_[0].child);
    EXPECT_TRUE(n2.pivots_[0].msgbuf != NULL);
    EXPECT_EQ(0U, n2.pivots_[0].msgbuf->count());



    size_t n = 13;
    //here is must same with util/bloom.cpp
    size_t size = 4 + ((13 * 12 + 7) / 8 + 1);
    EXPECT_EQ(size, n1.bloom_size(n));

    delete tree;
    delete cache;
    delete layout;
    delete file;
    delete dir;
    delete opts.comparator;
}

TEST(InnerNode, add_pivot)
{
    Status status;
    Options opts;
    opts.comparator = new LexicalComparator();
    opts.inner_node_children_number = 4;

    Directory *dir = new RAMDirectory();
    AIOFile *file = dir->open_aio_file("tree_test");
    Layout *layout = new Layout(file, 0, opts, &status);
    ASSERT_TRUE(layout->init(true));
    Cache *cache = new Cache(opts, &status);
    ASSERT_TRUE(cache->init());
    Tree *tree = new Tree("", opts, &status, cache, layout);
    ASSERT_TRUE(tree->init());

    InnerNode *n1 = tree->new_inner_node();

    n1->bottom_ = true;
    n1->first_child_ = NID_START + 100;
    n1->first_msgbuf_ = new MsgBuf(opts.comparator);
    n1->msgcnt_ = 0;
    n1->msgbufsz_ = n1->first_msgbuf_->size();

    n1->add_pivot("e", NID_START + 101);
    EXPECT_EQ(1U, n1->pivots_.size());
    EXPECT_EQ("e", n1->pivots_[0].key);

    n1->add_pivot("d", NID_START + 102);
    EXPECT_EQ(2U, n1->pivots_.size());
    EXPECT_EQ("d", n1->pivots_[0].key);
    EXPECT_EQ("e", n1->pivots_[1].key);

    n1->add_pivot("f", NID_START + 103);
    EXPECT_EQ(3U, n1->pivots_.size());
    EXPECT_EQ("d", n1->pivots_[0].key);
    EXPECT_EQ("e", n1->pivots_[1].key);
    EXPECT_EQ("f", n1->pivots_[2].key);

    delete tree;
    delete cache;
    delete layout;
    delete file;
    delete dir;
    delete opts.comparator;
}

TEST(InnerNode, split)
{
    Status status;
    Options opts;
    opts.comparator = new LexicalComparator();
    opts.inner_node_children_number = 3;

    Directory *dir = new RAMDirectory();
    AIOFile *file = dir->open_aio_file("tree_test");
    Layout *layout = new Layout(file, 0, opts, &status);
    ASSERT_TRUE(layout->init(true));
    Cache *cache = new Cache(opts, &status);
    ASSERT_TRUE(cache->init());
    Tree *tree = new Tree("", opts, &status, cache, layout);
    ASSERT_TRUE(tree->init());

    InnerNode *n1 = tree->new_inner_node();
    InnerNode *n2 = tree->new_inner_node();

    n1->bottom_ = false;
    n1->first_child_ = n2->nid_;
    n1->first_msgbuf_ = new MsgBuf(opts.comparator);
    n1->msgcnt_ = 0;
    n1->msgbufsz_ = n1->first_msgbuf_->size();

    n2->bottom_ = false;
    n2->first_child_ = NID_START + 100;
    n2->first_msgbuf_ = new MsgBuf(opts.comparator);
    n2->msgcnt_ = 0;
    n2->msgbufsz_ = n2->first_msgbuf_->size();

    // add 1th pivot
    n2->add_pivot("e", NID_START + 101);
    EXPECT_EQ(1U, n2->pivots_.size());
    EXPECT_EQ("e", n2->pivots_[0].key);

    // add 2th pivot
    n2->add_pivot("d", NID_START + 102);
    EXPECT_EQ(2U, n2->pivots_.size());
    EXPECT_EQ("d", n2->pivots_[0].key);
    EXPECT_EQ("e", n2->pivots_[1].key);

    // add 3th pivot
    n2->add_pivot("f", NID_START + 103);
    EXPECT_EQ(3U, n2->pivots_.size());

    n2->split(n1);
    EXPECT_EQ(1U, n2->pivots_.size());

    EXPECT_EQ("d", n2->pivots_[0].key);
    EXPECT_EQ(1U, n1->pivots_.size());
    EXPECT_EQ("e", n1->pivots_[0].key);
    EXPECT_NE(NID_NIL, n1->pivots_[0].child);
    InnerNode *n3 = (InnerNode*)tree->load_node(n1->pivots_[0].child, false);
    EXPECT_TRUE(n3 != NULL);
    EXPECT_EQ(1U, n3->pivots_.size());
    EXPECT_EQ("f", n3->pivots_[0].key);

    delete tree;
    delete cache;
    delete layout;
    delete file;
    delete dir;
    delete opts.comparator;
}


TEST(InnerNode, root_pileup)
{
    Status status;
    Options opts;
    opts.comparator = new LexicalComparator();
    opts.inner_node_children_number = 3;

    Directory *dir = new RAMDirectory();
    AIOFile *file = dir->open_aio_file("tree_test");
    Layout *layout = new Layout(file, 0, opts, &status);
    ASSERT_TRUE(layout->init(true));
    Cache *cache = new Cache(opts, &status);
    ASSERT_TRUE(cache->init());
    Tree *tree = new Tree("", opts, &status, cache, layout);
    ASSERT_TRUE(tree->init());

    // root pileup
    InnerNode *root = tree->root_;
    root->add_pivot("aa", NID_START + 102);
    root->add_pivot("bb", NID_START + 103);
    root->add_pivot("cc", NID_START + 104);
    root->split(NULL);
    EXPECT_EQ(1U, status.status_tree_pileup_num);


    delete tree;
    delete cache;
    delete layout;
    delete file;
    delete dir;
    delete opts.comparator;
}

TEST(InnerNode, rm_pivot)
{
    Status status;
    Options opts;
    opts.comparator = new LexicalComparator();
    opts.inner_node_children_number = 3;

    Directory *dir = new RAMDirectory();
    AIOFile *file = dir->open_aio_file("tree_test");
    Layout *layout = new Layout(file, 0, opts, &status);
    ASSERT_TRUE(layout->init(true));
    Cache *cache = new Cache(opts, &status);
    ASSERT_TRUE(cache->init());
    Tree *tree = new Tree("", opts, &status, cache, layout);
    ASSERT_TRUE(tree->init());

    InnerNode *n2 = tree->new_inner_node();

    n2->bottom_ = false;
    n2->first_child_ = NID_START + 100;
    n2->first_msgbuf_ = new MsgBuf(opts.comparator);
    n2->msgcnt_ = 0;
    n2->msgbufsz_ = n2->first_msgbuf_->size();

    // add 1th pivot
    n2->add_pivot("e", NID_START + 101);
    EXPECT_EQ(1U, n2->pivots_.size());
    EXPECT_EQ("e", n2->pivots_[0].key);

    // add 2th pivot
    n2->add_pivot("d", NID_START + 102);
    EXPECT_EQ(2U, n2->pivots_.size());
    EXPECT_EQ("d", n2->pivots_[0].key);
    EXPECT_EQ("e", n2->pivots_[1].key);

    // add 3th pivot
    n2->add_pivot("f", NID_START + 103);
    EXPECT_EQ(3U, n2->pivots_.size());

    // remove the 2th pivot
    n2->rm_pivot(NID_START + 101);
    EXPECT_EQ(2U, n2->pivots_.size());
    EXPECT_EQ("d", n2->pivots_[0].key);
    EXPECT_EQ("f", n2->pivots_[1].key);

    // remove root only one pivot
    // root to collapse
    InnerNode *old_root = tree->root_;
    tree->root_->rm_pivot(tree->root_->first_child_);
    EXPECT_TRUE(old_root != tree->root_);
    EXPECT_EQ(1U, status.status_tree_collapse_num);

    delete tree;
    delete cache;
    delete layout;
    delete file;
    delete dir;
    delete opts.comparator;
}

TEST(LeafNode, split_and_merge)
{
    Status status;
    Options opts;
    opts.comparator = new LexicalComparator();
    opts.inner_node_msg_count = 4;
    opts.inner_node_children_number = 2;
    opts.leaf_node_record_count = 4;

    Directory *dir = new RAMDirectory();
    AIOFile *file = dir->open_aio_file("tree_test");
    Layout *layout = new Layout(file, 0, opts, &status);
    ASSERT_TRUE(layout->init(true));
    Cache *cache = new Cache(opts, &status);
    ASSERT_TRUE(cache->init());
    Tree *tree = new Tree("", opts, &status, cache, layout);
    ASSERT_TRUE(tree->init());

    InnerNode *n1 = tree->new_inner_node();
    LeafNode *l1 = tree->new_leaf_node();

    tree->root_->first_child_ = n1->nid_;

    n1->bottom_ = true;
    n1->first_child_ = l1->nid_;
    n1->first_msgbuf_ = new MsgBuf(opts.comparator);
    PUT(*n1->first_msgbuf_, "a", "1");
    PUT(*n1->first_msgbuf_, "b", "1");
    PUT(*n1->first_msgbuf_, "c", "1");
    PUT(*n1->first_msgbuf_, "d", "1");
    n1->msgcnt_ = 4;
    n1->msgbufsz_ = n1->first_msgbuf_->size();

    l1->cascade(n1->first_msgbuf_, n1);

    PUT(*n1->first_msgbuf_, "e", "1");
    PUT(*n1->first_msgbuf_, "f", "1");
    PUT(*n1->first_msgbuf_, "g", "1");
    PUT(*n1->first_msgbuf_, "h", "1");
    n1->msgcnt_ = 4;
    n1->msgbufsz_ = n1->first_msgbuf_->size();

    l1->cascade(n1->first_msgbuf_, n1);

    EXPECT_EQ(1U, n1->pivots_.size());
    EXPECT_EQ("e", n1->pivots_[0].key);
    EXPECT_NE(NID_NIL, n1->pivots_[0].child);
    LeafNode *l2 = (LeafNode*)tree->load_node(n1->pivots_[0].child, false);
    EXPECT_TRUE(l2 != NULL);

    EXPECT_EQ(4U, l1->records_.size());
    EXPECT_EQ(4U, l2->records_.size());

    // remove l2 from n1, merge l2 to l1
    DEL(*n1->first_msgbuf_, "e");
    DEL(*n1->first_msgbuf_, "f");
    DEL(*n1->first_msgbuf_, "g");
    DEL(*n1->first_msgbuf_, "h");
    n1->msgcnt_ = 4;
    n1->msgbufsz_ = n1->first_msgbuf_->size();

    l2->cascade(n1->first_msgbuf_, n1);
    EXPECT_EQ(0U, n1->pivots_.size());


    // remove l1 from n1
    DEL(*n1->first_msgbuf_, "a");
    DEL(*n1->first_msgbuf_, "b");
    DEL(*n1->first_msgbuf_, "c");
    DEL(*n1->first_msgbuf_, "d");
    n1->msgcnt_ = 4;
    n1->msgbufsz_ = n1->first_msgbuf_->size();
    l1->cascade(n1->first_msgbuf_, n1);
    EXPECT_TRUE(n1->first_msgbuf_ == NULL);

    // remove n1 from root
    n1->cascade(tree->root_->first_msgbuf_, tree->root_);

    delete tree;
    delete cache;
    delete layout;
    delete file;
    delete dir;
    delete opts.comparator;
}
