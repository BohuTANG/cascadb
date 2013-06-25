#include <gtest/gtest.h>
#include "util/compressor.h"
#include "cascadb/options.h"

using namespace cascadb;
using namespace std;

const char *text =
  "Yet another write-optimized storage engine, "
  "using buffered B-tree algorithm inspired by TokuDB.";

TEST(Compressor, NoCompressMethod) {
    Compressor *compressor = new Compressor(kNoCompress);

    size_t len1;
    char* str1 = new char[compressor->max_compressed_length(strlen(text))];
    EXPECT_TRUE(compressor->compress(text, strlen(text), str1, &len1));
    EXPECT_TRUE(len1 == (strlen(text) + 1));

    char *str2 = new char[strlen(text)];
    EXPECT_TRUE(compressor->uncompress(str1, len1, str2));

    EXPECT_TRUE(strncmp(text, str2, strlen(text)) == 0);

    delete str1;
    delete str2;
    delete compressor;
}

TEST(Compressor, SnappyCompressMethod) {
    Compressor *compressor = new Compressor(kSnappyCompress);

    size_t len1;
    char* str1 = new char[compressor->max_compressed_length(strlen(text))];
    EXPECT_TRUE(compressor->compress(text, strlen(text), str1, &len1));
    EXPECT_NE(len1, strlen(text) + 1);

    char *str2 = new char[strlen(text)];
    EXPECT_TRUE(compressor->uncompress(str1, len1, str2));
    EXPECT_TRUE(strncmp(text, str2, strlen(text)) == 0);

    delete str1;
    delete str2;
    delete compressor;
}

TEST(Compressor, QuicklzCompressMethod) {
    Compressor *compressor = new Compressor(kQuicklzCompress);

    size_t len = compressor->max_compressed_length(strlen(text));
    EXPECT_TRUE(len == 496);

    size_t len1;
    char* str1 = new char[len];
    EXPECT_TRUE(compressor->compress(text, strlen(text), str1, &len1));

    char *str2 = new char[strlen(text)];
    EXPECT_TRUE(compressor->uncompress(str1, len1, str2));
    EXPECT_TRUE(strncmp(text, str2, strlen(text)) == 0);

    delete str1;
    delete str2;
    delete compressor;
}


/*
 * In this case, the uncompress will use the origin compress method, 
 * regardless of the value of method
 */
TEST(Compressor, MixCompressMethod) {
    Compressor *compressor1 = new Compressor(kSnappyCompress);

    // compress with snappy method
    size_t len1;
    char* str1 = new char[compressor1->max_compressed_length(strlen(text))];
    EXPECT_TRUE(compressor1->compress(text, strlen(text), str1, &len1));
    EXPECT_NE(len1, strlen(text) + 1);

    // decompress with no compress
    Compressor *compressor2 = new Compressor(kNoCompress);
    char *str2 = new char[strlen(text)];
    EXPECT_TRUE(compressor2->uncompress(str1, len1, str2));
    EXPECT_TRUE(strncmp(text, str2, strlen(text)) == 0);


    // decompress with quicklz
    Compressor *compressor3 = new Compressor(kQuicklzCompress);
    char *str3 = new char[strlen(text)];
    EXPECT_TRUE(compressor3->uncompress(str1, len1, str3));
    EXPECT_TRUE(strncmp(text, str3, strlen(text)) == 0);

    delete str1;
    delete str2;
    delete str3;
    delete compressor1;
    delete compressor2;
    delete compressor3;
}
