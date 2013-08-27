#include <gtest/gtest.h>
#include <stdlib.h>
#include "serialize/block.h"
#include "util/crc.h"

using namespace cascadb;
using namespace std;

#define BUFSIZE (4096)
TEST(crc16, calc) 
{
    void *buf;
    if (posix_memalign(&buf, BUFSIZE, BUFSIZE)) {
        assert(false);
    }

    Slice buffer = Slice((char *)buf, BUFSIZE);
    //eliminate the valgrind warnning
    memset((void*)buffer.data(), 0, buffer.size());

    Block block(buffer, 0, 0);
    BlockWriter writer(&block);
    writer.writeUInt64(1234567890);
    writer.writeUInt32(1234567);
    writer.writeUInt16(12345);

    EXPECT_EQ(cascadb::crc32(writer.start(), writer.pos()), 
            cascadb::crc32(writer.start(), writer.pos()));
}
