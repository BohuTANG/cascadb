// Copyright (c) 2013 The CascaDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef CASCADB_SERIALIZE_SUPER_BLOCK_H_
#define CASCADB_SERIALIZE_SUPER_BLOCK_H_

#include "block.h"

namespace cascadb {

#define SUPER_BLOCK_SIZE        4096
#define SUPER_BLOCK_MAGIC_NUM (0x6264616373616) // "cascadb

class BlockMeta;

class SuperBlock {
public:
    SuperBlock()
    {
        magic_number = SUPER_BLOCK_MAGIC_NUM;   // "cascadb
        major_version = 0;                      // "version 0.1"
        minor_version = 1;
        lsn = 0UL;

        index_block_meta = NULL;
    }

    uint64_t        magic_number;
    uint8_t         major_version;
    uint8_t         minor_version;
    uint64_t        lsn;                        // checkpoint lsn

    BlockMeta       *index_block_meta;
};

}

#endif
