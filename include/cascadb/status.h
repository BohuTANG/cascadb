// Copyright (c) 2013 The CascaDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef CASCADB_STATUS_H_
#define CASCADB_STATUS_H_

#include <stddef.h>
#include <stdint.h>

namespace cascadb {

class Status{
public:
    Status() {
        status_innernode_split_num = 0U;
        status_innernode_cascade_num = 0U;
        status_innernode_add_pivot_num = 0U;
        status_innernode_rm_pivot_num = 0U;
        status_innernode_created_num = 0U;

        status_leaf_split_num = 0U;
        status_leaf_merge_num = 0U;
        status_leaf_cascade_num = 0U;
        status_leaf_created_num = 0U;

        status_cache_put_num = 0U;
        status_cache_get_num = 0U;
        status_cache_evict_num = 0U;
        status_cache_writeback_num = 0U;

        status_block_read_num = 0U;
        status_subblock_read_num = 0U;

        status_async_write_num = 0U;
        status_async_write_byte = 0U;

        status_tree_pileup_num = 0U;
        status_tree_collapse_num = 0U;

        status_node_load_from_disk_num = 0U;
        status_node_load_from_disk_us = 0U;
        status_node_load_from_mem_num = 0U;
    }


    /********************************
            Status probes
    ********************************/

    // number of inner node split
    uint64_t status_innernode_split_num;

    // number of inner node cascade
    uint64_t status_innernode_cascade_num;

    // number of inner node created 
    uint64_t status_innernode_created_num;

    // number of leaf split
    uint64_t status_leaf_split_num;

    // number of leaf merge
    uint64_t status_leaf_merge_num;

    // number of leaf cascade
    uint64_t status_leaf_cascade_num;

    // number of leaf created 
    uint64_t status_leaf_created_num;

    // number of inner node add pivot
    uint64_t status_innernode_add_pivot_num;

    // number of inner node remove pivot
    uint64_t status_innernode_rm_pivot_num;

    // number of cache put
    uint64_t status_cache_put_num;

    // number of cache get
    uint64_t status_cache_get_num;

    // number of cache evict
    uint64_t status_cache_evict_num;

    // number of cache writeback
    uint64_t status_cache_writeback_num;

    // number of block read 
    uint64_t status_block_read_num;

    // number of subblock read 
    uint64_t status_subblock_read_num;

    // number of async write num
    uint64_t status_async_write_num;

    // bytes of async write 
    uint64_t status_async_write_byte;

    // number of tree pileup
    uint64_t status_tree_pileup_num;

    // number of tree collapse
    uint64_t status_tree_collapse_num;

    // number of node who read from disk
    uint64_t status_node_load_from_disk_num;

    uint64_t status_node_load_from_disk_us;

    // number of node who read from memory
    uint64_t status_node_load_from_mem_num;
};

}
#endif
