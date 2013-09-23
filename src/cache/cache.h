// Copyright (c) 2013 The CascaDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef CASCADB_CACHE_H_
#define CASCADB_CACHE_H_

#include "cascadb/options.h"
#include "cascadb/status.h"
#include "serialize/block.h"
#include "serialize/layout.h"
#include "log/log_mgr.h"
#include "tree/node.h"
#include "sys/sys.h"

#include <map>
#include <vector>

namespace cascadb {

class NodeFactory {
public:
    virtual Node* new_node(bid_t nid) = 0;
    virtual ~NodeFactory(){}
};

// Node cache of fixed size
// When the percentage of dirty nodes reaches the high watermark, or get expired,
// they're flushed out in the order of timestamp node is modified for the first time.
// A reference count is maintained for each node, When cache is getting almost full,
// clean nodes would be evicted if their reference count drops to 0.
// Nodes're evicted in LRU order.
// Cache can be shared among multiple tables.

struct TableSettings {
    NodeFactory     *factory;
    Layout          *layout;
    Tree            *tree;
    Time            last_checkpoint_time;
};

class Cache {
public:
    Cache(const Options& options,
            Status *status,
            LogMgr *logmgr);
    
    ~Cache();
    
    bool init();
    
    // Add a table to cache
    bool add_table(uint32_t tbn, NodeFactory *factory, Layout *layout, Tree *tree);

    // Get tablesetting from cache
    bool get_table_settings(uint32_t tbn, TableSettings& tbs);
    
    // Flush all dirty nodes in a table
    void flush_table(uint32_t tbn);

    // Delete a table from cache, all loaded nodes in the table're
    // destroied, dirty nodes're flushed by default
    void del_table(uint32_t tbn, bool flush = true);
    
    // Put newly created node into cache
    void put(uint32_t tbn, bid_t nid, Node* node);
    
    // Acquire node, if node doesn't exist in cache, load it from layout
    Node* get(uint32_t tbn, bid_t nid, bool skeleton_only);
    
    // Write back dirty nodes if any condition satisfied,
    // Sweep out dead nodes
    void write_back();

    void set_in_recovering() { recovering_ = true; }
    void set_out_recovering() { recovering_ = false; }

    void debug_print(std::ostream& out);

    // check the cache highwater is reached
    bool must_evict();

    // Evict least recently used clean nodes to make room
    void evict();

    // Maybe should do checkpoint
    void check_checkpoint();

protected:
    void update_last_checkpoint_time(uint32_t tbn, Time t);


    // Test whether the cache grows larger than high watermark
    bool need_evict();


    void flush_nodes(std::vector<Node*>& nodes);

    struct WriteCompleteContext {
        Node            *node;
        Layout          *layout;
        Block           *block;
    };

    void write_complete(WriteCompleteContext* context, bool succ);

    void delete_nodes(std::vector<Node*>& nodes);
    
private:
    Options options_;
    Status *status_;

    RWLock tables_lock_;
    std::map<uint32_t, TableSettings> tables_;

    // TODO make me atomic
    Mutex size_mtx_;
    // total memory size occupied by nodes,
    // updated everytime the flusher thread runs
    size_t size_;

    class CacheKey {
    public:
        CacheKey(uint32_t t, bid_t n): tbn(t), nid(n) {}

        bool operator<(const CacheKey& o) const {
            int c = tbn < o.tbn;
            if (c < 0) { return true; }
            else if (c > 0) { return false; }
            else { return nid < o.nid; }
        }
        
        uint32_t tbn;
        bid_t nid;
    };

    RWLock nodes_lock_;

    std::map<CacheKey, Node*> nodes_;

    // ensure there is only one thread is doing evict/flush
    Mutex global_mtx_;
    
    bool alive_;

    // db in recover flag
    bool recovering_;

    // scan nodes not being used,
    // async flush dirty page out
    Thread* flusher_;

    LogMgr *logmgr_;
};

}

#endif
