#include <algorithm>
#include <sstream>

#include "util/logger.h"
#include "cache.h"

using namespace std;
using namespace cascadb;

static void* flusher_main(void *arg)
{
    Cache *cache = (Cache*) arg;
    cache->write_back();
    return NULL;
}

class LRUComparator {
public:
    bool operator() (Node* x, Node* y)
    {
        return x->get_last_used_timestamp() < y->get_last_used_timestamp();
    }
};

class FirstWriteComparator {
public:
    bool operator() (Node* x, Node* y)
    {
        return x->get_first_write_timestamp() < y->get_first_write_timestamp();
    }
};

Cache::Cache(const Options& options)
: options_(options), 
  size_(0),
  alive_(false),
  flusher_(NULL)
{
}

Cache::~Cache()
{
    alive_ = false;
    if (flusher_) {
        flusher_->join();
        delete flusher_;
    }
}

bool Cache::init()
{
    assert(!alive_);

    alive_ = true;
    flusher_ = new Thread(flusher_main);
    if (flusher_) {
        flusher_->start(this);
        return true;
    }
    
    LOG_ERROR("cannot create flusher thread");
    return false;
}

bool Cache::add_table(const std::string& tbn, NodeFactory *factory, Layout *layout)
{
    ScopedMutex lock(&tables_mtx_);
    
    if (tables_.find(tbn) != tables_.end()) {
        LOG_ERROR("table " << tbn << " already registered in cache");
        return false;
    }

    TableSettings tbs;
    tbs.factory = factory;
    tbs.layout = layout;
    tables_[tbn] = tbs;
    return true;
}

void Cache::flush_table(const std::string& tbn)
{
    TableSettings tbs;
    if(!get_table_settings(tbn, tbs)) {
        assert(false);
    }

    Layout *layout = tbs.layout;
    vector<Node*> zombies;
    vector<Node*> dirty_nodes;
    size_t dirty_size = 0;

    ScopedMutex global_lock(&global_mtx_);

    ScopedMutex lock(&nodes_mtx_);
    for(map<CacheKey, Node*>::iterator it = nodes_.begin();
        it != nodes_.end(); it++ ) {
        if (it->first.tbn  == tbn) {
            Node *node = it->second;
            if (node->is_dead()) {
                zombies.push_back(node);
                nodes_.erase(it);
            } else {
                size_t sz = node->size();
                // TODO: flush all node
                if (node->is_dirty() && !node->is_flushing() && node->pin() == 0) {
                    node->write_lock();
                    node->set_flushing(true);
                    dirty_nodes.push_back(node);
                    dirty_size += sz;
                }
            }
        }
    }
    lock.unlock();

    if (dirty_nodes.size()) {
        LOG_INFO("flush table " << tbn << ", write " << dirty_nodes.size() << " nodes, "
            << dirty_size << " bytes total");
        flush_nodes(dirty_nodes);
    }

    if (zombies.size()) {
        LOG_INFO("flush table " << tbn << ", delete " << zombies.size() << " nodes");
        delete_nodes(zombies);
    }

    global_lock.unlock();

    layout->flush();
}

void Cache::del_table(const std::string& tbn, bool flush)
{
    if(flush) {
        flush_table(tbn);
    }

    ScopedMutex tables_lock(&tables_mtx_);
    map<string, TableSettings>::iterator it = tables_.find(tbn);
    if (it == tables_.end()) {
        return;
    }
    tables_.erase(it);
    tables_lock.unlock();

    size_t total_count = 0;

    ScopedMutex global_lock(&global_mtx_);

    ScopedMutex nodes_lock(&nodes_mtx_);
    // TODO: improve me
    for(map<CacheKey, Node*>::iterator it = nodes_.begin();
        it != nodes_.end(); it++ ) {
        if (it->first.tbn  == tbn) {
            Node *node = it->second;
            assert(node->ref() == 0);
            delete node;
            
            nodes_.erase(it);
            total_count ++;
        }
    }
    nodes_lock.unlock();

    global_lock.unlock();

    LOG_INFO("release " << total_count << " nodes in table " << tbn);
}

void Cache::put(const std::string& tbn, bid_t nid, Node* node)
{
    assert(node->ref() == 0);

    CacheKey key(tbn, nid);
    TableSettings tbs;
    if (!get_table_settings(tbn, tbs)) {
        assert(false);
    }

    while (must_evict()) {
        evict();
    }

    ScopedMutex lock(&nodes_mtx_);
    assert(nodes_.find(key) == nodes_.end());
    nodes_[key] = node;
    node->inc_ref();
}

Node* Cache::get(const std::string& tbn, bid_t nid)
{
    CacheKey key(tbn, nid);
    Node *node;

    TableSettings tbs;
    if (!get_table_settings(tbn, tbs)) {
        assert(false);
    }

    ScopedMutex lock(&nodes_mtx_);
    map<CacheKey, Node*>::iterator it = nodes_.find(key);
    if (it != nodes_.end()) {
        node = it->second;
        node->inc_ref();
        return node;
    }
    lock.unlock();

    while (must_evict()) {
        evict();
    }
    
    Block* block = tbs.layout->read(nid);
    if (block == NULL) return NULL;
    
    node = tbs.factory->new_node(nid);
    BlockReader reader(block);
    if (!node->read_from(reader)) {
        assert(false);
    }
    assert(node->size() == block->size());
    tbs.layout->destroy(block);
    
    lock.lock();
    it = nodes_.find(key);
    if (it != nodes_.end()) {
        LOG_WARN("detect multiple threads're loading node " << nid << " concurrently");
        delete node;
        node = it->second;
    } else {
        nodes_[key] = node;
    }

    node->inc_ref();
    return node;
}

bool Cache::get_table_settings(const std::string& tbn, TableSettings& tbs)
{
    ScopedMutex lock(&tables_mtx_);

    map<string, TableSettings>::iterator it = tables_.find(tbn);
    if (it != tables_.end()) {
        tbs = it->second;
        return true;
    }
    return false;
}

bool Cache::must_evict()
{
    ScopedMutex lock(&nodes_mtx_);

    return size_ >= options_.cache_limit;
}

bool Cache::need_evict()
{
    ScopedMutex lock(&nodes_mtx_);

    size_t threshold = (options_.cache_limit * 
        options_.cache_evict_high_watermark) / 100;

    return size_ > threshold;
}

void Cache::evict()
{
    size_t total_size = 0;
    size_t total_count = 0;

    size_t active_size = 0;
    size_t active_count = 0;

    size_t dirty_size = 0;
    size_t dirty_count = 0;

    size_t flushing_size = 0;
    size_t flushing_count = 0;

    size_t clean_size = 0;
    size_t clean_count = 0;

    vector<Node*> zombies;
    vector<Node*> clean_nodes;

    ScopedMutex lock(&nodes_mtx_);

    for(map<CacheKey, Node*>::iterator it = nodes_.begin();
        it != nodes_.end(); it++ ) {

        Node *node = it->second;
        assert(node->nid() == it->first.nid);

        if (node->is_dead()) {
            if (node->ref() == 0) {
                zombies.push_back(node);
                nodes_.erase(it);
            }
        } else {
            size_t size = node->size();

            total_size += size;
            total_count ++;

            if (node->ref()) {
                active_size += size;
                active_count ++;
            }

            if (node->is_dirty()) {
                dirty_size += size;
                dirty_count ++;
            } else if (node->is_flushing()) {
                flushing_size += size;
                flushing_count ++;
            } else {
                clean_size += size;
                clean_count ++;
                if (node->ref() == 0) {
                    clean_nodes.push_back(node);
                }
            }
        }
    }
    // update size
    size_ = total_size;
    
    LRUComparator comp;
    sort(clean_nodes.begin(), clean_nodes.end(), comp);

    size_t goal = (options_.cache_limit * options_.cache_evict_ratio)/100;
    size_t evicted_size = 0;
    size_t evicted_count = 0;
    
    for(size_t i = 0; i < clean_nodes.size(); i++) {
        if (evicted_size >= goal) break;

        Node *node = clean_nodes[i];

        // reference counting should keep zero when node map is locked,
        // so nobody outside the cache can modify this node,
        // so it is impossible to be flushed out
        assert(node->ref() == 0 && !node->is_dirty() && !node->is_flushing());

        // one and only one node is erased
        if (nodes_.erase(CacheKey(node->table_name(), node->nid())) != 1) {
            assert(false);
        }

        evicted_size += node->size();
        evicted_count ++;
        delete node;
    }

    // update size
    assert(size_ >= evicted_size);
    size_ -= evicted_size;

    lock.unlock();

    // clear zombies
    if (zombies.size()) {
        delete_nodes(zombies);
    }

#ifdef DEBUG_CACHE
    LOG_TRACE("Total " << total_count << " nodes (" 
        << total_size << " bytes), " 
        << active_count << " active nodes ("
        << active_size << " bytes), "
        << dirty_count << " dirty nodes ("
        << dirty_size << " bytes), "
        << flushing_count << " flushing nodes ("
        << flushing_size << " bytes), "
        << clean_count << " clean nodes ("
        << clean_size << " bytes), "
        << "Evict " << evicted_count << " nodes ("
        << evicted_size << " bytes), "
        << "Delete " << zombies.size() << " zombie nodes");
#endif
}

void Cache::write_back()
{
    while(alive_) {
        Time current = now();
        FirstWriteComparator comparator;
        size_t goal = (options_.cache_limit * 
            options_.cache_writeback_ratio)/100;

        size_t total_size = 0;
        size_t total_count = 0;

        size_t active_size = 0;
        size_t active_count = 0;

        size_t dirty_size = 0;
        size_t dirty_count = 0;

        vector<Node*> expired_nodes;
        size_t expired_size = 0;

        ScopedMutex lock(&nodes_mtx_);

        for(map<CacheKey, Node*>::iterator it = nodes_.begin();
            it != nodes_.end(); it++ ) {
            Node *node = it->second;

            if (!node->is_dead()) {
                size_t sz = node->size();

                total_size += sz;
                total_count ++;
                if (node->ref()) {
                    active_size += sz;
                    active_count ++;
                }

                if (node->is_dirty()) {
                    dirty_size += sz;
                    dirty_count ++;

                    bool expired = interval_us(node->get_first_write_timestamp(),
                        current) > options_.cache_dirty_expire * 1000;

                    // do not write node until last write is completed
                    if ( expired && !node->is_flushing() && node->pin() == 0) {
                        expired_nodes.push_back(node);
                        expired_size += sz;
                    }
                }
            }
        }
        // update size
        size_ = total_size;

        vector<Node*> flushed_nodes;
        size_t flushed_size = 0;

        sort(expired_nodes.begin(), expired_nodes.end(), comparator);

        for (size_t i = 0; i < expired_nodes.size(); i++ ) {
            if (flushed_size >= goal) break;

            Node *node = expired_nodes[i];

            // set write lock on node
            if (node->try_write_lock()) {
                // check again
                if (node->pin() == 0 && !node->is_dead()) {
                    node->set_flushing(true);
                    flushed_nodes.push_back(node);
                    flushed_size += node->size();
                } else {
                    node->unlock();
                }
            }
        }

        // need to flush more dirty pages
        if ((dirty_size - flushed_size) >= (options_.cache_limit * 
            options_.cache_dirty_high_watermark)/100 && flushed_size < goal) {
            vector<Node*> candidates;
            
            for (map<CacheKey, Node*>::iterator it = nodes_.begin();
                it != nodes_.end(); it++ ) {
                Node *node = it->second;

                if (node->is_dirty() && node->pin() == 0 
                    && !node->is_flushing() && !node->is_dead()) {
                    candidates.push_back(node);
                }
            }

            sort(candidates.begin(), candidates.end(), comparator);

            for (size_t i = 0; i < candidates.size(); i++ ) {
                if (flushed_size >= goal) break;

                Node *node = candidates[i];

                // set write lock on node
                if (node->try_write_lock()) {
                    // check again
                    if (node->pin() == 0 && !node->is_dead()) {
                        node->set_flushing(true);
                        flushed_nodes.push_back(node);
                        flushed_size += node->size();
                    } else {
                        node->unlock();
                    }
                }
            }
        }
        lock.unlock();
        
        // flush
        if (flushed_nodes.size()) {
            flush_nodes(flushed_nodes);
        }

#ifdef DEBUG_CACHE        
        LOG_TRACE("Total " << total_count << " nodes (" 
            << total_size << " bytes), " 
            << active_count << " active nodes ("
            << active_size << " bytes), "
            << dirty_count << " dirty nodes ("
            << dirty_size << " bytes), "
            << "Flush " << flushed_nodes.size() << " nodes ("
            << flushed_size << " bytes)");
#endif
        if (need_evict()) {
            evict();
        } else {
            usleep(options_.cache_writeback_interval * 1000);
        }
    }
}

void Cache::flush_nodes(vector<Node*>& nodes)
{
    LOG_TRACE("flush " << nodes.size() << " nodes");

    for (size_t i = 0 ; i < nodes.size(); i++) {
        Node* node = nodes[i];
        bid_t nid = node->nid();

        // TODO: test node is write locked

        TableSettings tbs;
        if (!get_table_settings(node->table_name(), tbs)) {
            assert(false);
        }
        Layout *layout = tbs.layout;
        assert(layout);
       
        Block *block = layout->create(node->size());
        assert(block);
        
        BlockWriter writer(block);
        if (!node->write_to(writer)) {
            assert(false);
        }
        assert(node->size() == block->size());
        node->set_dirty(false);

        // unlock node
        node->unlock();
        
        WriteCompleteContext *context = new WriteCompleteContext();
        context->node = node;
        context->layout = layout;
        context->block =block;
        Callback *cb = new Callback(this, &Cache::write_complete, context);
        layout->async_write(nid, block, cb);
    }
}

void Cache::write_complete(WriteCompleteContext* context, bool succ)
{
    Node *node = context->node;
    assert(node);
    Layout *layout = context->layout;
    assert(layout);
    Block *block = context->block;
    assert(block);

    if (succ) {
        LOG_TRACE("write node table " << node->table_name() << ", nid " << node->nid() << " ok" );
    } else {
        LOG_ERROR("write node table " << node->table_name() << ", nid " << node->nid() << " error");
        // TODO: handle the error
    }

    node->set_flushing(false);
    layout->destroy(block);
    delete context;
}

void Cache::delete_nodes(vector<Node*>& nodes)
{
    LOG_TRACE("delete " << nodes.size() << " nodes");

    for (size_t i = 0; i < nodes.size(); i++) {
        Node* node = nodes[i];
        bid_t nid = node->nid();

        delete node;

        // TODO: check node is stored to layout

        TableSettings tbs;
        if (!get_table_settings(node->table_name(), tbs)) {
            assert(false);
        }
        Layout *layout = tbs.layout;
        assert(layout);
       
        layout->delete_block(nid);
    }
}

void Cache::debug_print(std::ostream& out)
{
    size_t total_size = 0;
    size_t total_count = 0;

    size_t active_size = 0;
    size_t active_count = 0;

    size_t dirty_size = 0;
    size_t dirty_count = 0;

    size_t flushing_size = 0;
    size_t flushing_count = 0;

    size_t clean_size = 0;
    size_t clean_count = 0;

    ScopedMutex lock(&nodes_mtx_);

    for(map<CacheKey, Node*>::iterator it = nodes_.begin();
        it != nodes_.end(); it++ ) {
        Node *node = it->second;
        if (!node->is_dead()) {
            size_t size = node->size();

            total_size += size;
            total_count ++;
            if (node->ref()) {
                active_size += size;
                active_count ++;
            }

            if (node->is_dirty()) {
                dirty_size += size;
                dirty_count ++;
            } else if (node->is_flushing()) {
                flushing_size += size;
                flushing_count ++;
            } else {
                clean_size += size;
                clean_count ++;
            }
        }
    }

    lock.unlock();

    out << "### Dump Cache ###" << endl;
    out << "Total " << total_count << " nodes (" 
        << total_size << " bytes), " 
        << active_count << " active nodes ("
        << active_size << " bytes), "
        << dirty_count << " dirty nodes ("
        << dirty_size << " bytes), "
        << flushing_count << " flushing nodes ("
        << flushing_size << " bytes), "
        << clean_count << " clean nodes ("
        << clean_size << " bytes), "
        << endl;
}

CachedNodeStore::CachedNodeStore(Cache *cache, 
                                 const std::string& table_name,
                                 Layout *layout)
: cache_(cache), table_name_(table_name), layout_(layout)
{
}

CachedNodeStore::~CachedNodeStore()
{
    cache_->del_table(table_name_);
}

bool CachedNodeStore::init(NodeFactory *node_factory)
{
    return cache_->add_table(table_name_, node_factory, layout_);
}
    
void CachedNodeStore::put(bid_t nid, Node* node)
{
    cache_->put(table_name_, nid, node);
}
    
Node* CachedNodeStore::get(bid_t nid)
{
    return cache_->get(table_name_, nid);
}

void CachedNodeStore::flush()
{
    cache_->flush_table(table_name_);
}
