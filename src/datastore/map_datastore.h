// Copyright (c) 2017, Los Alamos National Security, LLC.
// All rights reserved.
#ifndef map_datastore_h
#define map_datastore_h

#include <map>
#include <cstring>
#include "kv-config.h"
#include "datastore/datastore.h"

class MapDataStore : public AbstractDataStore {

    private:

        struct keycmp {
            MapDataStore* _store;
            keycmp(MapDataStore* store)
                : _store(store) {}
            bool operator()(const data_slice& a, const data_slice& b) const {
                if(_store->_less)
                    return _store->_less((const void*)a.data(),
                            a.size(), (const void*)b.data(), b.size()) < 0;
                else
                    return std::less<data_slice>()(a,b);
            }
        };

    public:

        MapDataStore()
            : AbstractDataStore(), _less(nullptr), _map(keycmp(this)) {
            ABT_rwlock_create(&_map_lock);
        }

        MapDataStore(bool eraseOnGet, bool debug)
            : AbstractDataStore(eraseOnGet, debug), _less(nullptr), _map(keycmp(this)){
            ABT_rwlock_create(&_map_lock);
        }

        ~MapDataStore() {
            ABT_rwlock_free(&_map_lock);
        }

        virtual bool openDatabase(const std::string& db_name, const std::string& path) override {
            _name = db_name;
            _path = path;
            ABT_rwlock_wrlock(_map_lock);
            _map.clear();
            ABT_rwlock_unlock(_map_lock);
            return true;
        }

        virtual void sync() override {}

        virtual int put(const data_slice &key, const data_slice &data) override {
            ABT_rwlock_wrlock(_map_lock);
            auto x = _map.count(key);
            if(_no_overwrite && (x != 0)) {
                ABT_rwlock_unlock(_map_lock);
                return SDSKV_ERR_KEYEXISTS;
            }
            _map.insert(std::make_pair(key,data));
            ABT_rwlock_unlock(_map_lock);
            return SDSKV_SUCCESS;
        }

        virtual int put(const void* key, hg_size_t ksize, const void* value, hg_size_t vsize) override {
            data_slice k((const char*)key, ((const char*)key)+ksize);
            if(vsize != 0) {
                data_slice v((const char*)value, ((const char*)value)+vsize);
                return put(k, v);
            } else {
                data_slice v;
                return put(k, v);
            }
        }

        virtual int get(const data_slice &key, data_slice &data) override {
            ABT_rwlock_rdlock(_map_lock);
            auto it = _map.find(key);
            int ret = SDSKV_SUCCESS;
            if(it == _map.end()) {
                ret = SDSKV_ERR_UNKNOWN_KEY;
            } else {
                if(data.size() >= it->second.size()) {
                    memcpy(data.data(), it->second.data(), it->second.size());
                    data.resize(it->second.size());
                } else if(data.size() == 0) {
                    data = it->second;
                } else {
                    ret = SDSKV_ERR_SIZE;
                }
            }
            ABT_rwlock_unlock(_map_lock);
            return ret;
        }

        virtual bool length(const data_slice &key, size_t& result) override {
            bool r = true;
            ABT_rwlock_rdlock(_map_lock);
            auto it = _map.find(key);
            if(it == _map.end()) {
                r = false;
            } else {
                result = it->second.size();
            }
            ABT_rwlock_unlock(_map_lock);
            return r;
        }

        virtual bool exists(const data_slice& key) const override {
            ABT_rwlock_rdlock(_map_lock);
            bool e = _map.count(key) > 0;
            ABT_rwlock_unlock(_map_lock);
            return e;
        }

        virtual bool exists(const void* key, hg_size_t ksize) const override {
            return exists(data_slice((const char*)key, ((const char*)key)+ksize));
        }

        virtual bool erase(const data_slice &key) override {
            ABT_rwlock_wrlock(_map_lock);
            bool b = _map.find(key) != _map.end();
            _map.erase(key);
            ABT_rwlock_unlock(_map_lock);
            return b;
        }

        virtual void set_in_memory(bool enable) override {
            _in_memory = enable;
        }

        virtual void set_comparison_function(const std::string& name, comparator_fn less) override {
           _comp_fun_name = name;
           _less = less; 
        }

        virtual void set_no_overwrite() override {
            _no_overwrite = true;
        }

#ifdef USE_REMI
        virtual remi_fileset_t create_and_populate_fileset() const override {
            return REMI_FILESET_NULL;
        }
#endif

    protected:

        virtual void vlist_keys(
                uint64_t max_count,
                const data_slice &start_key,
                const data_slice &prefix,
                std::vector<data_slice>& result) const override {
            auto count = result.size();
            bool usermem = count != 0;
            if(!usermem) count = max_count;
            ABT_rwlock_rdlock(_map_lock);
            auto it = (start_key.size() > 0) ? _map.upper_bound(start_key) : _map.begin();
            unsigned i=0;
            bool size_error = false;
            while(i < count && it != _map.end()) {
                const auto& p = *it;
                if(prefix.size() > p.first.size()) {
                    ++it;
                    continue;
                }
                int c = std::memcmp(prefix.data(), p.first.data(), prefix.size());
                if(c > 0) {
                    ++it;
                    continue;
                } else if(c < 0) {
                    break; // we have exceeded prefix
                }
                if(usermem) {
                    auto& key_slice = result[i];
                    if(key_slice.size() < p.first.size()) {
                        size_error = true;
                    } else {
                        std::memcpy(key_slice.data(), p.first.data(), p.first.size());
                    }
                    key_slice.resize(p.first.size());
                } else {
                    result.push_back(p.first);
                }
                ++it;
                ++i;
            }
            result.resize(i);
            ABT_rwlock_unlock(_map_lock);
            if(size_error) throw SDSKV_ERR_SIZE;
        }

        virtual void vlist_keyvals(
                uint64_t max_count,
                const data_slice &start_key,
                const data_slice &prefix,
                std::vector<std::pair<data_slice,data_slice>>& result) const override {
            auto count = result.size();
            bool usermem = count != 0;
            if(!usermem) count = max_count;
            ABT_rwlock_rdlock(_map_lock);
            auto it = (start_key.size() > 0) ? _map.upper_bound(start_key) : _map.begin();
            unsigned i = 0;
            bool size_error = false;
            while(i < count && it != _map.end()) {
                const auto& p = *it;
                if(prefix.size() > p.first.size()) {
                    ++it;
                    continue;
                }
                int c = std::memcmp(prefix.data(), p.first.data(), prefix.size());
                if(c > 0) {
                    ++it;
                    continue;
                } else if(c < 0) {
                    break; // we have exceeded prefix
                }
                if(usermem) {
                    auto& key_slice = result[i].first;
                    auto& val_slice = result[i].second;
                    if(key_slice.size() < p.first.size()
                    || val_slice.size() < p.second.size()) {
                        size_error = true;
                    } else {
                        std::memcpy(key_slice.data(), p.first.data(), p.first.size());
                        if(p.second.size()) std::memcpy(val_slice.data(), p.second.data(), p.second.size());
                    }
                    key_slice.resize(p.first.size());
                    val_slice.resize(p.second.size());
                } else {
                    result.push_back(p);
                }
                ++it;
                ++i;
            }
            ABT_rwlock_unlock(_map_lock);
            result.resize(i);
            if(size_error) throw SDSKV_ERR_SIZE;
        }

        virtual void vlist_key_range(
                const data_slice &lower_bound,
                const data_slice &upper_bound,
                std::vector<data_slice>& result) const override {
            /*
            ABT_rwlock_rdlock(_map_lock);
            result.resize(0);
            decltype(_map.begin()) it, ub;
            // get the first element that goes immediately after lower_bound
            it = _map.upper_bound(lower_bound);
            if(it == _map.end()) {
                return;
            }
            // get the element that goes immediately before upper bound
            ub = _map.lower_bound(upper_bound);
            if(ub->first != upper_bound) ub++;

            while(it != ub) {
                result.push_back(it->second);
                it++;
                if(max_keys != 0 && result.size() == max_keys)
                    break;
            }
            ABT_rwlock_unlock(_map_lock);
            */
            throw SDSKV_OP_NOT_IMPL;
        }

        virtual void vlist_keyval_range(
                const data_slice &lower_bound,
                const data_slice& upper_bound,
                std::vector<std::pair<data_slice,data_slice>>& result) const override {
            /*
            ABT_rwlock_rdlock(_map_lock);
            result.resize(0);
            decltype(_map.begin()) it, ub;
            // get the first element that goes immediately after lower_bound
            it = _map.upper_bound(lower_bound);
            if(it == _map.end()) {
                return;
            }
            // get the element that goes immediately before upper bound
            ub = _map.lower_bound(upper_bound);
            if(ub->first != upper_bound) ub++;

            while(it != ub) {
                result.emplace_back(it->first,it->second);
                it++;
                if(max_keys != 0 && result.size() == max_keys)
                    break;
            }
            ABT_rwlock_unlock(_map_lock);
            */
            throw SDSKV_OP_NOT_IMPL;
        }

    private:
        AbstractDataStore::comparator_fn _less;
        std::map<data_slice, data_slice, keycmp> _map;
        ABT_rwlock _map_lock;
};

#endif
