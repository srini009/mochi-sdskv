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
            if(vsize != 0) {
                data_slice k((const char*)key, ((const char*)key)+ksize);
                data_slice v((const char*)value, ((const char*)value)+vsize);
                return put(k, v);
            } else {
                data_slice k((const char*)key, ((const char*)key)+ksize);
                data_slice v;
                return put(std::move(k), std::move(v));
            }
        }

        virtual bool get(const data_slice &key, data_slice &data) override {
            ABT_rwlock_rdlock(_map_lock);
            auto it = _map.find(key);
            if(it == _map.end()) {
                ABT_rwlock_unlock(_map_lock);
                return false;
            }
            data = it->second;
            ABT_rwlock_unlock(_map_lock);
            return true;
        }

        virtual bool get(const data_slice &key, std::vector<data_slice>& values) override {
            values.clear();
            values.resize(1);
            return get(key, values[0]);
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

        virtual std::vector<data_slice> vlist_keys(
                const data_slice &start_key, hg_size_t count, const data_slice &prefix) const override {
            ABT_rwlock_rdlock(_map_lock);
            std::vector<data_slice> result;
            decltype(_map.begin()) it;
            if(start_key.size() > 0) {
                it = _map.upper_bound(start_key);
            } else {
                it = _map.begin();
            }
            while(result.size() < count && it != _map.end()) {
                const auto& p = *it;
                if(prefix.size() > p.first.size()) continue;
                int c = std::memcmp(prefix.data(), p.first.data(), prefix.size());
                if(c == 0) {
                    result.push_back(p.first);
                } else if(c < 0) {
                    break; // we have exceeded prefix
                }
                it++;
            }
            ABT_rwlock_unlock(_map_lock);
            return result;
        }

        virtual std::vector<std::pair<data_slice,data_slice>> vlist_keyvals(
                const data_slice &start_key, hg_size_t count, const data_slice &prefix) const override {
            ABT_rwlock_rdlock(_map_lock);
            std::vector<std::pair<data_slice,data_slice>> result;
            decltype(_map.begin()) it;
            if(start_key.size() > 0) {
                it = _map.upper_bound(start_key);
            } else {
                it = _map.begin();
            }
            while(result.size() < count && it != _map.end()) {
                const auto& p = *it;
                if(prefix.size() > p.first.size()) continue;
                int c = std::memcmp(prefix.data(), p.first.data(), prefix.size());
                if(c == 0) {
                    result.push_back(p);
                } else if(c < 0) {
                    break; // we have exceeded prefix
                }
                it++;
            }
            ABT_rwlock_unlock(_map_lock);
            return result;
        }

        virtual std::vector<data_slice> vlist_key_range(
                const data_slice &lower_bound, const data_slice &upper_bound, hg_size_t max_keys) const override {
            ABT_rwlock_rdlock(_map_lock);
            std::vector<data_slice> result;
            decltype(_map.begin()) it, ub;
            // get the first element that goes immediately after lower_bound
            it = _map.upper_bound(lower_bound);
            if(it == _map.end()) {
                return result;
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
            return result;
        }

        virtual std::vector<std::pair<data_slice,data_slice>> vlist_keyval_range(
                const data_slice &lower_bound, const data_slice& upper_bound, hg_size_t max_keys) const override {
            ABT_rwlock_rdlock(_map_lock);
            std::vector<std::pair<data_slice,data_slice>> result;
            decltype(_map.begin()) it, ub;
            // get the first element that goes immediately after lower_bound
            it = _map.upper_bound(lower_bound);
            if(it == _map.end()) {
                return result;
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
            return result;
        }

    private:
        AbstractDataStore::comparator_fn _less;
        std::map<data_slice, data_slice, keycmp> _map;
        ABT_rwlock _map_lock;
};

#endif
