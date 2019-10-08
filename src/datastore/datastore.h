// Copyright (c) 2017, Los Alamos National Security, LLC.
// All rights reserved.
#ifndef datastore_h
#define datastore_h

#include "kv-config.h"
#include "bulk.h"
#include <margo.h>
#ifdef USE_REMI
#include "remi/remi-common.h"
#endif

#include <vector>

class AbstractDataStore {
    public:

        typedef int (*comparator_fn)(const void*, hg_size_t, const void*, hg_size_t);

        AbstractDataStore();
        AbstractDataStore(bool eraseOnGet, bool debug);
        virtual ~AbstractDataStore();
        virtual bool openDatabase(const std::string& db_name, const std::string& path)=0;
        virtual int put(const void* kdata, hg_size_t ksize, const void* vdata, hg_size_t vsize)=0;
        virtual int put(const ds_bulk_t &key, const ds_bulk_t &data) {
            return put(key.data(), key.size(), data.data(), data.size());
        }
        virtual int put_multi(hg_size_t num_items,
                              const void* const* keys,
                              const hg_size_t* ksizes,
                              const void* const* values,
                              const hg_size_t* vsizes)
        {
            int ret = 0;
            for(hg_size_t i=0; i < num_items; i++) {
                int r = put(keys[i], ksizes[i], values[i], vsizes[i]);
                ret = ret == 0 ? r : 0;
            }
            return ret;
        }
        virtual bool get(const ds_bulk_t &key, ds_bulk_t &data)=0;
        virtual bool get(const ds_bulk_t &key, std::vector<ds_bulk_t> &data)=0;
        virtual bool exists(const void* key, hg_size_t ksize) const = 0;
        virtual bool exists(const ds_bulk_t &key) const {
            return exists(key.data(), key.size());
        }
        virtual bool erase(const ds_bulk_t &key) = 0;
        virtual void set_in_memory(bool enable)=0; // enable/disable in-memory mode (where supported)
        virtual void set_comparison_function(const std::string& name, comparator_fn less)=0;
        virtual void set_no_overwrite()=0;
        virtual void sync() = 0;

#ifdef USE_REMI
        virtual remi_fileset_t create_and_populate_fileset() const = 0;
#endif

        const std::string& get_path() const {
            return _path;
        }

        const std::string& get_name() const {
            return _name;
        }

        const std::string& get_comparison_function_name() const {
            return _comp_fun_name;
        }

        std::vector<ds_bulk_t> list_keys(
                const ds_bulk_t &start_key, hg_size_t count, const ds_bulk_t& prefix=ds_bulk_t()) const {
            return vlist_keys(start_key, count, prefix);
        }

        std::vector<std::pair<ds_bulk_t,ds_bulk_t>> list_keyvals(
                const ds_bulk_t &start_key, hg_size_t count, const ds_bulk_t& prefix=ds_bulk_t()) const {
            return vlist_keyvals(start_key, count, prefix);
        }

        std::vector<ds_bulk_t> list_key_range(
                const ds_bulk_t &lower_bound, const ds_bulk_t &upper_bound, hg_size_t max_keys=0) const {
            return vlist_key_range(lower_bound, upper_bound, max_keys);
        }

        std::vector<std::pair<ds_bulk_t,ds_bulk_t>> list_keyval_range(
                const ds_bulk_t &lower_bound, const ds_bulk_t& upper_bound, hg_size_t max_keys=0) const {
            return vlist_keyval_range(lower_bound, upper_bound, max_keys);
        }

    protected:
        std::string _path;
        std::string _name;
        std::string _comp_fun_name;
        bool _no_overwrite = false;
        bool _eraseOnGet;
        bool _debug;
        bool _in_memory;

        virtual std::vector<ds_bulk_t> vlist_keys(
                const ds_bulk_t &start_key, hg_size_t count, const ds_bulk_t& prefix) const = 0;
        virtual std::vector<std::pair<ds_bulk_t,ds_bulk_t>> vlist_keyvals(
                const ds_bulk_t &start_key, hg_size_t count, const ds_bulk_t& prefix) const = 0;
        virtual std::vector<ds_bulk_t> vlist_key_range(
                const ds_bulk_t &lower_bound, const ds_bulk_t &upper_bound, hg_size_t max_keys) const = 0;
        virtual std::vector<std::pair<ds_bulk_t,ds_bulk_t>> vlist_keyval_range(
                const ds_bulk_t &lower_bound, const ds_bulk_t& upper_bound, hg_size_t max_keys) const = 0;
};

#endif // datastore_h
