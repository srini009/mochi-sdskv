// Copyright (c) 2017, Los Alamos National Security, LLC.
// All rights reserved.
#ifndef datastore_h
#define datastore_h

#include "kv-config.h"
#include "data_slice.h"
#include <margo.h>
#ifdef USE_REMI
#include "remi/remi-common.h"
#endif
#include "sdskv-common.h"

#include <vector>

class AbstractDataStore {
    public:

        typedef int (*comparator_fn)(const void*, hg_size_t, const void*, hg_size_t);

        AbstractDataStore();
        AbstractDataStore(bool eraseOnGet, bool debug);
        virtual ~AbstractDataStore();
        virtual bool openDatabase(const std::string& db_name, const std::string& path)=0;
        virtual int put(const void* kdata, hg_size_t ksize, const void* vdata, hg_size_t vsize)=0;
        virtual int put(const data_slice &key, const data_slice &data) {
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
        virtual int get(const data_slice &key, data_slice &data)=0;
        virtual int get(const void* kdata, hg_size_t ksize, void* vdata, hg_size_t *vsize) {
            data_slice key((const char*)kdata, ksize);
            data_slice val((const char*)vdata, *vsize);
            int ret = get(key, val);
            if(ret == SDSKV_SUCCESS)
                *vsize = val.size();
            return ret;
        }
        virtual bool length(const data_slice &key, size_t& result) {
            data_slice value;
            if(get(key, value) == SDSKV_SUCCESS) {
                result = value.size();
                return true;
            }
            return false;
        }
        virtual bool exists(const void* key, hg_size_t ksize) const = 0;
        virtual bool exists(const data_slice &key) const {
            return exists(key.data(), key.size());
        }
        virtual bool erase(const data_slice &key) = 0;
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

        std::vector<data_slice> list_keys(
                const data_slice &start_key, hg_size_t count, const data_slice& prefix=data_slice()) const {
            return vlist_keys(start_key, count, prefix);
        }

        std::vector<std::pair<data_slice,data_slice>> list_keyvals(
                const data_slice &start_key, hg_size_t count, const data_slice& prefix=data_slice()) const {
            return vlist_keyvals(start_key, count, prefix);
        }

        std::vector<data_slice> list_key_range(
                const data_slice &lower_bound, const data_slice &upper_bound, hg_size_t max_keys=0) const {
            return vlist_key_range(lower_bound, upper_bound, max_keys);
        }

        std::vector<std::pair<data_slice,data_slice>> list_keyval_range(
                const data_slice &lower_bound, const data_slice& upper_bound, hg_size_t max_keys=0) const {
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

        virtual std::vector<data_slice> vlist_keys(
                const data_slice &start_key, hg_size_t count, const data_slice& prefix) const = 0;
        virtual std::vector<std::pair<data_slice,data_slice>> vlist_keyvals(
                const data_slice &start_key, hg_size_t count, const data_slice& prefix) const = 0;
        virtual std::vector<data_slice> vlist_key_range(
                const data_slice &lower_bound, const data_slice &upper_bound, hg_size_t max_keys) const = 0;
        virtual std::vector<std::pair<data_slice,data_slice>> vlist_keyval_range(
                const data_slice &lower_bound, const data_slice& upper_bound, hg_size_t max_keys) const = 0;
};

#endif // datastore_h
