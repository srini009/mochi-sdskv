// Copyright (c) 2017, Los Alamos National Security, LLC.
// All rights reserved.
#ifndef bdb_datastore_h
#define bdb_datastore_h

#include "kv-config.h"
#include "datastore/datastore.h"
#include <db_cxx.h>
#include <dbstl_map.h>
#include "sdskv-common.h"

// may want to implement some caching for persistent stores like BerkeleyDB
class BerkeleyDBDataStore : public AbstractDataStore {

    private:
        struct DbWrapper {
            Db _db;
            AbstractDataStore::comparator_fn _less;

            template<typename ... T>
            DbWrapper(T&&... args) :
            _db(std::forward<T>(args)...), _less(nullptr) {}
        };

        static int compkeys(Db *db, const Dbt *dbt1, const Dbt *dbt2, hg_size_t *locp);

    public:
        BerkeleyDBDataStore();
        BerkeleyDBDataStore(bool eraseOnGet, bool debug);
        virtual ~BerkeleyDBDataStore();
        virtual bool openDatabase(const std::string& db_name, const std::string& path) override;
        virtual int put(const void* key, hg_size_t ksize, const void* value, hg_size_t vsize) override;
        virtual int put_multi(hg_size_t num_items,
                               const void* const* keys,
                               const hg_size_t* ksizes,
                               const void* const* values,
                               const hg_size_t* vsizes) override;
        virtual int get(const void* kdata, hg_size_t ksize, void* vdata, hg_size_t *vsize) override {
            data_slice key((const char*)kdata, ksize);
            data_slice val((const char*)vdata, *vsize);
            int ret = get(key, val);
            if(ret == SDSKV_SUCCESS)
                *vsize = val.size();
            return ret;
        }
        virtual int get(const data_slice &key, data_slice &data) override;
        virtual bool exists(const void* key, hg_size_t ksize) const override;
        virtual bool erase(const data_slice &key) override;
        virtual void set_in_memory(bool enable) override; // enable/disable in-memory mode
        virtual void set_comparison_function(const std::string& name, comparator_fn less) override;
        virtual void set_no_overwrite() override {
            _no_overwrite = true;
        }
        virtual void sync() override;
#ifdef USE_REMI
        virtual remi_fileset_t create_and_populate_fileset() const override;
#endif
    protected:

        virtual void vlist_keys(
                uint64_t max_count,
                const data_slice &start,
                const data_slice &prefix,
                std::vector<data_slice>& result) const override;

        virtual void vlist_keyvals(
                uint64_t max_count,
                const data_slice &start_key,
                const data_slice& prefix,
                std::vector<std::pair<data_slice,data_slice>>& result) const override;

        virtual void vlist_key_range(
                const data_slice &lower_bound, const data_slice &upper_bound,
                std::vector<data_slice>& result) const override;

        virtual void vlist_keyval_range(
                const data_slice &lower_bound, const data_slice& upper_bound,
                std::vector<std::pair<data_slice,data_slice>>& result) const override;

        DbEnv *_dbenv = nullptr;
        Db *_dbm = nullptr;
        DbWrapper* _wrapper = nullptr;
};

#endif // bdb_datastore_h
