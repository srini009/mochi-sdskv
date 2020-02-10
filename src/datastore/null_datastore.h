#ifndef null_datastore_h
#define null_datastore_h

#include <cstring>
#include "kv-config.h"
#include "data_slice.h"
#include "datastore/datastore.h"

class NullDataStore : public AbstractDataStore {

    public:

        NullDataStore()
        : AbstractDataStore() {
        }

        NullDataStore(bool eraseOnGet, bool debug)
        : AbstractDataStore(eraseOnGet, debug) {
        }

        ~NullDataStore() {
        }

        virtual bool openDatabase(const std::string& db_name, const std::string& path) override {
            _name = db_name;
            _path = path;
            return true;
        }

        virtual void sync() override {}

        virtual int put(const data_slice &key, const data_slice &data) override {
            return SDSKV_SUCCESS;
        }

        virtual int put(const void* key, hg_size_t ksize, const void* value, hg_size_t vsize) override {
            return SDSKV_SUCCESS;
        }

        virtual int get(const data_slice &key, data_slice &data) override {
            return SDSKV_ERR_UNKNOWN_KEY;
        }

        virtual bool exists(const data_slice& key) const override {
            return false;
        }

        virtual bool exists(const void* key, hg_size_t ksize) const override {
            return false;
        }

        virtual bool erase(const data_slice &key) override {
            return false;
        }

        virtual void set_in_memory(bool enable) override {
        }

        virtual void set_comparison_function(const std::string& name, comparator_fn less) override {
        }

        virtual void set_no_overwrite() override {
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
        }

        virtual void vlist_keyvals(
                uint64_t max_count,
                const data_slice &start_key,
                const data_slice &prefix,
                std::vector<std::pair<data_slice,data_slice>>& result) const override {
        }

        virtual void vlist_key_range(
                const data_slice &lower_bound,
                const data_slice &upper_bound,
                std::vector<data_slice>& result) const override {
            throw SDSKV_OP_NOT_IMPL;
        }

        virtual void vlist_keyval_range(
                const data_slice &lower_bound,
                const data_slice& upper_bound,
                std::vector<std::pair<data_slice,data_slice>>& result) const override {
            throw SDSKV_OP_NOT_IMPL;
        }
};

#endif
