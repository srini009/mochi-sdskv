#ifndef null_datastore_h
#define null_datastore_h

#include <cstring>
#include "kv-config.h"
#include "bulk.h"
#include "datastore/datastore.h"

class NullDataStore : public AbstractDataStore {

  public:
    NullDataStore() : AbstractDataStore() {}

    NullDataStore(bool eraseOnGet, bool debug)
        : AbstractDataStore(eraseOnGet, debug)
    {}

    ~NullDataStore() {}

    virtual bool openDatabase(const std::string& db_name,
                              const std::string& path) override
    {
        _name = db_name;
        _path = path;
        return true;
    }

    virtual void sync() override {}

    virtual int put(const ds_bulk_t& key, const ds_bulk_t& data) override
    {
        return SDSKV_SUCCESS;
    }

    virtual int put(ds_bulk_t&& key, ds_bulk_t&& data) override
    {
        return SDSKV_SUCCESS;
    }

    virtual int put(const void* key,
                    hg_size_t   ksize,
                    const void* value,
                    hg_size_t   vsize) override
    {
        return SDSKV_SUCCESS;
    }

    virtual bool get(const ds_bulk_t& key, ds_bulk_t& data) override
    {
        return true;
    }

    virtual bool get(const ds_bulk_t&        key,
                     std::vector<ds_bulk_t>& values) override
    {
        return true;
    }

    virtual bool exists(const ds_bulk_t& key) const override { return false; }

    virtual bool exists(const void* key, hg_size_t ksize) const override
    {
        return false;
    }

    virtual bool erase(const ds_bulk_t& key) override { return false; }

    virtual void set_in_memory(bool enable) override {}

    virtual void set_comparison_function(const std::string& name,
                                         comparator_fn      less) override
    {}

    virtual void set_no_overwrite() override {}

#ifdef USE_REMI
    virtual remi_fileset_t create_and_populate_fileset() const override
    {
        return REMI_FILESET_NULL;
    }
#endif

  protected:
    virtual std::vector<ds_bulk_t>
    vlist_keys(const ds_bulk_t& start_key,
               hg_size_t        count,
               const ds_bulk_t& prefix) const override
    {
        std::vector<ds_bulk_t> result;
        return result;
    }

    virtual std::vector<std::pair<ds_bulk_t, ds_bulk_t>>
    vlist_keyvals(const ds_bulk_t& start_key,
                  hg_size_t        count,
                  const ds_bulk_t& prefix) const override
    {
        std::vector<std::pair<ds_bulk_t, ds_bulk_t>> result;
        return result;
    }

    virtual std::vector<ds_bulk_t>
    vlist_key_range(const ds_bulk_t& lower_bound,
                    const ds_bulk_t& upper_bound,
                    hg_size_t        max_keys) const override
    {
        std::vector<ds_bulk_t> result;
        return result;
    }

    virtual std::vector<std::pair<ds_bulk_t, ds_bulk_t>>
    vlist_keyval_range(const ds_bulk_t& lower_bound,
                       const ds_bulk_t& upper_bound,
                       hg_size_t        max_keys) const override
    {
        std::vector<std::pair<ds_bulk_t, ds_bulk_t>> result;
        return result;
    }
};

#endif
