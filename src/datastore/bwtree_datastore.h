// Copyright (c) 2017, Los Alamos National Security, LLC.
// All rights reserved.
#ifndef bwtree_datastore_h
#define bwtree_datastore_h

#include "kv-config.h"
#include "bwtree.h"
#include "datastore.h"
#include "sdskv-common.h"

using namespace wangziqi2013::bwtree;

class BwTreeDataStore : public AbstractDataStore {
  public:
    BwTreeDataStore();
    BwTreeDataStore(Duplicates duplicates, bool eraseOnGet, bool debug);
    virtual ~BwTreeDataStore();
    virtual bool openDatabase(const std::string& db_name,
                              const std::string& path);
    virtual bool put(const ds_bulk_t& key, const ds_bulk_t& data);
    virtual bool get(const ds_bulk_t& key, ds_bulk_t& data);
    virtual bool get(const ds_bulk_t& key, std::vector<ds_bulk_t>& data);
    virtual bool exists(const ds_bulk_t& key)
    {
        ds_bulk_t data;
        return get(key, data);
    }
    virtual bool erase(const ds_bulk_t& key);
    virtual void set_in_memory(bool enable); // a no-op
    virtual void set_comparison_function(comparator_fn less);
    virtual void set_no_overwrite() { _no_overwrite = true; }

  protected:
    virtual std::vector<ds_bulk_t> vlist_keys(const ds_bulk_t& start,
                                              size_t           count,
                                              const ds_bulk_t& prefix) const;
    virtual std::vector<std::pair<ds_bulk_t, ds_bulk_t>>
                                   vlist_keyvals(const ds_bulk_t& start_key,
                                                 size_t           count,
                                                 const ds_bulk_t& prefix) const;
    virtual std::vector<ds_bulk_t> vlist_key_range(const ds_bulk_t& lower_bound,
                                                   const ds_bulk_t& upper_bound,
                                                   size_t max_keys) const;
    virtual std::vector<std::pair<ds_bulk_t, ds_bulk_t>>
                          vlist_keyval_range(const ds_bulk_t& lower_bound,
                                             const ds_bulk_t& upper_bound,
                                             size_t           max_keys) const;
    BwTree<ds_bulk_t,
           ds_bulk_t,
           ds_bulk_less,
           ds_bulk_equal,
           ds_bulk_hash,
           ds_bulk_equal,
           ds_bulk_hash>* _tree
        = NULL;
    bool _no_overwrite = false;
};

#endif // bwtree_datastore_h
