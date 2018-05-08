// Copyright (c) 2017, Los Alamos National Security, LLC.
// All rights reserved.
#ifndef datastore_h
#define datastore_h

#include "kv-config.h"
#include "bulk.h"

#include <vector>

enum class Duplicates : int {ALLOW, IGNORE};

class AbstractDataStore {
public:

  typedef int (*comparator_fn)(const void*, size_t, const void*, size_t);
  
  AbstractDataStore();
  AbstractDataStore(Duplicates duplicates, bool eraseOnGet, bool debug);
  virtual ~AbstractDataStore();
  virtual void createDatabase(const std::string& db_name, const std::string& path)=0;
  virtual bool put(const ds_bulk_t &key, const ds_bulk_t &data)=0;
  virtual bool get(const ds_bulk_t &key, ds_bulk_t &data)=0;
  virtual bool get(const ds_bulk_t &key, std::vector<ds_bulk_t> &data)=0;
  virtual bool erase(const ds_bulk_t &key) = 0;
  virtual void set_in_memory(bool enable)=0; // enable/disable in-memory mode (where supported)
  virtual void set_comparison_function(comparator_fn less)=0;

  std::vector<ds_bulk_t> list_keys(
          const ds_bulk_t &start_key, size_t count, const ds_bulk_t& prefix=ds_bulk_t()) {
      return vlist_keys(start_key, count, prefix);
  }

  std::vector<std::pair<ds_bulk_t,ds_bulk_t>> list_keyvals(
          const ds_bulk_t &start_key, size_t count, const ds_bulk_t& prefix=ds_bulk_t()) {
    return vlist_keyvals(start_key, count, prefix);
  }

protected:
  Duplicates _duplicates;
  bool _eraseOnGet;
  bool _debug;
  bool _in_memory;

  virtual std::vector<ds_bulk_t> vlist_keys(
          const ds_bulk_t &start_key, size_t count, const ds_bulk_t& prefix) = 0;
  virtual std::vector<std::pair<ds_bulk_t,ds_bulk_t>> vlist_keyvals(
          const ds_bulk_t &start_key, size_t count, const ds_bulk_t& prefix) = 0;
};

#endif // datastore_h
