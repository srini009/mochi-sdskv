// Copyright (c) 2017, Los Alamos National Security, LLC.
// All rights reserved.
#include "kv-config.h"
#if USE_BWTREE
#include "bwtree.h"
#endif
#include "sds-keyval.h"

#include <boost/functional/hash.hpp>
#include <vector>
#if USE_LEVELDB
#include <leveldb/db.h>
#include <leveldb/env.h>
#elif USE_BDB
#include <db_cxx.h>
#include <dbstl_map.h>
#elif USE_BWTREE
using namespace wangziqi2013::bwtree;
#else
#error "No backend selected at configure time"
#endif

#ifndef datastore_h
#define datastore_h

enum class Duplicates : int {ALLOW, IGNORE};

// implementation is std::vector<char> specific
// typedef is for convenience
typedef std::vector<char> ds_bulk_t;

struct my_hash {
  size_t operator()(const ds_bulk_t &v) const {
    size_t hash = 0;
    boost::hash_range(hash, v.begin(), v.end());
    return hash;
  }
};

struct my_equal {
  bool operator()(const ds_bulk_t &v1, const ds_bulk_t &v2) const {
    return (v1 == v2);
  }
};

struct my_less {
  bool operator()(const ds_bulk_t &v1, const ds_bulk_t &v2) const {
    return (v1 < v2);
  }
};

class AbstractDataStore {
public:
  AbstractDataStore();
  AbstractDataStore(Duplicates duplicates, bool eraseOnGet, bool debug);
  virtual ~AbstractDataStore();
  virtual void createDatabase(std::string db_name)=0;
  virtual bool put(ds_bulk_t &key, ds_bulk_t &data)=0;
  virtual bool get(ds_bulk_t &key, ds_bulk_t &data)=0;
  virtual bool get(ds_bulk_t &key, std::vector<ds_bulk_t> &data)=0;
  virtual void set_in_memory(bool enable)=0; // enable/disable in-memory mode (where supported)
  virtual std::vector<ds_bulk_t>* list(ds_bulk_t &start, size_t count)=0;
protected:
  Duplicates _duplicates;
  bool _eraseOnGet;
  bool _debug;
  bool _in_memory;
};

#if USE_BWTREE
class BwTreeDataStore : public AbstractDataStore {
public:
  BwTreeDataStore();
  BwTreeDataStore(Duplicates duplicates, bool eraseOnGet, bool debug);
  virtual ~BwTreeDataStore();
  virtual void createDatabase(std::string db_name);
  virtual bool put(ds_bulk_t &key, ds_bulk_t &data);
  virtual bool get(ds_bulk_t &key, ds_bulk_t &data);
  virtual bool get(ds_bulk_t &key, std::vector<ds_bulk_t> &data);
  virtual void set_in_memory(bool enable); // a no-op
  virtual std::vector<ds_bulk_t>* list(ds_bulk_t &start, size_t count);
protected:
  BwTree<ds_bulk_t, ds_bulk_t, 
	 my_less, my_equal, my_hash,
	 my_equal, my_hash> *_tree = NULL;
};
#elif USE_LEVELDB
// may want to implement some caching for persistent stores like LevelDB
class LevelDBDataStore : public AbstractDataStore {
public:
  LevelDBDataStore();
  LevelDBDataStore(Duplicates duplicates, bool eraseOnGet, bool debug);
  virtual ~LevelDBDataStore();
  virtual void createDatabase(std::string db_name);
  virtual bool put(ds_bulk_t &key, ds_bulk_t &data);
  virtual bool get(ds_bulk_t &key, ds_bulk_t &data);
  virtual bool get(ds_bulk_t &key, std::vector<ds_bulk_t> &data);
  virtual void set_in_memory(bool enable); // not supported, a no-op
  virtual std::vector<ds_bulk_t>* list(ds_bulk_t &start, size_t count);
protected:
  leveldb::DB *_dbm = NULL;
private:
  std::string toString(ds_bulk_t &key);
  ds_bulk_t fromString(std::string &keystr);
};
#elif USE_BDB
// may want to implement some caching for persistent stores like BerkeleyDB
class BerkeleyDBDataStore : public AbstractDataStore {
public:
  BerkeleyDBDataStore();
  BerkeleyDBDataStore(Duplicates duplicates, bool eraseOnGet, bool debug);
  virtual ~BerkeleyDBDataStore();
  virtual void createDatabase(std::string db_name);
  virtual bool put(ds_bulk_t &key, ds_bulk_t &data);
  virtual bool get(ds_bulk_t &key, ds_bulk_t &data);
  virtual bool get(ds_bulk_t &key, std::vector<ds_bulk_t> &data);
  virtual void set_in_memory(bool enable); // enable/disable in-memory mode
  virtual std::vector<ds_bulk_t>* list(ds_bulk_t &start, size_t count);
protected:
  DbEnv *_dbenv = NULL;
  Db *_dbm = NULL;
};
#else
#error "No datastore backend selected"
#endif

#endif // datastore_h
