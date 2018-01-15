// Copyright (c) 2017, Los Alamos National Security, LLC.
// All rights reserved.
#include "datastore.h"
#include "kv-config.h"
#include <chrono>
#include <iostream>

#include <boost/filesystem.hpp>

using namespace std::chrono;

AbstractDataStore::AbstractDataStore() {
  _duplicates = Duplicates::IGNORE;
  _eraseOnGet = false;
  _debug = false;
  _in_memory = false;
};

AbstractDataStore::AbstractDataStore(Duplicates duplicates, bool eraseOnGet, bool debug) {
  _duplicates = duplicates;
  _eraseOnGet = eraseOnGet;
  _debug = debug;
  _in_memory = false;
};

AbstractDataStore::~AbstractDataStore()
{};

#if USE_BWTREE  
BwTreeDataStore::BwTreeDataStore() :
  AbstractDataStore(Duplicates::IGNORE, false, false) {
  _tree = NULL;
};

BwTreeDataStore::BwTreeDataStore(Duplicates duplicates, bool eraseOnGet, bool debug) :
  AbstractDataStore(duplicates, eraseOnGet, debug) {
  _tree = NULL;
};

BwTreeDataStore::~BwTreeDataStore() {
  // deleting BwTree can cause core dump
  delete _tree;
};

void BwTreeDataStore::createDatabase(std::string db_name) {
  _tree = new BwTree<ds_bulk_t, ds_bulk_t, 
		     my_less, my_equal, my_hash,
		     my_equal, my_hash>();
  if (_debug) {
    _tree->SetDebugLogging(1);
  }
  else {
    _tree->SetDebugLogging(0);
  }
  _tree->UpdateThreadLocal(1);
  _tree->AssignGCID(0);
};

bool BwTreeDataStore::put(ds_bulk_t &key, ds_bulk_t &data) {
  std::vector<ds_bulk_t> values;
  bool success = false;
  
  if (_duplicates == Duplicates::ALLOW) {
    success = _tree->Insert(key, data);
  }
  else if (_duplicates == Duplicates::IGNORE) {
    _tree->GetValue(key, values);
    bool duplicate_key = (values.size() != 0);
    if (duplicate_key) {
      // silently ignore
      success = true;
    }
    else {
      success = _tree->Insert(key, data);
    }
  }
  else {
    std::cerr << "BwTreeDataStore::put: Unexpected Duplicates option = " << int32_t(_duplicates) << std::endl;
  }
  
  return success;
};

bool BwTreeDataStore::get(ds_bulk_t &key, ds_bulk_t &data) {
  std::vector<ds_bulk_t> values;
  bool success = false;

  _tree->GetValue(key, values);
  if (values.size() == 1) {
    data = values.front();
    success = true;
  }
  else if (values.size() > 1) {
    // this should only happen if duplicates are allowed
    if (_duplicates == Duplicates::ALLOW) {
      data = values.front(); // caller is asking for just 1
      success = true;
    }
  }

  if (success && _eraseOnGet) {
    bool status = _tree->Delete(key, data);
    if (!status) {
      success = false;
      std::cerr << "BwTreeDataStore::get: BwTree error on delete (eraseOnGet) = " << status << std::endl;
    }
  }

  return success;
};

bool BwTreeDataStore::get(ds_bulk_t &key, std::vector<ds_bulk_t> &data) {
  std::vector<ds_bulk_t> values;
  bool success = false;

  _tree->GetValue(key, values);
  if (values.size() > 1) {
    // this should only happen if duplicates are allowed
    if (_duplicates == Duplicates::ALLOW) {
      data = values;
      success = true;
    }
  }
  else {
    data = values;
    success = true;
  }
  
  return success;
};

void BwTreeDataStore::BwTreeDataStore::set_in_memory(bool enable)
{};

std::vector<ds_bulk_t>* BwTreeDataStore::BwTreeDataStore::list(ds_bulk_t &start, size_t count)
{
    auto keys = new std::vector<ds_bulk_t>;
    auto it = _tree->Begin(start);
    while (it.IsEnd() == false) {
	/* BUG: bwtree doesn't support "list keys" or "get a key" */
	//keys->push_back(it.GetLeafNode());
    }

    return keys;
}
#elif USE_LEVELDB
LevelDBDataStore::LevelDBDataStore() :
  AbstractDataStore(Duplicates::IGNORE, false, false) {
  _dbm = NULL;
};

LevelDBDataStore::LevelDBDataStore(Duplicates duplicates, bool eraseOnGet, bool debug) :
  AbstractDataStore(duplicates, eraseOnGet, debug) {
  _dbm = NULL;
};
  
std::string LevelDBDataStore::toString(ds_bulk_t &bulk_val) {
  std::string str_val(bulk_val.begin(), bulk_val.end());
  return str_val;
};

ds_bulk_t LevelDBDataStore::fromString(std::string &str_val) {
  ds_bulk_t bulk_val(str_val.begin(), str_val.end());
  return bulk_val;
};

LevelDBDataStore::~LevelDBDataStore() {
  delete _dbm;
  //leveldb::Env::Shutdown(); // Riak version only
};

void LevelDBDataStore::createDatabase(std::string db_name) {
  leveldb::Options options;
  leveldb::Status status;
  
  // db_name assumed to include the full path (e.g. /var/data/db.dat)
  boost::filesystem::path p(db_name);
  std::string basepath = p.parent_path().string();
  if (!basepath.empty()) {
    boost::filesystem::create_directories(basepath);
  }

  options.create_if_missing = true;
  status = leveldb::DB::Open(options, db_name, &_dbm);
  
  if (!status.ok()) {
    // error
    std::cerr << "LevelDBDataStore::createDatabase: LevelDB error on Open = " << status.ToString() << std::endl;
  }
  assert(status.ok()); // fall over
  
  // debugging support?
};

bool LevelDBDataStore::put(ds_bulk_t &key, ds_bulk_t &data) {
  leveldb::Status status;
  bool success = false;
  
  high_resolution_clock::time_point start = high_resolution_clock::now();
  // IGNORE case deals with redundant puts (where key/value is the same). In LevelDB a
  // redundant put simply overwrites previous value which is fine when key/value is the same.
  if (_duplicates == Duplicates::IGNORE) {
    status = _dbm->Put(leveldb::WriteOptions(), toString(key), toString(data));
    if (status.ok()) {
      success = true;
    }
    else {
      std::cerr << "LevelDBDataStore::put: LevelDB error on Put = " << status.ToString() << std::endl;
    }
  }
  else if (_duplicates == Duplicates::ALLOW) {
    std::cerr << "LevelDBDataStore::put: Duplicates::ALLOW set, LevelDB does not support duplicates" << std::endl;
  }
  else {
    std::cerr << "LevelDBDataStore::put: Unexpected Duplicates option = " << int32_t(_duplicates) << std::endl;
  }
  uint64_t elapsed = duration_cast<microseconds>(high_resolution_clock::now()-start).count();
  std::cout << "LevelDBDataStore::put time = " << elapsed << " microseconds" << std::endl;

  return success;
};

bool LevelDBDataStore::get(ds_bulk_t &key, ds_bulk_t &data) {
  leveldb::Status status;
  bool success = false;

  high_resolution_clock::time_point start = high_resolution_clock::now();
  data.clear();
  std::string value;
  status = _dbm->Get(leveldb::ReadOptions(), toString(key), &value);
  if (status.ok()) {
    data = fromString(value);
    success = true;
  }
  else if (!status.IsNotFound()) {
    std::cerr << "LevelDBDataStore::get: LevelDB error on Get = " << status.ToString() << std::endl;
  }
  uint64_t elapsed = duration_cast<microseconds>(high_resolution_clock::now()-start).count();
  std::cout << "LevelDBDataStore::get time = " << elapsed << " microseconds" << std::endl;

  return success;
};

bool LevelDBDataStore::get(ds_bulk_t &key, std::vector<ds_bulk_t> &data) {
  bool success = false;

  data.clear();
  ds_bulk_t value;
  if (get(key, value)) {
    data.push_back(value);
    success = true;
  }
  
  return success;
};

void LevelDBDataStore::LevelDBDataStore::set_in_memory(bool enable)
{};

std::vector<ds_bulk_t>* LevelDBDataStore::LevelDBDataStore::list(ds_bulk_t &start, size_t count)
{
    auto keys = new std::vector<ds_bulk_t>;

    leveldb::Iterator *it = _dbm->NewIterator(leveldb::ReadOptions());
    ds_bulk_t k;
    for (it->SeekToFirst(); it->Valid(); it->Next() ) {
	k.resize(it->key().size());
	memcpy(&(k[0]), it->key().data(), it->key().size() );
	keys->push_back(k);
    }
    return keys;
}
#elif USE_BDB
BerkeleyDBDataStore::BerkeleyDBDataStore() :
  AbstractDataStore(Duplicates::IGNORE, false, false) {
  _dbm = NULL;
  _dbenv = NULL;
};

BerkeleyDBDataStore::BerkeleyDBDataStore(Duplicates duplicates, bool eraseOnGet, bool debug) :
  AbstractDataStore(duplicates, eraseOnGet, debug) {
  _dbm = NULL;
  _dbenv = NULL;
};
  
BerkeleyDBDataStore::~BerkeleyDBDataStore() {
  delete _dbm;
  delete _dbenv;
};

void BerkeleyDBDataStore::createDatabase(std::string db_name) {
  int status = 0;

  // db_name assumed to include the full path (e.g. /var/data/db.dat)
  boost::filesystem::path path(db_name);
  std::string basepath = path.parent_path().string();
  std::string dbname = path.filename().string();
  if (!basepath.empty()) {
    boost::filesystem::create_directories(basepath);
  }

  // initialize the environment
  uint32_t flags = 0;
  if (_in_memory) {
    // not sure if we want all of these for in_memory
    flags =
      DB_CREATE      | // Create the environment if it does not exist
      DB_PRIVATE     |
      DB_RECOVER     | // Run normal recovery.
      DB_INIT_LOCK   | // Initialize the locking subsystem
      DB_INIT_LOG    | // Initialize the logging subsystem
      DB_INIT_TXN    | // Initialize the transactional subsystem. This
      DB_THREAD      | // Cause the environment to be free-threaded
      DB_AUTO_COMMIT |
      DB_INIT_MPOOL;   // Initialize the memory pool (in-memory cache)
  }
  else {
    flags =
      DB_CREATE      | // Create the environment if it does not exist
      DB_PRIVATE     |
      DB_RECOVER     | // Run normal recovery.
      DB_INIT_LOCK   | // Initialize the locking subsystem
      DB_INIT_LOG    | // Initialize the logging subsystem
      DB_INIT_TXN    | // Initialize the transactional subsystem. This
      DB_THREAD      | // Cause the environment to be free-threaded
      DB_AUTO_COMMIT |
      DB_INIT_MPOOL;   // Initialize the memory pool (in-memory cache)
  }

  try {
    // create and open the environment
    uint32_t flag = DB_CXX_NO_EXCEPTIONS;
    int scratch_size = 1; // 1GB cache
    _dbenv = new DbEnv(flag);
    _dbenv->set_error_stream(&std::cerr);
    _dbenv->set_cachesize(scratch_size, 0, 0);
    if (_in_memory) {
      _dbenv->log_set_config(DB_LOG_IN_MEMORY, 1);
      _dbenv->set_lg_bsize(scratch_size * 1024 * 1024 * 1024); // in GB
      _dbenv->open(NULL, flags, 0);
    }
    else {
      _dbenv->set_lk_detect(DB_LOCK_MINWRITE);
      _dbenv->open(basepath.c_str(), flags, 0644);
    }
  }
  catch (DbException &e) {
    std::cerr << "BerkeleyDBDataStore::createDatabase: BerkeleyDB error on environment open = " 
	      << e.what() << std::endl;
    status = 1; // failure
  }
  
  if (status == 0) {
    _dbm = new Db(_dbenv, DB_CXX_NO_EXCEPTIONS);

    if (_duplicates == Duplicates::ALLOW) {
      _dbm->set_flags(DB_DUP); // Allow duplicate keys
    }
  
    uint32_t flags = DB_CREATE | DB_AUTO_COMMIT | DB_THREAD; // Allow database creation
    if (_in_memory) {
      status = _dbm->open(NULL, // txn pointer
			  NULL, // NULL for in-memory DB
			  NULL, // logical DB name
			  DB_HASH, // DB type (e.g. BTREE, HASH)
			  flags,
			  0);
      if (status == 0) {
	DbMpoolFile *mpf = _dbm->get_mpf();
	mpf->set_flags(DB_MPOOL_NOFILE, 1);
      }
    }
    else {
      status = _dbm->open(NULL, // txn pointer
			  dbname.c_str(), // file name
			  NULL, // logical DB name
			  DB_HASH, // DB type (e.g. BTREE, HASH)
			  flags,
			  0);
    }
    if (status != 0) { // is this the right test for error?
      std::cerr << "BerkeleyDBDataStore::createDatabase: BerkeleyDB error on DB open" << std::endl;
    }
  }
  assert(status == 0); // fall over

  // debugging support?
};

bool BerkeleyDBDataStore::put(ds_bulk_t &key, ds_bulk_t &data) {
  int status = 0;
  bool success = false;
  
  // IGNORE case deals with redundant puts (where key/value is the same). In BerkeleyDB a
  // redundant may overwrite previous value which is fine when key/value is the same.
  // ALLOW case deals with actual duplicates (where key is the same but value is different).
  // This option might be used when eraseOnGet is set (e.g. ParSplice hotpoint use case).
  if (_duplicates == Duplicates::IGNORE || _duplicates == Duplicates::ALLOW) {
    Dbt db_key(&(key[0]), uint32_t(key.size()));
    Dbt db_data(&(data[0]), uint32_t(data.size()));
    db_key.set_flags(DB_DBT_USERMEM);
    db_data.set_flags(DB_DBT_USERMEM);
    uint32_t flags = DB_NOOVERWRITE; // to simply overwrite value, don't use this flag
    status = _dbm->put(NULL, &db_key, &db_data, flags);
    if (status == 0 || 
	(_duplicates == Duplicates::IGNORE && status == DB_KEYEXIST)) {
      success = true;
    }
    else {
      std::cerr << "BerkeleyDBDataStore::put: BerkeleyDB error on put = " << status << std::endl;
    }
  }
  else {
    std::cerr << "BerkeleyDBDataStore::put: Unexpected Duplicates option = " << int32_t(_duplicates) << std::endl;
  }

  return success;
};

// In the case where Duplicates::ALLOW, this will return the first
// value found using key.
bool BerkeleyDBDataStore::get(ds_bulk_t &key, ds_bulk_t &data) {
  int status = 0;
  bool success = false;

  data.clear();

  Dbt db_key(&(key[0]), uint32_t(key.size()));
  Dbt db_data;
  db_key.set_flags(DB_DBT_USERMEM);
  db_data.set_flags(DB_DBT_REALLOC);
  status = _dbm->get(NULL, &db_key, &db_data, 0);

  if (status != DB_NOTFOUND && status != DB_KEYEMPTY && db_data.get_size() > 0) {
    data.resize(db_data.get_size(), 0);
    memcpy(&(data[0]), db_data.get_data(), db_data.get_size());
    free(db_data.get_data());
    success = true;
  }
  else {
    std::cerr << "BerkeleyDBDataStore::get: BerkeleyDB error on Get = " << status << std::endl;
  }
  
  if (success && _eraseOnGet) {
    status = _dbm->del(NULL, &db_key, 0);
    if (status != 0) {
      success = false;
      std::cerr << "BerkeleyDBDataStore::get: BerkeleyDB error on delete (eraseOnGet) = " << status << std::endl;
    }
  }

  return success;
};

// TODO: To return more than 1 value (when Duplicates::ALLOW), this code should
// use the c_get interface.
bool BerkeleyDBDataStore::get(ds_bulk_t &key, std::vector<ds_bulk_t> &data) {
  bool success = false;

  data.clear();
  ds_bulk_t value;
  if (get(key, value)) {
    data.push_back(value);
    success = true;
  }
  
  return success;
};

void BerkeleyDBDataStore::BerkeleyDBDataStore::set_in_memory(bool enable) {
  _in_memory = enable;
};

std::vector<ds_bulk_t>* BerkeleyDBDataStore::BerkeleyDBDataStore::list(ds_bulk_t &start, size_t count)
{
    auto keys = new std::vector<ds_bulk_t>;
    Dbc * cursorp;
    Dbt key, data;
    ds_bulk_t k;
    int ret;
    _dbm->cursor(NULL, &cursorp, DB_SET_RANGE);
    while (ret = cursorp->get(&key, &data, DB_NEXT) == 0) {
	k.resize(key.get_size());
	memcpy(&(k[0]), key.get_data(), key.get_size());
	/* I hope this is a deep copy! */
	keys->push_back(k);
    }
    return keys;
}
#else
#error "No backend for datastore selected"
#endif
