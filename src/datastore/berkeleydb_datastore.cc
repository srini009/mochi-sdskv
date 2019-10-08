// Copyright (c) 2017, Los Alamos National Security, LLC.
// All rights reserved.
#include "berkeleydb_datastore.h"
#include "fs_util.h"
#include "kv-config.h"
#include <sstream>
#include <chrono>
#include <cstring>
#include <iostream>

using namespace std::chrono;

BerkeleyDBDataStore::BerkeleyDBDataStore() :
  AbstractDataStore(false, false) {
  _dbm = NULL;
  _dbenv = NULL;
  _in_memory = false;
};

BerkeleyDBDataStore::BerkeleyDBDataStore(bool eraseOnGet, bool debug) :
  AbstractDataStore(eraseOnGet, debug) {
  _dbm = NULL;
  _dbenv = NULL;
  _in_memory = false;
};
  
BerkeleyDBDataStore::~BerkeleyDBDataStore() {
//  delete _dbm;
  delete _wrapper;
  delete _dbenv;
};

bool BerkeleyDBDataStore::openDatabase(const std::string& db_name, const std::string& db_path) {
  int status = 0;

  _name = db_name;
  _path = db_path;
  std::string fullpath = db_path;
  if(fullpath[fullpath.size()-1] != '/') {
    fullpath += "/";
  }
  fullpath += db_name;

  if(!fullpath.empty()) {
    mkdirs(fullpath.c_str());
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
      _dbenv->open(fullpath.c_str(), flags, 0644);
    }
    _dbenv->set_flags(DB_TXN_WRITE_NOSYNC,1);
    _dbenv->set_flags(DB_TXN_NOSYNC,1);
  }
  catch (DbException &e) {
    std::cerr << "BerkeleyDBDataStore::createDatabase: BerkeleyDB error on environment open = " 
	      << e.what() << std::endl;
    status = 1; // failure
  }
  
  if (status == 0) {
    _wrapper = new DbWrapper(_dbenv, DB_CXX_NO_EXCEPTIONS);
    _dbm = &(_wrapper->_db);

    _dbm->set_bt_compare(&(BerkeleyDBDataStore::compkeys));
  
    uint32_t flags = DB_CREATE | DB_AUTO_COMMIT | DB_THREAD; // Allow database creation
    if (_in_memory) {
      status = _dbm->open(NULL, // txn pointer
			  NULL, // NULL for in-memory DB
			  NULL, // logical DB name
			  DB_BTREE, // DB type (e.g. BTREE, HASH)
			  flags,
			  0);
      if (status == 0) {
	DbMpoolFile *mpf = _dbm->get_mpf();
	mpf->set_flags(DB_MPOOL_NOFILE, 1);
      }
    }
    else {
      status = _dbm->open(NULL, // txn pointer
			  db_name.c_str(), // file name
			  NULL, // logical DB name
			  DB_BTREE, // DB type (e.g. BTREE, HASH)
			  flags,
			  0);
    }
    if (status != 0) { // is this the right test for error?
      std::cerr << "BerkeleyDBDataStore::createDatabase: BerkeleyDB error on DB open" << std::endl;
      std::cerr << "(database name = " << db_name << ", database path = " << db_path << ")" << std::endl;
      std::cerr << "status = " << status << std::endl;
    }
  }
  return (status == 0);
};

void BerkeleyDBDataStore::set_comparison_function(const std::string& name, comparator_fn less) {
    _comp_fun_name = name;
    _wrapper->_less = less;
}

int BerkeleyDBDataStore::put(const void* key, hg_size_t ksize, const void* val, hg_size_t vsize) {
  int status = 0;
  bool success = false;
  Dbt db_key((void*)key, ksize);
  Dbt db_data((void*)val, vsize);
  db_key.set_flags(DB_DBT_USERMEM);
  db_data.set_flags(DB_DBT_USERMEM);
  int flag = _no_overwrite ? DB_NOOVERWRITE : 0;
  status = _dbm->put(NULL, &db_key, &db_data, flag);
  if(status == 0) return SDSKV_SUCCESS;
  if(status == DB_KEYEXIST) return SDSKV_ERR_KEYEXISTS;
  return SDSKV_ERR_PUT;
};

int BerkeleyDBDataStore::put_multi(hg_size_t num_items,
        const void* const* keys,
        const hg_size_t* ksizes,
        const void* const* values,
        const hg_size_t* vsizes)
{
    hg_size_t sk = 0;
    hg_size_t sv = 0;
    for(unsigned i = 0; i < num_items; i++) {
        sk += ksizes[i];
        sv += vsizes[i]+8;
    }
    sk *= 2;
    sv *= 2;
    if(sk % 4 != 0) sk += (4 - (sk % 4));
    if(sv % 4 != 0) sv += (4 - (sv % 4));


    std::vector<char> kbuffer(sk);
    std::vector<char> vbuffer(sv);

    Dbt mkey, mdata;

    mkey.set_ulen(kbuffer.size());
    mkey.set_data(kbuffer.data());
    mkey.set_flags(DB_DBT_USERMEM);

    mdata.set_ulen(vbuffer.size());
    mdata.set_data(vbuffer.data());
    mdata.set_flags(DB_DBT_USERMEM);

    DbMultipleDataBuilder keybuilder(mkey);
    DbMultipleDataBuilder databuilder(mdata);

    for(hg_size_t i = 0; i < num_items; i++) {
        keybuilder.append((void*)keys[i], ksizes[i]);
        databuilder.append((void*)values[i], vsizes[i]);
    }
    int flag = DB_MULTIPLE;
    if(!_no_overwrite) flag |=  DB_OVERWRITE_DUP;
    int status = _dbm->put(NULL, &mkey, &mdata, flag);
    if(status == DB_KEYEXIST) return SDSKV_ERR_KEYEXISTS;
    if(status != 0) return SDSKV_ERR_PUT;
    return SDSKV_SUCCESS;
}

bool BerkeleyDBDataStore::exists(const void* key, hg_size_t size) const {
    Dbt db_key((void*)key, size);
    db_key.set_flags(DB_DBT_USERMEM);
    int status = _dbm->exists(NULL, &db_key, 0);
    return status != DB_NOTFOUND;
}

bool BerkeleyDBDataStore::erase(const data_slice &key) {
    Dbt db_key((void*)key.data(), key.size());
    int status = _dbm->del(NULL, &db_key, 0);
    return status == 0;
}

void BerkeleyDBDataStore::sync() {
    _dbm->sync(0);
}

// In the case where Duplicates::ALLOW, this will return the first
// value found using key.
bool BerkeleyDBDataStore::get(const data_slice &key, data_slice &data) {
  int status = 0;
  bool success = false;

  data.clear();

  Dbt db_key((void*)&(key[0]), uint32_t(key.size()));
  db_key.set_ulen(uint32_t(key.size()));
  Dbt db_data;
  db_key.set_flags(DB_DBT_USERMEM);
  db_data.set_flags(DB_DBT_MALLOC);
  status = _dbm->get(NULL, &db_key, &db_data, 0);

  if (status != DB_NOTFOUND && status != DB_KEYEMPTY) {
    data.resize(db_data.get_size(), 0);
    if(db_data.get_size() > 0) {
        memcpy(&(data[0]), db_data.get_data(), db_data.get_size());
    }
    free(db_data.get_data());
    success = true;
  }
  else {
    //std::cerr << "BerkeleyDBDataStore::get: BerkeleyDB error on Get = " << status << std::endl;
  }
  
  if (success && _eraseOnGet) {
    status = _dbm->del(NULL, &db_key, 0);
    if (status != 0) {
      success = false;
      //std::cerr << "BerkeleyDBDataStore::get: BerkeleyDB error on delete (eraseOnGet) = " << status << std::endl;
    }
  }

  return success;
};

// TODO: To return more than 1 value (when Duplicates::ALLOW), this code should
// use the c_get interface.
bool BerkeleyDBDataStore::get(const data_slice &key, std::vector<data_slice> &data) {
  bool success = false;

  data.clear();
  data_slice value;
  if (get(key, value)) {
    data.push_back(value);
    success = true;
  }
  
  return success;
};

void BerkeleyDBDataStore::set_in_memory(bool enable) {
  _in_memory = enable;
};

std::vector<data_slice> BerkeleyDBDataStore::vlist_keys(
        const data_slice &start, hg_size_t count, const data_slice &prefix) const
{
    std::vector<data_slice> keys;
    Dbc * cursorp;
    Dbt key, data;
    int ret;
    _dbm->cursor(NULL, &cursorp, 0);

    /* 'start' is like RADOS: not inclusive  */
    if (start.size()) {
	    key.set_size(start.size());
	    key.set_data((void *)start.data());
	    ret = cursorp->get(&key, &data, DB_SET_RANGE);
	    if (ret != 0) {
	        cursorp->close();
	        return keys;
	    }
    } else {
        ret = cursorp->get(&key, &data, DB_FIRST);
        if (ret != 0) {
            cursorp->close();
            return keys;
        }
    }

	data_slice k((char*)key.get_data(), ((char*)key.get_data())+key.get_size());
	/* SET_RANGE will return the smallest key greater than or equal to the
	 * requested key, but we want strictly greater than */
    int c = 0;
	if (k != start) {
        c = std::memcmp(prefix.data(), k.data(), prefix.size());
        if(c == 0) {
            keys.push_back(std::move(k));
        }
    }
    while (keys.size() < count && c >= 0) {
	    ret = cursorp->get(&key, &data, DB_NEXT);
	    if (ret !=0 ) break;
        
        data_slice k((char*)key.get_data(), ((char*)key.get_data())+key.get_size());
        c = std::memcmp(prefix.data(), k.data(), prefix.size());
        if(c == 0) {
            keys.push_back(std::move(k));
        }
    }
    cursorp->close();
    return keys;
}

std::vector<std::pair<data_slice,data_slice>> BerkeleyDBDataStore::vlist_keyvals(
        const data_slice &start, hg_size_t count, const data_slice &prefix) const
{
    std::vector<std::pair<data_slice,data_slice>> result;
    Dbc * cursorp;
    Dbt key, data;
    int ret;
    _dbm->cursor(NULL, &cursorp, 0);

    /* 'start' is like RADOS: not inclusive  */
    if (start.size()) {
	    key.set_size(start.size());
	    key.set_data((void *)start.data());
	    ret = cursorp->get(&key, &data, DB_SET_RANGE);
	    if (ret != 0) {
	        cursorp->close();
	        return result;
	    }
    } else {
        ret = cursorp->get(&key, &data, DB_FIRST);
        if (ret != 0) {
            cursorp->close();
            return result;
        }
    }

	data_slice k((char*)key.get_data(), ((char*)key.get_data())+key.get_size());
    data_slice v((char*)data.get_data(), ((char*)data.get_data())+data.get_size());

	/* SET_RANGE will return the smallest key greater than or equal to the
	 * requested key, but we want strictly greater than */
    int c = 0;
	if (k != start) {
        c = std::memcmp(prefix.data(), k.data(), prefix.size());
        if(c == 0) {
            result.push_back(std::make_pair(std::move(k),std::move(v)));
        }
    }
    while (result.size() < count && c >= 0) {
	    ret = cursorp->get(&key, &data, DB_NEXT);
	    if (ret !=0 ) break;
        
        data_slice k((char*)key.get_data(), ((char*)key.get_data())+key.get_size());
        data_slice v((char*)data.get_data(), ((char*)data.get_data())+data.get_size());

        c = std::memcmp(prefix.data(), k.data(), prefix.size());
        if(c == 0) {
            result.push_back(std::make_pair(std::move(k), std::move(v)));
        }
    }
    cursorp->close();
    return result;
}

std::vector<data_slice> BerkeleyDBDataStore::vlist_key_range(
        const data_slice &lower_bound, const data_slice &upper_bound, hg_size_t max_keys) const {
    std::vector<data_slice> result;
    // TODO implement this function
    throw SDSKV_OP_NOT_IMPL;
    return result;
}

std::vector<std::pair<data_slice,data_slice>> BerkeleyDBDataStore::vlist_keyval_range(
        const data_slice &lower_bound, const data_slice &upper_bound, hg_size_t max_keys) const {
    std::vector<std::pair<data_slice,data_slice>> result;
    // TODO implement this function
    throw SDSKV_OP_NOT_IMPL;
    return result;
}

int BerkeleyDBDataStore::compkeys(Db *db, const Dbt *dbt1, const Dbt *dbt2, hg_size_t *locp) {
    DbWrapper* _wrapper = (DbWrapper*)(((char*)db) - offsetof(BerkeleyDBDataStore::DbWrapper, _db));
    if(_wrapper->_less) {
        return (_wrapper->_less)(dbt1->get_data(), dbt1->get_size(), 
                dbt2->get_data(), dbt2->get_size());
    } else {
        hg_size_t s = dbt1->get_size() > dbt2->get_size() ? dbt2->get_size() : dbt1->get_size();
        int c = std::memcmp(dbt1->get_data(), dbt2->get_data(), s);
        if(c != 0) return c;
        if(dbt1->get_size() < dbt2->get_size()) return -1;
        if(dbt1->get_size() > dbt2->get_size()) return 1;
        return 0;
    }
}

#ifdef USE_REMI
remi_fileset_t BerkeleyDBDataStore::create_and_populate_fileset() const {
    remi_fileset_t fileset = REMI_FILESET_NULL;
    std::string local_root = _path;
    int ret;
    if(_path[_path.size()-1] != '/')
        local_root += "/";
    remi_fileset_create("sdskv", local_root.c_str(), &fileset);
    remi_fileset_register_directory(fileset, (_name+"/").c_str());
    //remi_fileset_register_file(fileset, "log.0000000001");
    remi_fileset_register_metadata(fileset, "database_type", "berkeleydb");
    remi_fileset_register_metadata(fileset, "comparison_function", _comp_fun_name.c_str());
    remi_fileset_register_metadata(fileset, "database_name", _name.c_str());
    if(_no_overwrite) {
        remi_fileset_register_metadata(fileset, "no_overwrite", "");
    }
    return fileset;
}
#endif
