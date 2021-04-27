// Copyright (c) 2017, Los Alamos National Security, LLC.
// All rights reserved.
#include "leveldb_datastore.h"
#include "fs_util.h"
#include "kv-config.h"
#include <cstring>
#include <chrono>
#include <iostream>
#include <sstream>

using namespace std::chrono;

LevelDBDataStore::LevelDBDataStore()
    : AbstractDataStore(false, false), _less(nullptr), _keycmp(this)
{
    _dbm = NULL;
};

LevelDBDataStore::LevelDBDataStore(bool eraseOnGet, bool debug)
    : AbstractDataStore(eraseOnGet, debug), _less(nullptr), _keycmp(this)
{
    _dbm = NULL;
};

std::string LevelDBDataStore::toString(const ds_bulk_t& bulk_val)
{
    std::string str_val(bulk_val.begin(), bulk_val.end());
    return str_val;
};

std::string LevelDBDataStore::toString(const char* buf, hg_size_t buf_size)
{
    std::string str_val(buf, buf_size);
    return str_val;
};

ds_bulk_t LevelDBDataStore::fromString(const std::string& str_val)
{
    ds_bulk_t bulk_val(str_val.begin(), str_val.end());
    return bulk_val;
};

LevelDBDataStore::~LevelDBDataStore()
{
    delete _dbm;
    // leveldb::Env::Shutdown(); // Riak version only
};

void LevelDBDataStore::sync() {}

bool LevelDBDataStore::openDatabase(const std::string& db_name,
                                    const std::string& db_path)
{
    _name = db_name;
    _path = db_path;

    leveldb::Options options;
    leveldb::Status  status;

    if (!db_path.empty()) { mkdirs(db_path.c_str()); }
    options.comparator        = &_keycmp;
    options.create_if_missing = true;
    std::string fullname      = db_path;
    if (!fullname.empty()) fullname += std::string("/");
    fullname += db_name;
    status = leveldb::DB::Open(options, fullname, &_dbm);

    if (!status.ok()) {
        // error
        std::cerr
            << "LevelDBDataStore::createDatabase: LevelDB error on Open = "
            << status.ToString() << std::endl;
        return false;
    }
    return true;
};

void LevelDBDataStore::set_comparison_function(const std::string& name,
                                               comparator_fn      less)
{
    _comp_fun_name = name;
    _less          = less;
}

int LevelDBDataStore::put(const void* key,
                          hg_size_t   ksize,
                          const void* value,
                          hg_size_t   vsize)
{
    leveldb::Status status;
    bool            success = false;

    if (_no_overwrite) {
        if (exists(key, ksize)) return SDSKV_ERR_KEYEXISTS;
    }

    status = _dbm->Put(leveldb::WriteOptions(),
                       leveldb::Slice((const char*)key, ksize),
                       leveldb::Slice((const char*)value, vsize));
    if (status.ok()) return SDSKV_SUCCESS;
    return SDSKV_ERR_PUT;
};

bool LevelDBDataStore::erase(const ds_bulk_t& key)
{
    leveldb::Status status;
    status = _dbm->Delete(leveldb::WriteOptions(), toString(key));
    return status.ok();
}

bool LevelDBDataStore::exists(const void* key, hg_size_t ksize) const
{
    leveldb::Status status;
    std::string     value;
    status = _dbm->Get(leveldb::ReadOptions(),
                       leveldb::Slice((const char*)key, ksize), &value);
    return status.ok();
}

bool LevelDBDataStore::get(const ds_bulk_t& key, ds_bulk_t& data)
{
    leveldb::Status status;
    bool            success = false;

    // high_resolution_clock::time_point start = high_resolution_clock::now();
    data.clear();
    std::string value;
    status = _dbm->Get(leveldb::ReadOptions(), toString(key), &value);
    if (status.ok()) {
        data    = fromString(value);
        success = true;
    } else if (!status.IsNotFound()) {
        std::cerr << "LevelDBDataStore::get: LevelDB error on Get = "
                  << status.ToString() << std::endl;
    }
    //  uint64_t elapsed =
    //  duration_cast<microseconds>(high_resolution_clock::now()-start).count();
    //  std::cout << "LevelDBDataStore::get time = " << elapsed << "
    //  microseconds" << std::endl;

    return success;
};

bool LevelDBDataStore::get(const ds_bulk_t& key, std::vector<ds_bulk_t>& data)
{
    bool success = false;

    data.clear();
    ds_bulk_t value;
    if (get(key, value)) {
        data.push_back(value);
        success = true;
    }

    return success;
};

void LevelDBDataStore::set_in_memory(bool enable){};

std::vector<ds_bulk_t> LevelDBDataStore::vlist_keys(
    const ds_bulk_t& start, hg_size_t count, const ds_bulk_t& prefix) const
{
    std::vector<ds_bulk_t> keys;

    leveldb::Iterator* it = _dbm->NewIterator(leveldb::ReadOptions());
    leveldb::Slice     start_slice(start.data(), start.size());

    int c = 0;

    if (start.size() > 0) {
        it->Seek(start_slice);
        /* we treat 'start' the way RADOS treats it: excluding it from returned
         * keys. LevelDB treats start inclusively, so skip over it if we found
         * an exact match */
        if (it->Valid() && (start.size() == it->key().size())
            && (memcmp(it->key().data(), start.data(), start.size()) == 0))
            it->Next();
    } else {
        it->SeekToFirst();
    }
    /* note: iterator initialized above, not in for loop */
    for (; it->Valid() && keys.size() < count; it->Next()) {
        ds_bulk_t k(it->key().size());
        memcpy(k.data(), it->key().data(), it->key().size());
        c = std::memcmp(prefix.data(), k.data(), prefix.size());
        if (c == 0) {
            keys.push_back(std::move(k));
        } else if (c < 0) {
            break;
        }
    }
    delete it;
    return keys;
}

std::vector<std::pair<ds_bulk_t, ds_bulk_t>> LevelDBDataStore::vlist_keyvals(
    const ds_bulk_t& start, hg_size_t count, const ds_bulk_t& prefix) const
{
    std::vector<std::pair<ds_bulk_t, ds_bulk_t>> result;

    leveldb::Iterator* it = _dbm->NewIterator(leveldb::ReadOptions());
    leveldb::Slice     start_slice(start.data(), start.size());

    int c = 0;

    if (start.size() > 0) {
        it->Seek(start_slice);
        /* we treat 'start' the way RADOS treats it: excluding it from returned
         * keys. LevelDB treats start inclusively, so skip over it if we found
         * an exact match */
        if (it->Valid() && (start.size() == it->key().size())
            && (memcmp(it->key().data(), start.data(), start.size()) == 0))
            it->Next();
    } else {
        it->SeekToFirst();
    }
    /* note: iterator initialized above, not in for loop */
    for (; it->Valid() && result.size() < count; it->Next()) {
        ds_bulk_t k(it->key().size());
        ds_bulk_t v(it->value().size());
        memcpy(k.data(), it->key().data(), it->key().size());
        memcpy(v.data(), it->value().data(), it->value().size());

        c = std::memcmp(prefix.data(), k.data(), prefix.size());
        if (c == 0) {
            result.push_back(std::make_pair(std::move(k), std::move(v)));
        } else if (c < 0) {
            break;
        }
    }
    delete it;
    return result;
}

std::vector<ds_bulk_t>
LevelDBDataStore::vlist_key_range(const ds_bulk_t& lower_bound,
                                  const ds_bulk_t& upper_bound,
                                  hg_size_t        max_keys) const
{
    std::vector<ds_bulk_t> result;
    // TODO implement this function
    throw SDSKV_OP_NOT_IMPL;
    return result;
}

std::vector<std::pair<ds_bulk_t, ds_bulk_t>>
LevelDBDataStore::vlist_keyval_range(const ds_bulk_t& lower_bound,
                                     const ds_bulk_t& upper_bound,
                                     hg_size_t        max_keys) const
{
    std::vector<std::pair<ds_bulk_t, ds_bulk_t>> result;
    // TODO implement this function
    throw SDSKV_OP_NOT_IMPL;
    return result;
}

#ifdef USE_REMI
remi_fileset_t LevelDBDataStore::create_and_populate_fileset() const
{
    remi_fileset_t fileset    = REMI_FILESET_NULL;
    std::string    local_root = _path;
    int            ret;
    if (_path[_path.size() - 1] != '/') local_root += "/";
    remi_fileset_create("sdskv", local_root.c_str(), &fileset);
    remi_fileset_register_directory(fileset, (_name + "/").c_str());
    remi_fileset_register_metadata(fileset, "database_type", "leveldb");
    remi_fileset_register_metadata(fileset, "comparison_function",
                                   _comp_fun_name.c_str());
    remi_fileset_register_metadata(fileset, "database_name", _name.c_str());
    if (_no_overwrite) {
        remi_fileset_register_metadata(fileset, "no_overwrite", "");
    }
    return fileset;
}
#endif
