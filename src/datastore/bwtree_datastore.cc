// Copyright (c) 2017, Los Alamos National Security, LLC.
// All rights reserved.
#include "bwtree_datastore.h"
#include "kv-config.h"
#include <chrono>
#include <iostream>

using namespace std::chrono;

BwTreeDataStore::BwTreeDataStore()
    : AbstractDataStore(Duplicates::IGNORE, false, false)
{
    _tree = NULL;
};

BwTreeDataStore::BwTreeDataStore(Duplicates duplicates,
                                 bool       eraseOnGet,
                                 bool       debug)
    : AbstractDataStore(duplicates, eraseOnGet, debug)
{
    _tree = NULL;
};

BwTreeDataStore::~BwTreeDataStore()
{
    // deleting BwTree can cause core dump
#if 0 // letting leak, for now
    delete _tree;
#endif
};

bool BwTreeDataStore::openDatabase(const std::string& db_name,
                                   const std::string& path)
{
    _tree = new BwTree<ds_bulk_t, ds_bulk_t, ds_bulk_less, ds_bulk_equal,
                       ds_bulk_hash, ds_bulk_equal, ds_bulk_hash>();
    if (_debug) {
        _tree->SetDebugLogging(1);
    } else {
        _tree->SetDebugLogging(0);
    }
    _tree->UpdateThreadLocal(1);
    _tree->AssignGCID(0);
    return true;
};

void BwTreeDataStore::set_comparison_function(comparator_fn less)
{
    // TODO
}

bool BwTreeDataStore::put(const ds_bulk_t& key, const ds_bulk_t& data)
{
    std::vector<ds_bulk_t> values;
    bool                   success = false;

    if (_no_overwrite) {
        if (exists(key)) return false;
    }

    if (!_tree) return false;

    if (_duplicates == Duplicates::ALLOW) {
        success = _tree->Insert(key, data);
    } else if (_duplicates == Duplicates::IGNORE) {
        _tree->GetValue(key, values);
        bool duplicate_key = (values.size() != 0);
        if (duplicate_key) {
            // silently ignore
            success = true;
        } else {
            success = _tree->Insert(key, data);
        }
    } else {
        std::cerr << "BwTreeDataStore::put: Unexpected Duplicates option = "
                  << int32_t(_duplicates) << std::endl;
    }

    return success;
};

bool BwTreeDataStore::get(const ds_bulk_t& key, ds_bulk_t& data)
{
    std::vector<ds_bulk_t> values;
    bool                   success = false;

    _tree->GetValue(key, values);
    if (values.size() == 1) {
        data    = std::move(values.front());
        success = true;
    } else if (values.size() > 1) {
        // this should only happen if duplicates are allowed
        if (_duplicates == Duplicates::ALLOW) {
            data    = std::move(values.front()); // caller is asking for just 1
            success = true;
        }
    }

    if (success && _eraseOnGet) {
        bool status = _tree->Delete(key, data);
        if (!status) {
            success = false;
            std::cerr << "BwTreeDataStore::get: BwTree error on delete "
                         "(eraseOnGet) = "
                      << status << std::endl;
        }
    }

    return success;
};

bool BwTreeDataStore::erase(const ds_bulk_t& key)
{
    ds_bulk_t data;
    if (!get(key, data)) return false;
    return _tree->Delete(key, data);
}

bool BwTreeDataStore::get(const ds_bulk_t& key, std::vector<ds_bulk_t>& data)
{
    std::vector<ds_bulk_t> values;
    bool                   success = false;

    _tree->GetValue(key, values);
    if (values.size() > 1) {
        // this should only happen if duplicates are allowed
        if (_duplicates == Duplicates::ALLOW) {
            data    = std::move(values);
            success = true;
        }
    } else {
        data    = std::move(values);
        success = true;
    }

    return success;
};

void BwTreeDataStore::set_in_memory(bool enable){};

std::vector<ds_bulk_t> BwTreeDataStore::vlist_keys(
    const ds_bulk_t& start, size_t count, const ds_bulk_t& prefix) const
{
    std::vector<ds_bulk_t> keys;
    throw SDSKV_OP_NOT_IMPL;
#if 0
    auto it = _tree->Begin(start);
    while (it.IsEnd() == false) {
	/* BUG: bwtree doesn't support "list keys" or "get a key" */
	//keys.push_back(it.GetLeafNode());
    }
#endif
    return keys;
}

std::vector<std::pair<ds_bulk_t, ds_bulk_t>> BwTreeDataStore::vlist_keyvals(
    const ds_bulk_t& start, size_t count, const ds_bulk_t& prefix) const
{
    std::vector<std::pair<ds_bulk_t, ds_bulk_t>> keyvals;
    throw SDSKV_OP_NOT_IMPL;
#if 0
    auto it = _tree->Begin(start);
    while (it.IsEnd() == false) {
	/* BUG: bwtree doesn't support "list keys" or "get a key" */
    }
#endif

    return keyvals;
}

std::vector<ds_bulk_t>
BwTreeDataStore::vlist_key_range(const ds_bulk_t& lower_bound,
                                 const ds_bulk_t& upper_bound,
                                 size_t           max_keys) const
{
    std::vector<ds_bulk_t> result;
    throw SDSKV_OP_NOT_IMPL;
    // TODO implement this function
    return result;
}

std::vector<std::pair<ds_bulk_t, ds_bulk_t>>
BwTreeDataStore::vlist_keyval_range(const ds_bulk_t& lower_bound,
                                    const ds_bulk_t& upper_bound,
                                    size_t           max_keys) const
{
    std::vector<std::pair<ds_bulk_t, ds_bulk_t>> result;
    throw SDSKV_OP_NOT_IMPL;
    // TODO implement this function
    return result;
}
