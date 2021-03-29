#ifndef datastore_factory_h
#define datastore_factory_h
#include <string>

#ifdef SDSKV
#include "sdskv-common.h"
#else
#include "sds-keyval.h"
#endif
#include "datastore.h"

#include "map_datastore.h"
#include "null_datastore.h"

#ifdef USE_BWTREE
#include "bwtree_datastore.h"
#endif

#ifdef USE_BDB
#include "berkeleydb_datastore.h"
#endif

#ifdef USE_LEVELDB
#include "leveldb_datastore.h"
#endif

class datastore_factory {

    static AbstractDataStore* open_map_datastore(
            const std::string& name, const std::string& path) {
        auto db = new MapDataStore();
        if(db->openDatabase(name, path)) {
            return db;
        } else {
            delete db;
            return nullptr;
        }
    }

    static AbstractDataStore* open_null_datastore(
            const std::string& name, const std::string& path) {
        auto db = new NullDataStore();
        if(db->openDatabase(name, path)) {
            return db;
        } else {
            delete db;
            return nullptr;
        }
    }

    static AbstractDataStore* open_bwtree_datastore(
            const std::string& name, const std::string& path) {
#ifdef USE_BWTREE
        auto db = new BwTreeDataStore();
        if(db->openDatabase(name, path)) {
            return db;
        } else {
            delete db;
            return nullptr;
        }
#else
        return nullptr;
#endif
    }

    static AbstractDataStore* open_berkeleydb_datastore(
            const std::string& name, const std::string& path) {
#ifdef USE_BDB
        auto db = new BerkeleyDBDataStore();
        if(db->openDatabase(name, path)) {
            return db;
        } else {
            delete db;
            return nullptr;
        }
#else
        return nullptr;
#endif
    }

    static AbstractDataStore* open_leveldb_datastore(
            const std::string& name, const std::string& path) {
#ifdef USE_LEVELDB
        auto db = new LevelDBDataStore();
        if(db->openDatabase(name, path)) {
            return db;
        } else {
            delete db;
            return nullptr;
        }
#else
        return nullptr;
#endif
    }

    public:

#ifdef SDSKV
    static AbstractDataStore* open_datastore(
            sdskv_db_type_t type,
            const std::string& name,
            const std::string& path)
#else
    static AbstractDataStore* open_datastore(
            kv_db_type_t type,
            const std::string& name="db",
            const std::string& path="db")
#endif
    {
        switch(type) {
            case KVDB_NULL:
                return open_null_datastore(name, path);
            case KVDB_MAP:
                return open_map_datastore(name, path);
            case KVDB_BWTREE:
                return open_bwtree_datastore(name, path);
            case KVDB_LEVELDB:
                return open_leveldb_datastore(name, path);
            case KVDB_BERKELEYDB:
                return open_berkeleydb_datastore(name, path);
        }
        return nullptr;
    };

};

#endif
