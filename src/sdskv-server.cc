/*
 * (C) 2015 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "kv-config.h"
#include <map>
#include <iostream>
#include <unordered_map>
#include <unordered_set>
#include <sstream>
#ifdef USE_REMI
    #include <remi/remi-client.h>
    #include <remi/remi-server.h>
#endif
#define SDSKV
#include "datastore/datastore_factory.h"
#include "sdskv-rpc-types.h"
#include "sdskv-server.h"

#ifdef USE_SYMBIOMON
#include <symbiomon/symbiomon-metric.h>
#include <symbiomon/symbiomon-common.h>
#include <symbiomon/symbiomon-server.h>
#include <unistd.h>
#endif

#include <dlfcn.h>

#include <json/json.h>

/* Useful construct and macros to simplify code */

/* Executes a function upon destruction */
template <typename F> struct scoped_call {
    F _f;
    scoped_call(const F& f) : _f(f) {}
    scoped_call(F&& f) : _f(std::move(f)) {}
    ~scoped_call() { _f(); }
};

/* Build a scoped_call from a lambda */
template <typename F> inline scoped_call<F> at_exit(F&& f)
{
    return scoped_call<F>(std::forward<F>(f));
}

#define DEFER(__name__, __action__) \
    auto __defered_##__name__ = at_exit([&]() { __action__; })

#define ENSURE_MARGO_DESTROY DEFER(margo_destroy, margo_destroy(handle))

#define ENSURE_MARGO_RESPOND DEFER(margo_respond, margo_respond(handle, &out))

#define ENSURE_MARGO_FREE_INPUT \
    DEFER(margo_free_input, margo_free_input(handle, &in))

#define SDSKV_LOG_ERROR(__mid__, __msg__, ...) \
    margo_error(__mid__, "%s:%d: " __msg__, __func__, __LINE__, ##__VA_ARGS__)

#define FIND_MID_AND_PROVIDER                                              \
    margo_instance_id     mid;                                             \
    sdskv_provider_t      provider;                                        \
    const struct hg_info* info;                                            \
    do {                                                                   \
        mid = margo_hg_handle_get_instance(handle);                        \
        if (!mid) {                                                        \
            margo_critical(                                                \
                0, "%s:%d: could not get margo instance from RPC handle",  \
                __func__, __LINE__);                                       \
            exit(-1);                                                      \
        }                                                                  \
        info     = margo_get_info(handle);                                 \
        provider = (sdskv_provider_t)margo_registered_data(mid, info->id); \
        if (!provider) {                                                   \
            SDSKV_LOG_ERROR(mid, "could not find provider with id %d",     \
                            info->id);                                     \
            out.ret = SDSKV_ERR_UNKNOWN_PR;                                \
            return;                                                        \
        }                                                                  \
    } while (0)

#define GET_INPUT                                                            \
    do {                                                                     \
        hret = margo_get_input(handle, &in);                                 \
        if (hret != HG_SUCCESS) {                                            \
            SDSKV_LOG_ERROR(mid, "margo_get_input failed (ret = %d)", hret); \
            out.ret = SDSKV_MAKE_HG_ERROR(hret);                             \
            return;                                                          \
        }                                                                    \
    } while (0)

#define FIND_DATABASE                                                          \
    ABT_rwlock_rdlock(provider->lock);                                         \
    auto it = provider->databases.find(in.db_id);                              \
    if (it == provider->databases.end()) {                                     \
        ABT_rwlock_unlock(provider->lock);                                     \
        out.ret = SDSKV_ERR_UNKNOWN_DB;                                        \
        SDSKV_LOG_ERROR(mid, "could not find database with id %lu", in.db_id); \
        return;                                                                \
    }                                                                          \
    auto db = it->second;                                                      \
    ABT_rwlock_unlock(provider->lock)

struct sdskv_server_context_t {
    margo_instance_id mid;

    std::unordered_map<sdskv_database_id_t, AbstractDataStore*> databases;
    std::map<std::string, sdskv_database_id_t>                  name2id;
    std::map<sdskv_database_id_t, std::string>                  id2name;
    std::map<std::string, sdskv_compare_fn>                     compfunctions;

#ifdef USE_REMI
    remi_client_t                    remi_client;
    remi_provider_t                  remi_provider;
    sdskv_pre_migration_callback_fn  pre_migration_callback;
    sdskv_post_migration_callback_fn post_migration_callback;
    void*                            migration_uargs;
#endif
#ifdef USE_SYMBIOMON
    symbiomon_provider_t metric_provider;
    uint8_t provider_id;
    symbiomon_metric_t put_latency;
    symbiomon_metric_t put_num_entrants;
    symbiomon_metric_t put_data_size;
    symbiomon_metric_t put_packed_latency;
    symbiomon_metric_t put_packed_batch_size;
    symbiomon_metric_t put_packed_data_size;
    symbiomon_metric_t putpacked_num_entrants;
#endif

    ABT_rwlock lock; // write-locked during migration, read-locked by all other
    // operations. There should be something better to avoid locking everything
    // but we are going with that for simplicity for now.

    hg_id_t sdskv_open_id;
    hg_id_t sdskv_count_databases_id;
    hg_id_t sdskv_list_databases_id;
    hg_id_t sdskv_put_id;
    hg_id_t sdskv_put_multi_id;
    hg_id_t sdskv_put_packed_id;
    hg_id_t sdskv_bulk_put_id;
    hg_id_t sdskv_get_id;
    hg_id_t sdskv_get_multi_id;
    hg_id_t sdskv_get_packed_id;
    hg_id_t sdskv_exists_id;
    hg_id_t sdskv_exists_multi_id;
    hg_id_t sdskv_erase_id;
    hg_id_t sdskv_erase_multi_id;
    hg_id_t sdskv_length_id;
    hg_id_t sdskv_length_multi_id;
    hg_id_t sdskv_length_packed_id;
    hg_id_t sdskv_bulk_get_id;
    hg_id_t sdskv_list_keys_id;
    hg_id_t sdskv_list_keyvals_id;
    /* migration */
    hg_id_t sdskv_migrate_keys_id;
    hg_id_t sdskv_migrate_key_range_id;
    hg_id_t sdskv_migrate_keys_prefixed_id;
    hg_id_t sdskv_migrate_all_keys_id;
    hg_id_t sdskv_migrate_database_id;

    Json::Value json_cfg;
};

DECLARE_MARGO_RPC_HANDLER(sdskv_open_ult)
DECLARE_MARGO_RPC_HANDLER(sdskv_count_db_ult)
DECLARE_MARGO_RPC_HANDLER(sdskv_list_db_ult)
DECLARE_MARGO_RPC_HANDLER(sdskv_put_ult)
DECLARE_MARGO_RPC_HANDLER(sdskv_put_multi_ult)
DECLARE_MARGO_RPC_HANDLER(sdskv_put_packed_ult)
DECLARE_MARGO_RPC_HANDLER(sdskv_length_ult)
DECLARE_MARGO_RPC_HANDLER(sdskv_length_multi_ult)
DECLARE_MARGO_RPC_HANDLER(sdskv_length_packed_ult)
DECLARE_MARGO_RPC_HANDLER(sdskv_get_ult)
DECLARE_MARGO_RPC_HANDLER(sdskv_get_multi_ult)
DECLARE_MARGO_RPC_HANDLER(sdskv_get_packed_ult)
DECLARE_MARGO_RPC_HANDLER(sdskv_bulk_put_ult)
DECLARE_MARGO_RPC_HANDLER(sdskv_bulk_get_ult)
DECLARE_MARGO_RPC_HANDLER(sdskv_list_keys_ult)
DECLARE_MARGO_RPC_HANDLER(sdskv_list_keyvals_ult)
DECLARE_MARGO_RPC_HANDLER(sdskv_erase_ult)
DECLARE_MARGO_RPC_HANDLER(sdskv_erase_multi_ult)
DECLARE_MARGO_RPC_HANDLER(sdskv_exists_ult)
DECLARE_MARGO_RPC_HANDLER(sdskv_exists_multi_ult)
DECLARE_MARGO_RPC_HANDLER(sdskv_migrate_keys_ult)
DECLARE_MARGO_RPC_HANDLER(sdskv_migrate_key_range_ult)
DECLARE_MARGO_RPC_HANDLER(sdskv_migrate_keys_prefixed_ult)
DECLARE_MARGO_RPC_HANDLER(sdskv_migrate_all_keys_ult)
DECLARE_MARGO_RPC_HANDLER(sdskv_migrate_database_ult)

static void sdskv_server_finalize_cb(void* data);

#ifdef USE_REMI

static int sdskv_pre_migration_callback(remi_fileset_t fileset, void* uargs);

static int sdskv_post_migration_callback(remi_fileset_t fileset, void* uargs);

#endif

static int populate_provider_from_config(sdskv_provider_t provider);

static int validate_and_complete_config(margo_instance_id mid,
                                        Json::Value&      config)
{
    /**
     * JSON config must have the following format:
     * {
     *    "comparators" : [
     *       { "name" : "<name>", "library" : "<library> (optional)" },
     *       ...
     *    ],
     *    "databases" : [
     *       { "name" : "<database-name>",         (required)
     *         "type" : "<database-type>",         (required)
     *         "path" : "<database-path>",         (required for some backends)
     *         "comparator" : "<comparator-name>", (optional, default to "")
     *         "no_overwrite" : true/false         (optional, default to false)
     *       },
     *       ...
     *    ]
     * }
     **/
    if (config.isNull()) { config = Json::Value(Json::objectValue); }
    // validate comparators
    if (config.isMember("comparators")) {
        if (!config["comparators"].isArray()) {
            SDSKV_LOG_ERROR(mid, "\"comparators\" field should be an array");
            return SDSKV_ERR_CONFIG;
        }
    } else {
        config["comparators"] = Json::Value(Json::arrayValue);
    }
    // check comparators
    auto&                           comparators = config["comparators"];
    std::unordered_set<std::string> comparator_names;
    for (auto it = comparators.begin(); it != comparators.end(); it++) {
        if (!it->isObject()) {
            SDSKV_LOG_ERROR(mid,
                            "\"comparators\" array should contain objects");
            return SDSKV_ERR_CONFIG;
        }
        if (!it->isMember("name")) {
            SDSKV_LOG_ERROR(mid, "missing \"name\" field in comparator");
            return SDSKV_ERR_CONFIG;
        }
        auto& name = (*it)["name"];
        if (!name.isString()) {
            SDSKV_LOG_ERROR(mid, "comparator name should be a string");
            return SDSKV_ERR_CONFIG;
        }
        if (name.asString().empty()) {
            SDSKV_LOG_ERROR(mid, "empty name in comparator");
            return SDSKV_ERR_CONFIG;
        }
        if (!it->isMember("library")) { (*it)["library"] = ""; }
        auto& library = (*it)["library"];
        if (!library.isString()) {
            SDSKV_LOG_ERROR(mid, "comparator library should be a string");
            return SDSKV_ERR_CONFIG;
        }
        if (comparator_names.count(name.asString())) {
            SDSKV_LOG_ERROR(mid, "multiple comparators with name \"%s\"",
                            name.asString().c_str());
            return SDSKV_ERR_CONFIG;
        } else {
            comparator_names.insert(name.asString());
        }
    }
    // validate databases
    if (config.isMember("databases")) {
        if (!config["databases"].isArray()) {
            SDSKV_LOG_ERROR(mid, "\"databases\" field should be an array");
            return SDSKV_ERR_CONFIG;
        }
    } else {
        config["databases"] = Json::Value(Json::arrayValue);
    }
    // check databases
    auto&                           databases = config["databases"];
    std::unordered_set<std::string> database_names;
    for (auto it = databases.begin(); it != databases.end(); it++) {
        auto& db = *it;
        if (!db.isMember("name")) {
            SDSKV_LOG_ERROR(mid, "missing \"name\" field in database");
            return SDSKV_ERR_CONFIG;
        }
        auto& name = db["name"];
        if (!name.isString()) {
            SDSKV_LOG_ERROR(mid, "database name should be a string");
            return SDSKV_ERR_CONFIG;
        }
        if (name.asString().empty()) {
            SDSKV_LOG_ERROR(mid, "database name is empty");
            return SDSKV_ERR_CONFIG;
        }
        if (!db.isMember("type")) {
            SDSKV_LOG_ERROR(mid, "missing \"type\" field in database");
            return SDSKV_ERR_CONFIG;
        }
        auto& type = db["type"];
        if (!type.isString()) {
            SDSKV_LOG_ERROR(mid, "database type should be a string");
            return SDSKV_ERR_CONFIG;
        }
        if (type.asString().empty()) {
            SDSKV_LOG_ERROR(mid, "database type is empty");
            return SDSKV_ERR_CONFIG;
        }
        if (!db.isMember("path")) db["path"] = "";
        if (!db.isMember("comparator")) db["comparator"] = "";
        if (!db.isMember("no_overwrite")) db["no_overwrite"] = false;
        auto& path         = db["path"];
        auto& comparator   = db["comparator"];
        auto& no_overwrite = db["no_overwrite"];
        if (!path.isString()) {
            SDSKV_LOG_ERROR(mid, "database path should be a string");
            return SDSKV_ERR_CONFIG;
        }
        if (!comparator.isString()) {
            SDSKV_LOG_ERROR(mid, "database comparator should be a string");
            return SDSKV_ERR_CONFIG;
        }
        if (!no_overwrite.isBool()) {
            SDSKV_LOG_ERROR(mid, "no_overwrite field should be a boolean");
            return SDSKV_ERR_CONFIG;
        }
        if (database_names.count(name.asString())) {
            SDSKV_LOG_ERROR(mid, "multiple databases with name \"%s\" found",
                            name.asString().c_str());
            return SDSKV_ERR_CONFIG;
        } else {
            database_names.insert(name.asString());
        }
    }
    return SDSKV_SUCCESS;
}

extern "C" int
sdskv_provider_register(margo_instance_id                      mid,
                        uint16_t                               provider_id,
                        const struct sdskv_provider_init_info* args,
                        sdskv_provider_t*                      provider)
{
    sdskv_server_context_t* tmp_provider;
    int                     ret;

    margo_trace(mid, "Registering SDSKV provider with provider id %d",
                provider_id);

    Json::Value config;
    if (args->json_config && args->json_config[0]) {
        try {
            std::stringstream ss(args->json_config);
            ss >> config;
        } catch (std::exception& ex) {
            SDSKV_LOG_ERROR(mid, "JSON error: %s", ex.what());
            return SDSKV_ERR_CONFIG;
        }
    }

    /* check if a provider with the same multiplex id already exists */
    {
        hg_id_t   id;
        hg_bool_t flag;
        margo_provider_registered_name(mid, "sdskv_put_rpc", provider_id, &id,
                                       &flag);
        if (flag == HG_TRUE) {
            SDSKV_LOG_ERROR(
                mid, "a provider with the same provider id (%d) already exists",
                provider_id);
            return SDSKV_ERR_PR_EXISTS;
        }
    }

    /* validate provided json, filling in default values where none prior exist
     */
    ret = validate_and_complete_config(mid, config);
    if (ret != SDSKV_SUCCESS) return ret;

    /* allocate the resulting structure */
    tmp_provider = new sdskv_server_context_t;
    if (!tmp_provider) return SDSKV_ERR_ALLOCATION;

    tmp_provider->mid      = mid;
    tmp_provider->json_cfg = config;

#ifdef USE_REMI
    tmp_provider->remi_client             = REMI_CLIENT_NULL;
    tmp_provider->remi_provider           = REMI_PROVIDER_NULL;
    tmp_provider->pre_migration_callback  = NULL;
    tmp_provider->post_migration_callback = NULL;
    tmp_provider->migration_uargs         = NULL;
#endif

    /* Create rwlock */
    ret = ABT_rwlock_create(&(tmp_provider->lock));
    if (ret != ABT_SUCCESS) {
        free(tmp_provider);
        SDSKV_LOG_ERROR(mid, "failed to create rwlock");
        return SDSKV_MAKE_ABT_ERROR(ret);
    }

    /* register RPCs */
    hg_id_t rpc_id;
    rpc_id
        = MARGO_REGISTER_PROVIDER(mid, "sdskv_open_rpc", open_in_t, open_out_t,
                                  sdskv_open_ult, provider_id, args->rpc_pool);
    tmp_provider->sdskv_open_id = rpc_id;
    margo_register_data(mid, rpc_id, (void*)tmp_provider, NULL);

    rpc_id = MARGO_REGISTER_PROVIDER(mid, "sdskv_count_databases_rpc", void,
                                     count_db_out_t, sdskv_count_db_ult,
                                     provider_id, args->rpc_pool);
    tmp_provider->sdskv_count_databases_id = rpc_id;
    margo_register_data(mid, rpc_id, (void*)tmp_provider, NULL);

    rpc_id = MARGO_REGISTER_PROVIDER(
        mid, "sdskv_list_databases_rpc", list_db_in_t, list_db_out_t,
        sdskv_list_db_ult, provider_id, args->rpc_pool);
    tmp_provider->sdskv_list_databases_id = rpc_id;
    margo_register_data(mid, rpc_id, (void*)tmp_provider, NULL);

    rpc_id
        = MARGO_REGISTER_PROVIDER(mid, "sdskv_put_rpc", put_in_t, put_out_t,
                                  sdskv_put_ult, provider_id, args->rpc_pool);
    tmp_provider->sdskv_put_id = rpc_id;
    margo_register_data(mid, rpc_id, (void*)tmp_provider, NULL);

    rpc_id = MARGO_REGISTER_PROVIDER(mid, "sdskv_put_multi_rpc", put_multi_in_t,
                                     put_multi_out_t, sdskv_put_multi_ult,
                                     provider_id, args->rpc_pool);
    tmp_provider->sdskv_put_multi_id = rpc_id;
    margo_register_data(mid, rpc_id, (void*)tmp_provider, NULL);

    rpc_id = MARGO_REGISTER_PROVIDER(
        mid, "sdskv_put_packed_rpc", put_packed_in_t, put_packed_out_t,
        sdskv_put_packed_ult, provider_id, args->rpc_pool);
    tmp_provider->sdskv_put_packed_id = rpc_id;
    margo_register_data(mid, rpc_id, (void*)tmp_provider, NULL);

    rpc_id = MARGO_REGISTER_PROVIDER(mid, "sdskv_bulk_put_rpc", bulk_put_in_t,
                                     bulk_put_out_t, sdskv_bulk_put_ult,
                                     provider_id, args->rpc_pool);
    tmp_provider->sdskv_bulk_put_id = rpc_id;
    margo_register_data(mid, rpc_id, (void*)tmp_provider, NULL);

    rpc_id
        = MARGO_REGISTER_PROVIDER(mid, "sdskv_get_rpc", get_in_t, get_out_t,
                                  sdskv_get_ult, provider_id, args->rpc_pool);
    tmp_provider->sdskv_get_id = rpc_id;
    margo_register_data(mid, rpc_id, (void*)tmp_provider, NULL);

    rpc_id = MARGO_REGISTER_PROVIDER(mid, "sdskv_get_multi_rpc", get_multi_in_t,
                                     get_multi_out_t, sdskv_get_multi_ult,
                                     provider_id, args->rpc_pool);
    tmp_provider->sdskv_get_multi_id = rpc_id;
    margo_register_data(mid, rpc_id, (void*)tmp_provider, NULL);

    rpc_id = MARGO_REGISTER_PROVIDER(
        mid, "sdskv_get_packed_rpc", get_packed_in_t, get_packed_out_t,
        sdskv_get_packed_ult, provider_id, args->rpc_pool);
    tmp_provider->sdskv_get_packed_id = rpc_id;
    margo_register_data(mid, rpc_id, (void*)tmp_provider, NULL);

    rpc_id = MARGO_REGISTER_PROVIDER(mid, "sdskv_length_rpc", length_in_t,
                                     length_out_t, sdskv_length_ult,
                                     provider_id, args->rpc_pool);
    tmp_provider->sdskv_length_id = rpc_id;
    margo_register_data(mid, rpc_id, (void*)tmp_provider, NULL);

    rpc_id = MARGO_REGISTER_PROVIDER(
        mid, "sdskv_length_multi_rpc", length_multi_in_t, length_multi_out_t,
        sdskv_length_multi_ult, provider_id, args->rpc_pool);
    tmp_provider->sdskv_length_multi_id = rpc_id;
    margo_register_data(mid, rpc_id, (void*)tmp_provider, NULL);

    rpc_id = MARGO_REGISTER_PROVIDER(
        mid, "sdskv_length_packed_rpc", length_packed_in_t, length_packed_out_t,
        sdskv_length_packed_ult, provider_id, args->rpc_pool);
    tmp_provider->sdskv_length_packed_id = rpc_id;
    margo_register_data(mid, rpc_id, (void*)tmp_provider, NULL);

    rpc_id = MARGO_REGISTER_PROVIDER(mid, "sdskv_exists_rpc", exists_in_t,
                                     exists_out_t, sdskv_exists_ult,
                                     provider_id, args->rpc_pool);
    tmp_provider->sdskv_exists_id = rpc_id;
    margo_register_data(mid, rpc_id, (void*)tmp_provider, NULL);

    rpc_id = MARGO_REGISTER_PROVIDER(
        mid, "sdskv_exists_multi_rpc", exists_multi_in_t, exists_multi_out_t,
        sdskv_exists_multi_ult, provider_id, args->rpc_pool);
    tmp_provider->sdskv_exists_multi_id = rpc_id;
    margo_register_data(mid, rpc_id, (void*)tmp_provider, NULL);

    rpc_id = MARGO_REGISTER_PROVIDER(mid, "sdskv_bulk_get_rpc", bulk_get_in_t,
                                     bulk_get_out_t, sdskv_bulk_get_ult,
                                     provider_id, args->rpc_pool);
    tmp_provider->sdskv_bulk_get_id = rpc_id;
    margo_register_data(mid, rpc_id, (void*)tmp_provider, NULL);

    rpc_id = MARGO_REGISTER_PROVIDER(mid, "sdskv_list_keys_rpc", list_keys_in_t,
                                     list_keys_out_t, sdskv_list_keys_ult,
                                     provider_id, args->rpc_pool);
    tmp_provider->sdskv_list_keys_id = rpc_id;
    margo_register_data(mid, rpc_id, (void*)tmp_provider, NULL);

    rpc_id = MARGO_REGISTER_PROVIDER(
        mid, "sdskv_list_keyvals_rpc", list_keyvals_in_t, list_keyvals_out_t,
        sdskv_list_keyvals_ult, provider_id, args->rpc_pool);
    tmp_provider->sdskv_list_keyvals_id = rpc_id;
    margo_register_data(mid, rpc_id, (void*)tmp_provider, NULL);

    rpc_id = MARGO_REGISTER_PROVIDER(mid, "sdskv_erase_rpc", erase_in_t,
                                     erase_out_t, sdskv_erase_ult, provider_id,
                                     args->rpc_pool);
    tmp_provider->sdskv_erase_id = rpc_id;
    margo_register_data(mid, rpc_id, (void*)tmp_provider, NULL);

    rpc_id = MARGO_REGISTER_PROVIDER(
        mid, "sdskv_erase_multi_rpc", erase_multi_in_t, erase_multi_out_t,
        sdskv_erase_multi_ult, provider_id, args->rpc_pool);
    tmp_provider->sdskv_erase_multi_id = rpc_id;
    margo_register_data(mid, rpc_id, (void*)tmp_provider, NULL);

    /* migration RPC */
    rpc_id = MARGO_REGISTER_PROVIDER(
        mid, "sdskv_migrate_keys_rpc", migrate_keys_in_t, migrate_keys_out_t,
        sdskv_migrate_keys_ult, provider_id, args->rpc_pool);
    tmp_provider->sdskv_migrate_keys_id = rpc_id;
    margo_register_data(mid, rpc_id, (void*)tmp_provider, NULL);

    rpc_id = MARGO_REGISTER_PROVIDER(mid, "sdskv_migrate_key_range_rpc",
                                     migrate_key_range_in_t, migrate_keys_out_t,
                                     sdskv_migrate_key_range_ult, provider_id,
                                     args->rpc_pool);
    tmp_provider->sdskv_migrate_key_range_id = rpc_id;
    margo_register_data(mid, rpc_id, (void*)tmp_provider, NULL);

    rpc_id = MARGO_REGISTER_PROVIDER(
        mid, "sdskv_migrate_keys_prefixed_rpc", migrate_keys_prefixed_in_t,
        migrate_keys_out_t, sdskv_migrate_keys_prefixed_ult, provider_id,
        args->rpc_pool);
    tmp_provider->sdskv_migrate_keys_prefixed_id = rpc_id;
    margo_register_data(mid, rpc_id, (void*)tmp_provider, NULL);

    rpc_id = MARGO_REGISTER_PROVIDER(mid, "sdskv_migrate_all_keys_rpc",
                                     migrate_all_keys_in_t, migrate_keys_out_t,
                                     sdskv_migrate_all_keys_ult, provider_id,
                                     args->rpc_pool);
    tmp_provider->sdskv_migrate_all_keys_id = rpc_id;
    margo_register_data(mid, rpc_id, (void*)tmp_provider, NULL);

    rpc_id = MARGO_REGISTER_PROVIDER(
        mid, "sdskv_migrate_database_rpc", migrate_database_in_t,
        migrate_database_out_t, sdskv_migrate_database_ult, provider_id,
        args->rpc_pool);
    tmp_provider->sdskv_migrate_database_id = rpc_id;
    margo_register_data(mid, rpc_id, (void*)tmp_provider, NULL);

#ifdef USE_REMI
    tmp_provider->remi_client   = (remi_client_t)(args->remi_client);
    tmp_provider->remi_provider = (remi_provider_t)(args->remi_provider);
    /* check if a REMI provider exists with the same provider id */
    if (tmp_provider->remi_provider) {
        ret = remi_provider_register_migration_class(
            tmp_provider->remi_provider, "sdskv", sdskv_pre_migration_callback,
            sdskv_post_migration_callback, NULL, tmp_provider);
        if (ret != REMI_SUCCESS) {
            SDSKV_LOG_ERROR(
                mid,
                "could not register REMI migration class for SDSKV provider");
            sdskv_server_finalize_cb(tmp_provider);
            return SDSKV_ERR_REMI;
        }
    }
#endif

    /* install the bake server finalize callback */
    margo_provider_push_finalize_callback(
        mid, tmp_provider, &sdskv_server_finalize_cb, tmp_provider);

    ret = populate_provider_from_config(tmp_provider);
    if (ret != SDSKV_SUCCESS) {
        sdskv_provider_destroy(tmp_provider);
        return ret;
    }

#ifdef USE_SYMBIOMON
    tmp_provider->metric_provider = NULL;
#endif

    if (provider != SDSKV_PROVIDER_IGNORE) *provider = tmp_provider;

    return SDSKV_SUCCESS;
}

extern "C" char* sdskv_provider_get_config(sdskv_provider_t provider)
{
    Json::StreamWriterBuilder builder;
    builder["indentation"]   = "";
    const std::string config = Json::writeString(builder, provider->json_cfg);
    return strdup(config.c_str());
}

extern "C" margo_instance_id sdskv_provider_get_mid(sdskv_provider_t provider)
{
    return (provider->mid);
}

#ifdef USE_SYMBIOMON
extern "C" int sdskv_provider_set_symbiomon(sdskv_provider_t provider, symbiomon_provider_t metric_provider)
{
    provider->metric_provider = metric_provider;

    fprintf(stderr, "Successfully set the SYMBIOMON provider\n");
    symbiomon_taglist_t taglist, taglist2, taglist3, taglist4;
    symbiomon_taglist_create(&taglist, 1, "dummytag");
    symbiomon_taglist_create(&taglist2, 1, "dummytag1");
    symbiomon_taglist_create(&taglist3, 1, "dummytag2");
    symbiomon_taglist_create(&taglist4, 1, "dummytag3");
    symbiomon_metric_create("sdskv", "put_latency", SYMBIOMON_TYPE_TIMER, "sdskv:put latency in seconds", taglist, &provider->put_latency, provider->metric_provider);
    symbiomon_metric_create("sdskv", "put_data_size", SYMBIOMON_TYPE_GAUGE, "sdskv:put_data_size", taglist, &provider->put_data_size, provider->metric_provider);
    symbiomon_metric_create("sdskv", "put_packed_latency", SYMBIOMON_TYPE_TIMER, "sdskv:put_packed latency in seconds", taglist2, &provider->put_packed_latency, provider->metric_provider);
    symbiomon_metric_create("sdskv", "put_packed_batch_size", SYMBIOMON_TYPE_GAUGE, "sdskv:put_packed_batch_size", taglist3, &provider->put_packed_batch_size, provider->metric_provider);
    symbiomon_metric_create("sdskv", "put_packed_data_size", SYMBIOMON_TYPE_GAUGE, "sdskv:put_packed_data_size", taglist3, &provider->put_packed_data_size, provider->metric_provider);
    symbiomon_metric_create("sdskv", "put_num_entrants", SYMBIOMON_TYPE_GAUGE, "sdskv:put_num_entrants", taglist3, &provider->put_num_entrants, provider->metric_provider);
    symbiomon_metric_create("sdskv", "putpacked_num_entrants", SYMBIOMON_TYPE_GAUGE, "sdskv:putpacked_num_entrants", taglist3, &provider->putpacked_num_entrants, provider->metric_provider);

    return SDSKV_SUCCESS;
}
#endif

extern "C" int sdskv_provider_destroy(sdskv_provider_t provider)
{
    margo_provider_pop_finalize_callback(provider->mid, provider);
    sdskv_server_finalize_cb(provider);
    return SDSKV_SUCCESS;
}

extern "C" int sdskv_provider_add_comparison_function(sdskv_provider_t provider,
                                                      const char* function_name,
                                                      sdskv_compare_fn comp_fn)
{
    if (provider->compfunctions.find(std::string(function_name))
        != provider->compfunctions.end()) {
        if (provider->compfunctions[std::string(function_name)] == comp_fn) {
            return SDSKV_SUCCESS;
        } else {
            SDSKV_LOG_ERROR(provider->mid,
                            "another comparison function with name \"%s\""
                            " is already registered",
                            function_name);
            return SDSKV_ERR_COMP_FUNC;
        }
    }
    provider->compfunctions[std::string(function_name)] = comp_fn;
    return SDSKV_SUCCESS;
}

extern "C" int sdskv_provider_find_comparison_function(
    sdskv_provider_t provider, const char* library, const char* function_name)
{
    void* handle = nullptr;
    if (library != nullptr && library[0] == '\0')
        handle = dlopen(nullptr, RTLD_NOW);
    else
        handle = dlopen(library, RTLD_NOW);
    if (handle == NULL) {
        SDSKV_LOG_ERROR(provider->mid,
                        "could not dlopen %s to find comparator function",
                        library);
        return SDSKV_ERR_COMP_FUNC;
    }
    sdskv_compare_fn comp_fn = (sdskv_compare_fn)dlsym(handle, function_name);
    if (comp_fn == NULL) {
        SDSKV_LOG_ERROR(provider->mid, "could not find comparator function %s",
                        function_name);
        return SDSKV_ERR_COMP_FUNC;
    }
    provider->compfunctions[std::string(function_name)] = comp_fn;

    return SDSKV_SUCCESS;
}
extern "C" int sdskv_provider_attach_database(sdskv_provider_t      provider,
                                              const sdskv_config_t* config,
                                              sdskv_database_id_t*  db_id)
{
    sdskv_compare_fn comp_fn = NULL;
    if (config->db_comp_fn_name && config->db_comp_fn_name[0]) {
        std::string k(config->db_comp_fn_name);
        auto        it = provider->compfunctions.find(k);
        if (it == provider->compfunctions.end()) {
            SDSKV_LOG_ERROR(provider->mid,
                            "could not find comparison function \"%s\"",
                            config->db_comp_fn_name);
            return SDSKV_ERR_COMP_FUNC;
        }
        comp_fn = it->second;
    }

    auto db = datastore_factory::open_datastore(config->db_type,
                                                std::string(config->db_name),
                                                std::string(config->db_path));
    if (db == nullptr) {
        SDSKV_LOG_ERROR(provider->mid,
                        "factory failed to create datastore \"%s\"",
                        config->db_name);
        return SDSKV_ERR_DB_CREATE;
    }
    if (comp_fn) {
        db->set_comparison_function(config->db_comp_fn_name, comp_fn);
    }
    sdskv_database_id_t id = (sdskv_database_id_t)(db);
    if (config->db_no_overwrite) { db->set_no_overwrite(); }

    ABT_rwlock_wrlock(provider->lock);
    provider->name2id[std::string(config->db_name)] = id;
    provider->id2name[id]   = std::string(config->db_name);
    provider->databases[id] = db;
    ABT_rwlock_unlock(provider->lock);

    *db_id = id;

    margo_trace(provider->mid,
                "Successfully opened database \"%s\" with id %lu",
                config->db_name, id);

    return SDSKV_SUCCESS;
}

extern "C" int sdskv_provider_remove_database(sdskv_provider_t    provider,
                                              sdskv_database_id_t db_id)
{
    ABT_rwlock_wrlock(provider->lock);
    auto r = at_exit([provider]() { ABT_rwlock_unlock(provider->lock); });
    if (provider->databases.count(db_id)) {
        auto dbname = provider->id2name[db_id];
        provider->id2name.erase(db_id);
        provider->name2id.erase(dbname);
        auto db = provider->databases[db_id];
        delete db;
        provider->databases.erase(db_id);
        margo_trace(provider->mid,
                    "Successfully removed database %lu from provider", db_id);
        return SDSKV_SUCCESS;
    } else {
        SDSKV_LOG_ERROR(provider->mid,
                        "could not find database id %lu in provider", db_id);
        return SDSKV_ERR_UNKNOWN_DB;
    }
}

extern "C" int sdskv_provider_remove_all_databases(sdskv_provider_t provider)
{
    ABT_rwlock_wrlock(provider->lock);
    for (auto db : provider->databases) { delete db.second; }
    provider->databases.clear();
    provider->name2id.clear();
    provider->id2name.clear();
    ABT_rwlock_unlock(provider->lock);
    margo_trace(provider->mid, "Successfully removed all databases");
    return SDSKV_SUCCESS;
}

extern "C" int sdskv_provider_count_databases(sdskv_provider_t provider,
                                              uint64_t*        num_db)
{
    ABT_rwlock_rdlock(provider->lock);
    *num_db = provider->databases.size();
    ABT_rwlock_unlock(provider->lock);
    return SDSKV_SUCCESS;
}

extern "C" int sdskv_provider_list_databases(sdskv_provider_t     provider,
                                             sdskv_database_id_t* targets)
{
    unsigned i = 0;
    ABT_rwlock_rdlock(provider->lock);
    for (auto p : provider->name2id) {
        targets[i] = p.second;
        i++;
    }
    ABT_rwlock_unlock(provider->lock);
    return SDSKV_SUCCESS;
}

extern "C" int sdskv_provider_compute_database_size(
    sdskv_provider_t provider, sdskv_database_id_t database_id, size_t* size)
{
#ifdef USE_REMI
    int ret;
    // find the database
    ABT_rwlock_rdlock(provider->lock);
    auto it = provider->databases.find(database_id);
    if (it == provider->databases.end()) {
        ABT_rwlock_unlock(provider->lock);
        return SDSKV_ERR_UNKNOWN_DB;
    }
    auto database = it->second;
    ABT_rwlock_unlock(provider->lock);

    database->sync();

    /* create a fileset */
    remi_fileset_t fileset = database->create_and_populate_fileset();
    if (fileset == REMI_FILESET_NULL) { return SDSKV_OP_NOT_IMPL; }
    /* issue the migration */
    ret = remi_fileset_compute_size(fileset, 0, size);
    if (ret != REMI_SUCCESS) {
        SDSKV_LOG_ERROR(provider->mid, "remi_fileset_compute_size returned %d",
                        ret);
        return SDSKV_ERR_REMI;
    }
    return SDSKV_SUCCESS;
#else
    // TODO: implement this without REMI
    return SDSKV_OP_NOT_IMPL;
#endif
}

extern "C" int
sdskv_provider_set_migration_callbacks(sdskv_provider_t                provider,
                                       sdskv_pre_migration_callback_fn pre_cb,
                                       sdskv_post_migration_callback_fn post_cb,
                                       void*                            uargs)
{
#ifdef USE_REMI
    provider->pre_migration_callback  = pre_cb;
    provider->post_migration_callback = post_cb;
    provider->migration_uargs         = uargs;
    return SDSKV_SUCCESS;
#else
    return SDSKV_OP_NOT_IMPL;
#endif
}

static void sdskv_open_ult(hg_handle_t handle)
{

    hg_return_t hret;
    open_in_t   in;
    open_out_t  out;

    ENSURE_MARGO_DESTROY;
    ENSURE_MARGO_RESPOND;
    FIND_MID_AND_PROVIDER;
    GET_INPUT;
    ENSURE_MARGO_FREE_INPUT;

    ABT_rwlock_rdlock(provider->lock);
    auto it = provider->name2id.find(std::string(in.name));
    if (it == provider->name2id.end()) {
        ABT_rwlock_unlock(provider->lock);
        SDSKV_LOG_ERROR(mid, "could not find database with name \"%s\"",
                        in.name);
        out.ret = SDSKV_ERR_DB_NAME;
        return;
    }
    auto db = it->second;
    ABT_rwlock_unlock(provider->lock);

    out.db_id = db;
    out.ret   = SDSKV_SUCCESS;
}
DEFINE_MARGO_RPC_HANDLER(sdskv_open_ult)

static void sdskv_count_db_ult(hg_handle_t handle)
{

    hg_return_t    hret;
    count_db_out_t out;

    ENSURE_MARGO_DESTROY;
    ENSURE_MARGO_RESPOND;
    FIND_MID_AND_PROVIDER;

    uint64_t count;
    sdskv_provider_count_databases(provider, &count);

    out.count = count;
    out.ret   = SDSKV_SUCCESS;
}
DEFINE_MARGO_RPC_HANDLER(sdskv_count_db_ult)

static void sdskv_list_db_ult(hg_handle_t handle)
{

    hg_return_t   hret;
    list_db_in_t  in;
    list_db_out_t out;

    std::vector<std::string> db_names;
    std::vector<char*>       db_names_c_str;
    std::vector<uint64_t>    db_ids;

    out.count = 0;
    out.ret   = SDSKV_SUCCESS;

    ENSURE_MARGO_DESTROY;
    ENSURE_MARGO_RESPOND;
    FIND_MID_AND_PROVIDER;
    GET_INPUT;
    ENSURE_MARGO_FREE_INPUT;

    ABT_rwlock_rdlock(provider->lock);
    unsigned i = 0;
    for (const auto& p : provider->name2id) {
        if (i >= in.count) break;
        db_names.push_back(p.first);
        db_ids.push_back(p.second);
        i += 1;
    }
    ABT_rwlock_unlock(provider->lock);

    out.count = i;
    for (i = 0; i < out.count; i++) {
        db_names_c_str.push_back(const_cast<char*>(db_names[i].c_str()));
    }
    out.db_names = db_names_c_str.data();
    out.db_ids   = &db_ids[0];
}
DEFINE_MARGO_RPC_HANDLER(sdskv_list_db_ult)

static void sdskv_put_ult(hg_handle_t handle)
{
    hg_return_t hret;
    put_in_t    in;
    put_out_t   out;

    ENSURE_MARGO_DESTROY;
    ENSURE_MARGO_RESPOND;
    FIND_MID_AND_PROVIDER;
    GET_INPUT;
    ENSURE_MARGO_FREE_INPUT;
    FIND_DATABASE;

    ds_bulk_t kdata(in.key.data, in.key.data + in.key.size);
    ds_bulk_t vdata(in.value.data, in.value.data + in.value.size);

    double start = ABT_get_wtime();

#ifdef USE_SYMBIOMON
    symbiomon_metric_update_gauge_by_fixed_amount(provider->put_num_entrants, 1);
#endif
    out.ret = db->put(kdata, vdata);

    double end = ABT_get_wtime();

#ifdef USE_SYMBIOMON
    symbiomon_metric_update_gauge_by_fixed_amount(provider->put_num_entrants, -1);
    symbiomon_metric_update(provider->put_latency, (end-start));
    symbiomon_metric_update(provider->put_data_size, (double)(in.key.size+in.value.size));
#endif 
}
DEFINE_MARGO_RPC_HANDLER(sdskv_put_ult)

static void sdskv_put_multi_ult(hg_handle_t handle)
{
    hg_return_t     hret;
    put_multi_in_t  in;
    put_multi_out_t out;
    out.ret = SDSKV_SUCCESS;
    std::vector<char> local_keys_buffer;
    std::vector<char> local_vals_buffer;
    hg_bulk_t         local_keys_bulk_handle;
    hg_bulk_t         local_vals_bulk_handle;

    ENSURE_MARGO_DESTROY;
    ENSURE_MARGO_RESPOND;
    FIND_MID_AND_PROVIDER;
    GET_INPUT;
    ENSURE_MARGO_FREE_INPUT;
    FIND_DATABASE;

    // allocate a buffer to receive the keys and a buffer to receive the values
    local_keys_buffer.resize(in.keys_bulk_size);
    local_vals_buffer.resize(in.vals_bulk_size);
    std::vector<void*> keys_addr(1);
    keys_addr[0] = (void*)local_keys_buffer.data();
    std::vector<void*> vals_addr(1);
    vals_addr[0] = (void*)local_vals_buffer.data();

    /* create bulk handle to receive keys */
    hret = margo_bulk_create(mid, 1, keys_addr.data(), &in.keys_bulk_size,
                             HG_BULK_WRITE_ONLY, &local_keys_bulk_handle);
    if (hret != HG_SUCCESS) {
        out.ret = SDSKV_MAKE_HG_ERROR(hret);
        SDSKV_LOG_ERROR(mid, "failed to create bulk handle (hret = %d)", hret);
        return;
    }
    DEFER(margo_bulk_free_local_keys, margo_bulk_free(local_keys_bulk_handle));

    /* create bulk handle to receive values */
    hret = margo_bulk_create(mid, 1, vals_addr.data(), &in.vals_bulk_size,
                             HG_BULK_WRITE_ONLY, &local_vals_bulk_handle);
    if (hret != HG_SUCCESS) {
        out.ret = SDSKV_MAKE_HG_ERROR(hret);
        SDSKV_LOG_ERROR(mid, "failed to create bulk handle (hret = %d)", hret);
        return;
    }
    DEFER(margo_bulk_free_local_vals, margo_bulk_free(local_vals_bulk_handle));

    /* transfer keys */
    hret = margo_bulk_transfer(mid, HG_BULK_PULL, info->addr,
                               in.keys_bulk_handle, 0, local_keys_bulk_handle,
                               0, in.keys_bulk_size);
    if (hret != HG_SUCCESS) {
        out.ret = SDSKV_MAKE_HG_ERROR(hret);
        SDSKV_LOG_ERROR(mid, "failed to issue bulk transfer (hret = %d)", hret);
        return;
    }

    /* transfer values */
    hret = margo_bulk_transfer(mid, HG_BULK_PULL, info->addr,
                               in.vals_bulk_handle, 0, local_vals_bulk_handle,
                               0, in.vals_bulk_size);
    if (hret != HG_SUCCESS) {
        out.ret = SDSKV_MAKE_HG_ERROR(hret);
        SDSKV_LOG_ERROR(mid, "failed to issue bulk transfer (hret = %d)", hret);
        return;
    }

    /* interpret beginning of the key buffer as a list of key sizes */
    hg_size_t* key_sizes = (hg_size_t*)local_keys_buffer.data();
    /* interpret beginning of the value buffer as a list of value sizes */
    hg_size_t* val_sizes = (hg_size_t*)local_vals_buffer.data();

    /* go through the key/value pairs and insert them */
    uint64_t                 keys_offset = sizeof(hg_size_t) * in.num_keys;
    uint64_t                 vals_offset = sizeof(hg_size_t) * in.num_keys;
    std::vector<const void*> kptrs(in.num_keys);
    std::vector<const void*> vptrs(in.num_keys);
    size_t tot_key_size, tot_val_size = 0;
    for (unsigned i = 0; i < in.num_keys; i++) {
        kptrs[i] = local_keys_buffer.data() + keys_offset;
        vptrs[i] = val_sizes[i] == 0 ? nullptr
                                     : local_vals_buffer.data() + vals_offset;
        keys_offset += key_sizes[i];
        vals_offset += val_sizes[i];
	tot_key_size += key_sizes[i];
	tot_val_size += val_sizes[i];
    }

    double start = ABT_get_wtime();
    double end;


#ifdef USE_SYMBIOMON
    symbiomon_metric_update_gauge_by_fixed_amount(provider->putpacked_num_entrants, 1);
#endif
    out.ret = db->put_multi(in.num_keys, kptrs.data(), key_sizes, vptrs.data(),
                            val_sizes);
    end = ABT_get_wtime();
#ifdef USE_SYMBIOMON
    symbiomon_metric_update_gauge_by_fixed_amount(provider->putpacked_num_entrants, -1);
    symbiomon_metric_update(provider->put_packed_latency, (end-start));
    symbiomon_metric_update(provider->put_packed_batch_size, (double)in.num_keys);
    symbiomon_metric_update(provider->put_packed_data_size, (double)tot_key_size+tot_val_size);
#endif 
}
DEFINE_MARGO_RPC_HANDLER(sdskv_put_multi_ult)

static void sdskv_put_packed_ult(hg_handle_t handle)
{
    hg_return_t      hret;
    put_packed_in_t  in;
    put_packed_out_t out;
    out.ret = SDSKV_SUCCESS;
    std::vector<char> local_buffer;
    hg_bulk_t         local_bulk_handle;
    hg_addr_t         origin_addr = HG_ADDR_NULL;

    ENSURE_MARGO_DESTROY;
    ENSURE_MARGO_RESPOND;
    FIND_MID_AND_PROVIDER;
    GET_INPUT;
    ENSURE_MARGO_FREE_INPUT;
    FIND_DATABASE;

    // find out the address of the origin
    if (in.origin_addr != NULL) {
        hret = margo_addr_lookup(mid, in.origin_addr, &origin_addr);
        if (hret != HG_SUCCESS) {
            SDSKV_LOG_ERROR(mid, "failed to lookup client address (hret = %d)",
                            hret);
            out.ret = SDSKV_MAKE_HG_ERROR(hret);
            return;
        }
    } else {
        hret = margo_addr_dup(mid, info->addr, &origin_addr);
        if (hret != HG_SUCCESS) {
            SDSKV_LOG_ERROR(
                mid, "failed to duplicate client address (hret = %d)", hret);
            out.ret = SDSKV_MAKE_HG_ERROR(hret);
            return;
        }
    }
    DEFER(margo_addr_free, margo_addr_free(mid, origin_addr));

    // allocate a buffer to receive the keys and a buffer to receive the values
    local_buffer.resize(in.bulk_size);
    void*     buf_ptr  = local_buffer.data();
    hg_size_t buf_size = in.bulk_size;

    /* create bulk handle to receive keys */
    hret = margo_bulk_create(mid, 1, &buf_ptr, &buf_size, HG_BULK_WRITE_ONLY,
                             &local_bulk_handle);
    if (hret != HG_SUCCESS) {
        SDSKV_LOG_ERROR(mid, "failed to create bulk handle (hret = %d)", hret);
        out.ret = SDSKV_MAKE_HG_ERROR(hret);
        return;
    }
    DEFER(margo_bulk_free, margo_bulk_free(local_bulk_handle));

    /* transfer data */
    hret = margo_bulk_transfer(mid, HG_BULK_PULL, origin_addr, in.bulk_handle,
                               0, local_bulk_handle, 0, in.bulk_size);
    if (hret != HG_SUCCESS) {
        SDSKV_LOG_ERROR(mid, "failed to issue bulk transfer (hret = %d)", hret);
        out.ret = SDSKV_MAKE_HG_ERROR(hret);
        return;
    }

    /* interpret buffer as a list of key sizes */
    hg_size_t* key_sizes = (hg_size_t*)local_buffer.data();
    /* interpret buffer as a list of value sizes */
    hg_size_t* val_sizes = key_sizes + in.num_keys;
    /* interpret buffer as list of keys */
    char* packed_keys = (char*)(val_sizes + in.num_keys);
    /* compute the size of part of the buffer that contain keys */
    size_t k = 0, v = 0;
    for (unsigned i = 0; i < in.num_keys; i++) k += key_sizes[i];
    /* interpret the rest of the buffer as list of values */
    char* packed_vals = packed_keys + k;
    for(unsigned i=0; i < in.num_keys; i++) v += val_sizes[i];

    /* insert key/vals into the DB */
    double data_size = (double)v+k;
    double start, end;
    /* insert key/vals into the DB */
    start = ABT_get_wtime();
#ifdef USE_SYMBIOMON
    symbiomon_metric_update_gauge_by_fixed_amount(provider->putpacked_num_entrants, 1);
#endif
    out.ret = db->put_packed(in.num_keys, packed_keys, key_sizes, packed_vals,
                             val_sizes);
    end = ABT_get_wtime();
#ifdef USE_SYMBIOMON
    symbiomon_metric_update_gauge_by_fixed_amount(provider->putpacked_num_entrants, -1);
    symbiomon_metric_update(provider->put_packed_latency, (end-start));
    symbiomon_metric_update(provider->put_packed_batch_size, (double)in.num_keys);
    symbiomon_metric_update(provider->put_packed_data_size, (double)data_size);
#endif 
}
DEFINE_MARGO_RPC_HANDLER(sdskv_put_packed_ult)

static void sdskv_length_ult(hg_handle_t handle)
{
    hg_return_t  hret;
    length_in_t  in;
    length_out_t out;

    ENSURE_MARGO_DESTROY;
    ENSURE_MARGO_RESPOND;
    FIND_MID_AND_PROVIDER;
    GET_INPUT;
    ENSURE_MARGO_FREE_INPUT;
    FIND_DATABASE;

    ds_bulk_t kdata(in.key.data, in.key.data + in.key.size);

    size_t vsize;
    if (db->length(kdata, &vsize)) {
        out.size = vsize;
        out.ret  = SDSKV_SUCCESS;
    } else {
        out.size = 0;
        out.ret  = SDSKV_ERR_UNKNOWN_KEY;
    }
}
DEFINE_MARGO_RPC_HANDLER(sdskv_length_ult)

static void sdskv_get_ult(hg_handle_t handle)
{
    hg_return_t hret;
    get_in_t    in;
    get_out_t   out;
    ds_bulk_t   kdata;
    ds_bulk_t   vdata;

    memset(&out, 0, sizeof(out));

    ENSURE_MARGO_DESTROY;
    ENSURE_MARGO_RESPOND;
    FIND_MID_AND_PROVIDER;
    GET_INPUT;
    ENSURE_MARGO_FREE_INPUT;
    FIND_DATABASE;

    kdata = ds_bulk_t(in.key.data, in.key.data + in.key.size);

    if (db->get(kdata, vdata)) {
        if (vdata.size() <= in.vsize) {
            out.vsize      = vdata.size();
            out.value.size = vdata.size();
            out.value.data = vdata.data();
            out.ret        = SDSKV_SUCCESS;
        } else {
            out.vsize      = vdata.size();
            out.value.size = 0;
            out.value.data = nullptr;
            out.ret        = SDSKV_ERR_SIZE;
        }
    } else {
        out.vsize      = 0;
        out.value.size = 0;
        out.value.data = nullptr;
        out.ret        = SDSKV_ERR_UNKNOWN_KEY;
    }
}
DEFINE_MARGO_RPC_HANDLER(sdskv_get_ult)

static void sdskv_get_multi_ult(hg_handle_t handle)
{

    hg_return_t     hret;
    get_multi_in_t  in;
    get_multi_out_t out;
    out.ret = SDSKV_SUCCESS;
    std::vector<char> local_keys_buffer;
    std::vector<char> local_vals_buffer;
    hg_bulk_t         local_keys_bulk_handle;
    hg_bulk_t         local_vals_bulk_handle;

    ENSURE_MARGO_DESTROY;
    ENSURE_MARGO_RESPOND;
    FIND_MID_AND_PROVIDER;
    GET_INPUT;
    ENSURE_MARGO_FREE_INPUT;
    FIND_DATABASE;

    /* allocate buffers to receive the keys */
    local_keys_buffer.resize(in.keys_bulk_size);
    std::vector<void*> keys_addr(1);
    keys_addr[0] = (void*)local_keys_buffer.data();

    /* create bulk handle to receive key sizes and packed keys */
    hret = margo_bulk_create(mid, 1, keys_addr.data(), &in.keys_bulk_size,
                             HG_BULK_WRITE_ONLY, &local_keys_bulk_handle);
    if (hret != HG_SUCCESS) {
        SDSKV_LOG_ERROR(mid, "failed to create bulk handle (hret = %d)", hret);
        out.ret = SDSKV_MAKE_HG_ERROR(hret);
        return;
    }
    DEFER(margo_bulk_free_local_keys, margo_bulk_free(local_keys_bulk_handle));

    /* allocate buffer to send/receive the values */
    local_vals_buffer.resize(in.vals_bulk_size);
    std::vector<void*> vals_addr(1);
    vals_addr[0] = (void*)local_vals_buffer.data();

    /* create bulk handle to receive max value sizes and to send values */
    hret = margo_bulk_create(mid, 1, vals_addr.data(), &in.vals_bulk_size,
                             HG_BULK_READWRITE, &local_vals_bulk_handle);
    if (hret != HG_SUCCESS) {
        SDSKV_LOG_ERROR(mid, "failed to create bulk handle (hret = %d)", hret);
        out.ret = SDSKV_MAKE_HG_ERROR(hret);
        return;
    }
    DEFER(margo_bulk_free_local_valss, margo_bulk_free(local_vals_bulk_handle));

    /* transfer keys */
    hret = margo_bulk_transfer(mid, HG_BULK_PULL, info->addr,
                               in.keys_bulk_handle, 0, local_keys_bulk_handle,
                               0, in.keys_bulk_size);
    if (hret != HG_SUCCESS) {
        SDSKV_LOG_ERROR(mid, "failed to issue bulk transfer (hret = %d)", hret);
        out.ret = SDSKV_MAKE_HG_ERROR(hret);
        return;
    }

    /* transfer sizes allocated by user for the values (beginning of value
     * segment) */
    hret = margo_bulk_transfer(mid, HG_BULK_PULL, info->addr,
                               in.vals_bulk_handle, 0, local_vals_bulk_handle,
                               0, in.num_keys * sizeof(hg_size_t));
    if (hret != HG_SUCCESS) {
        SDSKV_LOG_ERROR(mid, "failed to issue bulk transfer (hret = %d)", hret);
        out.ret = SDSKV_MAKE_HG_ERROR(hret);
        return;
    }

    /* interpret beginning of the key buffer as a list of key sizes */
    hg_size_t* key_sizes = (hg_size_t*)local_keys_buffer.data();
    /* find beginning of packed keys */
    char* packed_keys
        = local_keys_buffer.data() + in.num_keys * sizeof(hg_size_t);
    /* interpret beginning of the value buffer as a list of value sizes */
    hg_size_t* val_sizes = (hg_size_t*)local_vals_buffer.data();
    /* find beginning of region where to pack values */
    char* packed_values
        = local_vals_buffer.data() + in.num_keys * sizeof(hg_size_t);

    /* go through the key/value pairs and get the values from the database */
    for (unsigned i = 0; i < in.num_keys; i++) {
        ds_bulk_t kdata(packed_keys, packed_keys + key_sizes[i]);
        ds_bulk_t vdata;
        size_t    client_allocated_value_size = val_sizes[i];
        if (db->get(kdata, vdata)) {
            size_t old_vsize = val_sizes[i];
            if (vdata.size() > val_sizes[i]) {
                val_sizes[i] = 0;
            } else {
                val_sizes[i] = vdata.size();
                memcpy(packed_values, vdata.data(), val_sizes[i]);
            }
        } else {
            val_sizes[i] = 0;
        }
        packed_keys += key_sizes[i];
        packed_values += val_sizes[i];
    }

    /* do a PUSH operation to push back the values to the client */
    hret = margo_bulk_transfer(mid, HG_BULK_PUSH, info->addr,
                               in.vals_bulk_handle, 0, local_vals_bulk_handle,
                               0, in.vals_bulk_size);
    if (hret != HG_SUCCESS) {
        SDSKV_LOG_ERROR(mid, "failed to issue bulk transfer (hret = %d)", hret);
        out.ret = SDSKV_MAKE_HG_ERROR(hret);
        return;
    }
}
DEFINE_MARGO_RPC_HANDLER(sdskv_get_multi_ult)

static void sdskv_get_packed_ult(hg_handle_t handle)
{

    hg_return_t      hret;
    get_packed_in_t  in;
    get_packed_out_t out;
    out.ret      = SDSKV_SUCCESS;
    out.num_keys = 0;
    std::vector<char> local_keys_buffer;
    std::vector<char> local_vals_buffer;
    hg_bulk_t         local_keys_bulk_handle;
    hg_bulk_t         local_vals_bulk_handle;

    ENSURE_MARGO_DESTROY;
    ENSURE_MARGO_RESPOND;
    FIND_MID_AND_PROVIDER;
    GET_INPUT;
    ENSURE_MARGO_FREE_INPUT;
    FIND_DATABASE;

    /* allocate buffers to receive the keys */
    local_keys_buffer.resize(in.keys_bulk_size);
    std::vector<void*> keys_addr(1);
    keys_addr[0] = (void*)local_keys_buffer.data();

    /* create bulk handle to receive key sizes and packed keys */
    hret = margo_bulk_create(mid, 1, keys_addr.data(), &in.keys_bulk_size,
                             HG_BULK_WRITE_ONLY, &local_keys_bulk_handle);
    if (hret != HG_SUCCESS) {
        SDSKV_LOG_ERROR(mid, "failed to create bulk handle (hret = %d)", hret);
        out.ret = SDSKV_MAKE_HG_ERROR(hret);
        return;
    }
    DEFER(margo_bulk_free_local_keys, margo_bulk_free(local_keys_bulk_handle));

    /* allocate buffer to send the values */
    local_vals_buffer.resize(in.vals_bulk_size);
    std::vector<void*> vals_addr(1);
    vals_addr[0] = (void*)local_vals_buffer.data();

    /* create bulk handle to receive max value sizes and to send values */
    hret = margo_bulk_create(mid, 1, vals_addr.data(), &in.vals_bulk_size,
                             HG_BULK_READ_ONLY, &local_vals_bulk_handle);
    if (hret != HG_SUCCESS) {
        SDSKV_LOG_ERROR(mid, "failed to create bulk handle (hret = %d)", hret);
        out.ret = SDSKV_MAKE_HG_ERROR(hret);
        return;
    }
    DEFER(margo_bulk_free_local_vals, margo_bulk_free(local_vals_bulk_handle));

    /* transfer keys and key sizes */
    hret = margo_bulk_transfer(mid, HG_BULK_PULL, info->addr,
                               in.keys_bulk_handle, 0, local_keys_bulk_handle,
                               0, in.keys_bulk_size);
    if (hret != HG_SUCCESS) {
        SDSKV_LOG_ERROR(mid, "failed to issue bulk transfer (hret = %d)", hret);
        out.ret = SDSKV_MAKE_HG_ERROR(hret);
        return;
    }

    /* interpret beginning of the key buffer as a list of key sizes */
    hg_size_t* key_sizes = (hg_size_t*)local_keys_buffer.data();
    /* find beginning of packed keys */
    char* packed_keys
        = local_keys_buffer.data() + in.num_keys * sizeof(hg_size_t);
    /* interpret beginning of the value buffer as a list of value sizes */
    hg_size_t* val_sizes = (hg_size_t*)local_vals_buffer.data();
    /* find beginning of region where to pack values */
    char* packed_values
        = local_vals_buffer.data() + in.num_keys * sizeof(hg_size_t);

    /* go through the key/value pairs and get the values from the database */
    size_t available_client_memory
        = in.vals_bulk_size - in.num_keys * sizeof(hg_size_t);
    unsigned i = 0;
    for (unsigned i = 0; i < in.num_keys; i++) {
        ds_bulk_t kdata(packed_keys, packed_keys + key_sizes[i]);
        ds_bulk_t vdata;
        if (available_client_memory == 0) {
            val_sizes[i] = 0;
            out.ret      = SDSKV_ERR_SIZE;
            continue;
        }
        if (db->get(kdata, vdata)) {
            if (vdata.size() > available_client_memory) {
                available_client_memory = 0;
                out.ret                 = SDSKV_ERR_SIZE;
                val_sizes[i]            = 0;
            } else {
                out.num_keys += 1;
                val_sizes[i] = vdata.size();
                memcpy(packed_values, vdata.data(), val_sizes[i]);
                packed_values += val_sizes[i];
            }
        } else {
            val_sizes[i] = (hg_size_t)(-1);
        }
        packed_keys += key_sizes[i];
    }

    /* do a PUSH operation to push back the values to the client */
    hret = margo_bulk_transfer(mid, HG_BULK_PUSH, info->addr,
                               in.vals_bulk_handle, 0, local_vals_bulk_handle,
                               0, in.vals_bulk_size);
    if (hret != HG_SUCCESS) {
        SDSKV_LOG_ERROR(mid, "failed to issue bulk transfer (hret = %d)", hret);
        out.ret = SDSKV_MAKE_HG_ERROR(hret);
        return;
    }
}
DEFINE_MARGO_RPC_HANDLER(sdskv_get_packed_ult)

static void sdskv_length_multi_ult(hg_handle_t handle)
{

    hg_return_t        hret;
    length_multi_in_t  in;
    length_multi_out_t out;
    out.ret = SDSKV_SUCCESS;
    std::vector<char>      local_keys_buffer;
    std::vector<hg_size_t> local_vals_size_buffer;
    hg_bulk_t              local_keys_bulk_handle;
    hg_bulk_t              local_vals_size_bulk_handle;

    ENSURE_MARGO_DESTROY;
    ENSURE_MARGO_RESPOND;
    FIND_MID_AND_PROVIDER;
    GET_INPUT;
    ENSURE_MARGO_FREE_INPUT;
    FIND_DATABASE;

    /* allocate buffers to receive the keys */
    local_keys_buffer.resize(in.keys_bulk_size);
    std::vector<void*> keys_addr(1);
    keys_addr[0] = (void*)local_keys_buffer.data();

    /* create bulk handle to receive key sizes and packed keys */
    hret = margo_bulk_create(mid, 1, keys_addr.data(), &in.keys_bulk_size,
                             HG_BULK_WRITE_ONLY, &local_keys_bulk_handle);
    if (hret != HG_SUCCESS) {
        SDSKV_LOG_ERROR(mid, "failed to create bulk handle (hret = %d)", hret);
        out.ret = SDSKV_MAKE_HG_ERROR(hret);
        return;
    }
    DEFER(margo_bulk_free_local_keys, margo_bulk_free(local_keys_bulk_handle));

    /* allocate buffer to send the values */
    local_vals_size_buffer.resize(in.num_keys);
    std::vector<void*> vals_sizes_addr(1);
    hg_size_t local_vals_size_buffer_size = in.num_keys * sizeof(hg_size_t);
    vals_sizes_addr[0] = (void*)local_vals_size_buffer.data();

    /* create bulk handle to send values sizes */
    hret = margo_bulk_create(mid, 1, vals_sizes_addr.data(),
                             &local_vals_size_buffer_size, HG_BULK_READ_ONLY,
                             &local_vals_size_bulk_handle);
    if (hret != HG_SUCCESS) {
        SDSKV_LOG_ERROR(mid, "failed to create bulk handle (hret = %d)", hret);
        out.ret = SDSKV_MAKE_HG_ERROR(hret);
        return;
    }
    DEFER(margo_bulk_free_local_vals_size,
          margo_bulk_free(local_vals_size_bulk_handle));

    /* transfer keys */
    hret = margo_bulk_transfer(mid, HG_BULK_PULL, info->addr,
                               in.keys_bulk_handle, 0, local_keys_bulk_handle,
                               0, in.keys_bulk_size);
    if (hret != HG_SUCCESS) {
        SDSKV_LOG_ERROR(mid, "failed to issue bulk transfer (hret = %d)", hret);
        out.ret = SDSKV_MAKE_HG_ERROR(hret);
        return;
    }

    /* interpret beginning of the key buffer as a list of key sizes */
    hg_size_t* key_sizes = (hg_size_t*)local_keys_buffer.data();
    /* find beginning of packed keys */
    char* packed_keys
        = local_keys_buffer.data() + in.num_keys * sizeof(hg_size_t);

    /* go through the key/value pairs and get the values from the database */
    for (unsigned i = 0; i < in.num_keys; i++) {
        ds_bulk_t kdata(packed_keys, packed_keys + key_sizes[i]);
        size_t    vsize;
        if (db->length(kdata, &vsize)) {
            local_vals_size_buffer[i] = vsize;
        } else {
            local_vals_size_buffer[i] = 0;
        }
        packed_keys += key_sizes[i];
    }

    /* do a PUSH operation to push back the value sizes to the client */
    hret = margo_bulk_transfer(
        mid, HG_BULK_PUSH, info->addr, in.vals_size_bulk_handle, 0,
        local_vals_size_bulk_handle, 0, local_vals_size_buffer_size);
    if (hret != HG_SUCCESS) {
        SDSKV_LOG_ERROR(mid, "failed to issue bulk transfer (hret = %d)", hret);
        out.ret = SDSKV_MAKE_HG_ERROR(hret);
        return;
    }
}
DEFINE_MARGO_RPC_HANDLER(sdskv_length_multi_ult)

static void sdskv_exists_multi_ult(hg_handle_t handle)
{

    hg_return_t        hret;
    exists_multi_in_t  in;
    exists_multi_out_t out;
    out.ret = SDSKV_SUCCESS;
    std::vector<char>    local_keys_buffer;
    std::vector<uint8_t> local_flags_buffer;
    hg_bulk_t            local_keys_bulk_handle;
    hg_bulk_t            local_flags_bulk_handle;

    ENSURE_MARGO_DESTROY;
    ENSURE_MARGO_RESPOND;
    FIND_MID_AND_PROVIDER;
    GET_INPUT;
    ENSURE_MARGO_FREE_INPUT;
    FIND_DATABASE;

    /* allocate buffers to receive the keys */
    local_keys_buffer.resize(in.keys_bulk_size);
    std::vector<void*> keys_addr(1);
    keys_addr[0] = (void*)local_keys_buffer.data();

    /* create bulk handle to receive key sizes and packed keys */
    hret = margo_bulk_create(mid, 1, keys_addr.data(), &in.keys_bulk_size,
                             HG_BULK_WRITE_ONLY, &local_keys_bulk_handle);
    if (hret != HG_SUCCESS) {
        SDSKV_LOG_ERROR(mid, "failed to create bulk handle (hret = %d)", hret);
        out.ret = SDSKV_MAKE_HG_ERROR(hret);
        return;
    }
    DEFER(margo_bulk_free_local_keys, margo_bulk_free(local_keys_bulk_handle));

    /* allocate buffer to send the flags */
    hg_size_t local_flags_buffer_size
        = in.num_keys / 8 + (in.num_keys % 8 == 0 ? 0 : 1);
    local_flags_buffer.resize(local_flags_buffer_size, 0);
    std::vector<void*> flags_buffer_addr(1);
    flags_buffer_addr[0] = (void*)local_flags_buffer.data();

    /* create bulk handle to send flags */
    hret = margo_bulk_create(mid, 1, flags_buffer_addr.data(),
                             &local_flags_buffer_size, HG_BULK_READ_ONLY,
                             &local_flags_bulk_handle);
    if (hret != HG_SUCCESS) {
        SDSKV_LOG_ERROR(mid, "failed to create bulk handle (hret = %d)", hret);
        out.ret = SDSKV_MAKE_HG_ERROR(hret);
        return;
    }
    DEFER(margo_bulk_free_local_flags,
          margo_bulk_free(local_flags_bulk_handle));

    /* transfer keys */
    hret = margo_bulk_transfer(mid, HG_BULK_PULL, info->addr,
                               in.keys_bulk_handle, 0, local_keys_bulk_handle,
                               0, in.keys_bulk_size);
    if (hret != HG_SUCCESS) {
        SDSKV_LOG_ERROR(mid, "failed to issue bulk transfer (hret = %d)", hret);
        out.ret = SDSKV_MAKE_HG_ERROR(hret);
        return;
    }

    /* interpret beginning of the key buffer as a list of key sizes */
    hg_size_t* key_sizes = (hg_size_t*)local_keys_buffer.data();
    /* find beginning of packed keys */
    char* packed_keys
        = local_keys_buffer.data() + in.num_keys * sizeof(hg_size_t);

    /* go through the key/value pairs and get the values from the database */
    uint8_t mask = 1;
    for (unsigned i = 0; i < in.num_keys; i++) {
        auto current_key   = packed_keys;
        auto current_ksize = key_sizes[i];
        if (db->exists(current_key, current_ksize)) {
            local_flags_buffer[i / 8] |= mask;
        }
        mask = mask << 1;
        if (i % 8 == 7) mask = 1;
        packed_keys += key_sizes[i];
    }

    /* do a PUSH operation to push back the value sizes to the client */
    hret = margo_bulk_transfer(mid, HG_BULK_PUSH, info->addr,
                               in.flags_bulk_handle, 0, local_flags_bulk_handle,
                               0, local_flags_buffer_size);
    if (hret != HG_SUCCESS) {
        SDSKV_LOG_ERROR(mid, "failed to issue bulk transfer (hret = %d)", hret);
        out.ret = SDSKV_MAKE_HG_ERROR(hret);
        return;
    }
}
DEFINE_MARGO_RPC_HANDLER(sdskv_exists_multi_ult)

static void sdskv_length_packed_ult(hg_handle_t handle)
{

    hg_return_t         hret;
    length_packed_in_t  in;
    length_packed_out_t out;
    out.ret = SDSKV_SUCCESS;
    std::vector<char>      local_keys_buffer;
    std::vector<hg_size_t> local_vals_size_buffer;
    hg_bulk_t              local_keys_bulk_handle;
    hg_bulk_t              local_vals_size_bulk_handle;

    ENSURE_MARGO_DESTROY;
    ENSURE_MARGO_RESPOND;
    FIND_MID_AND_PROVIDER;
    GET_INPUT;
    ENSURE_MARGO_FREE_INPUT;
    FIND_DATABASE;

    /* allocate buffers to receive the keys and key sizes*/
    local_keys_buffer.resize(in.in_bulk_size);
    std::vector<void*> keys_addr(1);
    keys_addr[0] = (void*)local_keys_buffer.data();

    /* create bulk handle to receive key sizes and packed keys */
    hret = margo_bulk_create(mid, 1, keys_addr.data(), &in.in_bulk_size,
                             HG_BULK_WRITE_ONLY, &local_keys_bulk_handle);
    if (hret != HG_SUCCESS) {
        SDSKV_LOG_ERROR(mid, "failed to create bulk handle (hret = %d)", hret);
        out.ret = SDSKV_MAKE_HG_ERROR(hret);
        return;
    }
    DEFER(margo_bulk_free_local_keys, margo_bulk_free(local_keys_bulk_handle));

    /* allocate buffer to send the value sizes */
    local_vals_size_buffer.resize(in.num_keys);
    std::vector<void*> vals_sizes_addr(1);
    hg_size_t local_vals_size_buffer_size = in.num_keys * sizeof(hg_size_t);
    vals_sizes_addr[0] = (void*)local_vals_size_buffer.data();

    /* create bulk handle to send values sizes */
    hret = margo_bulk_create(mid, 1, vals_sizes_addr.data(),
                             &local_vals_size_buffer_size, HG_BULK_READ_ONLY,
                             &local_vals_size_bulk_handle);
    if (hret != HG_SUCCESS) {
        SDSKV_LOG_ERROR(mid, "failed to create bulk handle (hret = %d)", hret);
        out.ret = SDSKV_MAKE_HG_ERROR(hret);
        return;
    }
    DEFER(margo_bulk_free_local_vals_size,
          margo_bulk_free(local_vals_size_bulk_handle));

    /* transfer keys and ksizes */
    hret = margo_bulk_transfer(mid, HG_BULK_PULL, info->addr, in.in_bulk_handle,
                               0, local_keys_bulk_handle, 0, in.in_bulk_size);
    if (hret != HG_SUCCESS) {
        SDSKV_LOG_ERROR(mid, "failed to issue bulk transfer (hret = %d)", hret);
        out.ret = SDSKV_MAKE_HG_ERROR(hret);
        return;
    }

    /* interpret beginning of the key buffer as a list of key sizes */
    hg_size_t* key_sizes = (hg_size_t*)local_keys_buffer.data();
    /* find beginning of packed keys */
    char* packed_keys
        = local_keys_buffer.data() + in.num_keys * sizeof(hg_size_t);

    /* go through the key/value pairs and get the values from the database */
    for (unsigned i = 0; i < in.num_keys; i++) {
        ds_bulk_t kdata(packed_keys, packed_keys + key_sizes[i]);
        size_t    vsize;
        if (db->length(kdata, &vsize)) {
            local_vals_size_buffer[i] = vsize;
        } else {
            local_vals_size_buffer[i] = 0;
        }
        packed_keys += key_sizes[i];
    }

    /* do a PUSH operation to push back the value sizes to the client */
    hret = margo_bulk_transfer(
        mid, HG_BULK_PUSH, info->addr, in.out_bulk_handle, 0,
        local_vals_size_bulk_handle, 0, local_vals_size_buffer_size);
    if (hret != HG_SUCCESS) {
        SDSKV_LOG_ERROR(mid, "failed to issue bulk transfer (hret = %d)", hret);
        out.ret = SDSKV_MAKE_HG_ERROR(hret);
        return;
    }
}
DEFINE_MARGO_RPC_HANDLER(sdskv_length_packed_ult)

static void sdskv_bulk_put_ult(hg_handle_t handle)
{

    hg_return_t    hret;
    bulk_put_in_t  in;
    bulk_put_out_t out;
    hg_bulk_t      bulk_handle;

    ENSURE_MARGO_DESTROY;
    ENSURE_MARGO_RESPOND;
    FIND_MID_AND_PROVIDER;
    GET_INPUT;
    ENSURE_MARGO_FREE_INPUT;
    FIND_DATABASE;

    ds_bulk_t vdata(in.vsize);

    if (in.vsize > 0) {

        void*     buffer = (void*)vdata.data();
        hg_size_t size   = vdata.size();
        hret             = margo_bulk_create(mid, 1, (void**)&buffer, &size,
                                 HG_BULK_WRITE_ONLY, &bulk_handle);
        if (hret != HG_SUCCESS) {
            SDSKV_LOG_ERROR(mid, "failed to create bulk handle (hret = %d)",
                            hret);
            out.ret = SDSKV_MAKE_HG_ERROR(hret);
            return;
        }
        DEFER(margo_bulk_free, margo_bulk_free(bulk_handle));

        hret = margo_bulk_transfer(mid, HG_BULK_PULL, info->addr, in.handle, 0,
                                   bulk_handle, 0, vdata.size());
        if (hret != HG_SUCCESS) {
            SDSKV_LOG_ERROR(mid, "failed to issue bulk transfer (hret = %d)",
                            hret);
            out.ret = SDSKV_MAKE_HG_ERROR(hret);
            return;
        }
    }

    ds_bulk_t kdata(in.key.data, in.key.data + in.key.size);
    double start = ABT_get_wtime();

#ifdef USE_SYMBIOMON
    symbiomon_metric_update_gauge_by_fixed_amount(provider->put_num_entrants, 1);
#endif
    out.ret = db->put(kdata, vdata);
    double end = ABT_get_wtime();

#ifdef USE_SYMBIOMON
    symbiomon_metric_update_gauge_by_fixed_amount(provider->put_num_entrants, -1);
    symbiomon_metric_update(provider->put_latency, (end-start));
    symbiomon_metric_update(provider->put_data_size, (double)(in.key.size+in.vsize));
#endif 
}
DEFINE_MARGO_RPC_HANDLER(sdskv_bulk_put_ult)

static void sdskv_bulk_get_ult(hg_handle_t handle)
{

    hg_return_t    hret;
    bulk_get_in_t  in;
    bulk_get_out_t out;
    hg_bulk_t      bulk_handle;

    memset(&out, 0, sizeof(out));

    ENSURE_MARGO_DESTROY;
    ENSURE_MARGO_RESPOND;
    FIND_MID_AND_PROVIDER;
    GET_INPUT;
    ENSURE_MARGO_FREE_INPUT;
    FIND_DATABASE;

    ds_bulk_t kdata(in.key.data, in.key.data + in.key.size);

    ds_bulk_t vdata;
    auto      b = db->get(kdata, vdata);

    if (!b) {
        out.vsize = 0;
        out.ret   = SDSKV_ERR_UNKNOWN_KEY;
        return;
    }

    if (vdata.size() > in.vsize) {
        out.vsize = vdata.size();
        out.ret   = SDSKV_ERR_SIZE;
        return;
    }

    void*     buffer = (void*)vdata.data();
    hg_size_t size   = vdata.size();
    if (size > 0) {
        hret = margo_bulk_create(mid, 1, (void**)&buffer, &size,
                                 HG_BULK_READ_ONLY, &bulk_handle);
        if (hret != HG_SUCCESS) {
            SDSKV_LOG_ERROR(mid, "failed to create bulk handle (hret = %d)",
                            hret);
            out.vsize = 0;
            out.ret   = SDSKV_MAKE_HG_ERROR(hret);
            return;
        }
        DEFER(margo_bulk_free, margo_bulk_free(bulk_handle));

        hret = margo_bulk_transfer(mid, HG_BULK_PUSH, info->addr, in.handle, 0,
                                   bulk_handle, 0, vdata.size());
        if (hret != HG_SUCCESS) {
            SDSKV_LOG_ERROR(mid, "failed to issue bulk transfer (hret = %d)",
                            hret);
            out.vsize = 0;
            out.ret   = SDSKV_MAKE_HG_ERROR(hret);
            return;
        }
    }

    out.vsize = size;
    out.ret   = SDSKV_SUCCESS;
}
DEFINE_MARGO_RPC_HANDLER(sdskv_bulk_get_ult)

static void sdskv_erase_ult(hg_handle_t handle)
{

    hg_return_t hret;
    erase_in_t  in;
    erase_out_t out;

    ENSURE_MARGO_DESTROY;
    ENSURE_MARGO_RESPOND;
    FIND_MID_AND_PROVIDER;
    GET_INPUT;
    ENSURE_MARGO_FREE_INPUT;
    FIND_DATABASE;

    ds_bulk_t kdata(in.key.data, in.key.data + in.key.size);

    if (db->erase(kdata)) {
        out.ret = SDSKV_SUCCESS;
    } else {
        out.ret = SDSKV_ERR_ERASE;
    }
}
DEFINE_MARGO_RPC_HANDLER(sdskv_erase_ult)

static void sdskv_erase_multi_ult(hg_handle_t handle)
{

    hg_return_t       hret;
    erase_multi_in_t  in;
    erase_multi_out_t out;
    out.ret = SDSKV_SUCCESS;
    std::vector<char> local_keys_buffer;
    hg_bulk_t         local_keys_bulk_handle;

    ENSURE_MARGO_DESTROY;
    ENSURE_MARGO_RESPOND;
    FIND_MID_AND_PROVIDER;
    GET_INPUT;
    ENSURE_MARGO_FREE_INPUT;
    FIND_DATABASE;

    /* allocate buffers to receive the keys */
    local_keys_buffer.resize(in.keys_bulk_size);
    std::vector<void*> keys_addr(1);
    keys_addr[0] = (void*)local_keys_buffer.data();

    /* create bulk handle to receive key sizes and packed keys */
    hret = margo_bulk_create(mid, 1, keys_addr.data(), &in.keys_bulk_size,
                             HG_BULK_WRITE_ONLY, &local_keys_bulk_handle);
    if (hret != HG_SUCCESS) {
        SDSKV_LOG_ERROR(mid, "failed to create bulk handle (hret = %d)", hret);
        out.ret = SDSKV_MAKE_HG_ERROR(hret);
        return;
    }
    DEFER(margo_bulk_free_local_keys, margo_bulk_free(local_keys_bulk_handle));

    /* transfer keys */
    hret = margo_bulk_transfer(mid, HG_BULK_PULL, info->addr,
                               in.keys_bulk_handle, 0, local_keys_bulk_handle,
                               0, in.keys_bulk_size);
    if (hret != HG_SUCCESS) {
        SDSKV_LOG_ERROR(mid, "failed to issue bulk transfer (hret = %d)", hret);
        out.ret = SDSKV_MAKE_HG_ERROR(hret);
        return;
    }

    /* interpret beginning of the key buffer as a list of key sizes */
    hg_size_t* key_sizes = (hg_size_t*)local_keys_buffer.data();
    /* find beginning of packed keys */
    char* packed_keys
        = local_keys_buffer.data() + in.num_keys * sizeof(hg_size_t);

    /* go through the key/value pairs and erase them */
    for (unsigned i = 0; i < in.num_keys; i++) {
        ds_bulk_t kdata(packed_keys, packed_keys + key_sizes[i]);
        db->erase(kdata);
        packed_keys += key_sizes[i];
    }
}
DEFINE_MARGO_RPC_HANDLER(sdskv_erase_multi_ult)

static void sdskv_exists_ult(hg_handle_t handle)
{

    hg_return_t  hret;
    exists_in_t  in;
    exists_out_t out;

    ENSURE_MARGO_DESTROY;
    ENSURE_MARGO_RESPOND;
    FIND_MID_AND_PROVIDER;
    GET_INPUT;
    ENSURE_MARGO_FREE_INPUT;
    FIND_DATABASE;

    out.flag = db->exists(in.key.data, in.key.size) ? 1 : 0;
    out.ret  = SDSKV_SUCCESS;
}
DEFINE_MARGO_RPC_HANDLER(sdskv_exists_ult)

static void sdskv_list_keys_ult(hg_handle_t handle)
{

    hg_return_t     hret;
    list_keys_in_t  in;
    list_keys_out_t out;
    hg_bulk_t       ksizes_local_bulk = HG_BULK_NULL;
    hg_bulk_t       keys_local_bulk   = HG_BULK_NULL;

    out.ret   = SDSKV_SUCCESS;
    out.nkeys = 0;

    ENSURE_MARGO_DESTROY;
    ENSURE_MARGO_RESPOND;
    FIND_MID_AND_PROVIDER;
    GET_INPUT;
    ENSURE_MARGO_FREE_INPUT;
    FIND_DATABASE;

    /* create a bulk handle to receive and send key sizes from client */
    std::vector<hg_size_t> ksizes(in.max_keys);
    std::vector<void*>     ksizes_addr(1);
    ksizes_addr[0]             = (void*)ksizes.data();
    hg_size_t ksizes_bulk_size = ksizes.size() * sizeof(hg_size_t);
    hret = margo_bulk_create(mid, 1, ksizes_addr.data(), &ksizes_bulk_size,
                             HG_BULK_READWRITE, &ksizes_local_bulk);
    if (hret != HG_SUCCESS) {
        SDSKV_LOG_ERROR(mid, "failed to create bulk handle (hret = %d)", hret);
        out.ret = SDSKV_MAKE_HG_ERROR(hret);
        return;
    }
    DEFER(margo_bulk_free_ksizes, margo_bulk_free(ksizes_local_bulk));

    /* receive the key sizes from the client */
    hg_addr_t origin_addr = info->addr;
    hret                  = margo_bulk_transfer(mid, HG_BULK_PULL, origin_addr,
                               in.ksizes_bulk_handle, 0, ksizes_local_bulk, 0,
                               ksizes_bulk_size);
    if (hret != HG_SUCCESS) {
        SDSKV_LOG_ERROR(mid, "failed to issue bulk transfer (hret = %d)", hret);
        out.ret = SDSKV_MAKE_HG_ERROR(hret);
        return;
    }

    /* make a copy of the remote key sizes */
    std::vector<hg_size_t> remote_ksizes(ksizes.begin(), ksizes.end());

    /* get the keys from the underlying database */
    ds_bulk_t start_kdata(in.start_key.data,
                          in.start_key.data + in.start_key.size);
    ds_bulk_t prefix(in.prefix.data, in.prefix.data + in.prefix.size);
    auto      keys     = db->list_keys(start_kdata, in.max_keys, prefix);
    hg_size_t num_keys = std::min((size_t)keys.size(), (size_t)in.max_keys);

    if (num_keys == 0) {
        out.ret = SDSKV_SUCCESS;
        return;
    }

    /* create the array of actual sizes */
    std::vector<hg_size_t> true_ksizes(num_keys);
    hg_size_t              keys_bulk_size = 0;
    bool                   size_error     = false;
    for (unsigned i = 0; i < num_keys; i++) {
        true_ksizes[i] = keys[i].size();
        if (true_ksizes[i] > ksizes[i]) {
            // this key has a size that exceeds the allocated size on client
            size_error = true;
        }
        ksizes[i] = true_ksizes[i];
        keys_bulk_size += ksizes[i];
    }
    for (unsigned i = num_keys; i < in.max_keys; i++) { ksizes[i] = 0; }
    out.nkeys = num_keys;

    /* transfer the ksizes back to the client */
    hret = margo_bulk_transfer(mid, HG_BULK_PUSH, origin_addr,
                               in.ksizes_bulk_handle, 0, ksizes_local_bulk, 0,
                               ksizes_bulk_size);
    if (hret != HG_SUCCESS) {
        SDSKV_LOG_ERROR(mid, "failed to issue bulk transfer (hret = %d)", hret);
        out.ret = SDSKV_MAKE_HG_ERROR(hret);
        return;
    }

    /* if user provided a size too small for some key, return error (we
     * already set the right key sizes) */
    if (size_error) {
        out.ret = SDSKV_ERR_SIZE;
        return;
    }

    /* create an array of addresses pointing to keys */
    std::vector<void*> keys_addr(num_keys);
    for (unsigned i = 0; i < num_keys; i++) {
        keys_addr[i] = (void*)(keys[i].data());
    }

    /* expose the keys for bulk transfer */
    hret
        = margo_bulk_create(mid, num_keys, keys_addr.data(), true_ksizes.data(),
                            HG_BULK_READ_ONLY, &keys_local_bulk);
    if (hret != HG_SUCCESS) {
        SDSKV_LOG_ERROR(mid, "failed to create bulk handle (hret = %d)", hret);
        out.ret = SDSKV_MAKE_HG_ERROR(hret);
        return;
    }
    DEFER(margo_bulk_free_keys, margo_bulk_free(keys_local_bulk));

    /* transfer the keys to the client */
    uint64_t remote_offset = 0;
    uint64_t local_offset  = 0;
    for (unsigned i = 0; i < num_keys; i++) {

        if (true_ksizes[i] > 0) {
            hret = margo_bulk_transfer(
                mid, HG_BULK_PUSH, origin_addr, in.keys_bulk_handle,
                remote_offset, keys_local_bulk, local_offset, true_ksizes[i]);
            if (hret != HG_SUCCESS) {
                SDSKV_LOG_ERROR(
                    mid, "failed to issue bulk transfer (hret = %d)", hret);
                out.ret = SDSKV_MAKE_HG_ERROR(hret);
                return;
            }
        }

        remote_offset += remote_ksizes[i];
        local_offset += true_ksizes[i];
    }

    out.ret = SDSKV_SUCCESS;
}
DEFINE_MARGO_RPC_HANDLER(sdskv_list_keys_ult)

static void sdskv_list_keyvals_ult(hg_handle_t handle)
{

    hg_return_t        hret;
    list_keyvals_in_t  in;
    list_keyvals_out_t out;
    hg_bulk_t          ksizes_local_bulk = HG_BULK_NULL;
    hg_bulk_t          keys_local_bulk   = HG_BULK_NULL;
    hg_bulk_t          vsizes_local_bulk = HG_BULK_NULL;
    hg_bulk_t          vals_local_bulk   = HG_BULK_NULL;

    out.ret   = SDSKV_SUCCESS;
    out.nkeys = 0;

    ENSURE_MARGO_DESTROY;
    ENSURE_MARGO_RESPOND;
    FIND_MID_AND_PROVIDER;
    GET_INPUT;
    ENSURE_MARGO_FREE_INPUT;
    FIND_DATABASE;

    /* create a bulk handle to receive and send key sizes from client */
    std::vector<hg_size_t> ksizes(in.max_keys);
    std::vector<void*>     ksizes_addr(1);
    ksizes_addr[0]             = (void*)ksizes.data();
    hg_size_t ksizes_bulk_size = ksizes.size() * sizeof(hg_size_t);
    hret = margo_bulk_create(mid, 1, ksizes_addr.data(), &ksizes_bulk_size,
                             HG_BULK_READWRITE, &ksizes_local_bulk);
    if (hret != HG_SUCCESS) {
        SDSKV_LOG_ERROR(mid, "failed to create bulk handle (hret = %d)", hret);
        out.ret = SDSKV_MAKE_HG_ERROR(hret);
        return;
    }
    DEFER(margo_bulk_free_ksizes_local, margo_bulk_free(ksizes_local_bulk));

    /* create a bulk handle to receive and send value sizes from client */
    std::vector<hg_size_t> vsizes(in.max_keys);
    std::vector<void*>     vsizes_addr(1);
    vsizes_addr[0]             = (void*)vsizes.data();
    hg_size_t vsizes_bulk_size = vsizes.size() * sizeof(hg_size_t);
    hret = margo_bulk_create(mid, 1, vsizes_addr.data(), &vsizes_bulk_size,
                             HG_BULK_READWRITE, &vsizes_local_bulk);
    if (hret != HG_SUCCESS) {
        SDSKV_LOG_ERROR(mid, "failed to create bulk handle (hret = %d)", hret);
        out.ret = SDSKV_MAKE_HG_ERROR(hret);
        return;
    }
    DEFER(margo_bulk_free_vsizes_local, margo_bulk_free(vsizes_local_bulk));

    /* receive the key sizes from the client */
    hg_addr_t origin_addr = info->addr;
    hret                  = margo_bulk_transfer(mid, HG_BULK_PULL, origin_addr,
                               in.ksizes_bulk_handle, 0, ksizes_local_bulk, 0,
                               ksizes_bulk_size);
    if (hret != HG_SUCCESS) {
        SDSKV_LOG_ERROR(mid, "failed to issue bulk transfer (hret = %d)", hret);
        out.ret = SDSKV_MAKE_HG_ERROR(hret);
        return;
    }

    /* receive the values sizes from the client */
    hret = margo_bulk_transfer(mid, HG_BULK_PULL, origin_addr,
                               in.vsizes_bulk_handle, 0, vsizes_local_bulk, 0,
                               vsizes_bulk_size);
    if (hret != HG_SUCCESS) {
        SDSKV_LOG_ERROR(mid, "failed to issue bulk transfer (hret = %d)", hret);
        out.ret = SDSKV_MAKE_HG_ERROR(hret);
        return;
    }

    /* make a copy of the remote key sizes and value sizes */
    std::vector<hg_size_t> remote_ksizes(ksizes.begin(), ksizes.end());
    std::vector<hg_size_t> remote_vsizes(vsizes.begin(), vsizes.end());

    /* get the keys and values from the underlying database */
    ds_bulk_t start_kdata(in.start_key.data,
                          in.start_key.data + in.start_key.size);
    ds_bulk_t prefix(in.prefix.data, in.prefix.data + in.prefix.size);
    auto      keyvals  = db->list_keyvals(start_kdata, in.max_keys, prefix);
    hg_size_t num_keys = std::min((size_t)keyvals.size(), (size_t)in.max_keys);

    out.nkeys = num_keys;

    if (num_keys == 0) {
        out.ret = SDSKV_SUCCESS;
        return;
    }

    bool size_error = false;

    /* create the array of actual key sizes */
    std::vector<hg_size_t> true_ksizes(num_keys);
    hg_size_t              keys_bulk_size = 0;
    for (unsigned i = 0; i < num_keys; i++) {
        true_ksizes[i] = keyvals[i].first.size();
        if (true_ksizes[i] > ksizes[i]) {
            // this key has a size that exceeds the allocated size on client
            size_error = true;
        }
        ksizes[i] = true_ksizes[i];
        keys_bulk_size += ksizes[i];
    }
    for (unsigned i = num_keys; i < ksizes.size(); i++) ksizes[i] = 0;

    /* create the array of actual value sizes */
    std::vector<hg_size_t> true_vsizes(num_keys);
    hg_size_t              vals_bulk_size = 0;
    for (unsigned i = 0; i < num_keys; i++) {
        true_vsizes[i] = keyvals[i].second.size();
        if (true_vsizes[i] > vsizes[i]) {
            // this value has a size that exceeds the allocated size on
            // client
            size_error = true;
        }
        vsizes[i] = true_vsizes[i];
        vals_bulk_size += vsizes[i];
    }
    for (unsigned i = num_keys; i < vsizes.size(); i++) vsizes[i] = 0;

    /* transfer the ksizes back to the client */
    if (ksizes_bulk_size) {
        hret = margo_bulk_transfer(mid, HG_BULK_PUSH, origin_addr,
                                   in.ksizes_bulk_handle, 0, ksizes_local_bulk,
                                   0, ksizes_bulk_size);
        if (hret != HG_SUCCESS) {
            SDSKV_LOG_ERROR(mid, "failed to issue bulk transfer (hret = %d)",
                            hret);
            out.ret = SDSKV_MAKE_HG_ERROR(hret);
            return;
        }
    }

    /* transfer the vsizes back to the client */
    if (vsizes_bulk_size) {
        hret = margo_bulk_transfer(mid, HG_BULK_PUSH, origin_addr,
                                   in.vsizes_bulk_handle, 0, vsizes_local_bulk,
                                   0, vsizes_bulk_size);
        if (hret != HG_SUCCESS) {
            SDSKV_LOG_ERROR(mid, "failed to issue bulk transfer (hret = %d)",
                            hret);
            out.ret = SDSKV_MAKE_HG_ERROR(hret);
            return;
        }
    }

    if (size_error) out.ret = SDSKV_ERR_SIZE;

    /* create an array of addresses pointing to keys */
    std::vector<void*> keys_addr(num_keys);
    for (unsigned i = 0; i < num_keys; i++) {
        keys_addr[i] = (void*)(keyvals[i].first.data());
    }

    /* create an array of addresses pointing to values */
    std::vector<void*> vals_addr(num_keys);
    for (unsigned i = 0; i < num_keys; i++) {
        vals_addr[i] = (void*)(keyvals[i].second.data());
    }

    /* expose the keys for bulk transfer */
    hret
        = margo_bulk_create(mid, num_keys, keys_addr.data(), true_ksizes.data(),
                            HG_BULK_READ_ONLY, &keys_local_bulk);
    if (hret != HG_SUCCESS) {
        SDSKV_LOG_ERROR(mid, "could not create bulk");
        out.ret = SDSKV_MAKE_HG_ERROR(hret);
        return;
    }
    DEFER(margo_bulk_free_keys_local, margo_bulk_free(keys_local_bulk));

    /* expose the values for bulk transfer */
    hret
        = margo_bulk_create(mid, num_keys, vals_addr.data(), true_vsizes.data(),
                            HG_BULK_READ_ONLY, &vals_local_bulk);
    if (hret != HG_SUCCESS) {
        SDSKV_LOG_ERROR(mid, "could not create bulk");
        out.ret = SDSKV_MAKE_HG_ERROR(hret);
        return;
    }
    DEFER(margo_bulk_free_vals_local, margo_bulk_free(vals_local_bulk));

    uint64_t remote_offset = 0;
    uint64_t local_offset  = 0;

    /* transfer the keys to the client */
    for (unsigned i = 0; i < num_keys; i++) {
        if (true_ksizes[i] > 0) {
            hret = margo_bulk_transfer(
                mid, HG_BULK_PUSH, origin_addr, in.keys_bulk_handle,
                remote_offset, keys_local_bulk, local_offset, true_ksizes[i]);
            if (hret != HG_SUCCESS) {
                SDSKV_LOG_ERROR(
                    mid, "failed to issue bulk transfer (hret = %d)", hret);
                out.ret = SDSKV_MAKE_HG_ERROR(hret);
                return;
            }
        }
        remote_offset += remote_ksizes[i];
        local_offset += true_ksizes[i];
    }

    remote_offset = 0;
    local_offset  = 0;

    /* transfer the values to the client */
    for (unsigned i = 0; i < num_keys; i++) {
        if (true_vsizes[i] > 0) {
            hret = margo_bulk_transfer(
                mid, HG_BULK_PUSH, origin_addr, in.vals_bulk_handle,
                remote_offset, vals_local_bulk, local_offset, true_vsizes[i]);
            if (hret != HG_SUCCESS) {
                SDSKV_LOG_ERROR(
                    mid, "failed to issue bulk transfer (hret = %d)", hret);
                out.ret = SDSKV_MAKE_HG_ERROR(hret);
                return;
            }
        }
        remote_offset += remote_vsizes[i];
        local_offset += true_vsizes[i];
    }

    out.ret = SDSKV_SUCCESS;
}
DEFINE_MARGO_RPC_HANDLER(sdskv_list_keyvals_ult)

static void sdskv_migrate_keys_ult(hg_handle_t handle)
{
    hg_return_t        hret;
    migrate_keys_in_t  in;
    migrate_keys_out_t out;
    out.ret = SDSKV_SUCCESS;

    ENSURE_MARGO_DESTROY;
    ENSURE_MARGO_RESPOND;
    FIND_MID_AND_PROVIDER;
    GET_INPUT;
    ENSURE_MARGO_FREE_INPUT;

    ABT_rwlock_rdlock(provider->lock);
    auto it = provider->databases.find(in.source_db_id);
    if (it == provider->databases.end()) {
        ABT_rwlock_unlock(provider->lock);
        SDSKV_LOG_ERROR(mid, "couldn't find target database with id %lu",
                        in.source_db_id);
        out.ret = SDSKV_ERR_UNKNOWN_DB;
        return;
    }
    auto db = it->second;
    ABT_rwlock_unlock(provider->lock);

    /* lookup the address of the target provider */
    hg_addr_t target_addr = HG_ADDR_NULL;
    hret = margo_addr_lookup(mid, in.target_addr, &target_addr);
    if (hret != HG_SUCCESS) {
        SDSKV_LOG_ERROR(mid, "failed to lookup target address (hret = %d)",
                        hret);
        out.ret = SDSKV_MAKE_HG_ERROR(hret);
        return;
    }
    DEFER(margo_addr_free, margo_addr_free(mid, target_addr));

    /* create the bulk buffer to receive the keys */
    char* buffer = (char*)malloc(in.bulk_size);
    DEFER(free_buffer, free(buffer));
    hg_size_t bulk_size   = in.bulk_size;
    hg_bulk_t bulk_handle = HG_BULK_NULL;
    hret = margo_bulk_create(mid, 1, (void**)&buffer, &bulk_size,
                             HG_BULK_WRITE_ONLY, &bulk_handle);
    if (hret != HG_SUCCESS) {
        SDSKV_LOG_ERROR(mid, "failed to create bulk handle (hret = %d)", hret);
        out.ret = SDSKV_MAKE_HG_ERROR(hret);
        return;
    }
    DEFER(margo_bulk_free, margo_bulk_free(bulk_handle));

    /* issue a bulk pull */
    hret = margo_bulk_transfer(mid, HG_BULK_PULL, info->addr, in.keys_bulk, 0,
                               bulk_handle, 0, in.bulk_size);
    if (hret != HG_SUCCESS) {
        SDSKV_LOG_ERROR(mid, "failed to issue bulk transfer (hret = %d)", hret);
        out.ret = SDSKV_MAKE_HG_ERROR(hret);
        return;
    }

    /* remap the keys from the void* buffer */
    hg_size_t* seg_sizes   = (hg_size_t*)buffer;
    char*      packed_keys = buffer + in.num_keys * sizeof(hg_size_t);

    /* create a handle for a "put" RPC */
    hg_handle_t put_handle;
    hret = margo_create(mid, target_addr, provider->sdskv_put_id, &put_handle);
    if (hret != HG_SUCCESS) {
        SDSKV_LOG_ERROR(mid, "failed to create \"put\" RPC handle (hret = %d)",
                        hret);
        out.ret = SDSKV_MAKE_HG_ERROR(hret);
        return;
    }
    DEFER(margo_destroy_put_handle, margo_destroy(put_handle));

    /* iterate over the keys */
    size_t    offset = 0;
    put_in_t  put_in;
    put_out_t put_out;
    for (unsigned i = 0; i < in.num_keys; i++) {
        /* find the key */
        char*  key  = packed_keys + offset;
        size_t size = seg_sizes[i];
        offset += size;

        ds_bulk_t kdata(key, key + size);
        ds_bulk_t vdata;
        auto      b = db->get(kdata, vdata);
        if (!b) continue;

        /* issue a "put" for that key */
        put_in.db_id      = in.target_db_id;
        put_in.key.data   = (kv_ptr_t)kdata.data();
        put_in.key.size   = kdata.size();
        put_in.value.data = (kv_ptr_t)vdata.data();
        put_in.value.size = vdata.size();
        /* forward put call */
        hret = margo_provider_forward(in.target_provider_id, put_handle,
                                      &put_in);
        if (hret != HG_SUCCESS) {
            SDSKV_LOG_ERROR(mid, "failed to forward \"put\" RPC (hret = %d)",
                            hret);
            out.ret = SDSKV_ERR_MIGRATION;
            return;
        }
        /* get output of the put call */
        hret = margo_get_output(put_handle, &put_out);
        if (hret != HG_SUCCESS || put_out.ret != SDSKV_SUCCESS) {
            SDSKV_LOG_ERROR(mid,
                            "put RPC yielded incorrect output (hret = %d, "
                            "put_out.ret = %d)",
                            hret, put_out.ret);
            out.ret = SDSKV_ERR_MIGRATION;
            return;
        }
        margo_free_output(put_handle, &out);
        /* remove the key if needed */
        if (in.flag == SDSKV_REMOVE_ORIGINAL) { db->erase(kdata); }
    }
}
DEFINE_MARGO_RPC_HANDLER(sdskv_migrate_keys_ult)

static void sdskv_migrate_key_range_ult(hg_handle_t handle)
{
    hg_return_t            hret;
    migrate_key_range_in_t in;
    migrate_keys_out_t     out;

    ENSURE_MARGO_DESTROY;
    ENSURE_MARGO_RESPOND;
    FIND_MID_AND_PROVIDER;
    GET_INPUT;
    ENSURE_MARGO_FREE_INPUT;

    ABT_rwlock_rdlock(provider->lock);
    auto it = provider->databases.find(in.source_db_id);
    if (it == provider->databases.end()) {
        ABT_rwlock_unlock(provider->lock);
        SDSKV_LOG_ERROR(mid, "couldn't find target database with id %lu",
                        in.source_db_id);
        out.ret = SDSKV_ERR_UNKNOWN_DB;
        return;
    }
    auto db = it->second;
    ABT_rwlock_unlock(provider->lock);

    /* lock the provider */
    ABT_rwlock_rdlock(provider->lock);
    DEFER(rwlock_unlock, ABT_rwlock_unlock(provider->lock));

    // TODO implement this operation
    out.ret = SDSKV_OP_NOT_IMPL;
}
DEFINE_MARGO_RPC_HANDLER(sdskv_migrate_key_range_ult)

static void sdskv_migrate_keys_prefixed_ult(hg_handle_t handle)
{
    hg_return_t                hret;
    migrate_keys_prefixed_in_t in;
    migrate_keys_out_t         out;

    ENSURE_MARGO_DESTROY;
    ENSURE_MARGO_RESPOND;
    FIND_MID_AND_PROVIDER;
    GET_INPUT;
    ENSURE_MARGO_FREE_INPUT;

    ABT_rwlock_rdlock(provider->lock);
    auto it = provider->databases.find(in.source_db_id);
    if (it == provider->databases.end()) {
        ABT_rwlock_unlock(provider->lock);
        SDSKV_LOG_ERROR(mid, "couldn't find target database with id %lu",
                        in.source_db_id);
        out.ret = SDSKV_ERR_UNKNOWN_DB;
        return;
    }
    auto db = it->second;
    ABT_rwlock_unlock(provider->lock);

    /* lookup the address of the target provider */
    hg_addr_t target_addr = HG_ADDR_NULL;
    hret = margo_addr_lookup(mid, in.target_addr, &target_addr);
    if (hret != HG_SUCCESS) {
        SDSKV_LOG_ERROR(mid, "failed to lookup target address (hret = %d)",
                        hret);
        out.ret = SDSKV_MAKE_HG_ERROR(hret);
        return;
    }
    DEFER(margo_addr_free, margo_addr_free(mid, target_addr));

    /* create a handle for a "put" RPC */
    hg_handle_t put_handle;
    hret = margo_create(mid, target_addr, provider->sdskv_put_id, &put_handle);
    if (hret != HG_SUCCESS) {
        SDSKV_LOG_ERROR(mid, "failed to create \"put\" RPC handle (hret = %d)",
                        hret);
        out.ret = SDSKV_MAKE_HG_ERROR(hret);
        return;
    }
    DEFER(margo_destroy_put_handle, margo_destroy(put_handle));

    /* iterate over the keys by packets of 64 */
    /* XXX make this number configurable */
    std::vector<std::pair<ds_bulk_t, ds_bulk_t>> batch;
    ds_bulk_t                                    start_key;
    ds_bulk_t                                    prefix(in.key_prefix.data,
                     in.key_prefix.data + in.key_prefix.size);
    do {
        try {
            batch = db->list_keyvals(start_key, 64, prefix);
        } catch (int err) {
            out.ret = err;
            return;
        }
        if (batch.size() == 0) break;
        /* issue a put for all the keys in this batch */
        put_in_t  put_in;
        put_out_t put_out;
        for (auto& kv : batch) {
            put_in.db_id      = in.target_db_id;
            put_in.key.data   = (kv_ptr_t)kv.first.data();
            put_in.key.size   = kv.first.size();
            put_in.value.data = (kv_ptr_t)kv.second.data();
            put_in.value.size = kv.second.size();
            /* forward put call */
            hret = margo_provider_forward(in.target_provider_id, put_handle,
                                          &put_in);
            if (hret != HG_SUCCESS) {
                SDSKV_LOG_ERROR(
                    mid, "failed to forward \"put\" RPC (hret = %d)", hret);
                out.ret = SDSKV_ERR_MIGRATION;
                return;
            }
            /* get output of the put call */
            hret = margo_get_output(put_handle, &put_out);
            if (hret != HG_SUCCESS || put_out.ret != SDSKV_SUCCESS) {
                SDSKV_LOG_ERROR(mid, "\"put\" RPC failed (hret = %d, ret = %d)",
                                hret, put_out.ret);
                out.ret = SDSKV_ERR_MIGRATION;
                return;
            }
            margo_free_output(put_handle, &out);
            /* remove the key if needed */
            if (in.flag == SDSKV_REMOVE_ORIGINAL) { db->erase(kv.first); }
        }
        /* if original is removed, start_key can stay empty since we
           keep taking the beginning of the container, otherwise
           we need to update start_key. */
        if (in.flag != SDSKV_REMOVE_ORIGINAL) {
            start_key = std::move(batch.rbegin()->first);
        }
    } while (batch.size() == 64);
}
DEFINE_MARGO_RPC_HANDLER(sdskv_migrate_keys_prefixed_ult)

static void sdskv_migrate_all_keys_ult(hg_handle_t handle)
{
    hg_return_t           hret;
    migrate_all_keys_in_t in;
    migrate_keys_out_t    out;
    out.ret = SDSKV_SUCCESS;

    ENSURE_MARGO_DESTROY;
    ENSURE_MARGO_RESPOND;
    FIND_MID_AND_PROVIDER;
    GET_INPUT;
    ENSURE_MARGO_FREE_INPUT;

    ABT_rwlock_rdlock(provider->lock);
    auto it = provider->databases.find(in.source_db_id);
    if (it == provider->databases.end()) {
        ABT_rwlock_unlock(provider->lock);
        SDSKV_LOG_ERROR(mid, "couldn't find target database with id %lu",
                        in.source_db_id);
        out.ret = SDSKV_ERR_UNKNOWN_DB;
        return;
    }
    auto db = it->second;
    ABT_rwlock_unlock(provider->lock);

    /* lookup the address of the target provider */
    hg_addr_t target_addr = HG_ADDR_NULL;
    hret = margo_addr_lookup(mid, in.target_addr, &target_addr);
    if (hret != HG_SUCCESS) {
        SDSKV_LOG_ERROR(mid, "failed to lookup target address (hret = %d)",
                        hret);
        out.ret = SDSKV_MAKE_HG_ERROR(hret);
        return;
    }
    DEFER(margo_addr_free, margo_addr_free(mid, target_addr));

    /* create a handle for a "put" RPC */
    hg_handle_t put_handle;
    hret = margo_create(mid, target_addr, provider->sdskv_put_id, &put_handle);
    if (hret != HG_SUCCESS) {
        SDSKV_LOG_ERROR(mid, "failed to create \"put\" RPC handle (hret = %d)",
                        hret);
        out.ret = SDSKV_MAKE_HG_ERROR(hret);
        return;
    }
    DEFER(margo_destroy_put_handle, margo_destroy(put_handle));

    /* iterate over the keys by packets of 64 */
    /* XXX make this number configurable */
    std::vector<std::pair<ds_bulk_t, ds_bulk_t>> batch;
    ds_bulk_t                                    start_key;
    do {
        try {
            batch = db->list_keyvals(start_key, 64);
        } catch (int err) {
            SDSKV_LOG_ERROR(mid, "list_keyvals failed (err = %d)", err);
            out.ret = err;
            return;
        }
        if (batch.size() == 0) break;
        /* issue a put for all the keys in this batch */
        put_in_t  put_in;
        put_out_t put_out;
        for (auto& kv : batch) {
            put_in.db_id      = in.target_db_id;
            put_in.key.data   = (kv_ptr_t)kv.first.data();
            put_in.key.size   = kv.first.size();
            put_in.value.data = (kv_ptr_t)kv.second.data();
            put_in.value.size = kv.second.size();
            /* forward put call */
            hret = margo_provider_forward(in.target_provider_id, put_handle,
                                          &put_in);
            if (hret != HG_SUCCESS) {
                SDSKV_LOG_ERROR(
                    mid, "failed to forward \"put\" RPC (hret = %d)", hret);
                out.ret = SDSKV_ERR_MIGRATION;
                return;
            }
            /* get output of the put call */
            hret = margo_get_output(put_handle, &put_out);
            if (hret != HG_SUCCESS || put_out.ret != SDSKV_SUCCESS) {
                SDSKV_LOG_ERROR(mid, "\"put\" RPC failed (hret = %d, ret = %d)",
                                hret, put_out.ret);
                out.ret = SDSKV_ERR_MIGRATION;
                return;
            }
            margo_free_output(put_handle, &out);
            /* remove the key if needed */
            if (in.flag == SDSKV_REMOVE_ORIGINAL) { db->erase(kv.first); }
        }
        /* if original is removed, start_key can stay empty since we
           keep taking the beginning of the container, otherwise
           we need to update start_key. */
        if (in.flag != SDSKV_REMOVE_ORIGINAL) {
            start_key = std::move(batch.rbegin()->first);
        }
    } while (batch.size() == 64);
}
DEFINE_MARGO_RPC_HANDLER(sdskv_migrate_all_keys_ult)

static void sdskv_migrate_database_ult(hg_handle_t handle)
{
    migrate_database_in_t in;
    in.dest_remi_addr = NULL;
    in.dest_root      = NULL;
    migrate_database_out_t out;
    hg_addr_t              dest_addr = HG_ADDR_NULL;
    hg_return_t            hret;
    int                    ret;
#ifdef USE_REMI
    remi_provider_handle_t remi_ph       = REMI_PROVIDER_HANDLE_NULL;
    remi_fileset_t         local_fileset = REMI_FILESET_NULL;
#endif

    memset(&out, 0, sizeof(out));

    ENSURE_MARGO_DESTROY;
    ENSURE_MARGO_RESPOND;
    FIND_MID_AND_PROVIDER;
    GET_INPUT;
    ENSURE_MARGO_FREE_INPUT;

    ABT_rwlock_rdlock(provider->lock);
    auto it = provider->databases.find(in.source_db_id);
    if (it == provider->databases.end()) {
        ABT_rwlock_unlock(provider->lock);
        SDSKV_LOG_ERROR(mid, "couldn't find target database with id %lu",
                        in.source_db_id);
        out.ret = SDSKV_ERR_UNKNOWN_DB;
        return;
    }
    auto db = it->second;
    ABT_rwlock_unlock(provider->lock);

#ifdef USE_REMI
    if (provider->remi_client == NULL) {
        out.ret = SDSKV_ERR_REMI;
        return;
    }

    /* sync the database */
    db->sync();

    /* lookup the address of the destination REMI provider */
    hret = margo_addr_lookup(mid, in.dest_remi_addr, &dest_addr);
    if (hret != HG_SUCCESS) {
        SDSKV_LOG_ERROR(mid, "failed to lookup target address (hret = %d)",
                        hret);
        out.ret = SDSKV_MAKE_HG_ERROR(hret);
        return;
    }
    DEFER(margo_addr_free, margo_addr_free(mid, dest_addr));

    /* use the REMI client to create a REMI provider handle */
    ret = remi_provider_handle_create(provider->remi_client, dest_addr,
                                      in.dest_remi_provider_id, &remi_ph);
    if (ret != REMI_SUCCESS) {
        SDSKV_LOG_ERROR(mid, "failed to create REMI provider handle (ret = %d)",
                        ret);
        out.ret      = SDSKV_ERR_REMI;
        out.remi_ret = ret;
        return;
    }
    DEFER(remi_provider_handle_release, remi_provider_handle_release(remi_ph));

    /* create a fileset */
    local_fileset = db->create_and_populate_fileset();
    if (local_fileset == REMI_FILESET_NULL) {
        SDSKV_LOG_ERROR(mid, "failed to create and populate REMI fileset");
        out.ret = SDSKV_OP_NOT_IMPL;
        return;
    }
    DEFER(remi_fileset_free, remi_fileset_free(local_fileset));

    /* issue the migration */
    int status = 0;
    ret        = remi_fileset_migrate(remi_ph, local_fileset, in.dest_root,
                               in.remove_src, REMI_USE_ABTIO, &status);
    if (ret != REMI_SUCCESS) {
        out.remi_ret = ret;
        if (ret == REMI_ERR_USER)
            out.ret = status;
        else {
            out.ret = SDSKV_ERR_REMI;
        }
        SDSKV_LOG_ERROR(mid, "failed to migrate REMI fileset (ret = %d)", ret);
        return;
    }

    if (in.remove_src) {
        ret     = sdskv_provider_remove_database(provider, in.source_db_id);
        out.ret = ret;
    }
#else
    out.ret = SDSKV_OP_NOT_IMPL;
#endif
}
DEFINE_MARGO_RPC_HANDLER(sdskv_migrate_database_ult)

static void sdskv_server_finalize_cb(void* data)
{
    sdskv_provider_t provider = (sdskv_provider_t)data;
    assert(provider);
    margo_instance_id mid = provider->mid;

#ifdef USE_SYMBIOMON
    fprintf(stderr, "SDSKV provider destroy invoked\n");
    int pid = getpid();
    char * pid_s = (char*)malloc(40);
    char * pid_bs = (char*)malloc(40);
    char * pid_ds = (char*)malloc(40);
    char * pid_ne = (char*)malloc(40);
    char * pid_pds = (char*)malloc(40);
    char * pid_pl = (char*)malloc(40);
    char * pid_pne = (char*)malloc(40);

    sprintf(pid_s, "sdskv_putpacked_latency_%d_%d", pid, provider->provider_id);
    sprintf(pid_bs, "sdskv_putpacked_batch_size_%d_%d", pid, provider->provider_id);
    sprintf(pid_ds, "sdskv_putpacked_data_size_%d_%d", pid, provider->provider_id);
    sprintf(pid_pne, "sdskv_putpacked_num_entrants_%d_%d", pid, provider->provider_id);
    sprintf(pid_ne, "sdskv_put_num_entrants_%d_%d", pid, provider->provider_id);
    sprintf(pid_pds, "sdskv_put_data_size_%d_%d", pid, provider->provider_id);
    sprintf(pid_pl, "sdskv_put_latency_%d_%d", pid, provider->provider_id);

    symbiomon_metric_dump_raw_data(provider->put_packed_latency, pid_s);
    symbiomon_metric_dump_raw_data(provider->put_packed_batch_size, pid_bs);
    symbiomon_metric_dump_raw_data(provider->put_packed_data_size, pid_ds);
    symbiomon_metric_dump_raw_data(provider->put_num_entrants, pid_ne);
    symbiomon_metric_dump_raw_data(provider->put_data_size, pid_pds);
    symbiomon_metric_dump_raw_data(provider->put_latency, pid_pl);
    symbiomon_metric_dump_raw_data(provider->putpacked_num_entrants, pid_pne);

    free(pid_s); free(pid_bs);  free(pid_ds); free(pid_ne); free(pid_pds); 
    free(pid_pl); free(pid_pne);
#endif

    sdskv_provider_remove_all_databases(provider);

    margo_deregister(mid, provider->sdskv_open_id);
    margo_deregister(mid, provider->sdskv_count_databases_id);
    margo_deregister(mid, provider->sdskv_list_databases_id);
    margo_deregister(mid, provider->sdskv_put_id);
    margo_deregister(mid, provider->sdskv_put_multi_id);
    margo_deregister(mid, provider->sdskv_bulk_put_id);
    margo_deregister(mid, provider->sdskv_get_id);
    margo_deregister(mid, provider->sdskv_get_multi_id);
    margo_deregister(mid, provider->sdskv_exists_id);
    margo_deregister(mid, provider->sdskv_erase_id);
    margo_deregister(mid, provider->sdskv_erase_multi_id);
    margo_deregister(mid, provider->sdskv_length_id);
    margo_deregister(mid, provider->sdskv_length_multi_id);
    margo_deregister(mid, provider->sdskv_bulk_get_id);
    margo_deregister(mid, provider->sdskv_list_keys_id);
    margo_deregister(mid, provider->sdskv_list_keyvals_id);
    margo_deregister(mid, provider->sdskv_migrate_keys_id);
    margo_deregister(mid, provider->sdskv_migrate_key_range_id);
    margo_deregister(mid, provider->sdskv_migrate_keys_prefixed_id);
    margo_deregister(mid, provider->sdskv_migrate_all_keys_id);
    margo_deregister(mid, provider->sdskv_migrate_database_id);

    ABT_rwlock_free(&(provider->lock));

    delete provider;

    return;
}

struct migration_metadata {
    std::unordered_map<std::string, std::string> _metadata;
};

static void get_metadata(const char* key, const char* value, void* uargs)
{
    auto md            = static_cast<migration_metadata*>(uargs);
    md->_metadata[key] = value;
}

#ifdef USE_REMI

static int sdskv_pre_migration_callback(remi_fileset_t fileset, void* uargs)
{
    sdskv_provider_t   provider = (sdskv_provider_t)uargs;
    migration_metadata md;
    remi_fileset_foreach_metadata(fileset, get_metadata,
                                  static_cast<void*>(&md));
    // (1) check the metadata
    if (md._metadata.find("database_type") == md._metadata.end()
        || md._metadata.find("database_name") == md._metadata.end()
        || md._metadata.find("comparison_function") == md._metadata.end()) {
        return -101;
    }
    std::string       db_name = md._metadata["database_name"];
    std::string       db_type = md._metadata["database_type"];
    std::string       comp_fn = md._metadata["comparison_function"];
    std::vector<char> db_root;
    size_t            root_size = 0;
    remi_fileset_get_root(fileset, NULL, &root_size);
    db_root.resize(root_size + 1);
    remi_fileset_get_root(fileset, db_root.data(), &root_size);
    // (2) check that there isn't a database with the same name

    {
        ABT_rwlock_rdlock(provider->lock);
        auto unlock
            = at_exit([provider]() { ABT_rwlock_unlock(provider->lock); });
        if (provider->name2id.find(db_name) != provider->name2id.end()) {
            return -102;
        }
    }
    // (3) check that the type of database is ok to migrate
    if (db_type != "berkeleydb" && db_type != "leveldb") { return -103; }
    // (4) check that the comparison function exists
    if (comp_fn.size() != 0) {
        if (provider->compfunctions.find(comp_fn)
            == provider->compfunctions.end()) {
            return -104;
        }
    }
    // (5) fill up a config structure and call the user-defined pre-migration
    // callback
    if (provider->pre_migration_callback) {
        sdskv_config_t config;
        config.db_name = db_name.c_str();
        config.db_path = db_root.data();
        if (db_type == "berkeleydb")
            config.db_type = KVDB_BERKELEYDB;
        else if (db_type == "leveldb")
            config.db_type = KVDB_LEVELDB;
        if (comp_fn.size() != 0)
            config.db_comp_fn_name = comp_fn.c_str();
        else
            config.db_comp_fn_name = NULL;
        if (md._metadata.find("no_overwrite") != md._metadata.end())
            config.db_no_overwrite = 1;
        else
            config.db_no_overwrite = 0;
        (provider->pre_migration_callback)(provider, &config,
                                           provider->migration_uargs);
    }
    // all is fine
    return 0;
}

static int sdskv_post_migration_callback(remi_fileset_t fileset, void* uargs)
{
    sdskv_provider_t   provider = (sdskv_provider_t)uargs;
    migration_metadata md;
    remi_fileset_foreach_metadata(fileset, get_metadata,
                                  static_cast<void*>(&md));

    std::string db_name = md._metadata["database_name"];
    std::string db_type = md._metadata["database_type"];
    std::string comp_fn = md._metadata["comparison_function"];

    std::vector<char> db_root;
    size_t            root_size = 0;
    remi_fileset_get_root(fileset, NULL, &root_size);
    db_root.resize(root_size + 1);
    remi_fileset_get_root(fileset, db_root.data(), &root_size);

    sdskv_config_t config;
    config.db_name = db_name.c_str();
    config.db_path = db_root.data();
    if (db_type == "berkeleydb")
        config.db_type = KVDB_BERKELEYDB;
    else if (db_type == "leveldb")
        config.db_type = KVDB_LEVELDB;
    if (comp_fn.size() != 0)
        config.db_comp_fn_name = comp_fn.c_str();
    else
        config.db_comp_fn_name = NULL;
    if (md._metadata.find("no_overwrite") != md._metadata.end())
        config.db_no_overwrite = 1;
    else
        config.db_no_overwrite = 0;

    sdskv_database_id_t db_id;
    int ret = sdskv_provider_attach_database(provider, &config, &db_id);
    if (ret != SDSKV_SUCCESS) return -106;

    if (provider->post_migration_callback) {
        (provider->post_migration_callback)(provider, &config, db_id,
                                            provider->migration_uargs);
    }
    return 0;
}

#endif

static int check_provider_config(sdskv_provider_t provider)
{
    auto& cfg = provider->json_cfg;
    // null is ok
    if (cfg.isNull()) return SDSKV_SUCCESS;
    // otherwise config must be an object
    if (!cfg.isObject()) {
        SDSKV_LOG_ERROR(provider->mid, "config is not an object");
        return SDSKV_ERR_CONFIG;
    }
    // check comparators
    if (cfg.isMember("comparators")) {
        auto& comparators = cfg["comparators"];
        // comparators must be an array
        if (!comparators.isArray()) {
            SDSKV_LOG_ERROR(provider->mid,
                            "comparators field must be an array");
            return SDSKV_ERR_CONFIG;
        }
        // each element must be an object with "name" and "library" string
        // fields
        for (auto it = comparators.begin(); it != comparators.end(); it++) {
            if (!it->isObject()) {
                SDSKV_LOG_ERROR(provider->mid,
                                "comparator entry should be an object");
                return SDSKV_ERR_CONFIG;
            }
            if (!it->isMember("name")) {
                SDSKV_LOG_ERROR(provider->mid, "comparator should have a name");
                return SDSKV_ERR_CONFIG;
            }
            if (!(*it)["name"].isString()) {
                SDSKV_LOG_ERROR(provider->mid,
                                "comparator name should be a string");
                return SDSKV_ERR_CONFIG;
            }
            if (!it->isMember("library")) {
                // the string field is optional
                (*it)["library"] = "";
            }
            if (!(*it)["library"].isString()) {
                SDSKV_LOG_ERROR(provider->mid,
                                "comparator library should be a string");
                return SDSKV_ERR_CONFIG;
            }
        }
    }
    // check databases
    if (cfg.isMember("databases")) {
        auto& databases = cfg["databases"];
        // databases must be an array
        if (!databases.isArray()) {
            SDSKV_LOG_ERROR(provider->mid, "databases field must be an array");
            return SDSKV_ERR_CONFIG;
        }
        // check each database
        for (auto it = databases.begin(); it != databases.end(); it++) {
            if (!it->isObject()) {
                SDSKV_LOG_ERROR(provider->mid,
                                "database entry should be an object");
                return SDSKV_ERR_CONFIG;
            }
            // check name field
            if (!it->isMember("name")) {
                SDSKV_LOG_ERROR(provider->mid, "database should have a name");
                return SDSKV_ERR_CONFIG;
            }
            if (!(*it)["name"].isString()) {
                SDSKV_LOG_ERROR(provider->mid,
                                "database name should be a string");
                return SDSKV_ERR_CONFIG;
            }
            // check type field
            if (!it->isMember("type")) {
                SDSKV_LOG_ERROR(provider->mid, "database should have a type");
                return SDSKV_ERR_CONFIG;
            }
            if (!(*it)["type"].isString()) {
                SDSKV_LOG_ERROR(provider->mid,
                                "database type should be a string");
                return SDSKV_ERR_CONFIG;
            }
            // check path
            if (!it->isMember("path")) { (*it)["path"] = ""; }
            if (!(*it)["path"].isString()) {
                SDSKV_LOG_ERROR(provider->mid,
                                "database path should be a string");
                return SDSKV_ERR_CONFIG;
            }
            // check no_overwrite
            if (!it->isMember("no_overwrite")) {
                (*it)["no_overwrite"] = false;
            }
            if (!(*it)["no_overwrite"].isBool()) {
                SDSKV_LOG_ERROR(
                    provider->mid,
                    "database no_overwrite field should be a boolean");
                return SDSKV_ERR_CONFIG;
            }
            // check comparator
            if (!it->isMember("comparator")) { (*it)["comparator"] = ""; }
            if (!(*it)["comparator"].isString()) {
                SDSKV_LOG_ERROR(provider->mid,
                                "database comparator should be a string");
                return SDSKV_ERR_CONFIG;
            }
        }
    }
    return SDSKV_SUCCESS;
}

static int populate_provider_from_config(sdskv_provider_t provider)
{
    int ret = SDSKV_SUCCESS;

    ret = check_provider_config(provider);
    if (ret != SDSKV_SUCCESS) return ret;

    auto& comparators = provider->json_cfg["comparators"];
    for (auto it = comparators.begin(); it != comparators.end(); it++) {
        ret = sdskv_provider_find_comparison_function(
            provider, (*it)["library"].asString().c_str(),
            (*it)["name"].asString().c_str());
        if (ret != SDSKV_SUCCESS) { return ret; }
    }
    sdskv_database_id_t id;
    sdskv_config_t      db_cfg;
    auto&               databases = provider->json_cfg["databases"];
    for (auto it = databases.begin(); it != databases.end(); it++) {
        std::string name         = (*it)["name"].asString();
        std::string type         = (*it)["type"].asString();
        std::string path         = (*it)["path"].asString();
        std::string comp         = (*it)["comparator"].asString();
        bool        no_overwrite = (*it)["no_overwrite"].asBool();
        db_cfg.db_name           = name.c_str();
        db_cfg.db_path           = path.c_str();
        db_cfg.db_comp_fn_name   = comp.c_str();
        db_cfg.db_no_overwrite   = no_overwrite;
        if (type == "map")
            db_cfg.db_type = KVDB_MAP;
        else if (type == "null")
            db_cfg.db_type = KVDB_NULL;
        else if (type == "leveldb" || type == "ldb")
            db_cfg.db_type = KVDB_LEVELDB;
        else if (type == "berkeleydb" || type == "bdb")
            db_cfg.db_type = KVDB_BERKELEYDB;
        else if (type == "forward" || type == "fwd")
            db_cfg.db_type = KVDB_FORWARDDB;
        else {
            SDSKV_LOG_ERROR(provider->mid, "unknown database type \"%s\"",
                            type.c_str());
            ret = SDSKV_ERR_CONFIG;
            break;
        }
        ret = sdskv_provider_attach_database(provider, &db_cfg, &id);
        if (ret == SDSKV_SUCCESS) { (*it)["__database_id__"] = id; }
    }
    if (ret != SDSKV_SUCCESS) sdskv_provider_remove_all_databases(provider);
    return ret;
}
