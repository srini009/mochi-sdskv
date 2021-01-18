/*
 * (C) 2018 The University of Chicago
 * 
 * See COPYRIGHT in top-level directory.
 */

#ifndef __SDSKV_SERVER_H
#define __SDSKV_SERVER_H

#include <margo.h>
#include <abt-io.h>
#include <sdskv-common.h>

#ifdef __cplusplus
extern "C" {
#endif

#define SDSKV_ABT_POOL_DEFAULT ABT_POOL_NULL
#define SDSKV_PROVIDER_ID_DEFAULT 0
#define SDSKV_PROVIDER_IGNORE NULL
#define SDSKV_COMPARE_DEFAULT NULL

typedef struct sdskv_server_context_t* sdskv_provider_t;
typedef int (*sdskv_compare_fn)(const void*, hg_size_t, const void*, hg_size_t);

typedef struct sdskv_config_t {
    const char*      db_name;         // name of the database
    const char*      db_path;         // path to the database
    sdskv_db_type_t  db_type;         // type of database
    const char*      db_comp_fn_name; // name of registered comparison function (can be NULL)
    int              db_no_overwrite; // prevents overwritting data if set to 1
} sdskv_config_t;

#define SDSKV_CONFIG_DEFAULT { "", "", KVDB_MAP, SDSKV_COMPARE_DEFAULT, 0 }

typedef void (*sdskv_pre_migration_callback_fn)(sdskv_provider_t, const sdskv_config_t*, void*);
typedef void (*sdskv_post_migration_callback_fn)(sdskv_provider_t, const sdskv_config_t*, sdskv_database_id_t, void*);

/**
 * @brief Creates a new provider.
 *
 * @param[in] mid Margo instance
 * @param[in] provider_id provider id
 * @param[in] pool Argobots pool
 * @param[in] json_config Json string
 * @param[out] provider provider handle
 *
 * @return SDSKV_SUCCESS or error code defined in sdskv-common.h
 */
int sdskv_provider_register(
        margo_instance_id mid,
        uint16_t provider_id,
        ABT_pool pool,
        const char *json_config,
        sdskv_provider_t* provider);

/**
 * @brief Obtain a JSON string describing provider's configuration
 *
 */
char * sdskv_provider_get_json(sdskv_provider_t provider);

/**
 * @brief Obtain underlying margo identifier
 */
margo_instance_id sdskv_provider_get_mid(sdskv_provider_t provider);
/**
 * @brief Deregister the provider's RPCs and destroys the provider.
 *
 * @param provider Provider to destroy.
 *
 * @return SDSKV_SUCCESS  or error code defined in sdskv-common.h
 */
int sdskv_provider_destroy(sdskv_provider_t provider);

/**
 * @brief Registers a comparison function for databases to use.
 *
 * @param provider provider
 * @param function_name name of the comparison function
 * @param comp_fn pointer to the comparison function
 *
 * @return SDSKV_SUCCESS or error code defined in sdskv-common.h
 */
int sdskv_provider_add_comparison_function(
        sdskv_provider_t provider,
        const char* function_name,
        sdskv_compare_fn comp_fn);

/**
 * @brief
 * @pram provider provider
 * @pram library dynamically loaded library containing comparison function
 * @param function_name name of function to load from library
 *
 * @return SDSKV_SUCCES or error code defined in sdskv-common.h
 */

int sdskv_provider_find_comparison_function(
        sdskv_provider_t provider,
        const char* library,
        const char* function_name);
/**
 * Makes the provider start managing a database. The database will
 * be created if it does not exist. Otherwise, the provider will start
 * to manage the existing database.
 *
 * @param[in] provider provider
 * @param[in] config configuration object to use for the database
 * @param[out] db_id resulting id identifying the database
 *
 * @return SDSKV_SUCCESS or error code defined in sdskv-common.h
 */
int sdskv_provider_attach_database(
        sdskv_provider_t provider,
        const sdskv_config_t* config,
        sdskv_database_id_t* sb_id);

/**
 * Makes the provider stop managing a database and deletes the
 * database. This will effectively destroy the database if it is
 * not backed up by a file.
 *
 * @param provider provider
 * @param db_id id of the database to remove
 *
 * @return SDSKV_SUCCESS or error code defined in sdskv-common.h
 */
int sdskv_provider_remove_database(
        sdskv_provider_t provider,
        sdskv_database_id_t db_id);

/**
 * Removes all the databases associated with a provider.
 *
 * @param provider provider
 *
 * @return SDSKV_SUCCESS or error code defined in sdskv-common.h
 */
int sdskv_provider_remove_all_databases(
        sdskv_provider_t provider);

/**
 * Returns the number of databases that this provider manages.
 *
 * @param provider provider
 * @param num_db resulting number of databases
 *
 * @return SDSKV_SUCCESS or error code defined in sdskv-common.h
 */
int sdskv_provider_count_databases(
        sdskv_provider_t provider,
        uint64_t* num_db);

/**
 * List the database ids of the databases managed by this provider.
 * The databases array must be pre-allocated with at least enough
 * space to hold all the ids (use sdskv_provider_count_databases
 * to know how many databases are managed).
 *
 * @param[in] provider provider
 * @param[out] databases resulting database ids
 *
 * @return SDSKV_SUCCESS or error code defined in sdskv-common.h
 */
int sdskv_provider_list_databases(
        sdskv_provider_t provider,
        sdskv_database_id_t* databases);

/**
 * @brief Computes the database size.
 *
 * @param[in] provider provider.
 * @param[in] database_id Database id.
 * @param[out] size Resulting size.
 *
 * @return SDSKV_SUCCESS or error code defined in sdskv-common.h
 */
int sdskv_provider_compute_database_size(
        sdskv_provider_t provider,
        sdskv_database_id_t database_id,
        size_t* size);

/**
 * @brief Register custom migration callbacks to call before and
 * after a database is migrated to this provider.
 *
 * @param provider Provider in which to register the callbacks.
 * @param pre_cb Pre-migration callback.
 * @param post_cb Post-migration callback.
 * @param uargs User arguments.
 *
 * @return SDSKV_SUCCESS or error code defined in sdskv-common.h
 */
int sdskv_provider_set_migration_callbacks(
        sdskv_provider_t provider,
        sdskv_pre_migration_callback_fn pre_cb,
        sdskv_post_migration_callback_fn  post_cb,
        void* uargs);

/**
 * @brief Sets the ABT-IO instance to be used by REMI for migration IO.
 *
 * @param provider Provider.
 * @param abtio ABT-IO instance.
 *
 * @return SDSKV_SUCCESS or error code defined in sdskv-common.h
 */
int sdskv_provider_set_abtio_instance(
        sdskv_provider_t provider,
        abt_io_instance_id abtio);

#ifdef __cplusplus
}
#endif

#endif /* __BAKE_SERVER_H */
