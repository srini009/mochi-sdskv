#ifndef sds_keyval_client_h
#define sds_keyval_client_h

#include <margo.h>
#include <stdint.h>
#include <sdskv-common.h>

#if defined(__cplusplus)
extern "C" {
#endif

typedef struct sdskv_client* sdskv_client_t;
#define SDSKV_CLIENT_NULL ((sdskv_client_t)NULL)

typedef struct sdskv_provider_handle* sdskv_provider_handle_t;
#define SDSKV_PROVIDER_HANDLE_NULL ((sdskv_provider_handle_t)NULL)

/**
 * @brief Global variable recording the last error encountered by REMI.
 */
extern int32_t sdskv_remi_errno;

/**
 * @brief Creates a SDSKV client.
 *
 * @param[in] mid Margo instance
 * @param[out] client SDSKV client
 *
 * @return SDSKV_SUCCESS or error code defined in sdskv-common.h
 */
int sdskv_client_init(margo_instance_id mid, sdskv_client_t* client);

/**
 * @brief Finalizes a SDSKV client.
 *
 * @param[in] client SDSKV client to finalize
 *
 * @return SDSKV_SUCCESS or error code defined in sdskv-common.h
 */
int sdskv_client_finalize(sdskv_client_t client);

/**
 * @brief accessor for client margo identifider
 *
 * @return Margo ID
 */
margo_instance_id sdskv_client_get_mid(sdskv_client_t client);
/**
 * @brief Creates a SDSKV provider handle.
 *
 * @param[in] client SDSKV client responsible for the provider handle
 * @param[in] addr Mercury address of the provider
 * @param[in] provider_id id of the provider
 * @param[in] handle provider handle
 *
 * @return SDSKV_SUCCESS or error code defined in sdskv-common.h
 */
int sdskv_provider_handle_create(sdskv_client_t           client,
                                 hg_addr_t                addr,
                                 uint16_t                 provider_id,
                                 sdskv_provider_handle_t* handle);

/**
 * @brief Retrieves the information (string address and provider id)
 * from a provider handle. If any argument is NULL, the corresponding
 * field will be ignored.
 *
 * @param[in] ph Provider handle.
 * @param[inout] client Client.
 * @param[inout] addr Address.
 * @param[inout] provider_id
 *
 * @return SDSKV_SUCCESS or error code defined in sdskv-common.h
 */
int sdskv_provider_handle_get_info(sdskv_provider_handle_t ph,
                                   sdskv_client_t*         client,
                                   hg_addr_t*              addr,
                                   uint16_t*               provider_id);

/**
 * @brief Increments the reference counter of a provider handle.
 *
 * @param handle provider handle
 *
 * @return SDSKV_SUCCESS or error code defined in sdskv-common.h
 */
int sdskv_provider_handle_ref_incr(sdskv_provider_handle_t handle);

/**
 * @brief Releases the provider handle. This will decrement the
 * reference counter, and free the provider handle if the reference
 * counter reaches 0.
 *
 * @param[in] handle provider handle to release.
 *
 * @return SDSKV_SUCCESS or error code defined in sdskv-common.h
 */
int sdskv_provider_handle_release(sdskv_provider_handle_t handle);

/**
 * @brief Opens a database. This effectively contacts the provider
 * pointed to by the provider handle and request the database id
 * associated with the database name.
 *
 * @param[in] provider provider handle
 * @param[in] db_name name of the database to lookup
 * @param[out] db_id database id
 *
 * @return SDSKV_SUCCESS or error code defined in sdskv-common.h
 */
int sdskv_open(sdskv_provider_handle_t provider,
               const char*             db_name,
               sdskv_database_id_t*    db_id);

/**
 * @brief Gets the number of databases currently managed by a provider.
 *
 * @param[in] provider provider handle
 * @param[out] num number of databases
 *
 * @return SDSKV_SUCCESS or error code defined in sdskv-common.h
 */
int sdskv_count_databases(sdskv_provider_handle_t provider, size_t* num);

/**
 * @brief Lists the databases names and ids.
 * The caller is responsible for calling free() on each of the
 * entries in db_names.
 *
 * @param[in]    provider provider handle
 * @param[inout] count    max number of databases to query, actual number
 * returned
 * @param[out]   db_names database names
 * @param[out]   db_id    database ids
 *
 * @return SDSKV_SUCCESS or error code defined in sdskv-common.h
 */
int sdskv_list_databases(sdskv_provider_handle_t provider,
                         size_t*                 count,
                         char**                  db_name,
                         sdskv_database_id_t*    db_id);

/**
 * @brief Puts a key/value pair into the database.
 *
 * @param provider provider handle managing the database
 * @param db_id targeted database id
 * @param key key to store
 * @param ksize size (in bytes) of the key
 * @param value value to store
 * @param vsize size (in bytes) of the value
 *
 * @return SDSKV_SUCCESS or error code defined in sdskv-common.h
 */
int sdskv_put(sdskv_provider_handle_t provider,
              sdskv_database_id_t     db_id,
              const void*             key,
              hg_size_t               ksize,
              const void*             value,
              hg_size_t               vsize);

/**
 * @brief Puts multiple key/value pairs into the database.
 * This method will send all the key/value pairs in batch,
 * thus optimizing transfers by avoiding many RPC round trips.
 *
 * @param provider provider handle managing the database
 * @param db_id targeted database id
 * @param num number of key/value pairs to put
 * @param keys array of keys
 * @param ksizes array of key sizes
 * @param values array of values
 * @param vsizes array of value sizes
 *
 * @return SDSKV_SUCCESS or error code defined in sdskv-common.h
 */
int sdskv_put_multi(sdskv_provider_handle_t provider,
                    sdskv_database_id_t     db_id,
                    size_t                  num,
                    const void* const*      keys,
                    const hg_size_t*        ksizes,
                    const void* const*      values,
                    const hg_size_t*        vsizes);

/**
 * @brief Puts multiple key/value pairs into the database.
 * This method will send all the key/value pairs in batch,
 * thus optimizing transfers by avoiding many RPC round trips.
 * This version of put_multi assumes that the keys are packed
 * in back to back in a single buffer, and so are the values.
 *
 * @param provider provider handle managing the database
 * @param db_id targeted database id
 * @param num number of key/value pairs to put
 * @param packed_keys buffer containing the keys
 * @param ksizes array of key sizes
 * @param packed_values buffer containing the values
 * @param vsizes array of value sizes
 *
 * @return SDSKV_SUCCESS or error code defined in sdskv-common.h
 */
int sdskv_put_packed(sdskv_provider_handle_t provider,
                     sdskv_database_id_t     db_id,
                     size_t                  num,
                     const void*             packed_keys,
                     const hg_size_t*        ksizes,
                     const void*             packed_values,
                     const hg_size_t*        vsizes);

/**
 * @brief Puts multiple key/value pairs into the database.
 * This method will send all the key/value pairs in batch,
 * thus optimizing transfers by avoiding many RPC round trips.
 * This version of put_multi assumes that the keys are packed
 * in back to back in a single buffer, and so are the values.
 * This proxy version takes a bulk handle instead of the 4
 * buffers of sdskv_put_packed. The bulk handle should point
 * to memory with the following layout:
 * [num key sizes][ packed keys ][num val sizes][ packed values ]
 * Where the key sizes and value sizes are of type hg_size_t.
 *
 * If the origin_addr is NULL, the server will consider that the
 * bulk handle points to the sender's memory and will use the sender's
 * address.
 *
 * @param provider provider handle managing the database
 * @param db_id targeted database id
 * @param origin_addr Mercury address of the process owning the bulk handles
 * @param num number of key/value pairs to put
 * @param packed_data bulk handle to a buffer containing the key sizes, keys,
 * value sizes, and values
 *
 * @return SDSKV_SUCCESS or error code defined in sdskv-common.h
 */
int sdskv_proxy_put_packed(sdskv_provider_handle_t provider,
                           sdskv_database_id_t     db_id,
                           const char*             origin_addr,
                           size_t                  num,
                           hg_bulk_t               packed_data,
                           hg_size_t               packed_data_size);

/**
 * @brief Gets the value associated with a given key.
 * vsize needs to be set to the current size of the allocated
 * value buffer. After a succesful call to sdskv_get, vsize
 * will hold the actual size of the key. Note that the size
 * of a value can get obtained using sdskv_length.
 *
 * If value is NULL, the call will be equivalent
 * to sdskv_length and put the length of the value in *vsize.
 *
 * If the call returns SDSKV_ERR_SIZE, the value will be
 * unchanged but the vsize will be set to the required size.
 *
 * @param[in] provider provider handle
 * @param[in] db_id database id of the target database
 * @param[in] key key to lookup
 * @param[in] ksize size of the key
 * @param[out] value pointer to buffer to put the value
 * @param[inout] vsize size of the value
 *
 * @return SDSKV_SUCCESS or error code defined in sdskv-common.h
 */
int sdskv_get(sdskv_provider_handle_t provider,
              sdskv_database_id_t     db_id,
              const void*             key,
              hg_size_t               ksize,
              void*                   value,
              hg_size_t*              vsize);

/**
 * @brief Gets multiple values from the database. The transfers
 * will be performed in a single batch. The vsize array should
 * initially contain the size of the buffer allocated to receive
 * each value. After a successful call, this array will contain
 * the actual sizes of the values received. Contrary to sdskv_get,
 * this function will not produce an error if one of the keys
 * does not exist or if an allocated buffer is too small to hold
 * a given value: the corresponding value entry will simply not
 * be filled and its size will be set to 0 (so users must have
 * another way to distinguish a 0-sized value and a value for
 * which there was an error).
 *
 * If values is set to NULL, the call is equivalent to calling
 * sdskv_length_multi to get the size of the values.
 *
 * @param[in] provider provider handle
 * @param[in] db_id database id
 * @param[in] num number of keys to retrieve
 * @param[in] keys array of keys to retrieve
 * @param[in] ksizes size of the keys
 * @param[out] values array of allocated memory segments to receive the keys
 * @param[inout] vsizes sizes allocated (in) and actual sizes (out)
 *
 * @return SDSKV_SUCCESS or error code defined in sdskv-common.h
 */
int sdskv_get_multi(sdskv_provider_handle_t provider,
                    sdskv_database_id_t     db_id,
                    size_t                  num,
                    const void* const*      keys,
                    const hg_size_t*        ksizes,
                    void**                  values,
                    hg_size_t*              vsizes);

/**
 * @brief Get multiple values into a single packed buffer.
 *
 * @param[in] provider provider handle
 * @param[in] db_id database id
 * @param[inout] num number of values to retrieve, number of values actually
 * retrieved
 * @param[in] keys buffer of packed keys to retrieve
 * @param[in] ksizes size of the keys
 * @param[in] vbufsize size of the buffer allocated for the values
 * @param[out] values buffer allocated to receive packed values
 * @param[out] vsizes sizes of the values
 *
 * @return SDSKV_SUCCESS or error code defined in sdskv-common.h
 */
int sdskv_get_packed(sdskv_provider_handle_t provider,
                     sdskv_database_id_t     db_id,
                     size_t*                 num,
                     const void*             packed_keys,
                     const hg_size_t*        ksizes,
                     hg_size_t               vbufsize,
                     void*                   packed_values,
                     hg_size_t*              vsizes);

/**
 * @brief Gets the length of a value associated with a given key.
 *
 * @param[in] handle provider handle
 * @param[in] db_id database id
 * @param[in] key key to lookup
 * @param[in] ksize size of the key
 * @param[out] vsize size of the associated value
 *
 * @return SDSKV_SUCCESS or error code defined in sdskv-common.h
 */
int sdskv_length(sdskv_provider_handle_t handle,
                 sdskv_database_id_t     db_id,
                 const void*             key,
                 hg_size_t               ksize,
                 hg_size_t*              vsize);

/**
 * @brief Gets the length of values associated with multiple keys.
 * If a particular key does not exists, this function will set the length
 * of its value to 0 (so the user needs another way to differenciate
 * between a key that does not exists and a 0-sized value).
 *
 * @param[in] handle provider handle
 * @param[in] db_id database id
 * @param[in] num number of keys
 * @param[in] keys array of keys
 * @param[in] ksizes array of key sizes
 * @param[out] vsizes array where to put value sizes
 *
 * @return SDSKV_SUCCESS or error code defined in sdskv-common.h
 */
int sdskv_length_multi(sdskv_provider_handle_t handle,
                       sdskv_database_id_t     db_id,
                       size_t                  num,
                       const void* const*      keys,
                       const hg_size_t*        ksizes,
                       hg_size_t*              vsizes);

/**
 * @brief Gets the length of values associated with multiple keys.
 * If a particular key does not exists, this function will set the length
 * of its value to 0 (so the user needs another way to differenciate
 * between a key that does not exists and a 0-sized value).
 *
 * @param[in] handle provider handle
 * @param[in] db_id database id
 * @param[in] num number of keys
 * @param[in] keys buffer of packed keys
 * @param[in] ksizes array of key sizes
 * @param[out] vsizes array where to put value sizes
 *
 * @return SDSKV_SUCCESS or error code defined in sdskv-common.h
 */
int sdskv_length_packed(sdskv_provider_handle_t handle,
                        sdskv_database_id_t     db_id,
                        size_t                  num,
                        const void*             packed_keys,
                        const hg_size_t*        ksizes,
                        hg_size_t*              vsizes);

/**
 * @brief Checks if the given key exists in the database.
 *
 * @param[in] handle provider handle
 * @param[in] db_id database id
 * @param[in] key key to lookup
 * @param[in] ksize size of the key
 * @param[out] flag 1 if the key exists, 0 otherwise
 *
 * @return SDSKV_SUCCESS or error code defined in sdskv-common.h
 */
int sdskv_exists(sdskv_provider_handle_t handle,
                 sdskv_database_id_t     db_id,
                 const void*             key,
                 hg_size_t               ksize,
                 int*                    flag);

/**
 * @brief Checks if the given key exists in the database.
 *
 * @param[in] handle provider handle
 * @param[in] db_id database id
 * @param[in] count number of keys
 * @param[in] keys keys to lookup
 * @param[in] ksizes size of the keys
 * @param[out] array of count flags containing 1 if the key exists, 0 otherwise
 *
 * @return SDSKV_SUCCESS or error code defined in sdskv-common.h
 */
int sdskv_exists_multi(sdskv_provider_handle_t handle,
                       sdskv_database_id_t     db_id,
                       size_t                  count,
                       const void* const*      keys,
                       const hg_size_t*        ksizes,
                       int*                    flags);

/**
 * @brief Erases the key/value pair pointed by the given key.
 *
 * @param handle provider handle
 * @param db_id database id
 * @param key key to erase
 * @param ksize size of the key
 *
 * @return SDSKV_SUCCESS or error code defined in sdskv-common.h
 */
int sdskv_erase(sdskv_provider_handle_t handle,
                sdskv_database_id_t     db_id,
                const void*             key,
                hg_size_t               ksize);

/**
 * @brief Erases all the key/value pairs pointed to by the given keys.
 *
 * @param handle provider handle
 * @param db_id database id
 * @param num_keys number of keys
 * @param keys array of keys
 * @param ksizes array of key sizes
 *
 * @return SDSKV_SUCCESS or error code defined in sdskv-common.h
 */
int sdskv_erase_multi(sdskv_provider_handle_t handle,
                      sdskv_database_id_t     db_id,
                      size_t                  num_keys,
                      const void* const*      keys,
                      const hg_size_t*        ksizes);

/**
 * Lists at most max_keys keys starting strictly after start_key,
 * whether start_key is effectively in the database or not. "strictly after"
 * must be understood in the sens of the custom comparison function set
 * for the target database, if such comparison function has been set.
 * Passing NULL as start_key allows one to start listing from the beginning
 * of the database.
 * keys must be an array of max_keys void* elements, each element keys[i]
 * being a preallocated buffer of size ksizes[i]. ksizes must be an array
 * of sizes of the preallocated buffers. After a successful call, max_keys
 * is set to the actual number of keys retrieved, ksizes[i] is set to the
 * actual size of the i-th key, and keys[i] contains the i-th key.
 *
 * If at least one of the keys cannot fit in its buffer, the call will fail
 * and return SDSKV_ERR_SIZE, but the ksizes array will be updated to the
 * sizes required to fit each key, and max_keys will be updated to the number
 * of keys that can be listed. keys will be left untouched.
 *
 * @param[in] provider provider handle
 * @param[in] db_id database id
 * @param[in] start_key starting key
 * @param[in] start_ksize size of the starting key
 * @param[out] keys array of buffers to hold returned keys
 * @param[inout] ksizes array of key sizes
 * @param[inout] max_keys max keys requested
 *
 * @return SDSKV_SUCCESS or error code defined in sdskv-common.h
 */
int sdskv_list_keys(sdskv_provider_handle_t provider,
                    sdskv_database_id_t     db_id,
                    const void*             start_key,
                    hg_size_t               start_ksize,
                    void**                  keys,
                    hg_size_t*              ksizes,
                    hg_size_t*              max_keys);

/**
 * @brief Same as sdskv_list_keys, but provides a prefix
 * that returned keys must start with. Note that "prefix" is understood
 * in the sens of the bytes making up the key, not in the sens of any
 * comparison function set by the user on the provider side.
 *
 * @param[in] provider provider handle
 * @param[in] db_id database id
 * @param[in] start_key starting key
 * @param[in] start_ksize size of the starting key
 * @param[in] prefix prefix that returned keys must match
 * @param[in] prefix_size size of the prefix
 * @param[out] keys array of buffers to hold returned keys
 * @param[inout] ksizes array of key sizes
 * @param[inout] max_keys max keys requested
 *
 * @return SDSKV_SUCCESS or error code defined in sdskv-common.h
 */
int sdskv_list_keys_with_prefix(sdskv_provider_handle_t provider,
                                sdskv_database_id_t     db_id,
                                const void*             start_key,
                                hg_size_t               start_ksize,
                                const void*             prefix,
                                hg_size_t               prefix_size,
                                void**                  keys,
                                hg_size_t*              ksizes,
                                hg_size_t*              max_keys);

/**
 * @brief Same as sdskv_list_keys but returns also the values.
 * The values array must be an array of max_keys sizes, with
 * each element values[i] being a buffer of size vsizes[i].
 * After a successful call, values[i] will hold the i-th value
 * and vsizes[i] will be set to the actual size of the i-th value.
 *
 * If a buffer for a key or a value is too small, the call will fail
 * but ksizes and vsizes will be updated to the correct sizes needed,
 * and max_items will be updated to the number of entries that can be
 * listed.
 *
 * @param[in] provider provider handle
 * @param[in] db_id database id
 * @param[in] start_key starting key
 * @param[in] start_ksize size of the starting key
 * @param[out] keys array of buffers to hold returned keys
 * @param[inout] ksizes array of key sizes
 * @param[inout] values array of buffers to hold returned values
 * @param[inout] vsizes array of value sizes
 * @param[inout] max_keys max keys requested
 *
 * @return SDSKV_SUCCESS or error code defined in sdskv-common.h
 */
int sdskv_list_keyvals(sdskv_provider_handle_t provider,
                       sdskv_database_id_t     db_id,
                       const void*             start_key,
                       hg_size_t               start_ksize,
                       void**                  keys,
                       hg_size_t*              ksizes,
                       void**                  values,
                       hg_size_t*              vsizes,
                       hg_size_t*              max_items);

/**
 * @brief Same as sdskv_list_keyvals but lets the user provide
 * a prefix that returned keys must satisfy.
 *
 * @param[in] provider provider handle
 * @param[in] db_id database id
 * @param[in] start_key starting key
 * @param[in] start_ksize size of the starting key
 * @param[in] prefix prefix that returned keys must match
 * @param[in] prefix_size size of the prefix
 * @param[out] keys array of buffers to hold returned keys
 * @param[inout] ksizes array of key sizes
 * @param[inout] values array of buffers to hold returned values
 * @param[inout] vsizes array of value sizes
 * @param[inout] max_keys max keys requested
 *
 * @return SDSKV_SUCCESS or error code defined in sdskv-common.h
 */
int sdskv_list_keyvals_with_prefix(sdskv_provider_handle_t provider,
                                   sdskv_database_id_t     db_id,
                                   const void*             start_key,
                                   hg_size_t               start_ksize,
                                   const void*             prefix,
                                   hg_size_t               prefix_size,
                                   void**                  keys,
                                   hg_size_t*              ksizes,
                                   void**                  values,
                                   hg_size_t*              vsizes,
                                   hg_size_t*              max_items);

/**
 * @brief Migrates a set of keys/values from a source provider/database
 * to a target provider/database.
 *
 * @param source_provider source provider
 * @param source_db_id source database id
 * @param target_addr target address
 * @param target_provider_id target provider id
 * @param target_db_id target database id
 * @param num_keys number of keys
 * @param keys array of keys
 * @param key_sizes array of key sizes
 * @param flag SDSKV_KEEP_ORIGINAL, or SDSKV_REMOVE_ORIGINAL
 *
 * @return SDSKV_SUCCESS or error code defined in sdskv-common.h
 */
int sdskv_migrate_keys(sdskv_provider_handle_t source_provider,
                       sdskv_database_id_t     source_db_id,
                       const char*             target_addr,
                       uint16_t                target_provider_id,
                       sdskv_database_id_t     target_db_id,
                       hg_size_t               num_keys,
                       const void* const*      keys,
                       const hg_size_t*        key_sizes,
                       int                     flag);

/**
 * @brief Migrates a single key/value from a source provider/database
 * to a target provider/database.
 *
 * @param source_provider source provider
 * @param source_db_id source database id
 * @param target_addr target address
 * @param target_provider_id target provider id
 * @param target_db_id target database id
 * @param key key to migrate
 * @param key_size size of the key
 * @param flag SDSKV_KEEP_ORIGINAL, or SDSKV_REMOVE_ORIGINAL
 *
 * @return SDSKV_SUCCESS or error code defined in sdskv-common.h
 */
static inline int sdskv_migrate_key(sdskv_provider_handle_t source_provider,
                                    sdskv_database_id_t     source_db_id,
                                    const char*             target_addr,
                                    uint16_t                target_provider_id,
                                    sdskv_database_id_t     target_db_id,
                                    const void*             key,
                                    hg_size_t               key_size,
                                    int                     flag)
{
    return sdskv_migrate_keys(source_provider, source_db_id, target_addr,
                              target_provider_id, target_db_id, 1, &key,
                              &key_size, flag);
}

/**
 * @brief Migrates a set of keys/values from a source provider/database
 * to a target provider/database based on a range. The range is
 * expressed by the array key_range, which contains two elements.
 * key_range[0] must be a lower bound lb.
 * key_range[1] must be an upper bound ub.
 * The set of keys migrated are within the range ]lb, ub[ (i.e. lb
 * and ub not included).
 *
 * @param source_provider source provider
 * @param source_db_id source database id
 * @param target_addr target address
 * @param target_provider_id target provider id
 * @param target_db_id target database id
 * @param key_range range of keys to migrate
 * @param key_range_sizes size of the keys provided for the range
 * @param flag SDSKV_KEEP_ORIGINAL, or SDSKV_REMOVE_ORIGINAL
 *
 * @return SDSKV_SUCCESS or error code defined in sdskv-common.h
 */
int sdskv_migrate_key_range(sdskv_provider_handle_t source_provider,
                            sdskv_database_id_t     source_db_id,
                            const char*             target_addr,
                            uint16_t                target_provider_id,
                            sdskv_database_id_t     target_db_id,
                            const void*             key_range[],
                            const hg_size_t         key_range_sizes[],
                            int                     flag);

/**
 * @brief Migrates a set of keys/values from a source provider/database
 * to a target provider/database based on a prefix.
 * All key matching the provided prefix will be migrated.
 *
 * @param source_provider source provider
 * @param source_db_id source database id
 * @param target_addr target address
 * @param target_provider_id target provider id
 * @param target_db_id target database id
 * @param key_prefix prefix of keys to migrate
 * @param key_prefix_size size of the prefix provided
 * @param flag SDSKV_KEEP_ORIGINAL, or SDSKV_REMOVE_ORIGINAL
 *
 * @return SDSKV_SUCCESS or error code defined in sdskv-common.h
 */
int sdskv_migrate_keys_prefixed(sdskv_provider_handle_t source_provider,
                                sdskv_database_id_t     source_db_id,
                                const char*             target_addr,
                                uint16_t                target_provider_id,
                                sdskv_database_id_t     target_db_id,
                                const void*             key_prefix,
                                hg_size_t               key_prefix_size,
                                int                     flag);

/**
 * @brief Migrates all the keys/values from a source provider/database
 * to a target provider/database based on a prefix.
 *
 * @param source_provider source provider
 * @param source_db_id source database id
 * @param target_addr target address
 * @param target_provider_id target provider id
 * @param target_db_id target database id
 * @param flag SDSKV_KEEP_ORIGINAL, or SDSKV_REMOVE_ORIGINAL
 *
 * @return SDSKV_SUCCESS or error code defined in sdskv-common.h
 */
int sdskv_migrate_all_keys(sdskv_provider_handle_t source_provider,
                           sdskv_database_id_t     source_db_id,
                           const char*             target_addr,
                           uint16_t                target_provider_id,
                           sdskv_database_id_t     target_db_id,
                           int                     flag);

/**
 * @brief Migrate an entire database to a target destination provider.
 * Note that the database will not have the same id at the destination
 * so the user should call sdskv_open to re-open the database at its
 * destination.
 *
 * @param[in] source Source provider.
 * @param[in] source_db_id Source provider id.
 * @param[in] dest_addr Address of the destination provider.
 * @param[in] dest_provider_id Provider id of the destination provider.
 * @param[in] dest_root Root path at the destination.
 * @param[in] flag SDSKV_KEEP_ORIGINAL, or SDSKV_REMOVE_ORIGINAL
 *
 * @return SDSKV_SUCCESS or error code defined in sdskv-common.h
 */
int sdskv_migrate_database(sdskv_provider_handle_t source,
                           sdskv_database_id_t     source_db_id,
                           const char*             dest_addr,
                           uint16_t                dest_provider_id,
                           const char*             dest_root,
                           int                     flag);

/**
 * Shuts down a remote SDSKV service (given an address).
 * This will shutdown all the providers on the target address.
 *
 * @param [in] client SDSKV client
 * @param [in] addr address of the server
 *
 * @return SDSKV_SUCCESS or error code defined in sdskv-common.h
 */
int sdskv_shutdown_service(sdskv_client_t client, hg_addr_t addr);

#if defined(__cplusplus)
}
#endif

#endif
