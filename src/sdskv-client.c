#include "sdskv-client.h"
#include "sdskv-rpc-types.h"

#define MAX_RPC_MESSAGE_SIZE 4000 // in bytes

int32_t sdskv_remi_errno;

struct sdskv_client {
    margo_instance_id mid;
    /* opening and querying databases */
    hg_id_t sdskv_open_id;
    hg_id_t sdskv_count_databases_id;
    hg_id_t sdskv_list_databases_id;
    /* accessing database */
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

    uint64_t num_provider_handles;
};

struct sdskv_provider_handle {
    sdskv_client_t client;
    hg_addr_t      addr;
    uint16_t       provider_id;
    uint64_t       refcount;
};

static int sdskv_client_register(sdskv_client_t client, margo_instance_id mid)
{
    client->mid      = mid;
    sdskv_remi_errno = 0;

    /* check if RPCs have already been registered */
    hg_bool_t flag;
    hg_id_t   id;
    margo_registered_name(mid, "sdskv_put_rpc", &id, &flag);

    if (flag == HG_TRUE) { /* RPCs already registered */

        margo_registered_name(mid, "sdskv_open_rpc", &client->sdskv_open_id,
                              &flag);
        margo_registered_name(mid, "sdskv_count_databases_rpc",
                              &client->sdskv_count_databases_id, &flag);
        margo_registered_name(mid, "sdskv_list_databases_rpc",
                              &client->sdskv_list_databases_id, &flag);
        margo_registered_name(mid, "sdskv_put_rpc", &client->sdskv_put_id,
                              &flag);
        margo_registered_name(mid, "sdskv_put_multi_rpc",
                              &client->sdskv_put_multi_id, &flag);
        margo_registered_name(mid, "sdskv_put_packed_rpc",
                              &client->sdskv_put_packed_id, &flag);
        margo_registered_name(mid, "sdskv_bulk_put_rpc",
                              &client->sdskv_bulk_put_id, &flag);
        margo_registered_name(mid, "sdskv_get_rpc", &client->sdskv_get_id,
                              &flag);
        margo_registered_name(mid, "sdskv_get_multi_rpc",
                              &client->sdskv_get_multi_id, &flag);
        margo_registered_name(mid, "sdskv_get_packed_rpc",
                              &client->sdskv_get_packed_id, &flag);
        margo_registered_name(mid, "sdskv_erase_rpc", &client->sdskv_erase_id,
                              &flag);
        margo_registered_name(mid, "sdskv_erase_multi_rpc",
                              &client->sdskv_erase_multi_id, &flag);
        margo_registered_name(mid, "sdskv_exists_rpc", &client->sdskv_exists_id,
                              &flag);
        margo_registered_name(mid, "sdskv_exists_multi_rpc",
                              &client->sdskv_exists_multi_id, &flag);
        margo_registered_name(mid, "sdskv_length_rpc", &client->sdskv_length_id,
                              &flag);
        margo_registered_name(mid, "sdskv_length_multi_rpc",
                              &client->sdskv_length_multi_id, &flag);
        margo_registered_name(mid, "sdskv_length_packed_rpc",
                              &client->sdskv_length_packed_id, &flag);
        margo_registered_name(mid, "sdskv_bulk_get_rpc",
                              &client->sdskv_bulk_get_id, &flag);
        margo_registered_name(mid, "sdskv_list_keys_rpc",
                              &client->sdskv_list_keys_id, &flag);
        margo_registered_name(mid, "sdskv_list_keyvals_rpc",
                              &client->sdskv_list_keyvals_id, &flag);
        margo_registered_name(mid, "sdskv_migrate_keys_rpc",
                              &client->sdskv_migrate_keys_id, &flag);
        margo_registered_name(mid, "sdskv_migrate_key_range_rpc",
                              &client->sdskv_migrate_key_range_id, &flag);
        margo_registered_name(mid, "sdskv_migrate_keys_prefixed_rpc",
                              &client->sdskv_migrate_keys_prefixed_id, &flag);
        margo_registered_name(mid, "sdskv_migrate_all_keys_rpc",
                              &client->sdskv_migrate_all_keys_id, &flag);
        margo_registered_name(mid, "sdskv_migrate_database_rpc",
                              &client->sdskv_migrate_database_id, &flag);

    } else {

        client->sdskv_open_id = MARGO_REGISTER(mid, "sdskv_open_rpc", open_in_t,
                                               open_out_t, NULL);
        client->sdskv_count_databases_id = MARGO_REGISTER(
            mid, "sdskv_count_databases_rpc", void, count_db_out_t, NULL);
        client->sdskv_list_databases_id = MARGO_REGISTER(
            mid, "sdskv_list_databases_rpc", list_db_in_t, list_db_out_t, NULL);
        client->sdskv_put_id
            = MARGO_REGISTER(mid, "sdskv_put_rpc", put_in_t, put_out_t, NULL);
        client->sdskv_put_multi_id = MARGO_REGISTER(
            mid, "sdskv_put_multi_rpc", put_multi_in_t, put_multi_out_t, NULL);
        client->sdskv_put_packed_id
            = MARGO_REGISTER(mid, "sdskv_put_packed_rpc", put_packed_in_t,
                             put_packed_out_t, NULL);
        client->sdskv_bulk_put_id = MARGO_REGISTER(
            mid, "sdskv_bulk_put_rpc", bulk_put_in_t, bulk_put_out_t, NULL);
        client->sdskv_get_id
            = MARGO_REGISTER(mid, "sdskv_get_rpc", get_in_t, get_out_t, NULL);
        client->sdskv_get_multi_id = MARGO_REGISTER(
            mid, "sdskv_get_multi_rpc", get_multi_in_t, get_multi_out_t, NULL);
        client->sdskv_get_packed_id
            = MARGO_REGISTER(mid, "sdskv_get_packed_rpc", get_packed_in_t,
                             get_packed_out_t, NULL);
        client->sdskv_erase_id = MARGO_REGISTER(mid, "sdskv_erase_rpc",
                                                erase_in_t, erase_out_t, NULL);
        client->sdskv_erase_multi_id
            = MARGO_REGISTER(mid, "sdskv_erase_multi_rpc", erase_multi_in_t,
                             erase_multi_out_t, NULL);
        client->sdskv_exists_id = MARGO_REGISTER(
            mid, "sdskv_exists_rpc", exists_in_t, exists_out_t, NULL);
        client->sdskv_exists_multi_id
            = MARGO_REGISTER(mid, "sdskv_exists_multi_rpc", exists_multi_in_t,
                             exists_multi_out_t, NULL);
        client->sdskv_length_id = MARGO_REGISTER(
            mid, "sdskv_length_rpc", length_in_t, length_out_t, NULL);
        client->sdskv_length_multi_id
            = MARGO_REGISTER(mid, "sdskv_length_multi_rpc", length_multi_in_t,
                             length_multi_out_t, NULL);
        client->sdskv_length_packed_id
            = MARGO_REGISTER(mid, "sdskv_length_packed_rpc", length_packed_in_t,
                             length_packed_out_t, NULL);
        client->sdskv_bulk_get_id = MARGO_REGISTER(
            mid, "sdskv_bulk_get_rpc", bulk_get_in_t, bulk_get_out_t, NULL);
        client->sdskv_list_keys_id = MARGO_REGISTER(
            mid, "sdskv_list_keys_rpc", list_keys_in_t, list_keys_out_t, NULL);
        client->sdskv_list_keyvals_id
            = MARGO_REGISTER(mid, "sdskv_list_keyvals_rpc", list_keyvals_in_t,
                             list_keyvals_out_t, NULL);
        client->sdskv_migrate_keys_id
            = MARGO_REGISTER(mid, "sdskv_migrate_keys_rpc", migrate_keys_in_t,
                             migrate_keys_out_t, NULL);
        client->sdskv_migrate_key_range_id
            = MARGO_REGISTER(mid, "sdskv_migrate_key_range_rpc",
                             migrate_key_range_in_t, migrate_keys_out_t, NULL);
        client->sdskv_migrate_keys_prefixed_id = MARGO_REGISTER(
            mid, "sdskv_migrate_keys_prefixed_rpc", migrate_keys_prefixed_in_t,
            migrate_keys_out_t, NULL);
        client->sdskv_migrate_all_keys_id
            = MARGO_REGISTER(mid, "sdskv_migrate_all_keys_rpc",
                             migrate_all_keys_in_t, migrate_keys_out_t, NULL);
        client->sdskv_migrate_database_id = MARGO_REGISTER(
            mid, "sdskv_migrate_database_rpc", migrate_database_in_t,
            migrate_database_out_t, NULL);
    }

    return SDSKV_SUCCESS;
}

int sdskv_client_init(margo_instance_id mid, sdskv_client_t* client)
{
    sdskv_client_t c = (sdskv_client_t)calloc(1, sizeof(*c));
    if (!c) return SDSKV_ERR_ALLOCATION;

    c->num_provider_handles = 0;

    int ret = sdskv_client_register(c, mid);
    if (ret != 0) return ret;

    *client = c;
    return SDSKV_SUCCESS;
}

int sdskv_client_finalize(sdskv_client_t client)
{
    if (client->num_provider_handles != 0) {
        fprintf(stderr,
                "[SDSKV] Warning: %lu provider handles not released before "
                "sdskv_client_finalize was called\n",
                client->num_provider_handles);
    }
    free(client);
    return SDSKV_SUCCESS;
}

margo_instance_id sdskv_client_get_mid(sdskv_client_t client)
{
    return client->mid;
}

int sdskv_provider_handle_create(sdskv_client_t           client,
                                 hg_addr_t                addr,
                                 uint16_t                 provider_id,
                                 sdskv_provider_handle_t* handle)
{
    if (client == SDSKV_CLIENT_NULL) return SDSKV_ERR_INVALID_ARG;

    sdskv_provider_handle_t provider
        = (sdskv_provider_handle_t)calloc(1, sizeof(*provider));

    if (!provider) return SDSKV_ERR_ALLOCATION;

    hg_return_t hret = margo_addr_dup(client->mid, addr, &(provider->addr));
    if (hret != HG_SUCCESS) {
        free(provider);
        return SDSKV_MAKE_HG_ERROR(hret);
    }

    provider->client      = client;
    provider->provider_id = provider_id;
    provider->refcount    = 1;

    client->num_provider_handles += 1;

    *handle = provider;
    return SDSKV_SUCCESS;
}

int sdskv_provider_handle_get_info(sdskv_provider_handle_t ph,
                                   sdskv_client_t*         client,
                                   hg_addr_t*              addr,
                                   uint16_t*               provider_id)
{
    if (ph) {
        if (client) *client = ph->client;
        if (addr) *addr = ph->addr;
        if (provider_id) *provider_id = ph->provider_id;
    } else {
        if (client) *client = SDSKV_CLIENT_NULL;
        if (addr) *addr = HG_ADDR_NULL;
        if (provider_id) *provider_id = 0;
    }
    return SDSKV_SUCCESS;
}

int sdskv_provider_handle_ref_incr(sdskv_provider_handle_t handle)
{
    if (handle == SDSKV_PROVIDER_HANDLE_NULL) return SDSKV_ERR_INVALID_ARG;
    handle->refcount += 1;
    return SDSKV_SUCCESS;
}

int sdskv_provider_handle_release(sdskv_provider_handle_t handle)
{
    if (handle == SDSKV_PROVIDER_HANDLE_NULL) return -1;
    handle->refcount -= 1;
    if (handle->refcount == 0) {
        margo_addr_free(handle->client->mid, handle->addr);
        handle->client->num_provider_handles -= 1;
        free(handle);
    }
    return SDSKV_SUCCESS;
}

int sdskv_open(sdskv_provider_handle_t provider,
               const char*             db_name,
               sdskv_database_id_t*    db_id)
{
    hg_return_t hret;
    int         ret;
    open_in_t   in;
    open_out_t  out;
    hg_handle_t handle;

    /* create handle */
    hret = margo_create(provider->client->mid, provider->addr,
                        provider->client->sdskv_open_id, &handle);
    if (hret != HG_SUCCESS) return SDSKV_MAKE_HG_ERROR(hret);

    in.name = (char*)db_name;

    hret = margo_provider_forward(provider->provider_id, handle, &in);
    if (hret != HG_SUCCESS) {
        margo_destroy(handle);
        return SDSKV_MAKE_HG_ERROR(hret);
    }

    hret = margo_get_output(handle, &out);
    if (hret != HG_SUCCESS) {
        margo_destroy(handle);
        return SDSKV_MAKE_HG_ERROR(hret);
    }

    ret    = out.ret;
    *db_id = out.db_id;

    margo_free_output(handle, &out);
    margo_destroy(handle);

    return ret;
}

int sdskv_count_databases(sdskv_provider_handle_t provider, size_t* num)
{
    hg_return_t    hret;
    int            ret;
    count_db_out_t out;
    hg_handle_t    handle;

    /* create handle */
    hret = margo_create(provider->client->mid, provider->addr,
                        provider->client->sdskv_count_databases_id, &handle);
    if (hret != HG_SUCCESS) return SDSKV_MAKE_HG_ERROR(hret);

    hret = margo_provider_forward(provider->provider_id, handle, NULL);
    if (hret != HG_SUCCESS) {
        margo_destroy(handle);
        return SDSKV_MAKE_HG_ERROR(hret);
    }

    hret = margo_get_output(handle, &out);
    if (hret != HG_SUCCESS) {
        margo_destroy(handle);
        return SDSKV_MAKE_HG_ERROR(hret);
    }

    ret  = out.ret;
    *num = out.count;

    margo_free_output(handle, &out);
    margo_destroy(handle);

    return ret;
}

int sdskv_list_databases(sdskv_provider_handle_t provider,
                         size_t*                 count,
                         char**                  db_names,
                         sdskv_database_id_t*    db_ids)
{
    hg_return_t  hret;
    int          ret;
    list_db_in_t in;
    in.count = *count;
    list_db_out_t out;
    hg_handle_t   handle;

    /* create handle */
    hret = margo_create(provider->client->mid, provider->addr,
                        provider->client->sdskv_list_databases_id, &handle);
    if (hret != HG_SUCCESS) return SDSKV_MAKE_HG_ERROR(hret);

    hret = margo_provider_forward(provider->provider_id, handle, &in);
    if (hret != HG_SUCCESS) {
        margo_destroy(handle);
        return SDSKV_MAKE_HG_ERROR(hret);
    }

    hret = margo_get_output(handle, &out);
    if (hret != HG_SUCCESS) {
        margo_destroy(handle);
        return SDSKV_MAKE_HG_ERROR(hret);
    }

    ret = out.ret;
    if (ret == SDSKV_SUCCESS) {
        *count = out.count;
        unsigned i;
        for (i = 0; i < out.count; i++) {
            if (db_names != NULL) db_names[i] = strdup(out.db_names[i]);
            if (db_ids != NULL) db_ids[i] = out.db_ids[i];
        }
    } else {
        *count = 0;
    }

    margo_free_output(handle, &out);
    margo_destroy(handle);

    return ret;
}

int sdskv_put(sdskv_provider_handle_t provider,
              sdskv_database_id_t     db_id,
              const void*             key,
              hg_size_t               ksize,
              const void*             value,
              hg_size_t               vsize)
{
    hg_return_t hret;
    int         ret = SDSKV_SUCCESS;
    hg_handle_t handle;

    hg_size_t msize = ksize + vsize + 2 * sizeof(hg_size_t);

    if (msize <= MAX_RPC_MESSAGE_SIZE) {

        put_in_t  in;
        put_out_t out;

        in.db_id      = db_id;
        in.key.data   = (kv_ptr_t)key;
        in.key.size   = ksize;
        in.value.data = (kv_ptr_t)value;
        in.value.size = vsize;

        /* create handle */
        hret = margo_create(provider->client->mid, provider->addr,
                            provider->client->sdskv_put_id, &handle);
        if (hret != HG_SUCCESS) {
            fprintf(stderr, "[SDSKV] margo_create() failed in sdskv_put()\n");
            return SDSKV_MAKE_HG_ERROR(hret);
        }

        hret = margo_provider_forward(provider->provider_id, handle, &in);
        if (hret != HG_SUCCESS) {
            fprintf(stderr, "[SDSKV] margo_forward() failed in sdskv_put()\n");
            margo_destroy(handle);
            return SDSKV_MAKE_HG_ERROR(hret);
        }

        hret = margo_get_output(handle, &out);
        if (hret != HG_SUCCESS) {
            fprintf(stderr,
                    "[SDSKV] margo_get_output() failed in sdskv_put()\n");
            margo_destroy(handle);
            return SDSKV_MAKE_HG_ERROR(hret);
        }

        ret = out.ret;

        margo_free_output(handle, &out);

    } else {

        bulk_put_in_t  in;
        bulk_put_out_t out;

        in.db_id    = db_id;
        in.key.data = (kv_ptr_t)key;
        in.key.size = ksize;
        in.vsize    = vsize;

        hret = margo_bulk_create(provider->client->mid, 1, (void**)(&value),
                                 &in.vsize, HG_BULK_READ_ONLY, &in.handle);
        if (hret != HG_SUCCESS) {
            fprintf(stderr,
                    "[SDSKV] margo_bulk_create() failed in sdskv_put()\n");
            return SDSKV_MAKE_HG_ERROR(hret);
        }

        /* create handle */
        hret = margo_create(provider->client->mid, provider->addr,
                            provider->client->sdskv_bulk_put_id, &handle);
        if (hret != HG_SUCCESS) {
            fprintf(stderr, "[SDSKV] margo_create() failed in sdskv_put()\n");
            margo_bulk_free(in.handle);
            return SDSKV_MAKE_HG_ERROR(hret);
        }

        hret = margo_provider_forward(provider->provider_id, handle, &in);
        if (hret != HG_SUCCESS) {
            fprintf(stderr, "[SDSKV] margo_forward() failed in sdskv_put()\n");
            margo_bulk_free(in.handle);
            margo_destroy(handle);
            return SDSKV_MAKE_HG_ERROR(hret);
        }

        hret = margo_get_output(handle, &out);
        if (hret != HG_SUCCESS) {
            fprintf(stderr,
                    "[SDSKV] margo_get_output() failed in sdskv_put()\n");
            margo_bulk_free(in.handle);
            margo_destroy(handle);
            return SDSKV_MAKE_HG_ERROR(hret);
        }

        ret = out.ret;
        margo_free_output(handle, &out);
        margo_bulk_free(in.handle);
    }

    margo_destroy(handle);
    return ret;
}

int sdskv_put_multi(sdskv_provider_handle_t provider,
                    sdskv_database_id_t     db_id,
                    size_t                  num,
                    const void* const*      keys,
                    const hg_size_t*        ksizes,
                    const void* const*      values,
                    const hg_size_t*        vsizes)
{
    hg_return_t     hret;
    int             ret;
    hg_handle_t     handle = HG_HANDLE_NULL;
    put_multi_in_t  in;
    put_multi_out_t out;
    void**          key_seg_ptrs  = NULL;
    hg_size_t*      key_seg_sizes = NULL;
    void**          val_seg_ptrs  = NULL;
    hg_size_t*      val_seg_sizes = NULL;

    in.db_id            = db_id;
    in.num_keys         = num;
    in.keys_bulk_handle = HG_BULK_NULL;
    in.keys_bulk_size   = 0;
    in.vals_bulk_handle = HG_BULK_NULL;
    in.vals_bulk_size   = 0;

    /* check that none of the keys have a size of 0 */
    int i;
    for (i = 0; i < num; i++) {
        if (ksizes[i] == 0) return SDSKV_ERR_INVALID_ARG;
    }

    int non_empty_values = 0;
    /* check if we are trying to write some empty values */
    /* XXX normally we shouldn't have to do that but Mercury
       bugs if we try to expose empty segments for RDMA. */
    for (i = 0; i < num; i++) {
        if (vsizes[i] != 0) non_empty_values += 1;
    }

    /* create an array of key sizes and key pointers */
    key_seg_sizes    = malloc(sizeof(hg_size_t) * (num + 1));
    key_seg_sizes[0] = num * sizeof(hg_size_t);
    memcpy(key_seg_sizes + 1, ksizes, num * sizeof(hg_size_t));
    key_seg_ptrs    = malloc(sizeof(void*) * (num + 1));
    key_seg_ptrs[0] = (void*)ksizes;
    memcpy(key_seg_ptrs + 1, keys, num * sizeof(void*));
    for (i = 0; i < num + 1; i++) { in.keys_bulk_size += key_seg_sizes[i]; }
    val_seg_sizes    = malloc(sizeof(hg_size_t) * (non_empty_values + 1));
    val_seg_sizes[0] = num * sizeof(hg_size_t);
    int j            = 1;
    for (i = 0; i < num; i++) {
        if (vsizes[i] != 0) {
            val_seg_sizes[j] = vsizes[i];
            j++;
        }
    }
    val_seg_ptrs    = malloc(sizeof(void*) * (non_empty_values + 1));
    val_seg_ptrs[0] = (void*)vsizes;
    j               = 1;
    for (i = 0; i < num; i++) {
        if (vsizes[i] != 0) {
            val_seg_ptrs[j] = (void*)values[i];
            j++;
        }
    }
    for (i = 0; i < non_empty_values + 1; i++) {
        in.vals_bulk_size += val_seg_sizes[i];
    }

    /* create the bulk handle to access the keys */
    hret = margo_bulk_create(provider->client->mid, num + 1, key_seg_ptrs,
                             key_seg_sizes, HG_BULK_READ_ONLY,
                             &in.keys_bulk_handle);
    if (hret != HG_SUCCESS) {
        fprintf(stderr,
                "[SDSKV] margo_bulk_create() failed in sdskv_put_multi()\n");
        out.ret = SDSKV_MAKE_HG_ERROR(hret);
        goto finish;
    }

    /* create the bulk handle to access the values */
    hret = margo_bulk_create(provider->client->mid, non_empty_values + 1,
                             val_seg_ptrs, val_seg_sizes, HG_BULK_READ_ONLY,
                             &in.vals_bulk_handle);
    if (hret != HG_SUCCESS) {
        fprintf(stderr,
                "[SDSKV] margo_bulk_create() failed in sdskv_put_multi()\n");
        out.ret = SDSKV_MAKE_HG_ERROR(hret);
        goto finish;
    }

    /* create a RPC handle */
    hret = margo_create(provider->client->mid, provider->addr,
                        provider->client->sdskv_put_multi_id, &handle);
    if (hret != HG_SUCCESS) {
        fprintf(stderr, "[SDSKV] margo_create() failed in sdskv_put_multi()\n");
        out.ret = SDSKV_MAKE_HG_ERROR(hret);
        goto finish;
    }

    /* forward the RPC */
    hret = margo_provider_forward(provider->provider_id, handle, &in);
    if (hret != HG_SUCCESS) {
        fprintf(stderr,
                "[SDSKV] margo_forward() failed in sdskv_put_multi()\n");
        out.ret = SDSKV_MAKE_HG_ERROR(hret);
        goto finish;
    }

    /* get the output */
    hret = margo_get_output(handle, &out);
    if (hret != HG_SUCCESS) {
        fprintf(stderr,
                "[SDSKV] margo_get_output() failed in sdskv_put_multi()\n");
        out.ret = SDSKV_MAKE_HG_ERROR(hret);
        goto finish;
    }

    ret = out.ret;

finish:
    margo_free_output(handle, &out);
    margo_bulk_free(in.keys_bulk_handle);
    margo_bulk_free(in.vals_bulk_handle);
    free(key_seg_sizes);
    free(key_seg_ptrs);
    free(val_seg_sizes);
    free(val_seg_ptrs);
    margo_destroy(handle);
    return ret;
}

int sdskv_put_packed(sdskv_provider_handle_t provider,
                     sdskv_database_id_t     db_id,
                     size_t                  num,
                     const void*             packed_keys,
                     const hg_size_t*        ksizes,
                     const void*             packed_values,
                     const hg_size_t*        vsizes)
{
    hg_return_t hret;
    int         ret         = SDSKV_SUCCESS;
    hg_bulk_t   bulk_handle = HG_BULK_NULL;

    hg_size_t keys_buffer_size = 0;
    hg_size_t vals_buffer_size = 0;
    unsigned  i                = 0;
    for (i = 0; i < num; i++) {
        keys_buffer_size += ksizes[i];
        vals_buffer_size += vsizes[i];
    }
    hg_size_t bulk_size
        = keys_buffer_size + vals_buffer_size + 2 * num * sizeof(size_t);

    hg_size_t seg_sizes[4] = {num * sizeof(size_t), num * sizeof(size_t),
                              keys_buffer_size, vals_buffer_size};
    void*     seg_ptrs[4]  = {(void*)ksizes, (void*)vsizes, (void*)packed_keys,
                         (void*)packed_values};
    int       num_seg      = vals_buffer_size == 0 ? 3 : 4;

    hret = margo_bulk_create(provider->client->mid, num_seg, seg_ptrs,
                             seg_sizes, HG_BULK_READ_ONLY, &bulk_handle);
    if (hret != HG_SUCCESS) {
        fprintf(stderr,
                "[SDSKV] margo_bulk_create() failed in sdskv_put_packed()\n");
        return SDSKV_MAKE_HG_ERROR(hret);
    }

    ret = sdskv_proxy_put_packed(provider, db_id, NULL, num, bulk_handle,
                                 bulk_size);
    margo_bulk_free(bulk_handle);
    return ret;
}

int sdskv_proxy_put_packed(sdskv_provider_handle_t provider,
                           sdskv_database_id_t     db_id,
                           const char*             origin_addr,
                           size_t                  num,
                           hg_bulk_t               packed_data,
                           hg_size_t               bulk_data_size)
{
    hg_return_t hret;
    int         ret = SDSKV_SUCCESS;
    hg_handle_t handle;

    put_packed_in_t  in;
    put_packed_out_t out;

    in.db_id       = db_id;
    in.num_keys    = num;
    in.origin_addr = (char*)origin_addr;
    in.bulk_handle = packed_data;
    in.bulk_size   = bulk_data_size;

    /* create handle */
    hret = margo_create(provider->client->mid, provider->addr,
                        provider->client->sdskv_put_packed_id, &handle);
    if (hret != HG_SUCCESS) {
        fprintf(stderr,
                "[SDSKV] margo_create() failed in sdskv_put_packed()\n");
        margo_bulk_free(in.bulk_handle);
        return SDSKV_MAKE_HG_ERROR(hret);
    }

    hret = margo_provider_forward(provider->provider_id, handle, &in);
    if (hret != HG_SUCCESS) {
        fprintf(stderr,
                "[SDSKV] margo_forward() failed in sdskv_put_packed()\n");
        margo_bulk_free(in.bulk_handle);
        margo_destroy(handle);
        return SDSKV_MAKE_HG_ERROR(hret);
    }

    hret = margo_get_output(handle, &out);
    if (hret != HG_SUCCESS) {
        fprintf(stderr,
                "[SDSKV] margo_get_output() failed in sdskv_put_packed()\n");
        margo_bulk_free(in.bulk_handle);
        margo_destroy(handle);
        return SDSKV_MAKE_HG_ERROR(hret);
    }

    ret = out.ret;
    margo_free_output(handle, &out);

    margo_destroy(handle);
    return ret;
}

int sdskv_get(sdskv_provider_handle_t provider,
              sdskv_database_id_t     db_id,
              const void*             key,
              hg_size_t               ksize,
              void*                   value,
              hg_size_t*              vsize)
{
    hg_return_t hret;
    hg_size_t   size;
    hg_size_t   msize;
    int         ret;
    hg_handle_t handle;

    if (value == NULL) {
        return sdskv_length(provider, db_id, key, ksize, vsize);
    }

    size  = *(hg_size_t*)vsize;
    msize = size + sizeof(hg_size_t) + sizeof(hg_return_t);

    if (msize <= MAX_RPC_MESSAGE_SIZE) {

        get_in_t  in;
        get_out_t out;

        in.db_id    = db_id;
        in.key.data = (kv_ptr_t)key;
        in.key.size = ksize;
        in.vsize    = size;

        /* create handle */
        hret = margo_create(provider->client->mid, provider->addr,
                            provider->client->sdskv_get_id, &handle);
        if (hret != HG_SUCCESS) return SDSKV_MAKE_HG_ERROR(hret);

        hret = margo_provider_forward(provider->provider_id, handle, &in);
        if (hret != HG_SUCCESS) {
            margo_destroy(handle);
            return SDSKV_MAKE_HG_ERROR(hret);
        }

        hret = margo_get_output(handle, &out);
        if (hret != HG_SUCCESS) {
            margo_destroy(handle);
            return SDSKV_MAKE_HG_ERROR(hret);
        }

        ret = out.ret;

        if (ret == SDSKV_SUCCESS) {
            *vsize = out.vsize;
            if (out.value.size > 0) {
                memcpy(value, out.value.data, out.value.size);
            }
        } else if (ret == SDSKV_ERR_SIZE) {
            *vsize = out.vsize;
        }

        margo_free_output(handle, &out);

    } else {

        bulk_get_in_t  in;
        bulk_get_out_t out;

        in.db_id    = db_id;
        in.key.data = (kv_ptr_t)key;
        in.key.size = ksize;
        in.vsize    = size;

        hret = margo_bulk_create(provider->client->mid, 1, &value, &in.vsize,
                                 HG_BULK_WRITE_ONLY, &in.handle);
        if (hret != HG_SUCCESS) return SDSKV_MAKE_HG_ERROR(hret);

        /* create handle */
        hret = margo_create(provider->client->mid, provider->addr,
                            provider->client->sdskv_bulk_get_id, &handle);
        if (hret != HG_SUCCESS) {
            margo_bulk_free(in.handle);
            return SDSKV_MAKE_HG_ERROR(hret);
        }

        hret = margo_provider_forward(provider->provider_id, handle, &in);
        if (hret != HG_SUCCESS) {
            margo_bulk_free(in.handle);
            margo_destroy(handle);
            return SDSKV_MAKE_HG_ERROR(hret);
        }

        hret = margo_get_output(handle, &out);
        if (hret != HG_SUCCESS) {
            margo_bulk_free(in.handle);
            margo_destroy(handle);
            return SDSKV_MAKE_HG_ERROR(hret);
        }

        ret    = out.ret;
        *vsize = out.vsize;

        margo_free_output(handle, &out);
        margo_bulk_free(in.handle);
    }

    margo_destroy(handle);
    return ret;
}

int sdskv_get_multi(sdskv_provider_handle_t provider,
                    sdskv_database_id_t     db_id,
                    size_t                  num,
                    const void* const*      keys,
                    const hg_size_t*        ksizes,
                    void**                  values,
                    hg_size_t*              vsizes)
{
    /******* NOTE ********
     * This function works as follows:
     *  - in.keys_bulk_handle will be a multi-segment bulk handle of num+1
     * segments. The addresses to these segments are stored in key_seg_ptrs and
     * their size is in key_seg_sizes. Segment 0 will point to the ksizes array
     * and will be used to communicate the size of each key. The next segments
     * will be used to communicate the keys themselves.
     * - in.vals_bulk_handle will be a bulk handle exposing a large single
     * segment. The beginning of this segment will be used to hold num hg_size_t
     * values corresponding to the sizes allocated for each value, and that the
     * server will modifiy with the actual sizes. The rest of the buffer will be
     * used for the server to push actual values, which will be concatenated
     * back to back and will require unpacking to be put into the values input
     * buffers.
     */
    hg_return_t     hret;
    int             ret;
    hg_handle_t     handle = HG_HANDLE_NULL;
    get_multi_in_t  in;
    get_multi_out_t out;
    void**          key_seg_ptrs  = NULL;
    hg_size_t*      key_seg_sizes = NULL;
    char*           vals_buffer   = NULL;

    if (values == NULL) {
        return sdskv_length_multi(provider, db_id, num, keys, ksizes, vsizes);
    }

    in.db_id            = db_id;
    in.num_keys         = num;
    in.keys_bulk_handle = HG_BULK_NULL;
    in.keys_bulk_size   = 0;
    in.vals_bulk_handle = HG_BULK_NULL;
    in.vals_bulk_size   = 0;

    /* create an array of key sizes and key pointers */
    key_seg_sizes    = malloc(sizeof(hg_size_t) * (num + 1));
    key_seg_sizes[0] = num * sizeof(hg_size_t);
    memcpy(key_seg_sizes + 1, ksizes, num * sizeof(hg_size_t));
    key_seg_ptrs    = malloc(sizeof(void*) * (num + 1));
    key_seg_ptrs[0] = (void*)ksizes;
    memcpy(key_seg_ptrs + 1, keys, num * sizeof(void*));

    int i;
    for (i = 0; i < num + 1; i++) { in.keys_bulk_size += key_seg_sizes[i]; }

    /* create the bulk handle to access the keys */
    hret = margo_bulk_create(provider->client->mid, num + 1, key_seg_ptrs,
                             key_seg_sizes, HG_BULK_READ_ONLY,
                             &in.keys_bulk_handle);
    if (hret != HG_SUCCESS) {
        fprintf(stderr,
                "[SDSKV] margo_bulk_create() failed in sdskv_get_multi()\n");
        out.ret = SDSKV_MAKE_HG_ERROR(hret);
        goto finish;
    }

    /* allocate memory to send max value sizes and receive values */
    for (i = 0; i < num; i++) { in.vals_bulk_size += vsizes[i]; }
    in.vals_bulk_size += sizeof(hg_size_t) * num;
    vals_buffer = malloc(in.vals_bulk_size);
    hg_size_t* value_sizes
        = (hg_size_t*)vals_buffer; // beginning of the buffer used to hold sizes
    for (i = 0; i < num; i++) { value_sizes[i] = vsizes[i]; }

    /* create the bulk handle to access the values */
    hret = margo_bulk_create(provider->client->mid, 1, (void**)&vals_buffer,
                             &in.vals_bulk_size, HG_BULK_READWRITE,
                             &in.vals_bulk_handle);
    if (hret != HG_SUCCESS) {
        fprintf(stderr,
                "[SDSKV] margo_bulk_create() failed in sdskv_get_multi()\n");
        out.ret = SDSKV_MAKE_HG_ERROR(hret);
        goto finish;
    }

    /* create a RPC handle */
    hret = margo_create(provider->client->mid, provider->addr,
                        provider->client->sdskv_get_multi_id, &handle);
    if (hret != HG_SUCCESS) {
        fprintf(stderr, "[SDSKV] margo_create() failed in sdskv_get_multi()\n");
        out.ret = SDSKV_MAKE_HG_ERROR(hret);
        goto finish;
    }

    /* forward the RPC handle */
    hret = margo_provider_forward(provider->provider_id, handle, &in);
    if (hret != HG_SUCCESS) {
        fprintf(stderr,
                "[SDSKV] margo_forward() failed in sdskv_get_multi()\n");
        out.ret = SDSKV_MAKE_HG_ERROR(hret);
        goto finish;
    }

    /* get the response */
    hret = margo_get_output(handle, &out);
    if (hret != HG_SUCCESS) {
        fprintf(stderr,
                "[SDSKV] margo_get_output() failed in sdskv_put_multi()\n");
        out.ret = SDSKV_MAKE_HG_ERROR(hret);
        goto finish;
    }

    ret = out.ret;
    if (out.ret != SDSKV_SUCCESS) { goto finish; }

    /* copy the values from the buffer into the user-provided buffer */
    char* value_ptr = vals_buffer + num * sizeof(hg_size_t);
    for (i = 0; i < num; i++) {
        memcpy(values[i], value_ptr, value_sizes[i]);
        vsizes[i] = value_sizes[i];
        value_ptr += value_sizes[i];
    }

finish:
    margo_free_output(handle, &out);
    margo_bulk_free(in.keys_bulk_handle);
    margo_bulk_free(in.vals_bulk_handle);
    free(key_seg_sizes);
    free(key_seg_ptrs);
    free(vals_buffer);
    margo_destroy(handle);
    return ret;
}

int sdskv_exists(sdskv_provider_handle_t provider,
                 sdskv_database_id_t     db_id,
                 const void*             key,
                 hg_size_t               ksize,
                 int*                    flag)
{
    hg_return_t hret;
    int         ret;
    hg_handle_t handle;

    exists_in_t  in;
    exists_out_t out;

    in.db_id    = db_id;
    in.key.data = (kv_ptr_t)key;
    in.key.size = ksize;

    /* create handle */
    hret = margo_create(provider->client->mid, provider->addr,
                        provider->client->sdskv_exists_id, &handle);
    if (hret != HG_SUCCESS) return SDSKV_MAKE_HG_ERROR(hret);

    hret = margo_provider_forward(provider->provider_id, handle, &in);
    if (hret != HG_SUCCESS) {
        margo_destroy(handle);
        return SDSKV_MAKE_HG_ERROR(hret);
    }

    hret = margo_get_output(handle, &out);
    if (hret != HG_SUCCESS) {
        margo_destroy(handle);
        return SDSKV_MAKE_HG_ERROR(hret);
    }

    ret = out.ret;
    if (ret == 0) *flag = out.flag;

    margo_free_output(handle, &out);
    margo_destroy(handle);
    return ret;
}

int sdskv_exists_multi(sdskv_provider_handle_t provider,
                       sdskv_database_id_t     db_id,
                       size_t                  num,
                       const void* const*      keys,
                       const hg_size_t*        ksizes,
                       int*                    flags)
{
    hg_return_t        hret;
    int                ret;
    hg_handle_t        handle = HG_HANDLE_NULL;
    exists_multi_in_t  in;
    exists_multi_out_t out;
    void**             key_seg_ptrs  = NULL;
    hg_size_t*         key_seg_sizes = NULL;

    in.db_id             = db_id;
    in.num_keys          = num;
    in.keys_bulk_handle  = HG_BULK_NULL;
    in.keys_bulk_size    = 0;
    in.flags_bulk_handle = HG_BULK_NULL;

    /* create an array of key sizes and key pointers */
    key_seg_sizes    = malloc(sizeof(hg_size_t) * (num + 1));
    key_seg_sizes[0] = num * sizeof(hg_size_t);
    memcpy(key_seg_sizes + 1, ksizes, num * sizeof(hg_size_t));
    key_seg_ptrs    = malloc(sizeof(void*) * (num + 1));
    key_seg_ptrs[0] = (void*)ksizes;
    memcpy(key_seg_ptrs + 1, keys, num * sizeof(void*));

    int i;
    for (i = 0; i < num + 1; i++) { in.keys_bulk_size += key_seg_sizes[i]; }

    /* create the bulk handle to access the keys */
    hret = margo_bulk_create(provider->client->mid, num + 1, key_seg_ptrs,
                             key_seg_sizes, HG_BULK_READ_ONLY,
                             &in.keys_bulk_handle);
    if (hret != HG_SUCCESS) {
        fprintf(stderr,
                "[SDSKV] margo_bulk_create() for keys failed in "
                "sdskv_exists_multi()\n");
        out.ret = SDSKV_MAKE_HG_ERROR(hret);
        goto finish;
    }

    /* create the bulk handle for the server to whether the keys exist */
    hg_size_t exist_size = num / 8 + (num % 8 == 0 ? 0 : 1);
    uint8_t*  exist      = calloc(exist_size, 1);
    hret = margo_bulk_create(provider->client->mid, 1, (void**)&exist,
                             &exist_size, HG_BULK_WRITE_ONLY,
                             &in.flags_bulk_handle);
    if (hret != HG_SUCCESS) {
        fprintf(stderr,
                "[SDSKV] margo_bulk_create() for vsizes failed in "
                "sdskv_exists_multi()\n");
        out.ret = SDSKV_MAKE_HG_ERROR(hret);
        goto finish;
    }

    /* create a RPC handle */
    hret = margo_create(provider->client->mid, provider->addr,
                        provider->client->sdskv_exists_multi_id, &handle);
    if (hret != HG_SUCCESS) {
        fprintf(stderr,
                "[SDSKV] margo_create() failed in sdskv_exists_multi()\n");
        out.ret = SDSKV_MAKE_HG_ERROR(hret);
        goto finish;
    }

    /* forward the RPC handle */
    hret = margo_provider_forward(provider->provider_id, handle, &in);
    if (hret != HG_SUCCESS) {
        fprintf(stderr,
                "[SDSKV] margo_forward() failed in sdskv_exists_multi()\n");
        out.ret = SDSKV_MAKE_HG_ERROR(hret);
        goto finish;
    }

    /* get the response */
    hret = margo_get_output(handle, &out);
    if (hret != HG_SUCCESS) {
        fprintf(stderr,
                "[SDSKV] margo_get_output() failed in sdskv_exists_multi()\n");
        out.ret = SDSKV_MAKE_HG_ERROR(hret);
        goto finish;
    }

    ret = out.ret;
    if (out.ret != SDSKV_SUCCESS) { goto finish; }

    uint8_t mask = 1;
    for (i = 0; i < num; i++) {
        uint8_t c    = exist[i / 8];
        *(flags + i) = c & mask ? 1 : 0;
        if (i % 8 == 7) {
            mask = 1;
        } else {
            mask = mask << 1;
        }
    }

finish:
    margo_free_output(handle, &out);
    margo_bulk_free(in.keys_bulk_handle);
    margo_bulk_free(in.flags_bulk_handle);
    free(key_seg_sizes);
    free(key_seg_ptrs);
    free(exist);
    margo_destroy(handle);
    return ret;
}

int sdskv_length(sdskv_provider_handle_t provider,
                 sdskv_database_id_t     db_id,
                 const void*             key,
                 hg_size_t               ksize,
                 hg_size_t*              vsize)
{
    hg_return_t hret;
    int         ret;
    hg_handle_t handle;

    length_in_t  in;
    length_out_t out;

    in.db_id    = db_id;
    in.key.data = (kv_ptr_t)key;
    in.key.size = ksize;

    /* create handle */
    hret = margo_create(provider->client->mid, provider->addr,
                        provider->client->sdskv_length_id, &handle);
    if (hret != HG_SUCCESS) return SDSKV_MAKE_HG_ERROR(hret);

    hret = margo_provider_forward(provider->provider_id, handle, &in);
    if (hret != HG_SUCCESS) {
        margo_destroy(handle);
        return SDSKV_MAKE_HG_ERROR(hret);
    }

    hret = margo_get_output(handle, &out);
    if (hret != HG_SUCCESS) {
        margo_destroy(handle);
        return SDSKV_MAKE_HG_ERROR(hret);
    }

    ret = out.ret;
    if (ret == 0) *vsize = out.size;

    margo_free_output(handle, &out);
    margo_destroy(handle);
    return ret;
}

int sdskv_length_multi(sdskv_provider_handle_t provider,
                       sdskv_database_id_t     db_id,
                       size_t                  num,
                       const void* const*      keys,
                       const hg_size_t*        ksizes,
                       hg_size_t*              vsizes)
{
    hg_return_t        hret;
    int                ret;
    hg_handle_t        handle = HG_HANDLE_NULL;
    length_multi_in_t  in;
    length_multi_out_t out;
    void**             key_seg_ptrs  = NULL;
    hg_size_t*         key_seg_sizes = NULL;

    in.db_id                 = db_id;
    in.num_keys              = num;
    in.keys_bulk_handle      = HG_BULK_NULL;
    in.keys_bulk_size        = 0;
    in.vals_size_bulk_handle = HG_BULK_NULL;

    /* create an array of key sizes and key pointers */
    key_seg_sizes    = malloc(sizeof(hg_size_t) * (num + 1));
    key_seg_sizes[0] = num * sizeof(hg_size_t);
    memcpy(key_seg_sizes + 1, ksizes, num * sizeof(hg_size_t));
    key_seg_ptrs    = malloc(sizeof(void*) * (num + 1));
    key_seg_ptrs[0] = (void*)ksizes;
    memcpy(key_seg_ptrs + 1, keys, num * sizeof(void*));

    int i;
    for (i = 0; i < num + 1; i++) { in.keys_bulk_size += key_seg_sizes[i]; }

    /* create the bulk handle to access the keys */
    hret = margo_bulk_create(provider->client->mid, num + 1, key_seg_ptrs,
                             key_seg_sizes, HG_BULK_READ_ONLY,
                             &in.keys_bulk_handle);
    if (hret != HG_SUCCESS) {
        fprintf(stderr,
                "[SDSKV] margo_bulk_create() for keys failed in "
                "sdskv_length_multi()\n");
        out.ret = SDSKV_MAKE_HG_ERROR(hret);
        goto finish;
    }

    /* create the bulk handle for the server to put the values sizes */
    hg_size_t vals_size_bulk_size = num * sizeof(hg_size_t);
    hret = margo_bulk_create(provider->client->mid, 1, (void**)&vsizes,
                             &vals_size_bulk_size, HG_BULK_WRITE_ONLY,
                             &in.vals_size_bulk_handle);
    if (hret != HG_SUCCESS) {
        fprintf(stderr,
                "[SDSKV] margo_bulk_create() for vsizes failed in "
                "sdskv_length_multi()\n");
        out.ret = SDSKV_MAKE_HG_ERROR(hret);
        goto finish;
    }

    /* create a RPC handle */
    hret = margo_create(provider->client->mid, provider->addr,
                        provider->client->sdskv_length_multi_id, &handle);
    if (hret != HG_SUCCESS) {
        fprintf(stderr,
                "[SDSKV] margo_create() failed in sdskv_length_multi()\n");
        out.ret = SDSKV_MAKE_HG_ERROR(hret);
        goto finish;
    }

    /* forward the RPC handle */
    hret = margo_provider_forward(provider->provider_id, handle, &in);
    if (hret != HG_SUCCESS) {
        fprintf(stderr,
                "[SDSKV] margo_forward() failed in sdskv_length_multi()\n");
        out.ret = SDSKV_MAKE_HG_ERROR(hret);
        goto finish;
    }

    /* get the response */
    hret = margo_get_output(handle, &out);
    if (hret != HG_SUCCESS) {
        fprintf(stderr,
                "[SDSKV] margo_get_output() failed in sdskv_length_multi()\n");
        out.ret = SDSKV_MAKE_HG_ERROR(hret);
        goto finish;
    }

    ret = out.ret;
    if (out.ret != SDSKV_SUCCESS) { goto finish; }

finish:
    margo_free_output(handle, &out);
    margo_bulk_free(in.keys_bulk_handle);
    margo_bulk_free(in.vals_size_bulk_handle);
    free(key_seg_sizes);
    free(key_seg_ptrs);
    margo_destroy(handle);
    return ret;
}

int sdskv_length_packed(sdskv_provider_handle_t provider,
                        sdskv_database_id_t     db_id,
                        size_t                  num,
                        const void*             packed_keys,
                        const hg_size_t*        ksizes,
                        hg_size_t*              vsizes)
{
    hg_return_t hret;
    int         ret;
    hg_handle_t handle;

    length_packed_in_t  in;
    length_packed_out_t out;

    in.db_id           = db_id;
    in.num_keys        = num;
    in.in_bulk_size    = 0;
    in.in_bulk_handle  = HG_BULK_NULL;
    in.out_bulk_handle = HG_BULK_NULL;

    hg_size_t total_ksize = 0;
    unsigned  i           = 0;
    for (i = 0; i < num; i++) { total_ksize += ksizes[i]; }

    /* create bulk handle to expose the packed_keys and ksizes */
    void*     seg_ptrs[2]  = {(void*)ksizes, (void*)packed_keys};
    hg_size_t seg_sizes[2] = {num * sizeof(hg_size_t), total_ksize};
    in.in_bulk_size        = total_ksize + num * sizeof(hg_size_t);

    hret = margo_bulk_create(provider->client->mid, 2, seg_ptrs, seg_sizes,
                             HG_BULK_READ_ONLY, &in.in_bulk_handle);
    if (hret != HG_SUCCESS) {
        fprintf(stderr,
                "[SDSKV] margo_bulk_create() for keys/ksizes failed in "
                "sdskv_length_packed()\n");
        return SDSKV_MAKE_HG_ERROR(hret);
    }

    /* create bulk handle to expose the vsizes */
    hg_size_t val_size_buf_size = num * sizeof(hg_size_t);
    hret = margo_bulk_create(provider->client->mid, 1, (void**)(&vsizes),
                             &val_size_buf_size, HG_BULK_WRITE_ONLY,
                             &in.out_bulk_handle);
    if (hret != HG_SUCCESS) {
        fprintf(stderr,
                "[SDSKV] margo_bulk_create() for vsizes failed in "
                "sdskv_length_packed()\n");
        margo_bulk_free(in.in_bulk_handle);
        return SDSKV_MAKE_HG_ERROR(hret);
    }

    /* create RPC handle */
    hret = margo_create(provider->client->mid, provider->addr,
                        provider->client->sdskv_length_packed_id, &handle);
    if (hret != HG_SUCCESS) {
        fprintf(stderr,
                "[SDSKV] margo_create() failed in sdskv_length_packed()\n");
        margo_bulk_free(in.in_bulk_handle);
        margo_bulk_free(in.out_bulk_handle);
        return SDSKV_MAKE_HG_ERROR(hret);
    }

    /* forward RPC */
    hret = margo_provider_forward(provider->provider_id, handle, &in);
    if (hret != HG_SUCCESS) {
        fprintf(stderr,
                "[SDSKV] margo_provider_forward() failed in "
                "sdskv_length_packed()\n");
        margo_bulk_free(in.in_bulk_handle);
        margo_bulk_free(in.out_bulk_handle);
        margo_destroy(handle);
        return SDSKV_MAKE_HG_ERROR(hret);
    }

    /* Get output */
    hret = margo_get_output(handle, &out);
    if (hret != HG_SUCCESS) {
        fprintf(stderr,
                "[SDSKV] margo_get_output() failed in sdskv_length_packed()\n");
        margo_bulk_free(in.in_bulk_handle);
        margo_bulk_free(in.out_bulk_handle);
        margo_destroy(handle);
        return SDSKV_MAKE_HG_ERROR(hret);
    }

    ret = out.ret;
    margo_bulk_free(in.in_bulk_handle);
    margo_bulk_free(in.out_bulk_handle);
    margo_free_output(handle, &out);
    margo_destroy(handle);
    return ret;
}

int sdskv_get_packed(sdskv_provider_handle_t provider,
                     sdskv_database_id_t     db_id,
                     size_t*                 num,
                     const void*             packed_keys,
                     const hg_size_t*        ksizes,
                     hg_size_t               vbufsize,
                     void*                   packed_vals,
                     hg_size_t*              vsizes)
{
    hg_return_t hret;
    int         ret;
    hg_handle_t handle;

    get_packed_in_t  in;
    get_packed_out_t out;

    in.db_id            = db_id;
    in.num_keys         = *num;
    in.keys_bulk_size   = 0;
    in.keys_bulk_handle = HG_BULK_NULL;
    in.vals_bulk_size   = 0;
    in.vals_bulk_handle = HG_BULK_NULL;

    hg_size_t total_ksize = 0;
    unsigned  i           = 0;
    for (i = 0; i < *num; i++) { total_ksize += ksizes[i]; }

    /* create bulk handle to expose the packed_keys and ksizes */
    void*     seg_ptrs[2]  = {(void*)ksizes, (void*)packed_keys};
    hg_size_t seg_sizes[2] = {(*num) * sizeof(hg_size_t), total_ksize};
    in.keys_bulk_size      = total_ksize + (*num) * sizeof(hg_size_t);

    hret = margo_bulk_create(provider->client->mid, 2, seg_ptrs, seg_sizes,
                             HG_BULK_READ_ONLY, &in.keys_bulk_handle);
    if (hret != HG_SUCCESS) {
        fprintf(stderr,
                "[SDSKV] margo_bulk_create() for keys/ksizes failed in "
                "sdskv_get_packed()\n");
        return SDSKV_MAKE_HG_ERROR(hret);
    }

    /* create bulk handle to expose the packed_vals and vsizes */
    seg_ptrs[0]       = (void*)vsizes;
    seg_ptrs[1]       = (void*)packed_vals;
    seg_sizes[0]      = (*num) * sizeof(hg_size_t);
    seg_sizes[1]      = vbufsize;
    in.vals_bulk_size = (*num) * sizeof(hg_size_t) + vbufsize;

    hret = margo_bulk_create(provider->client->mid, 2, seg_ptrs, seg_sizes,
                             HG_BULK_WRITE_ONLY, &in.vals_bulk_handle);
    if (hret != HG_SUCCESS) {
        fprintf(stderr,
                "[SDSKV] margo_bulk_create() for vals/vsizes failed in "
                "sdskv_get_packed()\n");
        margo_bulk_free(in.keys_bulk_handle);
        return SDSKV_MAKE_HG_ERROR(hret);
    }

    /* create RPC handle */
    hret = margo_create(provider->client->mid, provider->addr,
                        provider->client->sdskv_get_packed_id, &handle);
    if (hret != HG_SUCCESS) {
        fprintf(stderr,
                "[SDSKV] margo_create() failed in sdskv_get_packed()\n");
        margo_bulk_free(in.keys_bulk_handle);
        margo_bulk_free(in.vals_bulk_handle);
        return SDSKV_MAKE_HG_ERROR(hret);
    }

    /* forward RPC */
    hret = margo_provider_forward(provider->provider_id, handle, &in);
    if (hret != HG_SUCCESS) {
        fprintf(
            stderr,
            "[SDSKV] margo_provider_forward() failed in sdskv_get_packed()\n");
        margo_bulk_free(in.keys_bulk_handle);
        margo_bulk_free(in.vals_bulk_handle);
        margo_destroy(handle);
        return SDSKV_MAKE_HG_ERROR(hret);
    }

    /* Get output */
    hret = margo_get_output(handle, &out);
    if (hret != HG_SUCCESS) {
        fprintf(stderr,
                "[SDSKV] margo_get_output() failed in sdskv_get_packed()\n");
        margo_bulk_free(in.keys_bulk_handle);
        margo_bulk_free(in.vals_bulk_handle);
        margo_destroy(handle);
        return SDSKV_MAKE_HG_ERROR(hret);
    }

    ret  = out.ret;
    *num = out.num_keys;

    margo_bulk_free(in.keys_bulk_handle);
    margo_bulk_free(in.vals_bulk_handle);
    margo_free_output(handle, &out);
    margo_destroy(handle);
    return ret;
}

int sdskv_erase(sdskv_provider_handle_t provider,
                sdskv_database_id_t     db_id,
                const void*             key,
                hg_size_t               ksize)
{
    hg_return_t hret;
    int         ret;
    hg_handle_t handle;

    erase_in_t  in;
    erase_out_t out;

    in.db_id    = db_id;
    in.key.data = (kv_ptr_t)key;
    in.key.size = ksize;

    /* create handle */
    hret = margo_create(provider->client->mid, provider->addr,
                        provider->client->sdskv_erase_id, &handle);
    if (hret != HG_SUCCESS) return SDSKV_MAKE_HG_ERROR(hret);

    hret = margo_provider_forward(provider->provider_id, handle, &in);
    if (hret != HG_SUCCESS) {
        margo_destroy(handle);
        return SDSKV_MAKE_HG_ERROR(hret);
    }

    hret = margo_get_output(handle, &out);
    if (hret != HG_SUCCESS) {
        margo_destroy(handle);
        return SDSKV_MAKE_HG_ERROR(hret);
    }

    ret = out.ret;

    margo_free_output(handle, &out);
    margo_destroy(handle);
    return ret;
}

int sdskv_erase_multi(sdskv_provider_handle_t provider,
                      sdskv_database_id_t     db_id,
                      size_t                  num,
                      const void* const*      keys,
                      const hg_size_t*        ksizes)
{
    hg_return_t       hret;
    int               ret;
    hg_handle_t       handle = HG_HANDLE_NULL;
    erase_multi_in_t  in;
    erase_multi_out_t out;
    void**            key_seg_ptrs  = NULL;
    hg_size_t*        key_seg_sizes = NULL;

    in.db_id            = db_id;
    in.num_keys         = num;
    in.keys_bulk_handle = HG_BULK_NULL;
    in.keys_bulk_size   = 0;

    /* create an array of key sizes and key pointers */
    key_seg_sizes    = malloc(sizeof(hg_size_t) * (num + 1));
    key_seg_sizes[0] = num * sizeof(hg_size_t);
    memcpy(key_seg_sizes + 1, ksizes, num * sizeof(hg_size_t));
    key_seg_ptrs    = malloc(sizeof(void*) * (num + 1));
    key_seg_ptrs[0] = (void*)ksizes;
    memcpy(key_seg_ptrs + 1, keys, num * sizeof(void*));

    int i;
    for (i = 0; i < num + 1; i++) { in.keys_bulk_size += key_seg_sizes[i]; }

    /* create the bulk handle to access the keys */
    hret = margo_bulk_create(provider->client->mid, num + 1, key_seg_ptrs,
                             key_seg_sizes, HG_BULK_READ_ONLY,
                             &in.keys_bulk_handle);
    if (hret != HG_SUCCESS) {
        fprintf(stderr,
                "[SDSKV] margo_bulk_create() failed in sdskv_erase_multi()\n");
        out.ret = SDSKV_MAKE_HG_ERROR(hret);
        goto finish;
    }

    /* create a RPC handle */
    hret = margo_create(provider->client->mid, provider->addr,
                        provider->client->sdskv_erase_multi_id, &handle);
    if (hret != HG_SUCCESS) {
        fprintf(stderr,
                "[SDSKV] margo_create() failed in sdskv_erase_multi()\n");
        out.ret = SDSKV_MAKE_HG_ERROR(hret);
        goto finish;
    }

    /* forward the RPC handle */
    hret = margo_provider_forward(provider->provider_id, handle, &in);
    if (hret != HG_SUCCESS) {
        fprintf(stderr,
                "[SDSKV] margo_forward() failed in sdskv_erase_multi()\n");
        out.ret = SDSKV_MAKE_HG_ERROR(hret);
        goto finish;
    }

    /* get the response */
    hret = margo_get_output(handle, &out);
    if (hret != HG_SUCCESS) {
        fprintf(stderr,
                "[SDSKV] margo_get_output() failed in sdskv_erase_multi()\n");
        out.ret = SDSKV_MAKE_HG_ERROR(hret);
        goto finish;
    }

    ret = out.ret;
    if (out.ret != SDSKV_SUCCESS) { goto finish; }

finish:
    margo_free_output(handle, &out);
    margo_bulk_free(in.keys_bulk_handle);
    free(key_seg_sizes);
    free(key_seg_ptrs);
    margo_destroy(handle);
    return ret;
}

int sdskv_list_keys(
    sdskv_provider_handle_t provider,
    sdskv_database_id_t     db_id, // db instance
    const void* start_key,         // we want keys strictly after this start_key
    hg_size_t   start_ksize,       // size of the start_key
    void**      keys,              // pointer to an array of void* pointers,
    //     this array has size *max_keys
    hg_size_t* ksizes, // pointer to an array of hg_size_t sizes
    //    representing sizes allocated in
    //     keys for each key
    hg_size_t* max_keys) // maximum number of keys requested
{
    return sdskv_list_keys_with_prefix(provider, db_id, start_key, start_ksize,
                                       NULL, 0, keys, ksizes, max_keys);
}

int sdskv_list_keys_with_prefix(
    sdskv_provider_handle_t provider,
    sdskv_database_id_t     db_id, // db instance
    const void* start_key,         // we want keys strictly after this start_key
    hg_size_t   start_ksize,       // size of the start_key
    const void* prefix,
    hg_size_t   prefix_size,
    void**      keys, // pointer to an array of void* pointers,
    //     this array has size *max_keys
    hg_size_t* ksizes, // pointer to an array of hg_size_t sizes
    //    representing sizes allocated in
    //     keys for each key
    hg_size_t* max_keys) // maximum number of keys requested
{
    list_keys_in_t  in;
    list_keys_out_t out;
    hg_return_t     hret   = HG_SUCCESS;
    hg_handle_t     handle = HG_HANDLE_NULL;
    int             ret    = SDSKV_SUCCESS;
    int             i;

    if (*max_keys == 0) { return SDSKV_SUCCESS; }

    in.db_id              = db_id;
    in.start_key.data     = (kv_ptr_t)start_key;
    in.start_key.size     = start_ksize;
    in.prefix.data        = (char*)prefix;
    in.prefix.size        = prefix_size;
    in.keys_bulk_handle   = HG_BULK_NULL;
    in.ksizes_bulk_handle = HG_BULK_NULL;
    in.max_keys           = *max_keys;

    /* create bulk handle to expose the segments with key sizes */
    hg_size_t ksize_bulk_size = (*max_keys) * sizeof(*ksizes);
    void*     buf_ptr[1]      = {ksizes};
    hret
        = margo_bulk_create(provider->client->mid, 1, buf_ptr, &ksize_bulk_size,
                            HG_BULK_READWRITE, &in.ksizes_bulk_handle);
    if (hret != HG_SUCCESS) {
        ret = SDSKV_MAKE_HG_ERROR(hret);
        goto finish;
    }

    /* make sure none of the buffers is NULL or 0-sized */
    int requesting_sizes = 0;
    if (keys == NULL) {
        requesting_sizes = 1;
    } else {
        for (i = 0; i < *max_keys; i++) {
            if (keys[i] == NULL || ksizes[i] == 0) {
                requesting_sizes = 1;
                break;
            }
        }
    }

    /* create bulk handle to expose where the keys should be placed */
    if (!requesting_sizes) {
        hret = margo_bulk_create(provider->client->mid, *max_keys, keys, ksizes,
                                 HG_BULK_WRITE_ONLY, &in.keys_bulk_handle);
        if (hret != HG_SUCCESS) {
            ret = SDSKV_MAKE_HG_ERROR(hret);
            goto finish;
        }
    }

    /* create handle */
    hret = margo_create(provider->client->mid, provider->addr,
                        provider->client->sdskv_list_keys_id, &handle);
    if (hret != HG_SUCCESS) {
        ret = SDSKV_MAKE_HG_ERROR(hret);
        goto finish;
    }

    /* forward to provider */
    hret = margo_provider_forward(provider->provider_id, handle, &in);
    if (hret != HG_SUCCESS) {
        ret = SDSKV_MAKE_HG_ERROR(hret);
        goto finish;
    }

    /* get the output from provider */
    hret = margo_get_output(handle, &out);
    if (hret != HG_SUCCESS) {
        ret = SDSKV_MAKE_HG_ERROR(hret);
        goto finish;
    }

    /* set return values */
    *max_keys = out.nkeys;
    ret       = out.ret;

finish:
    /* free everything we created */
    margo_bulk_free(in.ksizes_bulk_handle);
    margo_bulk_free(in.keys_bulk_handle);
    margo_free_output(handle, &out);
    margo_destroy(handle);

    return ret;
}

int sdskv_list_keyvals(
    sdskv_provider_handle_t provider,
    sdskv_database_id_t     db_id, // db instance
    const void* start_key,         // we want keys strictly after this start_key
    hg_size_t   start_ksize,       // size of the start_key
    void**      keys,              // pointer to an array of void* pointers,
                                   //     this array has size *max_keys
    hg_size_t* ksizes,             // pointer to an array of hg_size_t sizes
                                   //    representing sizes allocated in
                                   //     keys for each key
    void** values,                 // pointer to an array of void* pointers,
                                   //     this array has size *max_keys
    hg_size_t* vsizes,             // pointer to an array of hg_size_t sizes
                                   //    representing sizes allocated in
                                   //    values for each value
    hg_size_t* max_keys)           // maximum number of keys requested
{
    return sdskv_list_keyvals_with_prefix(provider, db_id, start_key,
                                          start_ksize, NULL, 0, keys, ksizes,
                                          values, vsizes, max_keys);
}

int sdskv_list_keyvals_with_prefix(
    sdskv_provider_handle_t provider,
    sdskv_database_id_t     db_id, // db instance
    const void* start_key,         // we want keys strictly after this start_key
    hg_size_t   start_ksize,       // size of the start_key
    const void* prefix,            // prefix of returned keys
    hg_size_t   prefix_size,       // size of prefix
    void**      keys,              // pointer to an array of void* pointers,
                                   //     this array has size *max_keys
    hg_size_t* ksizes,             // pointer to an array of hg_size_t sizes
                                   //    representing sizes allocated in
                                   //     keys for each key
    void** values,                 // pointer to an array of void* pointers,
                                   //     this array has size *max_keys
    hg_size_t* vsizes,             // pointer to an array of hg_size_t sizes
                                   //    representing sizes allocated in
                                   //    values for each value
    hg_size_t* max_keys)           // maximum number of keys requested
{
    list_keyvals_in_t  in;
    list_keyvals_out_t out;
    hg_return_t        hret   = HG_SUCCESS;
    hg_handle_t        handle = HG_HANDLE_NULL;
    int                ret    = SDSKV_SUCCESS;
    int                i;

    in.db_id              = db_id;
    in.start_key.data     = (kv_ptr_t)start_key;
    in.start_key.size     = start_ksize;
    in.prefix.data        = (char*)prefix;
    in.prefix.size        = prefix_size;
    in.max_keys           = *max_keys;
    in.keys_bulk_handle   = HG_BULK_NULL;
    in.ksizes_bulk_handle = HG_BULK_NULL;
    in.vals_bulk_handle   = HG_BULK_NULL;
    in.vsizes_bulk_handle = HG_BULK_NULL;

    /* create bulk handle to expose the segments with key sizes */
    hg_size_t ksize_bulk_size   = (*max_keys) * sizeof(*ksizes);
    void*     ksizes_buf_ptr[1] = {ksizes};
    hret = margo_bulk_create(provider->client->mid, 1, ksizes_buf_ptr,
                             &ksize_bulk_size, HG_BULK_READWRITE,
                             &in.ksizes_bulk_handle);
    if (hret != HG_SUCCESS) {
        ret = SDSKV_MAKE_HG_ERROR(hret);
        goto finish;
    }

    /* create bulk handle to expose the segments with value sizes */
    hg_size_t vsize_bulk_size   = (*max_keys) * sizeof(*vsizes);
    void*     vsizes_buf_ptr[1] = {vsizes};
    hret = margo_bulk_create(provider->client->mid, 1, vsizes_buf_ptr,
                             &ksize_bulk_size, HG_BULK_READWRITE,
                             &in.vsizes_bulk_handle);
    if (hret != HG_SUCCESS) {
        ret = SDSKV_MAKE_HG_ERROR(hret);
        goto finish;
    }

    /* create bulk handle to expose where the keys should be placed */
    if (keys) {
        hret = margo_bulk_create(provider->client->mid, *max_keys, keys, ksizes,
                                 HG_BULK_WRITE_ONLY, &in.keys_bulk_handle);
        if (hret != HG_SUCCESS) {
            ret = SDSKV_MAKE_HG_ERROR(hret);
            goto finish;
        }
    }

    /* create bulk handle to expose where the values should be placed */
    if (values) {
        hret = margo_bulk_create(provider->client->mid, *max_keys, values,
                                 vsizes, HG_BULK_WRITE_ONLY,
                                 &in.vals_bulk_handle);
        if (hret != HG_SUCCESS) {
            ret = SDSKV_MAKE_HG_ERROR(hret);
            goto finish;
        }
    }

    /* create handle */
    hret = margo_create(provider->client->mid, provider->addr,
                        provider->client->sdskv_list_keyvals_id, &handle);
    if (hret != HG_SUCCESS) {
        ret = SDSKV_MAKE_HG_ERROR(hret);
        goto finish;
    }

    /* forward to provider */
    hret = margo_provider_forward(provider->provider_id, handle, &in);
    if (hret != HG_SUCCESS) {
        ret = SDSKV_MAKE_HG_ERROR(hret);
        goto finish;
    }

    /* get the output from provider */
    hret = margo_get_output(handle, &out);
    if (hret != HG_SUCCESS) {
        ret = SDSKV_MAKE_HG_ERROR(hret);
        goto finish;
    }

    /* set return values */
    *max_keys = out.nkeys;
    ret       = out.ret;

finish:
    /* free everything we created */
    margo_bulk_free(in.ksizes_bulk_handle);
    margo_bulk_free(in.keys_bulk_handle);
    margo_bulk_free(in.vsizes_bulk_handle);
    margo_bulk_free(in.vals_bulk_handle);
    margo_free_output(handle, &out);
    margo_destroy(handle);

    return ret;
}

int sdskv_migrate_keys(sdskv_provider_handle_t source_provider,
                       sdskv_database_id_t     source_db_id,
                       const char*             target_addr,
                       uint16_t                target_provider_id,
                       sdskv_database_id_t     target_db_id,
                       hg_size_t               num_keys,
                       const void* const*      keys,
                       const hg_size_t*        key_sizes,
                       int                     flag)
{
    int                ret    = HG_SUCCESS;
    hg_return_t        hret   = HG_SUCCESS;
    hg_handle_t        handle = HG_HANDLE_NULL;
    migrate_keys_in_t  in;
    migrate_keys_out_t out;
    in.source_db_id       = source_db_id;
    in.target_addr        = (hg_string_t)target_addr;
    in.target_provider_id = target_provider_id;
    in.target_db_id       = target_db_id;
    in.keys_bulk          = HG_BULK_NULL;
    in.num_keys           = num_keys;
    in.flag               = flag;
    /* create bulk to expose key sizes and keys */
    hg_size_t* seg_sizes = (hg_size_t*)calloc(num_keys + 1, sizeof(hg_size_t));
    seg_sizes[0]         = num_keys * sizeof(hg_size_t);
    memcpy(seg_sizes + 1, key_sizes, num_keys * sizeof(hg_size_t));
    void** segs = (void**)calloc(num_keys + 1, sizeof(void*));
    segs[0]     = (void*)key_sizes;
    memcpy(segs + 1, keys, num_keys * sizeof(void*));
    /* compute the total size of the array */
    int i;
    in.bulk_size = 0;
    for (i = 0; i < num_keys + 1; i++) { in.bulk_size += seg_sizes[i]; }

    hret = margo_bulk_create(source_provider->client->mid, num_keys + 1, segs,
                             seg_sizes, HG_BULK_READ_ONLY, &in.keys_bulk);
    if (hret != HG_SUCCESS) {
        ret = SDSKV_MAKE_HG_ERROR(hret);
        goto finish;
    }

    /* create handle */
    hret = margo_create(source_provider->client->mid, source_provider->addr,
                        source_provider->client->sdskv_migrate_key_range_id,
                        &handle);
    if (hret != HG_SUCCESS) {
        ret = SDSKV_MAKE_HG_ERROR(hret);
        goto finish;
    }
    /* forward to provider */
    hret = margo_provider_forward(source_provider->provider_id, handle, &in);
    if (hret != HG_SUCCESS) {
        ret = SDSKV_MAKE_HG_ERROR(hret);
        goto finish;
    }
    /* get the output from provider */
    hret = margo_get_output(handle, &out);
    if (hret != HG_SUCCESS) {
        ret = SDSKV_MAKE_HG_ERROR(hret);
        goto finish;
    }
    ret = out.ret;

finish:
    free(seg_sizes);
    free(segs);
    margo_bulk_free(in.keys_bulk);
    margo_free_output(handle, &out);
    margo_destroy(handle);
    return ret;
}

int sdskv_migrate_key_range(sdskv_provider_handle_t source_provider,
                            sdskv_database_id_t     source_db_id,
                            const char*             target_addr,
                            uint16_t                target_provider_id,
                            sdskv_database_id_t     target_db_id,
                            const void*             key_range[],
                            const hg_size_t         key_range_sizes[],
                            int                     flag)
{
    int                    ret    = HG_SUCCESS;
    hg_return_t            hret   = HG_SUCCESS;
    hg_handle_t            handle = HG_HANDLE_NULL;
    migrate_key_range_in_t in;
    migrate_keys_out_t     out;
    in.source_db_id       = source_db_id;
    in.target_addr        = (hg_string_t)target_addr;
    in.target_provider_id = target_provider_id;
    in.target_db_id       = target_db_id;
    in.key_lb.size        = key_range_sizes[0];
    in.key_lb.data        = (void*)key_range[0];
    in.key_ub.size        = key_range_sizes[1];
    in.key_ub.data        = (void*)key_range[1];
    in.flag               = flag;
    /* create handle */
    hret = margo_create(source_provider->client->mid, source_provider->addr,
                        source_provider->client->sdskv_migrate_key_range_id,
                        &handle);
    if (hret != HG_SUCCESS) {
        ret = SDSKV_MAKE_HG_ERROR(hret);
        goto finish;
    }
    /* forward to provider */
    hret = margo_provider_forward(source_provider->provider_id, handle, &in);
    if (hret != HG_SUCCESS) {
        ret = SDSKV_MAKE_HG_ERROR(hret);
        goto finish;
    }
    /* get the output from provider */
    hret = margo_get_output(handle, &out);
    if (hret != HG_SUCCESS) {
        ret = SDSKV_MAKE_HG_ERROR(hret);
        goto finish;
    }
    ret = out.ret;
finish:
    margo_free_output(handle, &out);
    margo_destroy(handle);
    return ret;
}

int sdskv_migrate_keys_prefixed(sdskv_provider_handle_t source_provider,
                                sdskv_database_id_t     source_db_id,
                                const char*             target_addr,
                                uint16_t                target_provider_id,
                                sdskv_database_id_t     target_db_id,
                                const void*             key_prefix,
                                hg_size_t               key_prefix_size,
                                int                     flag)
{
    int                        ret    = HG_SUCCESS;
    hg_return_t                hret   = HG_SUCCESS;
    hg_handle_t                handle = HG_HANDLE_NULL;
    migrate_keys_prefixed_in_t in;
    migrate_keys_out_t         out;
    in.source_db_id       = source_db_id;
    in.target_addr        = (hg_string_t)target_addr;
    in.target_provider_id = target_provider_id;
    in.target_db_id       = target_db_id;
    in.key_prefix.size    = key_prefix_size;
    in.key_prefix.data    = (void*)key_prefix;
    in.flag               = flag;
    /* create handle */
    hret = margo_create(source_provider->client->mid, source_provider->addr,
                        source_provider->client->sdskv_migrate_keys_prefixed_id,
                        &handle);
    if (hret != HG_SUCCESS) {
        ret = SDSKV_MAKE_HG_ERROR(hret);
        goto finish;
    }
    /* forward to provider */
    hret = margo_provider_forward(source_provider->provider_id, handle, &in);
    if (hret != HG_SUCCESS) {
        ret = SDSKV_MAKE_HG_ERROR(hret);
        goto finish;
    }
    /* get the output from provider */
    hret = margo_get_output(handle, &out);
    if (hret != HG_SUCCESS) {
        ret = SDSKV_MAKE_HG_ERROR(hret);
        goto finish;
    }
    ret = out.ret;
finish:
    margo_free_output(handle, &out);
    margo_destroy(handle);
    return ret;
}

int sdskv_migrate_all_keys(sdskv_provider_handle_t source_provider,
                           sdskv_database_id_t     source_db_id,
                           const char*             target_addr,
                           uint16_t                target_provider_id,
                           sdskv_database_id_t     target_db_id,
                           int                     flag)
{
    int                   ret    = HG_SUCCESS;
    hg_return_t           hret   = HG_SUCCESS;
    hg_handle_t           handle = HG_HANDLE_NULL;
    migrate_all_keys_in_t in;
    migrate_keys_out_t    out;
    in.source_db_id       = source_db_id;
    in.target_addr        = (hg_string_t)target_addr;
    in.target_provider_id = target_provider_id;
    in.target_db_id       = target_db_id;
    in.flag               = flag;
    /* create handle */
    hret = margo_create(source_provider->client->mid, source_provider->addr,
                        source_provider->client->sdskv_migrate_all_keys_id,
                        &handle);
    if (hret != HG_SUCCESS) {
        ret = SDSKV_MAKE_HG_ERROR(hret);
        goto finish;
    }
    /* forward to provider */
    hret = margo_provider_forward(source_provider->provider_id, handle, &in);
    if (hret != HG_SUCCESS) {
        ret = SDSKV_MAKE_HG_ERROR(hret);
        goto finish;
    }
    /* get the output from provider */
    hret = margo_get_output(handle, &out);
    if (hret != HG_SUCCESS) {
        ret = SDSKV_MAKE_HG_ERROR(hret);
        goto finish;
    }
    ret = out.ret;
finish:
    margo_free_output(handle, &out);
    margo_destroy(handle);
    return ret;
}

int sdskv_migrate_database(sdskv_provider_handle_t source,
                           sdskv_database_id_t     source_db_id,
                           const char*             dest_addr,
                           uint16_t                dest_provider_id,
                           const char*             dest_root,
                           int                     flag)
{
    hg_return_t            hret;
    hg_handle_t            handle;
    migrate_database_in_t  in;
    migrate_database_out_t out;
    int                    ret;

    in.source_db_id          = source_db_id;
    in.remove_src            = flag;
    in.dest_remi_addr        = dest_addr;
    in.dest_remi_provider_id = dest_provider_id;
    in.dest_root             = dest_root;

    hret = margo_create(source->client->mid, source->addr,
                        source->client->sdskv_migrate_database_id, &handle);
    if (hret != HG_SUCCESS) return SDSKV_MAKE_HG_ERROR(hret);

    hret = margo_provider_forward(source->provider_id, handle, &in);
    if (hret != HG_SUCCESS) {
        margo_destroy(handle);
        return SDSKV_MAKE_HG_ERROR(hret);
    }

    hret = margo_get_output(handle, &out);
    if (hret != HG_SUCCESS) {
        margo_destroy(handle);
        return SDSKV_MAKE_HG_ERROR(hret);
    }

    ret              = out.ret;
    sdskv_remi_errno = out.remi_ret;

    margo_free_output(handle, &out);
    margo_destroy(handle);
    return ret;
}

int sdskv_shutdown_service(sdskv_client_t client, hg_addr_t addr)
{
    return margo_shutdown_remote_instance(client->mid, addr);
}
