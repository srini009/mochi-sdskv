#ifndef SDSKV_RPC_TYPE_H
#define SDSKV_RPC_TYPE_H

#include <stdint.h>
#include <assert.h>

#include <mercury.h>
#include <mercury_macros.h>
#include <mercury_proc_string.h>

#include <margo.h>

#include "sdskv-common.h"

// setup to support opaque type handling
typedef char* kv_ptr_t;

typedef struct {
    hg_size_t size;
    kv_ptr_t  data;
} kv_data_t;

static inline hg_return_t hg_proc_kv_data_t(hg_proc_t proc, void* arg)
{
    hg_return_t ret;
    kv_data_t*  in = (kv_data_t*)arg;
    ret            = hg_proc_hg_size_t(proc, &in->size);
    if (ret != HG_SUCCESS) return ret;
    if (in->size) {
        switch (hg_proc_get_op(proc)) {
        case HG_ENCODE:
            ret = hg_proc_raw(proc, in->data, in->size);
            if (ret != HG_SUCCESS) return ret;
            break;
        case HG_DECODE:
            in->data = (kv_ptr_t)malloc(in->size);
            ret      = hg_proc_raw(proc, in->data, in->size);
            if (ret != HG_SUCCESS) return ret;
            break;
        case HG_FREE:
            free(in->data);
            break;
        default:
            break;
        }
    }
    return HG_SUCCESS;
}

// ------------- OPEN ------------- //
MERCURY_GEN_PROC(open_in_t, ((hg_string_t)(name)))
MERCURY_GEN_PROC(open_out_t, ((uint64_t)(db_id))((int32_t)(ret)))

// ------------- COUNT DATABASES - //
MERCURY_GEN_PROC(count_db_out_t, ((uint64_t)(count))((int32_t)(ret)))

// ------------- LIST DATABASES -- //
MERCURY_GEN_PROC(list_db_in_t, ((uint64_t)(count)))

typedef struct {
    size_t    count;
    char**    db_names;
    uint64_t* db_ids;
    int32_t   ret;
} list_db_out_t;

static hg_return_t hg_proc_list_db_out_t(hg_proc_t proc, void* arg)
{
    hg_return_t    ret;
    list_db_out_t* in = (list_db_out_t*)arg;

    unsigned i;
    ret = hg_proc_hg_size_t(proc, &in->count);
    if (ret != HG_SUCCESS) return ret;
    if (in->count) {
        switch (hg_proc_get_op(proc)) {
        case HG_ENCODE:
            for (i = 0; i < in->count; i++) {
                ret = hg_proc_hg_string_t(proc, &(in->db_names[i]));
                if (ret != HG_SUCCESS) return ret;
            }
            ret = hg_proc_raw(proc, in->db_ids,
                              in->count * sizeof(*(in->db_ids)));
            if (ret != HG_SUCCESS) return ret;
            ret = hg_proc_int32_t(proc, &(in->ret));
            if (ret != HG_SUCCESS) return ret;
            break;
        case HG_DECODE:
            in->db_names = (char**)malloc(in->count * sizeof(char*));
            in->db_ids   = (uint64_t*)malloc(in->count * sizeof(uint64_t));
            for (i = 0; i < in->count; i++) {
                ret = hg_proc_hg_string_t(proc, &(in->db_names[i]));
                if (ret != HG_SUCCESS) {
                    free(in->db_names);
                    free(in->db_ids);
                    return ret;
                }
            }
            ret = hg_proc_raw(proc, in->db_ids,
                              in->count * sizeof(*(in->db_ids)));
            if (ret != HG_SUCCESS) {
                free(in->db_names);
                free(in->db_ids);
                return ret;
            }
            ret = hg_proc_int32_t(proc, &(in->ret));
            if (ret != HG_SUCCESS) return ret;
            break;
        case HG_FREE:
            for (i = 0; i < in->count; i++) {
                hg_proc_hg_string_t(proc, &(in->db_names[i]));
            }
            free(in->db_names);
            free(in->db_ids);
            break;
        default:
            break;
        }
    }
    return HG_SUCCESS;
}

// ------------- PUT ------------- //
MERCURY_GEN_PROC(put_in_t,
                 ((uint64_t)(db_id))((kv_data_t)(key))((kv_data_t)(value)))
MERCURY_GEN_PROC(put_out_t, ((int32_t)(ret)))

// ------------- GET ------------- //
MERCURY_GEN_PROC(get_in_t,
                 ((uint64_t)(db_id))((kv_data_t)(key))((hg_size_t)(vsize)))

MERCURY_GEN_PROC(get_out_t,
                 ((int32_t)(ret))((kv_data_t)(value))((hg_size_t)(vsize)))

// ------------- LENGTH ------------- //
MERCURY_GEN_PROC(length_in_t, ((uint64_t)(db_id))((kv_data_t)(key)))
MERCURY_GEN_PROC(length_out_t, ((hg_size_t)(size))((int32_t)(ret)))

// ------------- EXISTS ------------- //
MERCURY_GEN_PROC(exists_in_t, ((uint64_t)(db_id))((kv_data_t)(key)))
MERCURY_GEN_PROC(exists_out_t, ((int32_t)(flag))((int32_t)(ret)))

// ------------- ERASE ------------- //
MERCURY_GEN_PROC(erase_out_t, ((int32_t)(ret)))
MERCURY_GEN_PROC(erase_in_t, ((uint64_t)(db_id))((kv_data_t)(key)))

// ------------- LIST KEYS ------------- //
MERCURY_GEN_PROC(list_keys_in_t,
                 ((uint64_t)(db_id))((kv_data_t)(start_key))(
                     (kv_data_t)(prefix))((hg_size_t)(max_keys))((hg_bulk_t)(
                     ksizes_bulk_handle))((hg_bulk_t)(keys_bulk_handle)))
MERCURY_GEN_PROC(list_keys_out_t, ((hg_size_t)(nkeys))((int32_t)(ret)))

// ------------- LIST KEYVALS ------------- //
MERCURY_GEN_PROC(
    list_keyvals_in_t,
    ((uint64_t)(db_id))((kv_data_t)(start_key))((kv_data_t)(prefix))(
        (hg_size_t)(max_keys))((hg_bulk_t)(ksizes_bulk_handle))(
        (hg_bulk_t)(keys_bulk_handle))((hg_bulk_t)(vsizes_bulk_handle))(
        (hg_bulk_t)(vals_bulk_handle)))
MERCURY_GEN_PROC(list_keyvals_out_t, ((hg_size_t)(nkeys))((int32_t)(ret)))

// ------------- BULK PUT ------------- //
MERCURY_GEN_PROC(bulk_put_in_t,
                 ((uint64_t)(db_id))((kv_data_t)(key))((hg_size_t)(vsize))(
                     (hg_bulk_t)(handle)))
MERCURY_GEN_PROC(bulk_put_out_t, ((int32_t)(ret)))

// ------------- BULK GET ------------- //
MERCURY_GEN_PROC(bulk_get_in_t,
                 ((uint64_t)(db_id))((kv_data_t)(key))((hg_size_t)(vsize))(
                     (hg_bulk_t)(handle)))
MERCURY_GEN_PROC(bulk_get_out_t, ((hg_size_t)(vsize))((int32_t)(ret)))

// ------------- PUT MULTI ------------- //
MERCURY_GEN_PROC(put_multi_in_t,
                 ((uint64_t)(db_id))((hg_size_t)(num_keys))((hg_bulk_t)(
                     keys_bulk_handle))((hg_size_t)(keys_bulk_size))((
                     hg_bulk_t)(vals_bulk_handle))((hg_size_t)(vals_bulk_size)))
MERCURY_GEN_PROC(put_multi_out_t, ((int32_t)(ret)))

// ------------- PUT PACKED ------------- //
MERCURY_GEN_PROC(
    put_packed_in_t,
    ((uint64_t)(db_id))((hg_string_t)(origin_addr))((hg_size_t)(num_keys))(
        (hg_size_t)(bulk_size))((hg_bulk_t)(bulk_handle)))
MERCURY_GEN_PROC(put_packed_out_t, ((int32_t)(ret)))

// ------------- GET MULTI ------------- //
MERCURY_GEN_PROC(get_multi_in_t,
                 ((uint64_t)(db_id))((hg_size_t)(num_keys))((hg_bulk_t)(
                     keys_bulk_handle))((hg_size_t)(keys_bulk_size))((
                     hg_bulk_t)(vals_bulk_handle))((hg_size_t)(vals_bulk_size)))
MERCURY_GEN_PROC(get_multi_out_t, ((int32_t)(ret)))

// ------------- GET PACKED ------------- //
MERCURY_GEN_PROC(get_packed_in_t,
                 ((uint64_t)(db_id))((hg_size_t)(num_keys))((hg_size_t)(
                     keys_bulk_size))((hg_bulk_t)(keys_bulk_handle))((
                     hg_size_t)(vals_bulk_size))((hg_bulk_t)(vals_bulk_handle)))
MERCURY_GEN_PROC(get_packed_out_t, ((int32_t)(ret))((hg_size_t)(num_keys)))

// ------------- LENGTH MULTI ------------- //
MERCURY_GEN_PROC(
    length_multi_in_t,
    ((uint64_t)(db_id))((hg_size_t)(num_keys))((hg_bulk_t)(keys_bulk_handle))(
        (hg_size_t)(keys_bulk_size))((hg_bulk_t)(vals_size_bulk_handle)))
MERCURY_GEN_PROC(length_multi_out_t, ((int32_t)(ret)))

// ------------- LENGTH PACKED ------------- //
MERCURY_GEN_PROC(
    length_packed_in_t,
    ((uint64_t)(db_id))((hg_size_t)(num_keys))((hg_size_t)(in_bulk_size))(
        (hg_bulk_t)(in_bulk_handle))((hg_bulk_t)(out_bulk_handle)))
MERCURY_GEN_PROC(length_packed_out_t, ((int32_t)(ret)))

// ------------- EXIST MULTI ------------- //
MERCURY_GEN_PROC(
    exists_multi_in_t,
    ((uint64_t)(db_id))((hg_size_t)(num_keys))((hg_bulk_t)(keys_bulk_handle))(
        (hg_size_t)(keys_bulk_size))((hg_bulk_t)(flags_bulk_handle)))
MERCURY_GEN_PROC(exists_multi_out_t, ((int32_t)(ret)))

// ------------- ERASE MULTI ------------- //
MERCURY_GEN_PROC(erase_multi_in_t,
                 ((uint64_t)(db_id))((hg_size_t)(num_keys))((hg_bulk_t)(
                     keys_bulk_handle))((hg_size_t)(keys_bulk_size)))
MERCURY_GEN_PROC(erase_multi_out_t, ((int32_t)(ret)))

// ------------- MIGRATE KEYS ----------- //
MERCURY_GEN_PROC(migrate_keys_in_t,
                 ((uint64_t)(source_db_id))((hg_string_t)(target_addr))(
                     (uint16_t)(target_provider_id))((uint64_t)(target_db_id))(
                     (hg_size_t)(num_keys))((hg_size_t)(bulk_size))(
                     (hg_bulk_t)(keys_bulk))((int32_t)(flag)))

MERCURY_GEN_PROC(migrate_keys_out_t, ((int32_t)(ret)))

// ------------- MIGRATE KEY RANGE ----------- //
MERCURY_GEN_PROC(migrate_key_range_in_t,
                 ((uint64_t)(source_db_id))((hg_string_t)(target_addr))(
                     (uint16_t)(target_provider_id))((uint64_t)(target_db_id))(
                     (kv_data_t)(key_lb))((kv_data_t)(key_ub))((int32_t)(flag)))

// ------------- MIGRATE KEY PREFIXED -------- //
MERCURY_GEN_PROC(migrate_keys_prefixed_in_t,
                 ((uint64_t)(source_db_id))((hg_string_t)(target_addr))(
                     (uint16_t)(target_provider_id))((uint64_t)(target_db_id))(
                     (kv_data_t)(key_prefix))((int32_t)(flag)))

// ------------- MIGRATE ALL KEYS ----------- //
MERCURY_GEN_PROC(migrate_all_keys_in_t,
                 ((uint64_t)(source_db_id))((hg_string_t)(target_addr))(
                     (uint16_t)(target_provider_id))((uint64_t)(target_db_id))(
                     (int32_t)(flag)))

// ------------- MIGRATE DATABASE ---------- //
MERCURY_GEN_PROC(migrate_database_in_t,
                 ((uint64_t)(source_db_id))((int32_t)(remove_src))(
                     (hg_const_string_t)(dest_remi_addr))((uint16_t)(
                     dest_remi_provider_id))((hg_const_string_t)(dest_root)))

MERCURY_GEN_PROC(migrate_database_out_t, ((int32_t)(ret))((int32_t)(remi_ret)))

#endif
