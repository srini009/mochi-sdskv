#include <stdint.h>
#include <assert.h>

#include <mercury.h>
#include <mercury_macros.h>
#include <mercury_proc_string.h>

#include <margo.h>
#include <abt.h>
#include <abt-snoozer.h>

#include "sds-keyval.h"

#ifndef keyval_internal_h
#define keyval_internal_h

// uncomment to re-enable print statements
//#define KV_DEBUG

#if defined(__cplusplus)
extern "C" {
#endif

typedef int kv_id;

/* 'Context' describes operations available to keyval clients */
/* do we need one for server, one for client? */
typedef struct kv_context_s {
	margo_instance_id mid;
	hg_id_t put_id;
	hg_id_t bulk_put_id;
	hg_id_t get_id;
	hg_id_t bulk_get_id;
	hg_id_t open_id;
	hg_id_t close_id;
	hg_id_t bench_id;
	hg_id_t shutdown_id;
	hg_id_t list_id;
	kv_id kv;
} kv_context_t;

/* 'Database' contains server-specific information: the instantiation of a
 * particular keyval service; the handles used to send information back and
 * forth */
typedef struct kv_database_s  {
	margo_instance_id mid;   /* bulk xfer needs to create bulk handles */
	hg_addr_t svr_addr;
	hg_handle_t close_handle;
    	hg_handle_t put_handle;
        hg_handle_t bulk_put_handle;
	hg_handle_t get_handle;
	hg_handle_t bulk_get_handle;
	hg_handle_t shutdown_handle;
	hg_handle_t bench_handle;
	hg_handle_t list_handle;
} kv_database_t;


#define MAX_RPC_MESSAGE_SIZE 4000 // in bytes

// setup to support opaque type handling
typedef char* kv_data_t;

typedef struct {
  kv_data_t key;
  hg_size_t ksize;
  kv_data_t value;
  hg_size_t vsize;
} kv_put_in_t;

typedef struct {
  kv_data_t key;
  hg_size_t ksize;
  hg_size_t vsize;
} kv_get_in_t;

typedef struct {
  kv_data_t value;
  hg_size_t vsize;
  hg_return_t ret;
} kv_get_out_t;

static inline hg_return_t hg_proc_hg_return_t(hg_proc_t proc, void *data)
{
  return hg_proc_hg_int32_t(proc, data);
}

static inline hg_return_t hg_proc_kv_put_in_t(hg_proc_t proc, void *data)
{
  hg_return_t ret;
  kv_put_in_t *in = (kv_put_in_t*)data;

  ret = hg_proc_hg_size_t(proc, &in->ksize);
  assert(ret == HG_SUCCESS);
  if (in->ksize) {
    switch (hg_proc_get_op(proc)) {
    case HG_ENCODE:
      ret = hg_proc_raw(proc, in->key, in->ksize);
      assert(ret == HG_SUCCESS);
      break;
    case HG_DECODE:
      in->key = (kv_data_t)malloc(in->ksize);
      ret = hg_proc_raw(proc, in->key, in->ksize);
      assert(ret == HG_SUCCESS);
      break;
    case HG_FREE:
      free(in->key);
      break;
    default:
      break;
    }
  }
  ret = hg_proc_hg_size_t(proc, &in->vsize);
  assert(ret == HG_SUCCESS);
  if (in->vsize) {
    switch (hg_proc_get_op(proc)) {
    case HG_ENCODE:
      ret = hg_proc_raw(proc, in->value, in->vsize);
      assert(ret == HG_SUCCESS);
      break;
    case HG_DECODE:
      in->value = (kv_data_t)malloc(in->vsize);
      ret = hg_proc_raw(proc, in->value, in->vsize);
      assert(ret == HG_SUCCESS);
      break;
    case HG_FREE:
      free(in->value);
      break;
    default:
      break;
    }
  }

  return HG_SUCCESS;
}

static inline hg_return_t hg_proc_kv_get_in_t(hg_proc_t proc, void *data)
{
  hg_return_t ret;
  kv_get_in_t *in = (kv_get_in_t*)data;

  ret = hg_proc_hg_size_t(proc, &in->ksize);
  assert(ret == HG_SUCCESS);
  if (in->ksize) {
    switch (hg_proc_get_op(proc)) {
    case HG_ENCODE:
      ret = hg_proc_raw(proc, in->key, in->ksize);
      assert(ret == HG_SUCCESS);
      break;
    case HG_DECODE:
      in->key = (kv_data_t)malloc(in->ksize);
      ret = hg_proc_raw(proc, in->key, in->ksize);
      assert(ret == HG_SUCCESS);
      break;
    case HG_FREE:
      free(in->key);
      break;
    default:
      break;
    }
  }
  ret = hg_proc_hg_size_t(proc, &in->vsize);
  assert(ret == HG_SUCCESS);

  return HG_SUCCESS;
}

static inline hg_return_t hg_proc_kv_get_out_t(hg_proc_t proc, void *data)
{
  hg_return_t ret;
  kv_get_out_t *out = (kv_get_out_t*)data;

  ret = hg_proc_hg_size_t(proc, &out->vsize);
  assert(ret == HG_SUCCESS);
  if (out->vsize) {
    switch (hg_proc_get_op(proc)) {
    case HG_ENCODE:
      ret = hg_proc_raw(proc, out->value, out->vsize);
      assert(ret == HG_SUCCESS);
      break;
    case HG_DECODE:
      out->value = (kv_data_t)malloc(out->vsize);
      ret = hg_proc_raw(proc, out->value, out->vsize);
      assert(ret == HG_SUCCESS);
      break;
    case HG_FREE:
      free(out->value);
      break;
    default:
      break;
    }
  }
  ret = hg_proc_hg_return_t(proc, &out->ret);
  assert(ret == HG_SUCCESS);

  return HG_SUCCESS;
}

MERCURY_GEN_PROC(put_in_t, ((kv_put_in_t)(pi)))
MERCURY_GEN_PROC(put_out_t, ((hg_return_t)(ret)))
DECLARE_MARGO_RPC_HANDLER(put_handler)

MERCURY_GEN_PROC(get_in_t, ((kv_get_in_t)(gi)))
MERCURY_GEN_PROC(get_out_t, ((kv_get_out_t)(go)))
DECLARE_MARGO_RPC_HANDLER(get_handler)

MERCURY_GEN_PROC(open_in_t, ((hg_string_t)(name)))
MERCURY_GEN_PROC(open_out_t, ((hg_return_t)(ret)))
DECLARE_MARGO_RPC_HANDLER(open_handler)

MERCURY_GEN_PROC(close_out_t, ((hg_return_t)(ret)))
DECLARE_MARGO_RPC_HANDLER(close_handler)


// for handling bulk puts/gets (e.g. for ParSplice use case)
typedef struct {
  kv_data_t key;
  hg_size_t ksize;
  hg_size_t vsize;
  hg_bulk_t handle;
} kv_bulk_t;

static inline hg_return_t hg_proc_kv_bulk_t(hg_proc_t proc, void *data)
{
  hg_return_t ret;
  kv_bulk_t *bulk = (kv_bulk_t*)data;

  ret = hg_proc_hg_size_t(proc, &bulk->ksize);
  assert(ret == HG_SUCCESS);
  if (bulk->ksize) {
    switch (hg_proc_get_op(proc)) {
    case HG_ENCODE:
      ret = hg_proc_raw(proc, bulk->key, bulk->ksize);
      assert(ret == HG_SUCCESS);
      break;
    case HG_DECODE:
      bulk->key = (kv_data_t)malloc(bulk->ksize);
      ret = hg_proc_raw(proc, bulk->key, bulk->ksize);
      assert(ret == HG_SUCCESS);
      break;
    case HG_FREE:
      free(bulk->key);
      break;
    default:
      break;
    }
  }
  ret = hg_proc_hg_size_t(proc, &bulk->vsize);
  assert(ret == HG_SUCCESS);
  ret = hg_proc_hg_bulk_t(proc, &bulk->handle);
  assert(ret == HG_SUCCESS);

  return HG_SUCCESS;
}

MERCURY_GEN_PROC(bulk_put_in_t, ((kv_bulk_t)(bulk)))
MERCURY_GEN_PROC(bulk_put_out_t, ((hg_return_t)(ret)))
DECLARE_MARGO_RPC_HANDLER(bulk_put_handler)

MERCURY_GEN_PROC(bulk_get_in_t, ((kv_bulk_t)(bulk)))
MERCURY_GEN_PROC(bulk_get_out_t, ((hg_size_t)(size)) ((hg_return_t)(ret)))
DECLARE_MARGO_RPC_HANDLER(bulk_get_handler)

DECLARE_MARGO_RPC_HANDLER(shutdown_handler)

MERCURY_GEN_PROC(list_in_t, ((hg_size_t)(start))
	    ((hg_size_t) (max_keys)) )
MERCURY_GEN_PROC(list_out_t, ((hg_size_t)(nkeys))
	    ((hg_bulk_t)(bulk_keys))
	    ((hg_bulk_t)(bulk_sizes)) )
DECLARE_MARGO_RPC_HANDLER(list_keys_handler)

// some setup to support simple benchmarking
static inline hg_return_t hg_proc_double(hg_proc_t proc, void *data)
{
  hg_return_t ret;
  hg_size_t size = sizeof(double);

  ret = hg_proc_raw(proc, data, size);
  assert(ret == HG_SUCCESS);

  return HG_SUCCESS;
}

static inline hg_return_t hg_proc_bench_result_t(hg_proc_t proc, void *data)
{
  hg_return_t ret;
  bench_result_t *in = (bench_result_t*)data;

  ret = hg_proc_hg_size_t(proc, &in->nkeys);
  assert(ret == HG_SUCCESS);
  ret = hg_proc_double(proc, &in->insert_time);
  assert(ret == HG_SUCCESS);
  ret = hg_proc_double(proc, &in->read_time);
  assert(ret == HG_SUCCESS);
  ret = hg_proc_double(proc, &in->overhead);
  assert(ret == HG_SUCCESS);

  return HG_SUCCESS;
}

MERCURY_GEN_PROC(bench_in_t, ((int32_t)(count)))
MERCURY_GEN_PROC(bench_out_t, ((bench_result_t)(result)))
DECLARE_MARGO_RPC_HANDLER(bench_handler)


#if defined(__cplusplus)
}
#endif

#endif
