#include <stdint.h>
#include <assert.h>

#include <mercury.h>
#include <mercury_macros.h>
#include <mercury_proc_string.h>

#include <margo.h>
#include <abt.h>
#include <abt-snoozer.h>

#ifndef sds_keyval_h
#define sds_keyval_h

// uncomment to re-enable print statements
//#define KV_DEBUG

#if defined(__cplusplus)
extern "C" {
#endif

typedef int kv_id;

/* do we need one for server, one for client? */
typedef struct kv_context_s {
	margo_instance_id mid;
	hg_addr_t svr_addr;
	hg_id_t put_id;
	hg_id_t bulk_put_id;
	hg_id_t get_id;
	hg_id_t bulk_get_id;
	hg_id_t open_id;
	hg_id_t close_id;
	hg_id_t bench_id;
	hg_id_t shutdown_id;
	hg_handle_t put_handle;
        hg_handle_t bulk_put_handle;
	hg_handle_t get_handle;
	hg_handle_t bulk_get_handle;
	hg_handle_t shutdown_handle;
	kv_id kv;
} kv_context_t;


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


// some setup to support simple benchmarking
typedef struct {
  hg_size_t nkeys;
  double insert_time;
  double read_time;
  double overhead;
} bench_result_t;

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


// kv-client API
kv_context_t *kv_client_register(const margo_instance_id mid);
kv_context_t *kv_server_register(const margo_instance_id mid);

// both the same: should probably move to common?
hg_return_t kv_client_deregister(kv_context_t *context);
hg_return_t kv_server_deregister(kv_context_t *context);

// server-side routine
hg_return_t kv_server_wait_for_shutdown(kv_context_t *context);

// client-side routines wrapping up all the RPC stuff
hg_return_t kv_client_signal_shutdown(kv_context_t *context);
hg_return_t kv_open(kv_context_t *context, const char *server, const char *db_name);
hg_return_t kv_put(kv_context_t *context, void *key, hg_size_t ksize, void *value, hg_size_t vsize);
hg_return_t kv_get(kv_context_t *context, void *key, hg_size_t ksize, void *value, hg_size_t *vsize);
hg_return_t kv_close(kv_context_t *context);

// benchmark routine
bench_result_t *kv_benchmark(kv_context_t *context, int32_t count);

static inline char *kv_protocol(const char *addr_str) {
  int psize = 24;
  
  char *protocol = (char*)malloc(psize);
  memset(protocol, 0, psize);

  /* we only need to the proto portion of the address to initialize */
  for(int i=0; i<psize && addr_str[i] != '\0' && addr_str[i] != ':'; i++)
    protocol[i] = addr_str[i];

  return protocol;
}

// pass in address string and mode
// string can be just the protocol (e.g. "ofi+tcp")
// mode can be MARGO_CLIENT_MODE or MARGO_SERVER_MODE
static inline margo_instance_id kv_margo_init(const char *addr_str, int mode)
{
  margo_instance_id mid;
  assert(addr_str != NULL); // better pass something!
  assert(mode == MARGO_CLIENT_MODE || mode == MARGO_SERVER_MODE);
  mid = margo_init(addr_str, mode, 0, -1);
  assert(mid != MARGO_INSTANCE_NULL);
  margo_diag_start(mid); // initialize diagnostic support
  return mid;
}

static inline void kv_margo_finalize(margo_instance_id mid)
{
  margo_finalize(mid);
}

#if defined(__cplusplus)
}
#endif

#endif // sds_keyval_h
