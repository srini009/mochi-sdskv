#ifndef sds_keyval_h
#define sds_keyval_h

#if defined(__cplusplus)
extern "C" {
#endif
#include <margo.h>
#include <stdint.h>

// some setup to support simple benchmarking
typedef struct {
  int64_t nkeys;
  double insert_time;
  double read_time;
  double overhead;
} bench_result_t;

// helper routine for stripping protocol part of address string
// e.g. given "ofi+sockets://192.168.101.20:36259" returns "ofi+sockets"
// clients can use to dynamically match server's protocol
// caller is responsible for freeing up char buffer
static inline char *kv_protocol(const char *addr_str) {
  int psize = 24;
  int i;
  
  char *protocol = (char*)malloc(psize);
  memset(protocol, 0, psize);

  /* we only need to the proto portion of the address to initialize */
  for(i=0; i<psize && addr_str[i] != '\0' && addr_str[i] != ':'; i++)
    protocol[i] = addr_str[i];

  return protocol;
}

typedef struct kv_context_s kv_context_t;
typedef struct kv_database_s kv_database_t;

// kv-client API
kv_context_t *kv_client_register(const margo_instance_id mid);
kv_context_t *kv_server_register(const margo_instance_id mid);

// both the same: should probably move to common?
hg_return_t kv_client_deregister(kv_context_t *context);
hg_return_t kv_server_deregister(kv_context_t *context);

// server-side routine
// server doesn't have a "database" -- that's a client notion
hg_return_t kv_server_wait_for_shutdown(kv_context_t *context);

// client-side routines wrapping up all the RPC stuff
hg_return_t kv_client_signal_shutdown(kv_database_t *db);

kv_database_t * kv_open(kv_context_t *context,
	const char *server, const char *db_name);
hg_return_t kv_put(kv_database_t *db, void *key, hg_size_t ksize,
	void *value, hg_size_t vsize);
hg_return_t kv_get(kv_database_t *db, void *key, hg_size_t ksize,
	void *value, hg_size_t *vsize);
hg_return_t kv_close(kv_database_t * db);

hg_return_t kv_list_keys(kv_database_t *db,     // db instance
         const void *start_key,  // we want keys strictly after this start_key
         hg_size_t start_ksize,  // size of the start_key
         void **keys,            // pointer to an array of void* pointers,
                                 //     this array has size *max_keys
         hg_size_t* ksizes,      // pointer to an array of hg_size_t sizes
                                 //    representing sizes allocated in
                                 //     keys for each key
         hg_size_t* max_keys);   // maximum number of keys requested

hg_return_t kv_list_keys_with_prefix(kv_database_t *db,
         const void *start_key,  // we want keys strictly after this start_key
         hg_size_t start_ksize,  // size of the start_key
	 const void *prefix,     // return only keys that begin with 'prefix'
	 hg_size_t prefix_size,
         void **keys,            // pointer to an array of void* pointers,
                                 // this array has size *max_keys
         hg_size_t* ksizes,      // pointer to an array of hg_size_t sizes 
                                 // representing sizes allocated in
                                 // keys for each key
         hg_size_t* max_keys);   // maximum number of keys requested

// benchmark routine
bench_result_t *kv_benchmark(kv_database_t *context, int32_t count);

#if defined(__cplusplus)
}
#endif

#endif // sds_keyval_h
