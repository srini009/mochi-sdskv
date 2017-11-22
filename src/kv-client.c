#include "sds-keyval.h"
#include <mercury.h>
#include <margo.h>
#include <abt-snoozer.h>
#include <abt.h>

#include <assert.h>

// pass in Margo instance ID
kv_context_t *kv_client_register(const margo_instance_id mid) {
  hg_return_t ret;
  kv_context_t *context;
  context = (kv_context_t*)malloc(sizeof(kv_context_t));
  memset(context, 0, sizeof(kv_context_t));

  context->mid = mid;
  
  context->put_id = MARGO_REGISTER(context->mid, "put",
				   put_in_t, put_out_t, NULL);

  context->bulk_put_id = MARGO_REGISTER(context->mid, "bulk_put",
					bulk_put_in_t, bulk_put_out_t, NULL);

  context->get_id = MARGO_REGISTER(context->mid, "get",
				   get_in_t, get_out_t, NULL);

  context->bulk_get_id = MARGO_REGISTER(context->mid, "bulk_get",
					bulk_get_in_t, bulk_get_out_t, NULL);

  context->open_id = MARGO_REGISTER(context->mid, "open",
				    open_in_t, open_out_t, NULL);

  context->close_id = MARGO_REGISTER(context->mid, "close",
				     void, close_out_t, NULL);

  context->bench_id = MARGO_REGISTER(context->mid, "bench",
				     bench_in_t, bench_out_t, NULL);

  context->shutdown_id = MARGO_REGISTER(context->mid, "shutdown",
					void, void, NULL);

  return context;
}

hg_return_t kv_open(kv_context_t *context, const char *server_addr, const char *db_name) {
  hg_return_t ret = HG_SUCCESS;
  hg_handle_t handle;
  open_in_t open_in;
  open_out_t open_out;

  printf("kv-client: kv_open, server_addr %s\n", server_addr);
  ret = margo_addr_lookup(context->mid, server_addr, &(context->svr_addr));
  assert(ret == HG_SUCCESS);

  ret = margo_create(context->mid, context->svr_addr,
		     context->open_id, &handle);
  assert(ret == HG_SUCCESS);

  open_in.name = (hg_string_t)db_name;
	
  ret = margo_forward(handle, &open_in);
  assert(ret == HG_SUCCESS);
  ret = margo_get_output(handle, &open_out);
  assert(ret == HG_SUCCESS);
  assert(open_out.ret == HG_SUCCESS);

  /* set up the other calls here: idea is we'll pay the registration cost
   * once here but can reuse the handles and identifiers multiple times */
  
  /* put/get handles: can have as many in flight as we have registered.
   * BAKE has a handle-caching mechanism we should consult.
   * should margo be caching handles? */
  
  ret = margo_create(context->mid, context->svr_addr,
		     context->put_id, &(context->put_handle));
  assert(ret == HG_SUCCESS);
  ret = margo_create(context->mid, context->svr_addr,
		     context->bulk_put_id, &(context->bulk_put_handle));
  assert(ret == HG_SUCCESS);
  ret = margo_create(context->mid, context->svr_addr,
		     context->get_id, &(context->get_handle));
  assert(ret == HG_SUCCESS);
  ret = margo_create(context->mid, context->svr_addr,
		     context->bulk_get_id, &(context->bulk_get_handle));
  assert(ret == HG_SUCCESS);
  ret = margo_create(context->mid, context->svr_addr,
		     context->shutdown_id, &(context->shutdown_handle));
  assert(ret == HG_SUCCESS);
	
  margo_free_output(handle, &open_out);
  margo_destroy(handle);
	
  return HG_SUCCESS;
}

/* we gave types in the open call.  Will need to maintain in 'context' the
 * size. */
hg_return_t kv_put(kv_context_t *context, 
		   void *key, hg_size_t ksize,
		   void *value, hg_size_t vsize) {
  hg_return_t ret;
  hg_size_t msize;
  
  msize = ksize + vsize + 2*sizeof(hg_size_t);

  printf("kv_put ksize %lu, vsize %lu, msize %lu\n", ksize, vsize, msize);
  /* 
   * If total payload is large, we'll do our own
   * explicit transfer of the value data.
   */
  double st1, et1, st2, et2;
  st1 = ABT_get_wtime();
  if (msize <= MAX_RPC_MESSAGE_SIZE) {
    put_in_t pin;
    put_out_t pout;

    pin.pi.key = (kv_data_t)key;
    pin.pi.ksize = ksize;
    pin.pi.value = (kv_data_t)value;
    pin.pi.vsize = vsize;

    st2 = ABT_get_wtime();
    ret = margo_forward(context->put_handle, &pin);
    et2 = ABT_get_wtime();
    printf("kv_put forward time: %f microseconds, vsize = %lu\n", (et2-st2)*1000000, vsize);
    assert(ret == HG_SUCCESS);

    ret = margo_get_output(context->put_handle, &pout);
    assert(ret == HG_SUCCESS);
    ret = pout.ret;

    margo_free_output(context->put_handle, &pout);
  }
  else {
    // use bulk transfer method to move value
    bulk_put_in_t bpin;
    bulk_put_out_t bpout;

    /*
     * If (ksize + sizeof(hg_size_t) is too large
     * we'll simply rely on HG to handle it rather
     * than do multiple bulk transfers. Most likely
     * key payload size is << value payload size
     */
    bpin.bulk.key = (kv_data_t)key;
    bpin.bulk.ksize = ksize;
    bpin.bulk.vsize = vsize;

    st2 = ABT_get_wtime();
    ret = margo_bulk_create(context->mid, 1, &value, &bpin.bulk.vsize,
			    HG_BULK_READ_ONLY, &bpin.bulk.handle);
    et2 = ABT_get_wtime();
    printf("kv_put bulk create time: %f microseconds\n", (et2-st2)*1000000);
    assert(ret == HG_SUCCESS);

    st2 = ABT_get_wtime();
    ret = margo_forward(context->bulk_put_handle, &bpin);
    et2 = ABT_get_wtime();
    printf("kv_put bulk forward time: %f microseconds, vsize = %lu\n", (et2-st2)*1000000, vsize);
    assert(ret == HG_SUCCESS);

    ret = margo_get_output(context->bulk_put_handle, &bpout);
    assert(ret == HG_SUCCESS);
    ret = bpout.ret; // make sure the server side says all is OK

    margo_free_output(context->bulk_put_handle, &bpout);
  }
  et1 = ABT_get_wtime();
  printf("kv_put time: %f microseconds\n", (et1-st1)*1000000);

    return ret;
}

// vsize is in/out
hg_return_t kv_get(kv_context_t *context, 
		   void *key, hg_size_t ksize,
		   void *value, hg_size_t *vsize)
{
  hg_return_t ret;
  hg_size_t size;
  hg_size_t msize;
  
  size = *(hg_size_t*)vsize;
  msize = size + sizeof(hg_size_t) + sizeof(hg_return_t);

  printf("kv_get ksize %lu, vsize %lu, msize %lu\n", ksize, size, msize);
  /* 
   * If return payload is large, we'll do our own
   * explicit transfer of the value data.
   */
  double st1, et1, st2, et2;
  st1 = ABT_get_wtime();
  if (msize <= MAX_RPC_MESSAGE_SIZE) {
    get_in_t gin;
    get_out_t gout;

    gin.gi.key = (kv_data_t)key;
    gin.gi.ksize = ksize;
    gin.gi.vsize = size;

    st2 = ABT_get_wtime();
    ret = margo_forward(context->get_handle, &gin);
    et2 = ABT_get_wtime();
    printf("kv_get forward time: %f microseconds, vsize = %lu\n", (et2-st2)*1000000, size);
    assert(ret == HG_SUCCESS);

    ret = margo_get_output(context->get_handle, &gout);
    assert(ret == HG_SUCCESS);
    ret = gout.go.ret;

    /*
     * Return size of data transferred. Note that
     * size may be zero if there was a problem 
     * with the transfer.
     */
    *vsize = (hg_size_t)gout.go.vsize;
    if (gout.go.vsize > 0) {
      memcpy(value, gout.go.value, gout.go.vsize);
    }

    margo_free_output(context->get_handle, &gout);
  }
  else {
    bulk_get_in_t bgin;
    bulk_get_out_t bgout;

    bgin.bulk.key = (kv_data_t)key;
    bgin.bulk.ksize = ksize;
    bgin.bulk.vsize = size;

    st2 = ABT_get_wtime();
    ret = margo_bulk_create(context->mid, 1, &value, &bgin.bulk.vsize,
			    HG_BULK_WRITE_ONLY, &bgin.bulk.handle);
    et2 = ABT_get_wtime();
    printf("kv_get bulk create time: %f microseconds\n", (et2-st2)*1000000);
    assert(ret == HG_SUCCESS);

    st2 = ABT_get_wtime();
    ret = margo_forward(context->bulk_get_handle, &bgin);
    et2 = ABT_get_wtime();
    printf("kv_get bulk forward time: %f microseconds, vsize = %lu\n", (et2-st2)*1000000, size);
    assert(ret == HG_SUCCESS);

    ret = margo_get_output(context->bulk_get_handle, &bgout);
    assert(ret == HG_SUCCESS);
    ret = bgout.ret; // make sure the server side says all is OK

    /*
     * Return size of data transferred. Note that
     * size may be zero if there was a problem 
     * with the transfer.
     */
    *vsize = (hg_size_t)bgout.size;

    margo_free_output(context->bulk_get_handle, &bgout);
  }
  et1 = ABT_get_wtime();
  printf("kv_get time: %f microseconds\n", (et1-st1)*1000000);

  return ret;
}

hg_return_t kv_close(kv_context_t *context)
{
    hg_return_t ret;
    hg_handle_t handle;
    close_out_t close_out;

    ret = margo_create(context->mid, context->svr_addr,
	    context->close_id, &handle);
    assert(ret == HG_SUCCESS);

    ret = margo_forward(handle, NULL);
    assert(ret == HG_SUCCESS);

    ret = margo_get_output(handle, &close_out);
    assert(ret == HG_SUCCESS);
    assert(close_out.ret == HG_SUCCESS);

    margo_free_output(handle, &close_out);
    margo_destroy(handle);

    return HG_SUCCESS;
}

bench_result_t *kv_benchmark(kv_context_t *context, int32_t count) {
  hg_return_t ret;
  hg_handle_t handle;
  bench_in_t bench_in;
  bench_out_t bench_out;
  bench_result_t *result = NULL;

    ret = margo_create(context->mid, context->svr_addr,
	    context->bench_id, &handle);
    assert(ret == HG_SUCCESS);

    ret = margo_forward(handle, &bench_in);
    assert(ret == HG_SUCCESS);

  ret = margo_get_output(handle, &bench_out);
  assert(ret == HG_SUCCESS);
    
  result = malloc(sizeof(bench_result_t));
  result->nkeys = bench_out.result.nkeys;
  result->insert_time = bench_out.result.insert_time;
  result->read_time = bench_out.result.read_time;
  result->overhead = bench_out.result.overhead;

  margo_free_output(handle, &bench_out);
  margo_destroy(handle);

    return result;
}

hg_return_t kv_client_deregister(kv_context_t *context) {
  hg_return_t ret;

  margo_destroy(context->put_handle);
  margo_destroy(context->get_handle);
  margo_destroy(context->bulk_put_handle);
  margo_destroy(context->bulk_get_handle);
  margo_destroy(context->shutdown_handle);

  ret = margo_addr_free(context->mid, context->svr_addr);
  assert(ret == HG_SUCCESS);

  free(context);

  return HG_SUCCESS;
}

// only one client calls shutdown
hg_return_t kv_client_signal_shutdown(kv_context_t *context) {
  hg_return_t ret;

  ret = margo_forward(context->shutdown_handle, NULL);
  assert(ret == HG_SUCCESS);

  return HG_SUCCESS;
}
