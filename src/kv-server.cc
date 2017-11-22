#include "sds-keyval.h"
#include "datastore.h"

#include <mercury.h>
#include <margo.h>
#include <abt-snoozer.h>
#include <abt.h>
#include <ssg.h>

//#include <random>
#include <stdlib.h>
#include <time.h>
#include <iostream>
#include <assert.h>

// since this is global, we're assuming this server instance will manage a single DB
AbstractDataStore *datastore = NULL;
std::string db_name;

static hg_return_t open_handler(hg_handle_t handle)
{
  hg_return_t ret;
  open_in_t in;
  open_out_t out;

  ret = margo_get_input(handle, &in);
  std::string in_name(in.name);
#ifdef KV_DEBUG
  std::cout << "SERVER: OPEN " << in_name << std::endl;
#endif

  if (!datastore) {
#if BWTREE
    datastore = new BwTreeDataStore(); // testing BwTree
#elif BERKELEYDB
    datastore = new BerkeleyDBDataStore(); // testing BerkeleyDB
    // in-memory implementation not working, needs debugging
    //datastore->set_in_memory(true); // testing in-memory BerkeleyDB
#elif LEVELDB
    datastore = new LevelDBDataStore(); // testing LevelDB
#endif
    db_name = in_name;
    datastore->createDatabase(db_name);
#ifdef KV_DEBUG
    std::cout << "SERVER OPEN: DataStore initialized and ready for " << db_name << std::endl;
#endif
    out.ret = HG_SUCCESS;
  }
  else {
    if (db_name == in_name) {
#ifdef KV_DEBUG
      std::cout << "SERVER OPEN: DataStore initialized and ready for " << db_name << std::endl;
#endif
      out.ret = HG_SUCCESS;
    }
    else {
#ifdef KV_DEBUG
      std::cout << "SERVER OPEN failed: currently managing " << db_name
		<< " and unable to process OPEN request for " << in_name << std::endl;
#endif
      out.ret = HG_OTHER_ERROR;
    }
  }

    ret = margo_respond(handle, &out);
    assert(ret == HG_SUCCESS);

    margo_free_input(handle, &in);
    margo_destroy(handle);

    return HG_SUCCESS;
}
DEFINE_MARGO_RPC_HANDLER(open_handler)

static hg_return_t close_handler(hg_handle_t handle)
{
  hg_return_t ret;
  close_out_t out;

  out.ret = HG_SUCCESS;
  
  ret = margo_respond(handle, &out);
  assert(ret == HG_SUCCESS);

  margo_destroy(handle);

  return HG_SUCCESS;
}
DEFINE_MARGO_RPC_HANDLER(close_handler)

static hg_return_t put_handler(hg_handle_t handle)
{
  hg_return_t ret;
  put_in_t pin;
  put_out_t pout;
  double st1, et1;

  st1 = ABT_get_wtime();
  ret = margo_get_input(handle, &pin);
  assert(ret == HG_SUCCESS);
	
#ifdef KV_DEBUG
  std::cout << "put_handler processing key with key size " << pin.pi.ksize << std::endl;
#endif
  ds_bulk_t kdata;
  kdata.resize(pin.pi.ksize);
  memcpy(kdata.data(), pin.pi.key, pin.pi.ksize);
#ifdef KV_DEBUG
  std::cout << "put_handler processing value with value size " << pin.pi.vsize << std::endl;
#endif
  ds_bulk_t vdata;
  vdata.resize(pin.pi.vsize);
  memcpy(vdata.data(), pin.pi.value, pin.pi.vsize);

#ifdef KV_DEBUG
  std::cout << "put_handler calling datastore->put()" << std::endl;
#endif
  if (datastore->put(kdata, vdata)) {
    pout.ret = HG_SUCCESS;
  }
  else {
    pout.ret = HG_OTHER_ERROR;
  }

  et1 = ABT_get_wtime();
  std::cout << "put_handler time: " <<  (et1-st1)*1000000 << " microseconds" << std::endl;

  ret = margo_respond(handle, &pout);
  assert(ret == HG_SUCCESS);

  margo_free_input(handle, &pin);
  margo_destroy(handle);

  return HG_SUCCESS;
}
DEFINE_MARGO_RPC_HANDLER(put_handler)

static hg_return_t bulk_put_handler(hg_handle_t handle)
{
  hg_return_t ret;
  bulk_put_in_t bpin;
  bulk_put_out_t bpout;
  hg_bulk_t bulk_handle;
  const struct hg_info *hgi;
  margo_instance_id mid;
  double st1, et1;

  st1 = ABT_get_wtime();
  ret = margo_get_input(handle, &bpin);
  assert(ret == HG_SUCCESS);

  /* get handle info and margo instance */
  hgi = margo_get_info(handle);
  assert(hgi);
  mid = margo_hg_info_get_instance(hgi);
  assert(mid != MARGO_INSTANCE_NULL);

#ifdef KV_DEBUG
  std::cout << "bulk_put_handler processing key with key size " << bpin.bulk.ksize << std::endl;
#endif
  ds_bulk_t kdata;
  kdata.resize(bpin.bulk.ksize);
  memcpy(kdata.data(), bpin.bulk.key, bpin.bulk.ksize);
#ifdef KV_DEBUG
  std::cout << "bulk_put_handler processing value with value size " << bpin.bulk.vsize << std::endl;
#endif
  ds_bulk_t vdata;
  vdata.resize(bpin.bulk.vsize);
  void *buffer = (void*)vdata.data();
  ret = margo_bulk_create(mid, 1, (void**)&buffer, &bpin.bulk.vsize, 
			  HG_BULK_WRITE_ONLY, &bulk_handle);
  assert(ret == HG_SUCCESS);
  ret = margo_bulk_transfer(mid, HG_BULK_PULL, hgi->addr, bpin.bulk.handle, 0,
			    bulk_handle, 0, bpin.bulk.vsize);
  assert(ret == HG_SUCCESS);

#ifdef KV_DEBUG
  std::cout << "bulk_put_handler calling datastore->put()" << std::endl;
#endif
  if (datastore->put(kdata, vdata)) {
    bpout.ret = HG_SUCCESS;
  }
  else {
    // e.g. put returns false if the key-value pair already
    // exists in the DB and duplicates are not allowed or ignored
    bpout.ret = HG_OTHER_ERROR;
  }

#ifdef KV_DEBUG
  std::cout << "bulk_put_handler sending response back with ret=" << bpout.ret << std::endl;
#endif
  et1 = ABT_get_wtime();
  std::cout << "bulk_put_handler time: " <<  (et1-st1)*1000000 << " microseconds" << std::endl;

  ret = margo_respond(handle, &bpout);
  assert(ret == HG_SUCCESS);

#ifdef KV_DEBUG
  std::cout << "bulk_put_handler performing cleanup" << std::endl;
#endif
  margo_free_input(handle, &bpin);
  margo_bulk_free(bulk_handle);
  margo_destroy(handle);
	
  return HG_SUCCESS;
}
DEFINE_MARGO_RPC_HANDLER(bulk_put_handler)

static hg_return_t get_handler(hg_handle_t handle)
{
  hg_return_t ret;
  get_in_t gin;
  get_out_t gout;
  double st1, et1;

  st1 = ABT_get_wtime();
  ret = margo_get_input(handle, &gin);
  assert(ret == HG_SUCCESS);

#ifdef KV_DEBUG
  std::cout << "get_handler processing key with key size " << gin.gi.ksize << std::endl;
  std::cout << "get_handler receive buffer size " << gin.gi.vsize << std::endl;
#endif
  ds_bulk_t kdata;
  kdata.resize(gin.gi.ksize);
  memcpy(kdata.data(), gin.gi.key, gin.gi.ksize);
  ds_bulk_t vdata;
  if (datastore->get(kdata, vdata)) {
    gout.go.vsize = vdata.size();
#ifdef KV_DEBUG
    std::cout << "get_handler datastore returned value with size " << gout.go.vsize << std::endl;
#endif
    if (gout.go.vsize <= gin.gi.vsize) {
      gout.go.value = (kv_data_t)malloc(gout.go.vsize);
      memcpy(gout.go.value, vdata.data(), gout.go.vsize);
      gout.go.ret = HG_SUCCESS;
    }
    else {
      gout.go.vsize = 0; // not returning a value
      gout.go.ret = HG_SIZE_ERROR; // caller should be checking return value
    }
  }
  else {
    // get on key did not succeed
    // rethink the use of HG_OTHER_ERROR here
    // maybe define a kv_return_t that is a superset of hg_return_t?
#ifdef KV_DEBUG
    std::cout << "get_handler datastore did not return a value" << std::endl;
#endif
    gout.go.vsize = 0; // not returning a value
    gout.go.ret = HG_OTHER_ERROR; // caller should be checking return value
  }

  et1 = ABT_get_wtime();
  std::cout << "get_handler time: " <<  (et1-st1)*1000000 << " microseconds" << std::endl;

  ret = margo_respond(handle, &gout);
  assert(ret == HG_SUCCESS);

  margo_free_input(handle, &gin);
  margo_destroy(handle);

  return HG_SUCCESS;
}
DEFINE_MARGO_RPC_HANDLER(get_handler)

static hg_return_t bulk_get_handler(hg_handle_t handle)
{
  hg_return_t ret;
  bulk_get_in_t bgin;
  bulk_get_out_t bgout;
  hg_bulk_t bulk_handle;
  const struct hg_info *hgi;
  margo_instance_id mid;
  double st1, et1;

  st1 = ABT_get_wtime();
  ret = margo_get_input(handle, &bgin);
  assert(ret == HG_SUCCESS);

#ifdef KV_DEBUG
  std::cout << "bulk_get_handler processing key with key size " << bgin.bulk.ksize << std::endl;
  std::cout << "bulk_get_handler receive buffer size " << bgin.bulk.vsize << std::endl;
#endif
  ds_bulk_t kdata;
  kdata.resize(bgin.bulk.ksize);
  memcpy(kdata.data(), bgin.bulk.key, bgin.bulk.ksize);
  ds_bulk_t vdata;
  if (datastore->get(kdata, vdata)) {
    // will the transfer fit on the client side?
    bgout.size = vdata.size();
#ifdef KV_DEBUG
    std::cout << "bulk_get_handler datastore returned value with size " << bgout.size << std::endl;
#endif
    if (bgout.size <= bgin.bulk.vsize) {
      /* get handle info and margo instance */
      hgi = margo_get_info(handle);
      assert(hgi);
      mid = margo_hg_info_get_instance(hgi);
      assert(mid != MARGO_INSTANCE_NULL);

#ifdef KV_DEBUG
      std::cout << "bulk_get_handler tranferring data with size " << bgout.size << std::endl;
#endif
      void *buffer = (void*)vdata.data();
      ret = margo_bulk_create(mid, 1, (void**)&buffer, &bgout.size, 
			      HG_BULK_READ_ONLY, &bulk_handle);
      assert(ret == HG_SUCCESS);
      ret = margo_bulk_transfer(mid, HG_BULK_PUSH, hgi->addr, bgin.bulk.handle, 0,
				bulk_handle, 0, bgout.size);
      assert(ret == HG_SUCCESS);

      bgout.ret = HG_SUCCESS;
    }
    else {
      bgout.size = 0; // not returning a value
      bgout.ret = HG_SIZE_ERROR; // assuming caller will check return code
    }
  }
  else {
    // get on key did not find a value (return 0 for size)
    bgout.size = 0; // not returning a value
    bgout.ret = HG_OTHER_ERROR; // assuming caller will check return code
  }
	
#ifdef KV_DEBUG
  std::cout << "bulk_get_handler returning ret = " << bgout.ret
	    << ", anad size = " << bgout.size << std::endl;
#endif
  et1 = ABT_get_wtime();
  std::cout << "bulk_get_handler time: " <<  (et1-st1)*1000000 << " microseconds" << std::endl;

  ret = margo_respond(handle, &bgout);
  assert(ret == HG_SUCCESS);

  margo_free_input(handle, &bgin);
  margo_bulk_free(bulk_handle);
  margo_destroy(handle);
	
  return HG_SUCCESS;
}
DEFINE_MARGO_RPC_HANDLER(bulk_get_handler)

static void shutdown_handler(hg_handle_t handle)
{
  hg_return_t ret;
  margo_instance_id mid;

  std::cout << "SERVER: got RPC request to shutdown" << std::endl;

  /* get handle info and margo instance */
  mid = margo_hg_handle_get_instance(handle);
  assert(mid != MARGO_INSTANCE_NULL);

  ret = margo_respond(handle, NULL);
  assert(ret == HG_SUCCESS);

  margo_destroy(handle);

  /* NOTE: We assume that the server daemon is using
   * margo_wait_for_finalize() to suspend until this
   * RPC executes, so there is no need to send any 
   * extra signal to notify it.
   */
  ssg_finalize(); // ignore return and should be a no-op?
  margo_finalize(mid);

  std::cout << "SERVER: margo finalized" << std::endl;
}
DEFINE_MARGO_RPC_HANDLER(shutdown_handler)

#if BWTREE
/*
 * from BwTree tests:
 * RandomInsertSpeedTest() - Tests how fast it is to insert keys randomly
 */

static void RandomInsertSpeedTest(int32_t key_num, bench_result_t *results)
{
  //std::random_device r{};
  //std::default_random_engine e1(r());
  //std::uniform_int_distribution<int32_t> uniform_dist(0, key_num - 1);

  srand(time(NULL)); // initialize random seed

  BwTree<int32_t, int32_t> *t = new BwTree<int32_t, int32_t>();
  t->SetDebugLogging(0);
  t->UpdateThreadLocal(1);
  t->AssignGCID(0);

  std::chrono::time_point<std::chrono::system_clock> start, end;

  // We loop for keynum * 2 because in average half of the insertion
  // will hit an empty slot
  start = std::chrono::system_clock::now();
  for(int32_t i = 0;i < key_num * 2;i++) {
    //int32_t key = uniform_dist(e1);
    int32_t key = rand() % key_num;

    t->Insert(key, key);
  }
  end = std::chrono::system_clock::now();

  std::chrono::duration<double> elapsed_seconds = end - start;

  results->nkeys = (hg_size_t)key_num;
  results->insert_time = elapsed_seconds.count();

  std::cout << "BwTree: at least " << (key_num * 2.0 / (1000 * 1000)) / elapsed_seconds.count()
	    << " million random insertion/sec" << "\n";

  // Then test random read after random insert
  std::vector<int32_t> v{};
  v.reserve(1);

  start = std::chrono::system_clock::now();
  for(int32_t i = 0;i < key_num * 2;i++) {
    //int32_t key = uniform_dist(e1);
    int32_t key = rand() % key_num;

    t->GetValue(key, v);
    v.clear();
  }
  end = std::chrono::system_clock::now();

  elapsed_seconds = end - start;
  results->read_time = elapsed_seconds.count();

  std::cout << "BwTree: at least " << (key_num * 2.0 / (1000 * 1000)) / elapsed_seconds.count()
            << " million random read after random insert/sec" << "\n";

  // Measure the overhead

  start = std::chrono::system_clock::now();
  for(int32_t i = 0;i < key_num * 2;i++) {
    //int32_t key = uniform_dist(e1);
    int32_t key = rand() % key_num;

    v.push_back(key);
    v.clear();
  }
  end = std::chrono::system_clock::now();

  std::chrono::duration<double> overhead = end - start;

  std::cout << "    Overhead = " << overhead.count() << " seconds" << std::endl;
  results->overhead = overhead.count();

  delete t;

  return;
}
#else
static void RandomInsertSpeedTest(int32_t key_num, bench_result_t *results)
{
  std::cout << "BwTree not enabled, no results" << std::endl;
  results->nkeys = 0;
  results->insert_time = 0.0;
  results->read_time = 0.0;
  results->overhead = 0.0;
}
#endif

static hg_return_t bench_handler(hg_handle_t handle)
{
  hg_return_t ret = HG_SUCCESS;
  bench_in_t bench_in;
  bench_out_t bench_out;
  bench_result_t random_insert;

  ret = margo_get_input(handle, &bench_in);
  assert(ret == HG_SUCCESS);

  printf("benchmarking %d keys\n", bench_in.count);
  RandomInsertSpeedTest(bench_in.count, &random_insert);

  bench_out.result.nkeys = random_insert.nkeys*2;
  bench_out.result.insert_time = random_insert.insert_time;
  bench_out.result.read_time = random_insert.read_time;
  bench_out.result.overhead = random_insert.overhead;

  ret = margo_respond(handle, &bench_out);
  assert(ret == HG_SUCCESS);

  margo_free_input(handle, &bench_in);
  margo_destroy(handle);

  return HG_SUCCESS;
}
DEFINE_MARGO_RPC_HANDLER(bench_handler)

kv_context_t *kv_server_register(const margo_instance_id mid)
{
  hg_return_t ret;
  hg_addr_t addr_self;
  char addr_self_string[128];
  hg_size_t addr_self_string_sz = 128;
  kv_context_t *context = NULL;
	
  /* sds keyval server init */
  context = (kv_context_t*)malloc(sizeof(kv_context_t));
  memset(context, 0, sizeof(kv_context_t));

  context->mid = mid;

    /* figure out what address this server is listening on */
    ret = margo_addr_self(context->mid, &addr_self);
    if(ret != HG_SUCCESS)
    {
	std::cerr << "Error: margo_addr_self()" << std::endl;
	margo_finalize(context->mid);
	return NULL;
    }
    ret = margo_addr_to_string(context->mid, addr_self_string,
	    &addr_self_string_sz, addr_self);
    if(ret != HG_SUCCESS)
    {
	std::cerr << "Error: margo_addr_to_string()" << std::endl;
	margo_finalize(context->mid);
	return NULL;
    }
    margo_addr_free(context->mid, addr_self);
    std::cout << "accepting RPCs on address " << std::string(addr_self_string) << std::endl;

    context->open_id = MARGO_REGISTER(context->mid, "open",
	    open_in_t, open_out_t, open_handler);

    context->close_id = MARGO_REGISTER(context->mid, "close",
	    void, close_out_t, close_handler);

    context->put_id = MARGO_REGISTER(context->mid, "put",
	    put_in_t, put_out_t, put_handler);

    context->bulk_put_id = MARGO_REGISTER(context->mid, "bulk_put",
	    bulk_put_in_t, bulk_put_out_t, bulk_put_handler);

    context->get_id = MARGO_REGISTER(context->mid, "get",
	    get_in_t, get_out_t, get_handler);

    context->bulk_get_id = MARGO_REGISTER(context->mid, "bulk_get",
	    bulk_get_in_t, bulk_get_out_t, bulk_get_handler);

    context->bench_id = MARGO_REGISTER(context->mid, "bench",
	    bench_in_t, bench_out_t, bench_handler);

    context->shutdown_id = MARGO_REGISTER(context->mid, "shutdown",
	    void, void, shutdown_handler);
    return context;
}

hg_return_t kv_server_wait_for_shutdown(kv_context_t *context) {
  margo_wait_for_finalize(context->mid);
  return HG_SUCCESS;
}

/* this is the same as client. should be moved to common utility library */
hg_return_t kv_server_deregister(kv_context_t *context) {
  free(context);
  delete datastore;
  std::cout << "SERVER: cleanup complete, deregistered" << std::endl;
  return HG_SUCCESS;
}
