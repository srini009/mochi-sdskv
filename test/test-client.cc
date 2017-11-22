#include "sds-keyval.h"
#include <unistd.h>
#include <assert.h>
#include <vector>
#include <iostream>

int main(int argc, char **argv) {
  hg_return_t ret;

  assert(argc == 2);
  char *server_addr_str = argv[1];
  
  margo_instance_id mid = margo_init(argv[1], MARGO_CLIENT_MODE, 0, -1);
  kv_context_t *context = kv_client_register(mid);

  /* open */
  ret = kv_open(context, server_addr_str, "db/booger");
  assert(ret == HG_SUCCESS);

  /* put */
  int key = 10;
  int val = 10;
  printf("putting val with size %lu for key %d\n", sizeof(val), key);
  ret = kv_put(context, &key, sizeof(key), &val, sizeof(val));
  assert(ret == HG_SUCCESS);

  /* get */
  int remote_val;
  hg_size_t vsize = sizeof(remote_val);
  ret = kv_get(context, &key, sizeof(key), &remote_val, &vsize);
  std::cout << "kv_get returned size " << vsize << std::endl;
  assert(ret == HG_SUCCESS);
  if (val == remote_val) {
    printf("put/get succeeded, key: %d in: %d out: %d\n", key, val, remote_val);
  }
  else {
    printf("put/get failed, key: %d in: %d out: %d\n", key, val, remote_val);
  }

  key = 20;
  std::vector<int32_t> bulk_val(256, 10);
  vsize = bulk_val.size() * sizeof(int32_t);
  printf("putting bulk_val with size %lu for key %d\n", vsize, key);
  ret = kv_put(context, &key, sizeof(key), bulk_val.data(), vsize);
  assert(ret == HG_SUCCESS);

  std::vector<char> data;
  std::vector<int32_t> remote_bulk_val;
  vsize = bulk_val.size() * sizeof(int32_t);
  data.resize(vsize);
  ret = kv_get(context, &key, sizeof(key), data.data(), &vsize);
  std::cout << "kv_get returned size " << vsize << std::endl;
  assert(ret == HG_SUCCESS);
  std::cout << "resizing remote_bulk_val to " << vsize/sizeof(int32_t) << std::endl;
  remote_bulk_val.resize(vsize/sizeof(int32_t));
  memcpy(remote_bulk_val.data(), data.data(), vsize);
  printf("bulk_val size %lu, remote_bulk_val size %lu\n", bulk_val.size(), remote_bulk_val.size());
  if (bulk_val == remote_bulk_val) {
    printf("bulk value put/get succeeded for key: %d\n", key);
  }
  else {
    printf("bulk value put/get failed for key: %d\n", key);
    std::cout << "bulk_val: " << bulk_val[0] << std::endl;
    std::cout << "remote_bulk_val: " << remote_bulk_val[0] << std::endl;
  }

#if BWTREE // kv_benchmark code only supported when BwTree enabled
  bench_result_t *output;
  output = kv_benchmark(context, 1000);
  printf("inserts: %zd keys in %f seconds: %f Million-inserts per sec\n",
	 output->nkeys, output->insert_time,
	 output->nkeys/(output->insert_time*1000*1000) );
  printf("reads: %zd keys in %f seconds: %f Million-reads per sec\n",
	 output->nkeys, output->read_time,
	 output->nkeys/(output->read_time*1000*1000) );
  printf("overhead: %f seconds\n", output->overhead);
  free(output);
#endif
  
  /* close */
  ret = kv_close(context);
  assert(ret == HG_SUCCESS);

  /* signal server */
  ret = kv_client_signal_shutdown(context);
  assert(ret == HG_SUCCESS);

  /* cleanup */
  ret = kv_client_deregister(context);
  assert(ret == HG_SUCCESS);

  kv_margo_finalize(mid);
}
