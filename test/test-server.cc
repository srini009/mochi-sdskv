#include "sds-keyval.h"
#include <assert.h>

int main(int argc, char **argv) {

  assert(argc == 2);

  margo_instance_id mid = margo_init(argv[1] == NULL ? "ofi+tcp://" : argv[1],
	 MARGO_SERVER_MODE, 0, -1);
  kv_context_t *context = kv_server_register(mid);

  hg_return_t ret;
  ret = kv_server_wait_for_shutdown(context);
  assert(ret == HG_SUCCESS);

  ret = kv_server_deregister(context);
  assert(ret == HG_SUCCESS);
}
