#include <bedrock/module.h>
#include "sdskv-server.h"
#include "sdskv-client.h"

static int sdskv_register_provider(bedrock_args_t             args,
                                   bedrock_module_provider_t* provider)
{
    margo_instance_id mid         = bedrock_args_get_margo_instance(args);
    uint16_t          provider_id = bedrock_args_get_provider_id(args);
    ABT_pool          pool        = bedrock_args_get_pool(args);
    const char*       config      = bedrock_args_get_config(args);
    const char*       name        = bedrock_args_get_name(args);

    struct sdskv_provider_init_info sdskv_args = SDSKV_PROVIDER_INIT_INFO_INIT;
    sdskv_args.rpc_pool                        = pool;
    sdskv_args.json_config                     = config;

    if (bedrock_args_get_num_dependencies(args, "remi_provider")) {
        sdskv_args.remi_provider
            = bedrock_args_get_dependency(args, "remi_provider", 0);
    } else {
        sdskv_args.remi_provider = NULL;
    }

    if (bedrock_args_get_num_dependencies(args, "remi_client")) {
        sdskv_args.remi_client
            = bedrock_args_get_dependency(args, "remi_client", 0);
    } else {
        sdskv_args.remi_client = NULL;
    }

    if (sdskv_provider_register(mid, provider_id, &sdskv_args,
                                (sdskv_provider_t*)provider)
        == SDSKV_SUCCESS)
        return BEDROCK_SUCCESS;
    return -1;
}

static int sdskv_deregister_provider(bedrock_module_provider_t provider)
{
    if (sdskv_provider_destroy(provider) == SDSKV_SUCCESS)
        return BEDROCK_SUCCESS;
    return -1;
}

static char* sdskv_get_provider_config(bedrock_module_provider_t provider)
{
    return sdskv_provider_get_config((sdskv_provider_t)provider);
}

static int sdskv_init_client(bedrock_args_t           args,
                             bedrock_module_client_t* client)
{
    int ret = sdskv_client_init(bedrock_args_get_margo_instance(args),
                                (sdskv_client_t*)client);
    if (ret == SDSKV_SUCCESS) return BEDROCK_SUCCESS;
    return ret;
}

static int sdskv_finalize_client(bedrock_module_client_t client)
{
    int ret = sdskv_client_finalize((sdskv_client_t)client);
    if (ret == SDSKV_SUCCESS) return BEDROCK_SUCCESS;
    return ret;
}

static int sdskv_create_provider_handle(bedrock_module_client_t client,
                                        hg_addr_t               address,
                                        uint16_t                provider_id,
                                        bedrock_module_provider_handle_t* ph)
{
    int ret = sdskv_provider_handle_create(client, address, provider_id,
                                           (sdskv_provider_handle_t*)ph);
    if (ret == SDSKV_SUCCESS) return BEDROCK_SUCCESS;
    return ret;
}

static int sdskv_destroy_provider_handle(bedrock_module_provider_handle_t ph)
{
    sdskv_client_t client;
    hg_addr_t      addr;
    uint16_t       provider_id;

    sdskv_provider_handle_get_info(ph, &client, &addr, &provider_id);
    int ret = sdskv_provider_handle_release((sdskv_provider_handle_t)ph);
    if (ret == SDSKV_SUCCESS) return BEDROCK_SUCCESS;
    return ret;
}

static struct bedrock_dependency sdskv_provider_deps[3]
    = {{"remi_provider", "remi", 0},
       {"remi_client", "remi", 0},
       BEDROCK_NO_MORE_DEPENDENCIES};

static struct bedrock_module sdskv
    = {.register_provider       = sdskv_register_provider,
       .deregister_provider     = sdskv_deregister_provider,
       .get_provider_config     = sdskv_get_provider_config,
       .init_client             = sdskv_init_client,
       .finalize_client         = sdskv_finalize_client,
       .create_provider_handle  = sdskv_create_provider_handle,
       .destroy_provider_handle = sdskv_destroy_provider_handle,
       .client_dependencies     = NULL,
       .provider_dependencies   = sdskv_provider_deps};

BEDROCK_REGISTER_MODULE(sdskv, sdskv);
