/*
 * (C) 2015 The University of Chicago
 * 
 * See COPYRIGHT in top-level directory.
 */

#include <stdio.h>
#include <assert.h>
#include <unistd.h>
#include <margo.h>
#include <string>
#include <vector>

#include "sdskv-client.h"

static std::string gen_random_string(size_t len);

int main(int argc, char *argv[])
{
    char cli_addr_prefix[64] = {0};
    char *sdskv_svr_addr_strA;
    char *sdskv_svr_addr_strB;
    char *db_nameA;
    char *db_nameB;
    margo_instance_id mid;
    hg_addr_t svr_addrA;
    hg_addr_t svr_addrB;
    uint8_t mplex_idA, mplex_idB;
    uint32_t num_keys;
    sdskv_client_t kvcl;
    sdskv_provider_handle_t kvphA, kvphB;
    hg_return_t hret;
    int ret;

    int use_poolset = 0;
    hg_size_t poolset_npools = 4;
    hg_size_t poolset_nbufs = 8;
    hg_size_t poolset_first_size = 10240;
    hg_size_t poolset_size_multiple = 2;

    char* var = getenv("SDSKV_USE_POOLSET");
    if(var != NULL) {
        use_poolset = 1;
        var = getenv("SDSKV_POOLSET_NPOOLS");
        if(var != NULL) poolset_npools = atol(var);
        var = getenv("SDSKV_POOLSET_NBUFS");
        if(var != NULL) poolset_nbufs = atol(var);
        var = getenv("SDSKV_POOLSET_FIRST_SIZE");
        if(var != NULL) poolset_first_size = atol(var);
        var = getenv("SDSKV_POOLSET_SIZE_MULTIPLE");
        if(var != NULL) poolset_size_multiple = atol(var);
    }

    if(argc != 8)
    {
        fprintf(stderr, "Usage: %s <server_addrA> <mplex_idA> <db_nameA> <server_addrB> <mplex_idB> <db_nameB> <num_keys>\n", argv[0]);
        fprintf(stderr, "  Example: %s tcp://localhost:1234 1 foo tcp://localhost:1235 1 bar 1000\n", argv[0]);
        return(-1);
    }
    sdskv_svr_addr_strA = argv[1];
    mplex_idA           = atoi(argv[2]);
    db_nameA            = argv[3];
    sdskv_svr_addr_strB = argv[4];
    mplex_idB           = atoi(argv[5]);
    db_nameB            = argv[6];
    num_keys            = atoi(argv[7]);

    /* initialize Margo using the transport portion of the server
     * address (i.e., the part before the first : character if present)
     */
    for(unsigned i=0; (i<63 && sdskv_svr_addr_strA[i] != '\0' && sdskv_svr_addr_strA[i] != ':'); i++)
        cli_addr_prefix[i] = sdskv_svr_addr_strA[i];

    /* start margo */
    mid = margo_init(cli_addr_prefix, MARGO_SERVER_MODE, 0, 0);
    if(mid == MARGO_INSTANCE_NULL)
    {
        fprintf(stderr, "Error: margo_init()\n");
        return(-1);
    }

    ret = sdskv_client_init(mid, &kvcl);
    if(ret != 0)
    {
        fprintf(stderr, "Error: sdskv_client_init()\n");
        margo_finalize(mid);
        return -1;
    }

    /* setup poolset if requested */
    if(use_poolset) {
        ret = sdskv_client_configure_bulk_poolset(
                kvcl, poolset_npools, poolset_nbufs,
                poolset_first_size, poolset_size_multiple);
        if(ret != 0) {
            fprintf(stderr, "Error: sdskv_client_configure_bulk_poolset()\n");
            margo_finalize(mid);
            return -1;
        }
    }

    /* look up the SDSKV server address A */
    hret = margo_addr_lookup(mid, sdskv_svr_addr_strA, &svr_addrA);
    if(hret != HG_SUCCESS)
    {
        fprintf(stderr, "Error: margo_addr_lookup()\n");
        sdskv_client_finalize(kvcl);
        margo_finalize(mid);
        return(-1);
    }
    /* look up the SDSKV server address B */
    hret = margo_addr_lookup(mid, sdskv_svr_addr_strB, &svr_addrB);
    if(hret != HG_SUCCESS)
    {
        fprintf(stderr, "Error: margo_addr_lookup()\n");
        margo_addr_free(mid, svr_addrA);
        sdskv_client_finalize(kvcl);
        margo_finalize(mid);
        return(-1);
    }

    /* create a SDSKV provider handle for provider A */
    ret = sdskv_provider_handle_create(kvcl, svr_addrA, mplex_idA, &kvphA);
    if(ret != 0)
    {
        fprintf(stderr, "Error: sdskv_provider_handle_create()\n");
        margo_addr_free(mid, svr_addrA);
        margo_addr_free(mid, svr_addrB);
        sdskv_client_finalize(kvcl);
        margo_finalize(mid);
        return(-1);
    }

    /* create a SDSKV provider handle for provider B */
    ret = sdskv_provider_handle_create(kvcl, svr_addrB, mplex_idB, &kvphB);
    if(ret != 0)
    {
        fprintf(stderr, "Error: sdskv_provider_handle_create()\n");
        sdskv_provider_handle_release(kvphA);
        margo_addr_free(mid, svr_addrA);
        margo_addr_free(mid, svr_addrB);
        sdskv_client_finalize(kvcl);
        margo_finalize(mid);
        return(-1);
    }

    /* open the database A */
    sdskv_database_id_t db_idA;
    ret = sdskv_open(kvphA, db_nameA, &db_idA);
    if(ret == 0) {
        printf("Successfuly open database %s, id is %ld\n", db_nameA, db_idA);
    } else {
        fprintf(stderr, "Error: could not open database %s\n", db_nameA);
        sdskv_provider_handle_release(kvphA);
        sdskv_provider_handle_release(kvphB);
        margo_addr_free(mid, svr_addrA);
        margo_addr_free(mid, svr_addrB);
        sdskv_client_finalize(kvcl);
        margo_finalize(mid);
        return(-1);
    }

    /* open the database B */
    sdskv_database_id_t db_idB;
    ret = sdskv_open(kvphB, db_nameB, &db_idB);
    if(ret == 0) {
        printf("Successfuly open database %s, id is %ld\n", db_nameB, db_idB);
    } else {
        fprintf(stderr, "Error: could not open database %s\n", db_nameB);
        sdskv_provider_handle_release(kvphA);
        sdskv_provider_handle_release(kvphB);
        margo_addr_free(mid, svr_addrA);
        margo_addr_free(mid, svr_addrB);
        sdskv_client_finalize(kvcl);
        margo_finalize(mid);
        return(-1);
    }

    /* **** put keys ***** */
    std::vector<std::string> keys;
    std::vector<std::string> data;
    int max_value_size = 24;
    for(unsigned i=0; i < num_keys; i++) {
        auto k = gen_random_string(16);
        // half of the entries will be put using bulk
        auto v = gen_random_string(3+i*(max_value_size-3)/num_keys);
        ret = sdskv_put(kvphA, db_idA,
                (const void *)k.data(), k.size(),
                (const void *)v.data(), v.size());
        keys.push_back(k);
        data.push_back(v);
        if(ret != 0) {
            fprintf(stderr, "Error: sdskv_put() failed (iteration %d)\n", i);
            sdskv_provider_handle_release(kvphA);
            sdskv_provider_handle_release(kvphB);
            margo_addr_free(mid, svr_addrA);
            margo_addr_free(mid, svr_addrB);
            sdskv_client_finalize(kvcl);
            margo_finalize(mid);
            return -1;
        }
    }

    /* **** migrate all the keys to the second provider **** */
    ret = sdskv_migrate_all_keys(kvphA, db_idA, sdskv_svr_addr_strB,
            mplex_idB, db_idB, SDSKV_REMOVE_ORIGINAL);
    if(ret != SDSKV_SUCCESS) {
        fprintf(stderr, "Error: sdskv_migrate_all_keys() failed (ret = %d)\n", ret);
        sdskv_provider_handle_release(kvphA);
        sdskv_provider_handle_release(kvphB);
        margo_addr_free(mid, svr_addrA);
        margo_addr_free(mid, svr_addrB);
        sdskv_client_finalize(kvcl);
        margo_finalize(mid);
        return -1;
    }

    /* **** check that we can get the keys from the second provider now **** */
    for(unsigned int i=0; i < keys.size(); i++) {
        auto& k = keys[i];
        size_t value_size = max_value_size;
        std::vector<char> v(max_value_size);
        ret = sdskv_get(kvphB, db_idB,
                (const void *)k.data(), k.size(),
                (void *)v.data(), &value_size);
        if(ret != SDSKV_SUCCESS) {
            fprintf(stderr,"Error: sdskv_get() failed\n");
            sdskv_provider_handle_release(kvphA);
            sdskv_provider_handle_release(kvphB);
            margo_addr_free(mid, svr_addrA);
            margo_addr_free(mid, svr_addrB);
            sdskv_client_finalize(kvcl);
            margo_finalize(mid);
            return -1;
        }
        std::string s(v.data());
        if(s != data[i]) {
            fprintf(stderr, "Migrated value mismatch %s != %s\n", v.data(), data[i].data());
            sdskv_provider_handle_release(kvphA);
            sdskv_provider_handle_release(kvphB);
            margo_addr_free(mid, svr_addrA);
            margo_addr_free(mid, svr_addrB);
            sdskv_client_finalize(kvcl);
            margo_finalize(mid);
            return -1;
        }
    }

    /* shutdown the server */
    ret = sdskv_shutdown_service(kvcl, svr_addrA);
    ret = sdskv_shutdown_service(kvcl, svr_addrB);

    /**** cleanup ****/
    sdskv_provider_handle_release(kvphA);
    sdskv_provider_handle_release(kvphB);
    margo_addr_free(mid, svr_addrA);
    margo_addr_free(mid, svr_addrB);
    sdskv_client_finalize(kvcl);
    margo_finalize(mid);
    return 0;
}

static std::string gen_random_string(size_t len) {
    static const char alphanum[] =
                "0123456789"
                "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
                "abcdefghijklmnopqrstuvwxyz";
    std::string s(len, ' ');
    for (unsigned i = 0; i < len; ++i) {
        s[i] = alphanum[rand() % (sizeof(alphanum) - 1)];
    }
    return s;
}
