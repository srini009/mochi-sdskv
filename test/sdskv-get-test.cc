/*
 * (C) 2015 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */

#include <stdio.h>
#include <assert.h>
#include <unistd.h>
#include <margo.h>
#include <iostream>
#include <string>
#include <vector>
#include <algorithm>
#include <map>

#include "sdskv-client.h"

static std::string gen_random_string(size_t len);

int main(int argc, char *argv[])
{
    char cli_addr_prefix[64] = {0};
    char *sdskv_svr_addr_str;
    char *db_name;
    margo_instance_id mid;
    hg_addr_t svr_addr;
    uint8_t mplex_id;
    uint32_t num_keys;
    sdskv_client_t kvcl;
    sdskv_provider_handle_t kvph;
    hg_return_t hret;
    int ret;

    if(argc != 5)
    {
        fprintf(stderr, "Usage: %s <sdskv_server_addr> <mplex_id> <db_name> <num_keys>\n", argv[0]);
        fprintf(stderr, "  Example: %s tcp://localhost:1234 1 foo 1000\n", argv[0]);
        return(-1);
    }
    sdskv_svr_addr_str = argv[1];
    mplex_id          = atoi(argv[2]);
    db_name           = argv[3];
    num_keys          = atoi(argv[4]);

    /* initialize Margo using the transport portion of the server
     * address (i.e., the part before the first : character if present)
     */
    for(unsigned i=0; (i<63 && sdskv_svr_addr_str[i] != '\0' && sdskv_svr_addr_str[i] != ':'); i++)
        cli_addr_prefix[i] = sdskv_svr_addr_str[i];

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

    /* look up the SDSKV server address */
    hret = margo_addr_lookup(mid, sdskv_svr_addr_str, &svr_addr);
    if(hret != HG_SUCCESS)
    {
        fprintf(stderr, "Error: margo_addr_lookup()\n");
        sdskv_client_finalize(kvcl);
        margo_finalize(mid);
        return(-1);
    }

    /* create a SDSKV provider handle */
    ret = sdskv_provider_handle_create(kvcl, svr_addr, mplex_id, &kvph);
    if(ret != 0)
    {
        fprintf(stderr, "Error: sdskv_provider_handle_create()\n");
        margo_addr_free(mid, svr_addr);
        sdskv_client_finalize(kvcl);
        margo_finalize(mid);
        return(-1);
    }

    /* open the database */
    sdskv_database_id_t db_id;
    ret = sdskv_open(kvph, db_name, &db_id);
    if(ret == 0) {
        printf("Successfuly open database %s, id is %ld\n", db_name, db_id);
    } else {
        fprintf(stderr, "Error: could not open database %s\n", db_name);
        sdskv_provider_handle_release(kvph);
        margo_addr_free(mid, svr_addr);
        sdskv_client_finalize(kvcl);
        margo_finalize(mid);
        return(-1);
    }

    /* **** put keys ***** */
    std::vector<std::string> keys;
    std::map<std::string, std::string> reference;
    size_t max_value_size = 24;

    for(unsigned i=0; i < num_keys; i++) {
        auto k = gen_random_string(16);
        // half of the entries will be put using bulk
        auto v = gen_random_string(3+i*(max_value_size-3)/num_keys);
        ret = sdskv_put(kvph, db_id,
                (const void *)k.data(), k.size(),
                (const void *)v.data(), v.size());
        if(ret != 0) {
            fprintf(stderr, "Error: sdskv_put() failed (iteration %d)\n", i);
            sdskv_shutdown_service(kvcl, svr_addr);
            sdskv_provider_handle_release(kvph);
            margo_addr_free(mid, svr_addr);
            sdskv_client_finalize(kvcl);
            margo_finalize(mid);
            return -1;
        }
        std::cout << "Inserted " << k << "\t ===> " << v << std::endl;
        reference[k] = v;
        keys.push_back(k);
    }
    printf("Successfuly inserted %d keys\n", num_keys);

    /* **** get keys **** */
    for(unsigned i=0; i < num_keys; i++) {
        auto k = keys[rand() % keys.size()];
        size_t value_size = max_value_size;
        std::vector<char> v(max_value_size);
        ret = sdskv_get(kvph, db_id,
                (const void *)k.data(), k.size(),
                (void *)v.data(), &value_size);
        if(ret != 0) {
            fprintf(stderr, "Error: sdskv_get() failed (key was %s)\n", k.c_str());
            sdskv_shutdown_service(kvcl, svr_addr);
            sdskv_provider_handle_release(kvph);
            margo_addr_free(mid, svr_addr);
            sdskv_client_finalize(kvcl);
            margo_finalize(mid);
            return -1;
        }
        std::string vstring((char*)(v.data()));
        std::cout << "Got " << k << " ===> " << vstring << std::endl;
        if(vstring != reference[k]) {
            fprintf(stderr, "Error: sdskv_get() returned a value different from the reference\n");
            sdskv_shutdown_service(kvcl, svr_addr);
            sdskv_provider_handle_release(kvph);
            margo_addr_free(mid, svr_addr);
            sdskv_client_finalize(kvcl);
            margo_finalize(mid);
            return -1;
        }
    }

    /* **** override keys ***** */
    reference.clear();
    for(unsigned i=0; i < num_keys; i++) {
        const auto& k = keys[i];
        // the keys have 2 characters more than the previously stored ones
        auto v = gen_random_string(5+i*(max_value_size-5)/num_keys);
        ret = sdskv_put(kvph, db_id,
                (const void *)k.data(), k.size(),
                (const void *)v.data(), v.size());
        if(ret != 0) {
            fprintf(stderr, "Error: sdskv_put() failed when overwritting (iteration %d)\n", i);
            sdskv_shutdown_service(kvcl, svr_addr);
            sdskv_provider_handle_release(kvph);
            margo_addr_free(mid, svr_addr);
            sdskv_client_finalize(kvcl);
            margo_finalize(mid);
            return -1;
        }
        std::cout << "Inserted " << k << "\t ===> " << v << std::endl;
        reference[k] = v;
    }
    printf("Successfuly overwritten %d keys\n", num_keys);

    /* **** get keys **** */
    for(unsigned i=0; i < num_keys; i++) {
        auto k = keys[rand() % keys.size()];
        size_t value_size = max_value_size;
        std::vector<char> v(max_value_size);
        ret = sdskv_get(kvph, db_id,
                (const void *)k.data(), k.size(),
                (void *)v.data(), &value_size);
        if(ret != 0) {
            fprintf(stderr, "Error: sdskv_get() failed (key was %s)\n", k.c_str());
            sdskv_shutdown_service(kvcl, svr_addr);
            sdskv_provider_handle_release(kvph);
            margo_addr_free(mid, svr_addr);
            sdskv_client_finalize(kvcl);
            margo_finalize(mid);
            return -1;
        }
        std::string vstring((char*)(v.data()));
        std::cout << "Got " << k << " ===> " << vstring << std::endl;
        if(vstring != reference[k]) {
            fprintf(stderr, "Error: sdskv_get() returned a value different from the reference\n");
            sdskv_shutdown_service(kvcl, svr_addr);
            sdskv_provider_handle_release(kvph);
            margo_addr_free(mid, svr_addr);
            sdskv_client_finalize(kvcl);
            margo_finalize(mid);
            return -1;
        }
    }

    /* shutdown the server */
    ret = sdskv_shutdown_service(kvcl, svr_addr);

    /**** cleanup ****/
    sdskv_provider_handle_release(kvph);
    margo_addr_free(mid, svr_addr);
    sdskv_client_finalize(kvcl);
    margo_finalize(mid);
    return(ret);
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
