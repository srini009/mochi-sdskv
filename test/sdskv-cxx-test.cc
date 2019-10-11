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

#include "sdskv-client.hpp"

static std::string gen_random_string(size_t len);

static int put_get_erase_test(sdskv::database& DB, uint32_t num_keys);
static int put_get_erase_multi_test(sdskv::database& DB, uint32_t num_keys);
static int list_keys_test(sdskv::database& DB, uint32_t num_keys);
static int list_keyvals_test(sdskv::database& DB, uint32_t num_keys);

int main(int argc, char *argv[])
{
    char cli_addr_prefix[64] = {0};
    std::string sdskv_svr_addr;
    std::string db_name;
    margo_instance_id mid;
    hg_addr_t svr_addr;
    uint16_t provider_id;
    uint32_t num_keys;

    hg_return_t hret;

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


    if(argc != 5)
    {
        fprintf(stderr, "Usage: %s <sdskv_server_addr> <mplex_id> <db_name> <num_keys>\n", argv[0]);
        fprintf(stderr, "  Example: %s tcp://localhost:1234 1 foo 1000\n", argv[0]);
        return(-1);
    }
    sdskv_svr_addr = argv[1];
    provider_id    = atoi(argv[2]);
    db_name        = argv[3];
    num_keys       = atoi(argv[4]);

    /* initialize Margo using the transport portion of the server
     * address (i.e., the part before the first : character if present)
     */
    for(unsigned i=0; (i<63 && sdskv_svr_addr[i] != '\0' && sdskv_svr_addr[i] != ':'); i++)
        cli_addr_prefix[i] = sdskv_svr_addr[i];

    /* start margo */
    mid = margo_init(cli_addr_prefix, MARGO_SERVER_MODE, 0, 0);
    if(mid == MARGO_INSTANCE_NULL)
        throw std::runtime_error("margo_init failed");

    {

        sdskv::client kvcl(mid);
        sdskv::provider_handle kvph;

        /* look up the SDSKV server address */
        hret = margo_addr_lookup(mid, sdskv_svr_addr.c_str(), &svr_addr);
        if(hret != HG_SUCCESS)
            throw std::runtime_error("margo_addr_lookup failed");

        /* create a SDSKV provider handle */
        kvph = sdskv::provider_handle(kvcl, svr_addr, provider_id);

        /* open the database */
        sdskv::database DB = kvcl.open(kvph, db_name);

        /* Put get erase test */
        put_get_erase_test(DB, num_keys);
        put_get_erase_multi_test(DB, num_keys);
        list_keys_test(DB, num_keys);

        /* shutdown the server */
        kvcl.shutdown(svr_addr);

        /**** cleanup ****/
        margo_addr_free(mid, svr_addr);

    }
    
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

static int put_get_erase_test(sdskv::database& DB, uint32_t num_keys) {

    /* **** put keys ***** */
    std::vector<std::string> keys;
    std::map<std::string, std::string> reference;
    size_t max_value_size = 24;

    for(unsigned i=0; i < num_keys; i++) {
        auto k = gen_random_string(16);
        // half of the entries will be put using bulk
        auto v = gen_random_string(3+i*(max_value_size-3)/num_keys);
        DB.put(k, v);
        std::cout << "Inserted " << k << "\t ===> " << v << std::endl;
        reference[k] = v;
        keys.push_back(k);
    }
    std::cout << "Successfuly inserted " << num_keys << " keys" << std::endl;

    /* **** get keys **** */
    for(unsigned i=0; i < num_keys; i++) {
        auto k = keys[rand() % keys.size()];
        size_t value_size = max_value_size;
        std::vector<char> v(max_value_size);
        DB.get(k,v);
        std::string vstring((char*)(v.data()));
        std::cout << "Got " << k << " ===> " << vstring << std::endl;
        if(vstring != reference[k]) {
            throw std::runtime_error("DB.get() returned a value different from the reference");
        }
    }

    /* erase keys */
    for(unsigned i=0; i < num_keys; i++) {
        DB.erase(keys[i]);
    }

    /* try getting a random key that should have been deleted */
    try {
        std::vector<char> v(max_value_size);
        DB.get(keys[num_keys/2], v);
        throw std::runtime_error("DB.get() succeeded when it shouldn't have");
    } catch(sdskv::exception& ex) {
        std::cout << "Correctly thronw exception: " << ex.what() << std::endl;
    }

    return 0;
}

static int put_get_erase_multi_test(sdskv::database& DB, uint32_t num_keys) {

    std::cout << "============== put_get_erase_multi_test ==============" << std::endl;
    /* **** put keys ***** */
    std::vector<std::string> keys;
    std::vector<std::string> values;
    std::map<std::string, std::string> reference;
    size_t max_value_size = 24;

    for(unsigned i=0; i < num_keys; i++) {
        auto k = gen_random_string(16);
        // half of the entries will be put using bulk
        auto v = gen_random_string(3+i*(max_value_size-3)/num_keys);
        reference[k] = v;
        keys.push_back(k);
        values.push_back(v);
        std::cout << "Inserting " << k << "===>\t" << v << std::endl;
    }
    DB.put(keys, values);
    std::cout << "Successfuly inserted " << num_keys << " keys" << std::endl;

    /* **** get keys **** */
    unsigned i1 = rand() % (keys.size()/3);
    unsigned i2 = i1 + (rand() % (keys.size()/3));
    std::cout << "Will do a get_multi on the following keys: " << std::endl;
    std::vector<std::string> keys_subset;
    for(; i1 < i2+1; i1++) {
        auto k = keys[rand() % keys.size()];
        std::cout << "      " << k << std::endl;
        keys_subset.push_back(k);
    }
    std::vector<std::string> vals_subset;
    for(unsigned i=0; i < keys_subset.size(); i++) {
        vals_subset.push_back(std::string(max_value_size, 0));
    }

    DB.get(keys_subset, vals_subset);

    for(unsigned i=0; i < keys_subset.size(); i++) {
        if(vals_subset[i] != reference[keys_subset[i]]) {
            std::cerr << "Error in get multi, resulting values don't match: "
                << " keys: " << keys_subset[i] << " val read: " << vals_subset[i] << " (size " << vals_subset[i].size() 
                << ") val expected: " << reference[keys_subset[i]]  << " (size " << reference[keys_subset[i]].size() << ")" << std::endl;
            throw std::runtime_error("Error in get multi, resulting values don't match");
        }
    }
    

    /* erase keys */
    /*
    for(auto& k : keys)
        DB.erase(k);
    */
    DB.erase_multi(keys);
    return 0;
}

static int list_keys_test(sdskv::database& DB, uint32_t num_keys) {

    /* **** put keys ***** */
    std::vector<std::string> keys;
    std::map<std::string, std::string> reference;
    size_t max_value_size = 24;

    for(unsigned i=0; i < num_keys; i++) {
        auto k = gen_random_string(16);
        // half of the entries will be put using bulk
        auto v = gen_random_string(3+i*(max_value_size-3)/num_keys);
        DB.put(k, v);
        std::cout << "Inserted " << k << "\t ===> " << v << std::endl;
        reference[k] = v;
        keys.push_back(k);
    }
    std::cout << "Successfuly inserted " << num_keys << " keys" << std::endl;

    std::string start_key;
    std::vector<std::string> keys_out(3);
    auto it = reference.begin();
    do {
        DB.list_keys(start_key, keys_out);
        if(keys_out.size() == 0) break;
        start_key = keys_out[keys_out.size()-1];
        for(auto& k : keys_out) {
            if(k != it->first) {
                std::cerr << "Error: key " << k << " != " << it->first << std::endl;
                throw std::runtime_error("list_keys error");
            } else {
                std::cerr << "Key OK: key " << k << " == " << it->first << std::endl;
            }
            ++it;
        }
    } while(keys_out.size() == 3);

    /* erase keys */
    for(unsigned i=0; i < num_keys; i++) {
        DB.erase(keys[i]);
    }

    return 0;
}

static int list_keyvals_test(sdskv::database& DB, uint32_t num_keys) {

    /* **** put keys ***** */
    std::vector<std::string> keys;
    std::map<std::string, std::string> reference;
    size_t max_value_size = 24;

    for(unsigned i=0; i < num_keys; i++) {
        auto k = gen_random_string(16);
        // half of the entries will be put using bulk
        auto v = gen_random_string(3+i*(max_value_size-3)/num_keys);
        DB.put(k, v);
        std::cout << "Inserted " << k << "\t ===> " << v << std::endl;
        reference[k] = v;
        keys.push_back(k);
    }
    std::cout << "Successfuly inserted " << num_keys << " keys" << std::endl;

    std::string start_key;
    std::vector<std::string> keys_out(3);
    std::vector<std::string> vals_out(3);
    auto it = reference.begin();
    do {
        DB.list_keyvals(start_key, keys_out, vals_out);
        if(keys_out.size() == 0) break;
        start_key = keys_out[keys_out.size()-1];
        for(unsigned i = 0; i < keys_out.size(); i++) {
            auto& k = keys_out[i];
            auto& v = vals_out[i];
            if(k != it->first) {
                std::cerr << "Error: key " << k << " != " << it->first << std::endl;
                throw std::runtime_error("list_keys error");
            } else {
                std::cerr << "Key OK: key " << k << " == " << it->first << std::endl;
            }
            if(v != it->second) {
                std::cerr << "Error: value " << v << " != " << it->second << std::endl;
                throw std::runtime_error("list_keys error");
            } else {
                std::cerr << "Val OK: val " << v << " == " << it->second << std::endl;
            }
            ++it;
        }
    } while(keys_out.size() == 3);

    /* erase keys */
    for(unsigned i=0; i < num_keys; i++) {
        DB.erase(keys[i]);
    }

    return 0;
}
