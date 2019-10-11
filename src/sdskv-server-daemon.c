/*
 * (C) 2015 The University of Chicago
 * 
 * See COPYRIGHT in top-level directory.
 */

#include <stdio.h>
#include <assert.h>
#include <unistd.h>
#include <margo.h>
#include <sdskv-server.h>

typedef enum {
    MODE_DATABASES = 0,
    MODE_PROVIDERS = 1
} kv_mplex_mode_t;

struct options
{
    char *listen_addr_str;
    unsigned num_db;
    char **db_names;
    sdskv_db_type_t *db_types;
    char *host_file;
    kv_mplex_mode_t mplex_mode;
};

static void usage(int argc, char **argv)
{
    fprintf(stderr, "Usage: sdskv-server-daemon [OPTIONS] <listen_addr> <db name 1>[:map|:bwt|:bdb|:ldb] <db name 2>[:map|:bwt|:bdb|:ldb] ...\n");
    fprintf(stderr, "       listen_addr is the Mercury address to listen on\n");
    fprintf(stderr, "       db name X are the names of the databases\n");
    fprintf(stderr, "       [-f filename] to write the server address to a file\n");
    fprintf(stderr, "       [-m mode] multiplexing mode (providers or databases) for managing multiple databases (default is databases)\n"); 
    fprintf(stderr, "Example: ./sdskv-server-daemon tcp://localhost:1234 foo:bdb bar\n");
    return;
}

static sdskv_db_type_t parse_db_type(char* db_fullname) {
    char* column = strstr(db_fullname, ":");
    if(column == NULL) {
        return KVDB_MAP;
    }
    *column = '\0';
    char* db_type = column + 1;
    if(strcmp(db_type, "map") == 0) {
        return KVDB_MAP;
    } else if(strcmp(db_type, "bwt") == 0) {
        return KVDB_BWTREE;
    } else if(strcmp(db_type, "bdb") == 0) {
        return KVDB_BERKELEYDB;
    } else if(strcmp(db_type, "ldb") == 0) {
        return KVDB_LEVELDB;
    }
    fprintf(stderr, "Unknown database type \"%s\"\n", db_type);
    exit(-1);
}

static void parse_args(int argc, char **argv, struct options *opts)
{
    int opt;

    memset(opts, 0, sizeof(*opts));

    /* get options */
    while((opt = getopt(argc, argv, "f:m:")) != -1)
    {
        switch(opt)
        {
            case 'f':
                opts->host_file = optarg;
                break;
            case 'm':
                if(0 == strcmp(optarg, "databases"))
                    opts->mplex_mode = MODE_DATABASES;
                else if(0 == strcmp(optarg, "providers"))
                    opts->mplex_mode = MODE_PROVIDERS;
                else {
                    fprintf(stderr, "Unrecognized multiplexing mode \"%s\"\n", optarg);
                    exit(EXIT_FAILURE);
                }
                break;
            default:
                usage(argc, argv);
                exit(EXIT_FAILURE);
        }
    }

    /* get required arguments after options */
    if((argc - optind) < 2)
    {
        usage(argc, argv);
        exit(EXIT_FAILURE);
    }
    opts->num_db = argc - optind - 1;
    opts->listen_addr_str = argv[optind++];
    opts->db_names = calloc(opts->num_db, sizeof(char*));
    opts->db_types = calloc(opts->num_db, sizeof(sdskv_db_type_t));
    int i;
    for(i=0; i < opts->num_db; i++) {
        opts->db_names[i] = argv[optind++];
        opts->db_types[i] = parse_db_type(opts->db_names[i]);
    }

    return;
}

int main(int argc, char **argv) 
{
    struct options opts;
    margo_instance_id mid;
    int ret;

    parse_args(argc, argv, &opts);

    int use_poolset = 0;
    hg_size_t poolset_npools = 6;
    hg_size_t poolset_nbufs = 8;
    hg_size_t poolset_first_size = 128;
    hg_size_t poolset_size_multiple = 2;

    char* var = getenv("SDSKV_SERVER_USE_POOLSET");
    if(var != NULL) {
        use_poolset = 1;
        var = getenv("SDSKV_SERVER_POOLSET_NPOOLS");
        if(var != NULL) poolset_npools = atol(var);
        var = getenv("SDSKV_SERVER_POOLSET_NBUFS");
        if(var != NULL) poolset_nbufs = atol(var);
        var = getenv("SDSKV_SERVER_POOLSET_FIRST_SIZE");
        if(var != NULL) poolset_first_size = atol(var);
        var = getenv("SDSKV_SERVER_POOLSET_SIZE_MULTIPLE");
        if(var != NULL) poolset_size_multiple = atol(var);
    }

    /* start margo */
    /* use the main xstream for driving progress and executing rpc handlers */
    mid = margo_init(opts.listen_addr_str, MARGO_SERVER_MODE, 0, -1);
    if(mid == MARGO_INSTANCE_NULL)
    {
        fprintf(stderr, "Error: margo_init()\n");
        return(-1);
    }

    margo_enable_remote_shutdown(mid);

    if(opts.host_file)
    {
        /* write the server address to file if requested */
        FILE *fp;
        hg_addr_t self_addr;
        char self_addr_str[128];
        hg_size_t self_addr_str_sz = 128;
        hg_return_t hret;

        /* figure out what address this server is listening on */
        hret = margo_addr_self(mid, &self_addr);
        if(hret != HG_SUCCESS)
        {
            fprintf(stderr, "Error: margo_addr_self()\n");
            margo_finalize(mid);
            return(-1);
        }
        hret = margo_addr_to_string(mid, self_addr_str, &self_addr_str_sz, self_addr);
        if(hret != HG_SUCCESS)
        {
            fprintf(stderr, "Error: margo_addr_to_string()\n");
            margo_addr_free(mid, self_addr);
            margo_finalize(mid);
            return(-1);
        }
        margo_addr_free(mid, self_addr);

        fp = fopen(opts.host_file, "w");
        if(!fp)
        {
            perror("fopen");
            margo_finalize(mid);
            return(-1);
        }

        fprintf(fp, "%s", self_addr_str);
        fclose(fp);
    }

    /* initialize the SDSKV server */
    if(opts.mplex_mode == MODE_PROVIDERS) {
        int i;
        for(i=0; i< opts.num_db; i++) {
            sdskv_provider_t provider;
            ret = sdskv_provider_register(mid, i+1,
                    SDSKV_ABT_POOL_DEFAULT,
                    &provider);

            if(ret != 0)
            {
                fprintf(stderr, "Error: sdskv_provider_register()\n");
                margo_finalize(mid);
                return(-1);
            }

            char* path = opts.db_names[i];
            char* x = strrchr(path, '/');
            char* db_name = path;
            if(x != NULL) {
                db_name = x+1;
                *x = '\0';
            }

            sdskv_database_id_t db_id;
            sdskv_config_t db_config = { 
                .db_name = db_name,
                .db_path = (x == NULL ? "" : path),
                .db_type = opts.db_types[i],
                .db_comp_fn_name = SDSKV_COMPARE_DEFAULT,
                .db_no_overwrite = 0
            };
            ret = sdskv_provider_attach_database(provider, &db_config, &db_id);

            if(ret != 0)
            {
                fprintf(stderr, "Error: bake_provider_attach_database()\n");
                margo_finalize(mid);
                return(-1);
            }

            if(use_poolset) {
                sdskv_provider_configure_bulk_poolset(provider,
                        poolset_npools, poolset_nbufs, poolset_first_size, poolset_size_multiple);
            }
            printf("Provider %d managing database \"%s\" at multiplex id %d\n", i, opts.db_names[i], i+1);
        }

    } else {

        int i;
        sdskv_provider_t provider;
        ret = sdskv_provider_register(mid, 1,
                SDSKV_ABT_POOL_DEFAULT,
                &provider);

        if(ret != 0)
        {
            fprintf(stderr, "Error: sdskv_provider_register()\n");
            margo_finalize(mid);                                    
            return(-1);
        }

        for(i=0; i < opts.num_db; i++) {
            sdskv_database_id_t db_id;
            char* path = opts.db_names[i];
            char* x = strrchr(path, '/');
            char* db_name = path;
            if(x != NULL) {
                db_name = x+1;
                *x = '\0';
            }
            sdskv_config_t db_config = {
                .db_name = db_name,
                .db_path = (x == NULL ? "" : path),
                .db_type = opts.db_types[i],
                .db_comp_fn_name = SDSKV_COMPARE_DEFAULT,
                .db_no_overwrite = 0
            };
            ret = sdskv_provider_attach_database(provider, &db_config, &db_id);

            if(ret != 0)
            {
                fprintf(stderr, "Error: sdskv_provider_attach_database()\n");
                margo_finalize(mid);                                    
                return(-1);
            }

            if(use_poolset) {
                sdskv_provider_configure_bulk_poolset(provider,
                        poolset_npools, poolset_nbufs, poolset_first_size, poolset_size_multiple);
            }
            printf("Provider 0 managing database \"%s\" at multiplex id %d\n", opts.db_names[i] , 1);
        }
    }

    /* suspend until the BAKE server gets a shutdown signal from the client */
    margo_wait_for_finalize(mid);

    free(opts.db_names);

    return(0);
}
