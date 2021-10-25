/*
 * (C) 2015 The University of Chicago
 * 
 * See COPYRIGHT in top-level directory.
 */

#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <unistd.h>
#include <margo.h>
#include <sdskv-server.h>
#include <mpi.h>
#include <string.h>

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
    fprintf(stderr, "Usage: sdskv-aggr-service [OPTIONS] <listen_addr> <db name 1>[:map|:bwt|:bdb|:ldb] <db name 2>[:map|:bwt|:bdb|:ldb] ...\n");
    fprintf(stderr, "       listen_addr is the Mercury address to listen on\n");
    fprintf(stderr, "       db name X are the names of the databases\n");
    fprintf(stderr, "       [-f filename] to write the server address to a file\n");
    fprintf(stderr, "       [-m mode] multiplexing mode (providers or databases) for managing multiple databases (default is databases)\n"); 
    fprintf(stderr, "Example: mpirun -np 4 sdskv-aggr-service ofi+verbs:// foo:bdb \n");
    return;
}

static sdskv_db_type_t parse_db_type(char* db_fullname) {
    char* column = strstr(db_fullname, ":");
    if(column == NULL) {
        return KVDB_MAP;
    }
    *column = '\0';
    char* db_type = column + 1;
    if(strcmp(db_type, "null") == 0) {
        return KVDB_NULL;
    } else if(strcmp(db_type, "map") == 0) {
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
    MPI_Init(&argv, &argv);
    int rank, size;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    struct options opts;
    margo_instance_id mid;
    int ret;

    parse_args(argc, argv, &opts);

    /* start margo */
    /* use the main xstream for driving progress and executing rpc handlers */
    mid = margo_init(opts.listen_addr_str, MARGO_SERVER_MODE, 1, 1);
    if(mid == MARGO_INSTANCE_NULL)
    {
        fprintf(stderr, "Error: margo_init()\n");
        return(-1);
    }

    margo_enable_remote_shutdown(mid);

    /* initialize the SDSKV server */
    	int i;
	sdskv_provider_t provider;
	ret = sdskv_provider_register(mid, 1,
                SDSKV_PROVIDER_INIT_INFO_INIT,
                &provider);

        if(ret != 0)
        {
            fprintf(stderr, "Error: sdskv_provider_register()\n");
            margo_finalize(mid);                                    
            return(-1);
        }

        sdskv_database_id_t db_id;
        char* path_ = opts.db_names[0];
        char* x = strrchr(path_, '/');
        char* db_name = path_;
        if(x != NULL) {
            db_name = x+1;
            *x = '\0';
        }

	char rank_str[20];
	sprintf(rank_str, "%d", rank);
        char * path = (char*)malloc((strlen(path_)+20)*sizeof(char));
	strcpy(path, path_);	
	strcat(path, rank_str);

        sdskv_config_t db_config = {
            .db_name = db_name,
            .db_path = (x == NULL ? "" : path),
            .db_type = opts.db_types[0],
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

        //printf("Provider 0 managing database \"%s\" at multiplex id %d\n", db_name , 1);

#ifdef USE_SYMBIOMON
        /* initialize SYMBIOMON */
        struct symbiomon_provider_args args = SYMBIOMON_PROVIDER_ARGS_INIT;
        args.push_finalize_callback = 0;


        symbiomon_provider_t metric_provider;
        ret = symbiomon_provider_register(mid, 42, &args, &metric_provider);
        if(ret != 0)
            fprintf(stderr, "Error: symbiomon_provider_register() failed. Continuing on.\n");
           
        ret = sdskv_provider_set_symbiomon(provider, metric_provider);
        if(ret != 0)
            fprintf(stderr, "Error: sdskv_provider_set_symbiomon() failed. Contuinuing on.\n");
#endif

    if(opts.host_file)
    {
        /* write the server address to file if requested */
        FILE *fp;
        hg_addr_t self_addr;
        char self_addr_str[128];
        hg_size_t self_addr_str_sz = 128;
        hg_return_t hret;
        fprintf(stderr, "Writing addresses to host file...\n");

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

        // Write addresses to a file
        if(!rank) {
            fp = fopen(opts.host_file, "w");
	    if(!fp)
            {
	    	perror("fopen");
	        margo_finalize(mid);
	        return(-1);
	    }
	    fprintf(fp, "%d\n", size);
	    fclose(fp);
	}

	MPI_Barrier(MPI_COMM_WORLD);
	    
	int i = 0;
	for(i = 0; i < size; i++) {
        	if(rank == i) {
		    fprintf(stderr, "Rank: %d is writing out its address..\n", i);
	            fp = fopen(opts.host_file, "a");
	            if(!fp)
        	    {
	                perror("fopen");
	                margo_finalize(mid);
	                return(-1);
	            }
 
	            fprintf(fp, "%s %d %s\n", self_addr_str, 1, db_name);
		    fflush(fp);

	            fclose(fp);
 	       }
	       MPI_Barrier(MPI_COMM_WORLD);
	}
         

        margo_addr_free(mid, self_addr);

    }
    /* suspend until the BAKE server gets a shutdown signal from the client */
    margo_wait_for_finalize(mid);

    free(opts.db_names);

    MPI_Finalize();

    return(0);
}
