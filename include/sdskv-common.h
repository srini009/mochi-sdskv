#ifndef SDSKV_COMMON_H
#define SDSKV_COMMON_H

#if defined(__cplusplus)
extern "C" {
#endif

typedef enum sdskv_db_type_t
{
    KVDB_MAP = 0, /* Datastore implementation using std::map   */
    KVDB_NULL, /* Datastore implementation that discards evrything it receives*/
    KVDB_BWTREE,     /* Datastore implementation using a BwTree   */
    KVDB_LEVELDB,    /* Datastore implementation using LevelDB    */
    KVDB_BERKELEYDB, /* Datastore implementation using BerkeleyDB */
    KVDB_FORWARDDB   /* Datastore implementation forwarding to secondary DB */
} sdskv_db_type_t;

typedef uint64_t sdskv_database_id_t;
#define SDSKV_DATABASE_ID_INVALID 0

#define SDSKV_KEEP_ORIGINAL 0 /* for migration operations, keep original */
#define SDSKV_REMOVE_ORIGINAL \
    1 /* for migration operations, remove the origin after migrating */

/* Errors in SDSKV are int32_t. The most-significant byte stores the Argobots
 * error, if any. The second most-significant byte stores the Mercury error, if
 * any. The next 2 bytes store the SDSKV error code defined bellow. Argobots and
 * Mercury errors should be built using SDSKV_MAKE_HG_ERROR and
 * SDSKV_MAKE_ABT_ERROR. */

#define SDSKV_RETURN_VALUES                               \
    X(SDSKV_SUCCESS, "Success")                           \
    X(SDSKV_ERR_ALLOCATION, "Allocation error")           \
    X(SDSKV_ERR_INVALID_ARG, "Invalid argument")          \
    X(SDSKV_ERR_PR_EXISTS, "Provide id already used")     \
    X(SDSKV_ERR_DB_CREATE, "Could not create database")   \
    X(SDSKV_ERR_DB_NAME, "Invalid database name")         \
    X(SDSKV_ERR_UNKNOWN_DB, "Invalid database id")        \
    X(SDSKV_ERR_UNKNOWN_PR, "Invalid provider id")        \
    X(SDSKV_ERR_PUT, "Error writing in the database")     \
    X(SDSKV_ERR_UNKNOWN_KEY, "Unknown key")               \
    X(SDSKV_ERR_SIZE, "Provided buffer size too small")   \
    X(SDSKV_ERR_ERASE, "Error erasing from the database") \
    X(SDSKV_ERR_MIGRATION, "Migration error")             \
    X(SDSKV_OP_NOT_IMPL, "Function not implemented")      \
    X(SDSKV_ERR_COMP_FUNC, "Invalid comparison function") \
    X(SDSKV_ERR_REMI, "REMI error")                       \
    X(SDSKV_ERR_KEYEXISTS, "Key exists")                  \
    X(SDSKV_ERR_CONFIG, "Bad configuration")              \
    X(SDSKV_ERR_MAX, "End of range for valid error codes")

#define X(__err__, __msg__) __err__,
typedef enum
{
    SDSKV_RETURN_VALUES
} sdskv_return_t;
#undef X

static const char* const sdskv_error_messages[] = {
#define X(__err__, __msg__) __msg__,
    SDSKV_RETURN_VALUES
#undef X
};

#define SDSKV_MAKE_HG_ERROR(__hg_err__) (((int32_t)(__hg_err__)) << 24)

#define SDSKV_MAKE_ABT_ERROR(__abt_err__) (((int32_t)(__abt_err__)) << 16)

#define SDSKV_GET_HG_ERROR(__err__) (((__err__) & (0b11111111 << 24)) >> 24)

#define SDSKV_GET_ABT_ERROR(__err__) (((__err__) & (0b11111111 << 16)) >> 16)

#define SDSKV_ERROR_IS_HG(__err__) ((__err__) & (0b11111111 << 24))

#define SDSKV_ERROR_IS_ABT(__err__) ((__err__) & (0b11111111 << 16))

#if defined(__cplusplus)
}
#endif

#endif
