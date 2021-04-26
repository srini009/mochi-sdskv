#ifndef __SDSKV_SERVER_HPP
#define __SDSKV_SERVER_HPP

#include <sdskv-server.h>
#include <sdskv-common.hpp>

#define _CHECK_RET(__ret) \
            if(__ret != SDSKV_SUCCESS) throw exception(__ret)

namespace sdskv {

/**
 * @brief The provider class wraps an sdskv_provider_t C handle.
 */
class provider {

    margo_instance_id m_mid     = MARGO_INSTANCE_NULL;
    sdskv_provider_t m_provider = NULL;

    /**
     * @brief Constructor is private. Use the create() factory method.
     *
     * @param mid Margo instance.
     * @param provider_id Provider id.
     * @param pool Argobots pool.
     * @param config JSON configuration.
     */
    provider(margo_instance_id mid,
             uint16_t provider_id = 0,
             ABT_pool pool = SDSKV_ABT_POOL_DEFAULT,
             const std::string& config = std::string())
    : m_mid(mid)
    {
        sdskv_provider_init_info args;
        args.json_config = config.empty() ? nullptr : config.c_str();
        args.rpc_pool = pool;
        int ret = sdskv_provider_register(mid, provider_id, &args, &m_provider);
        _CHECK_RET(ret);
    }

    static void finalize_callback(void* args) {
        auto* p = static_cast<provider*>(args);
        delete p;
    }

    public:

    /**
     * @brief Create a pointer to a provider. The provider will be automatically
     * deleted when Margo finalizes, unless the user calls delete before.
     *
     * @param mid Margo instance.
     * @param provider_id Provider id.
     * @param pool Argobots pool.
     * @param config JSON configuration.
     *
     * @return Pointer to a newly allocated provider.
     */
    static provider* create(margo_instance_id mid,
                            uint16_t provider_id = 0,
                            ABT_pool pool = SDSKV_ABT_POOL_DEFAULT,
                            const std::string& config = std::string())
    {
        auto p = new provider(mid, provider_id, pool, config);
        margo_provider_push_finalize_callback(mid, p, &finalize_callback, p);
        return p;
    }

    /**
     * @brief Copy constructor is deleted.
     */
    provider(const provider&) = delete;

    /**
     * @brief Move constructor is deleted.
     */
    provider(provider&& other) = delete;

    /**
     * @brief Copy-assignment operator is deleted.
     */
    provider& operator=(const provider&) = delete;

    /**
     * @brief Move-assignment operator is deleted.
     */
    provider& operator=(provider&&) = delete;

    /**
     * @brief Destructor. Deregisters the provider.
     */
    ~provider() {
        margo_provider_pop_finalize_callback(m_mid, this);
        sdskv_provider_destroy(m_provider);
    }

    /**
     * @brief Add a comparison function.
     *
     * @param name Name for the comparison function.
     * @param comp_fn Comparison function pointer.
     */
    void add_comparison_function(const std::string& name,
                                 sdskv_compare_fn comp_fn) {
        int ret = sdskv_provider_add_comparison_function(m_provider, name.c_str(), comp_fn);
        _CHECK_RET(ret);
    }

    /**
     * @brief Attach a database and returns the database id.
     *
     * @param config Database configuration.
     *
     * @return Database id.
     */
    sdskv_database_id_t attach_database(const sdskv_config_t& config) {
        sdskv_database_id_t db_id;
        int ret = sdskv_provider_attach_database(m_provider, &config, &db_id);
        _CHECK_RET(ret);
        return db_id;
    }

    /**
     * @brief Remove a database (this will not remove the underlying files).
     *
     * @param db_id Database id of the database to remove.
     */
    void remove_database(sdskv_database_id_t db_id) {
        int ret = sdskv_provider_remove_database(m_provider, db_id);
        _CHECK_RET(ret);
    }

    /**
     * @brief Remove all the databases from this provider (this will not
     * remove the underlying files).
     */
    void remove_all_databases() {
        int ret = sdskv_provider_remove_all_databases(m_provider);
        _CHECK_RET(ret);
    }

    /**
     * @brief Returns the list of databases handled by this provider.
     *
     * @return Vector of database ids.
     */
    std::vector<sdskv_database_id_t> databases() const {
        std::vector<sdskv_database_id_t> dbs;
        uint64_t num_dbs;
        int ret = sdskv_provider_count_databases(m_provider, &num_dbs);
        _CHECK_RET(ret);
        dbs.resize(num_dbs);
        ret = sdskv_provider_list_databases(m_provider, dbs.data());
        _CHECK_RET(ret);
    }

    /**
     * @brief Compute the size of a database (combined sizes of all files
     * that make up the database). Requires REMI support.
     *
     * @param db_id Database id.
     *
     * @return Size of the specified database.
     */
    size_t compute_database_size(sdskv_database_id_t db_id) const {
        size_t size;
        int ret = sdskv_provider_compute_database_size(
                    m_provider,
                    db_id,
                    &size);
        _CHECK_RET(ret);
        return size;
    }

    /**
     * @brief Registers migration callbacks for REMI to use.
     *
     * @param pre_cb Pre-migration callback.
     * @param post_cb Post-migration callback.
     * @param uargs User arguments.
     */
    void set_migration_callbacks(sdskv_pre_migration_callback_fn pre_cb,
                                 sdskv_post_migration_callback_fn post_cb,
                                 void* uargs) {
        int ret = sdskv_provider_set_migration_callbacks(m_provider,
                        pre_cb, post_cb, uargs);
        _CHECK_RET(ret);
    }

};

}
#undef _CHECK_RET
#endif
