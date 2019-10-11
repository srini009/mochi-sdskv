#ifndef __SDSKV_CLIENT_HPP
#define __SDSKV_CLIENT_HPP

#include <type_traits>
#include <stdexcept>
#include <vector>
#include <string>
#include <sdskv-client.h>
#include <sdskv-common.hpp>

#define _CHECK_RET(__ret) \
        if(__ret != SDSKV_SUCCESS) throw exception(__ret)

namespace sdskv {


// The functions bellow (object_size, object_data, object_resize) are defined
// for std::string and std::vector<X> when X is a standard layout type. They
// are used in place of x.size(), x.data(), and x.resize() to allow better
// genericity. Also if a user wants to add support for another type, overloading
// these functions is the way to go: the object_data() function must return a
// void* pointer to where data from an object can be accessed directory.
// object_size() functions must return the size (in bytes) of the underlying
// buffer. object_resize must be able to resize the object.

template<typename T, std::enable_if_t<std::is_standard_layout<T>::value, int> = 0>
inline size_t object_size(const std::vector<T>& v) {
    return sizeof(v[0])*v.size();
}

inline size_t object_size(const std::string& s) {
    return s.size();
}

template<typename T, std::enable_if_t<std::is_standard_layout<T>::value, int> = 0>
inline void* object_data(std::vector<T>& v) {
    return (void*)v.data();
}

inline void* object_data(std::string& s) {
    return (void*)s.data();
}

template<typename T, std::enable_if_t<std::is_standard_layout<T>::value, int> = 0>
inline const void* object_data(const std::vector<T>& v) {
    return (const void*)v.data();
}

inline const void* object_data(const std::string& s) {
    return (const void*)s.data();
}

template<typename T, std::enable_if_t<std::is_standard_layout<T>::value, int> = 0>
inline void object_resize(std::vector<T>& v, size_t new_size) {
    v.resize(new_size);
}

inline void object_resize(std::string& s, size_t new_size) {
    s.resize(new_size);
}

class provider_handle;
class database;

/**
 * @brief The sdskv::client class is the C++ equivalent of a C sdskv_client_t.
 */
class client {

    friend class provider_handle;

    margo_instance_id m_mid = MARGO_INSTANCE_NULL;
    sdskv_client_t m_client = SDSKV_CLIENT_NULL;
    bool           m_owns_client = true;


    public:

    /**
     * @brief Default constructor. Will create an invalid client.
     */
    client() = default;

    /**
     * @brief Main constructor. Creates a valid client from a Margo instance.
     *
     * @param mid Margo instance.
     */
    client(margo_instance_id mid)
    : m_mid(mid) {
        sdskv_client_init(mid, &m_client);
    }

    /**
     * @brief Creates a valid client from an existing sdskv_client_t.
     *
     * @param mid Margo instance.
     * @param c Existing C client.
     * @param transfer_ownership Whether this client instance should take ownership
     * and free the underlying handle in its destructor.
     */
    client(margo_instance_id mid, sdskv_client_t c, bool transfer_ownership=false)
    : m_mid(mid)
    , m_client(c)
    , m_owns_client(transfer_ownership) {}

    /**
     * @brief Deleted copy constructor.
     */
    client(const client& c) = delete;

    /**
     * @brief Move constructor, will invalidate the client that is moved from.
     */
    client(client&& c)
    : m_mid(c.m_mid)
    , m_client(c.m_client)
    , m_owns_client(c.m_owns_client) {
        c.m_client = SDSKV_CLIENT_NULL;
    }

    /**
     * @brief Deleted copy-assignment operator.
     */
    client& operator=(const client& c) = delete;

    /**
     * @brief Move assignment operator, will invalidate the client that is moved from.
     */
    client& operator=(client&& c) {
        if(this == &c) return *this;
        if(m_client && m_owns_client) {
            sdskv_client_finalize(m_client);
        }
        m_mid           = c.m_mid;
        m_client        = c.m_client;
        m_owns_client   = c.m_owns_client;
        c.m_client      = SDSKV_CLIENT_NULL;
        c.m_owns_client = true;
        return *this;
    }

    /**
     * @brief Destructor. If this instance owns the underlying C handle, this handle
     * will be destroyed.
     */
    ~client() {
        if(m_owns_client && m_client)
            sdskv_client_finalize(m_client);
    }

    /**
     * @brief Cast operator to sdskv_client_t.
     */
    operator sdskv_client_t() const {
        return m_client;
    }

    /**
     * @brief Cast to bool.
     *
     * @return true if the client is valid, false otherwise.
     */
    operator bool() const {
        return m_client != SDSKV_CLIENT_NULL;
    }

    /**
     * @brief Configures a poolset to be used for all transfers
     * from this client.
     *
     * @param npools Number of pools
     * @param nbufs Number of buffers per pool
     * @param first_size Size of buffers in the first pool
     * @param size_multiple Buffer size multiple from a pool to the next one
     */
    void configure_bulk_poolset(
            size_t npools,
            size_t nbufs,
            size_t first_size,
            size_t size_multiple) {
        int ret = sdskv_client_configure_bulk_poolset(m_client,
                npools, nbufs, first_size, size_multiple);
        _CHECK_RET(ret);
    }

    /**
     * @brief Open a database held by a given provider.
     *
     * @param ph Provider handle.
     * @param db_name Database name.
     *
     * @return database instance.
     */
    database open(const provider_handle& ph, const std::string& db_name) const;

    /**
     * @brief Open all the databases held by a given provider.
     *
     * @param ph Provider handle.
     *
     * @return Vector of database instances.
     */
    std::vector<database> open(const provider_handle& ph) const;

    //////////////////////////
    // PUT methods
    //////////////////////////

    /**
     * @brief Equivalent of sdskv_put.
     *
     * @param db Database instance.
     * @param key Key.
     * @param ksize Size of the key in bytes.
     * @param value Value.
     * @param vsize Size of the value in bytes.
     */
    void put(const database& db,
             const void *key, hg_size_t ksize,
             const void *value, hg_size_t vsize) const;

    /**
     * @brief Put method taking templated keys and values. This method
     * is meant to work with std::vector<X> and std::string. X must be
     * a standard layout type.
     *
     * @tparam K std::vector<char> or std::string.
     * @tparam V std::vector<char> or std::string.
     * @param db Database instance.
     * @param key Key.
     * @param value Value.
     */
    template<typename K, typename V>
    inline void put(const database& db,
             const K& key, const V& value) const {
        put(db, object_data(key), object_size(key), object_data(value), object_size(value));
    }

    //////////////////////////
    // PUT_MULTI methods
    //////////////////////////

    /**
     * @brief Equivalent to sdskv_put_multi.
     *
     * @param db Database instance.
     * @param count Number of key/val pairs.
     * @param keys Array of keys.
     * @param ksizes Array of key sizes.
     * @param values Array of values.
     * @param vsizes Array of value sizes.
     */
    void put_multi(const database& db,
             hg_size_t count, const void* const* keys, const hg_size_t* ksizes,
             const void* const* values, const hg_size_t *vsizes) const;


    /**
     * @brief Version of put taking std::vectors instead of arrays of pointers.
     *
     * @param db Database instance.
     * @param keys Vector of pointers to keys.
     * @param ksizes Vector of key sizes.
     * @param values Vector of pointers to values.
     * @param vsizes Vector of value sizes.
     */
    inline void put_multi(const database& db,
             const std::vector<const void*>& keys, const std::vector<hg_size_t>& ksizes,
             const std::vector<const void*>& values, const  std::vector<hg_size_t>& vsizes) const {
        if(keys.size() != ksizes.size()
        || keys.size() != values.size()
        || keys.size() != vsizes.size()) {
            throw std::length_error("Provided vectors should have the same size");
        }
        put_multi(db, keys.size(), keys.data(),  ksizes.data(), values.data(), vsizes.data());
    }

    /**
     * @brief Templated put method. Meant to work with std::vector<X> and std::string types.
     * X must be a standard layout  type.
     *
     * @tparam K std::vector<char> or std::string.
     * @tparam V std::vector<char> or std::string.
     * @param db Database instance.
     * @param keys Vector of keys.
     * @param values Vector of values.
     */
    template<typename K, typename V>
    inline void put_multi(const database& db,
             const std::vector<K>& keys, const std::vector<V>& values) {
        if(keys.size() != values.size()) {
            throw std::length_error("Provided vectors should have the same size");
        }
        std::vector<const void*> kdata; kdata.reserve(keys.size());
        std::vector<const void*> vdata; vdata.reserve(values.size());
        std::vector<hg_size_t> ksizes; ksizes.reserve(keys.size());
        std::vector<hg_size_t> vsizes; vsizes.reserve(values.size());
        for(const auto& k : keys) {
            ksizes.push_back(object_size(k));
            kdata.push_back(object_data(k));
        }
        for(const auto& v : values) {
            vsizes.push_back(object_size(v));
            vdata.push_back(object_data(v));
        }
        put_multi(db, kdata, ksizes, vdata, vsizes);
    }

    /**
     * @brief Put method taking iterators to templated key and value types,
     * meant to work with keys and values of type std::vector<X> and std::string.
     * X must be a standard layout type.
     *
     * @tparam IK Type of iterator to keys.
     * @tparam IV Type of iterator to values.
     * @param db Database instance.
     * @param kbegin beginning of the iterator to keys.
     * @param kend end of the iterator to keys.
     * @param vbegin beginning of the iterator to values.
     * @param vend end of the iterator to values.
     */
    template<typename IK, typename IV>
    inline void put_multi(const database& db,
             const IK& kbegin, const IK& kend,
             const IV& vbegin, const IV& vend) const {
        hg_size_t count = std::distance(kbegin, kend);
        if(count != std::distance(vbegin, vend)) {
            throw std::length_error("Provided iterators should point to the same number of objects");
        }
        std::vector<const void*> kdata; kdata.reserve(count);
        std::vector<const void*> vdata; vdata.reserve(count);
        std::vector<hg_size_t> ksizes; ksizes.reserve(count);
        std::vector<hg_size_t> vsizes; vsizes.reserve(count);
        for(auto it = kbegin; it != kend; it++) {
            ksizes.push_back(object_size(*it));
            kdata.push_back(object_data(*it));
        }
        for(auto it = vbegin; it != vend; it++) {
            vsizes.push_back(object_size(*it));
            vdata.push_back(object_data(*it));
        }
        put_multi(db, kdata, ksizes, vdata, vsizes);
    }

    //////////////////////////
    // EXISTS methods
    //////////////////////////

    /**
     * @brief Equivalent of sdskv_exists.
     *
     * @param db Database instance.
     * @param key Key.
     * @param ksize Size of the key.
     *
     * @return true if key exists, false otherwise.
     */
    bool exists(const database& db, const void* key, hg_size_t ksize) const;

    /**
     * @brief Templated version of exists method, meant to work with
     * std::string and std::vector<X>. X must be a standard layout type.
     *
     * @tparam K Key type.
     * @param db Database instance.
     * @param key Key.
     *
     * @return true if key exists, false otherwise.
     */
    template<typename K>
    inline bool exists(const database& db, const K& key) const {
        return exists(db, object_data(key), object_size(key));
    }

    //////////////////////////
    // LENGTH methods
    //////////////////////////

    /**
     * @brief Length of the value associated with the provided key.
     *
     * @param db Database instance.
     * @param key Key.
     * @param ksize Size of the key.
     *
     * @return size of the corresponding value.
     */
    hg_size_t length(const database& db,
            const void* key, hg_size_t ksize) const;

    /**
     * @brief Templated version of the length method, meant to
     * work with std::vector<X> and std::string. X must be a standard
     * layout type.
     *
     * @tparam K Key type.
     * @param db Database instance.
     * @param key Key.
     *
     * @return size of the corresponding value.
     */
    template<typename K>
    inline hg_size_t length(const database& db,
            const K& key) const {
        return length(db, object_data(key), object_size(key));
    }

    //////////////////////////
    // LENGTH_MULTI methods
    //////////////////////////

    /**
     * @brief Equivalent to sdskv_length_multi.
     *
     * @param db Database instance.
     * @param num Number of keys.
     * @param keys Array of keys.
     * @param ksizes Array of key sizes.
     * @param vsizes Resulting value sizes.
     */
    bool length_multi(const database& db,
            hg_size_t num, const void* const* keys,
            const hg_size_t* ksizes, hg_size_t* vsizes) const;

    /**
     * @brief Templated version of length, meant to work with
     * std::vector<X> and std::string. X must be a standard layout type.
     *
     * @tparam K Type of keys.
     * @param db Database instance.
     * @param keys Vector of keys.
     * @param vsizes Resulting vector of value sizes.
     */
    template<typename K>
    inline bool length_multi(const database& db,
            const std::vector<K>& keys,
            std::vector<hg_size_t>& vsizes) const {
        vsizes.resize(keys.size());
        std::vector<const void*> kdata; kdata.reserve(keys.size());
        std::vector<hg_size_t> ksizes; ksizes.reserve(keys.size());
        for(const auto& k : keys) {
            kdata.push_back(object_data(k));
            ksizes.push_back(object_size(k));
        }
        return length_multi(db, keys.size(), kdata.data(), ksizes.data(), vsizes.data());
    }

    /**
     * @brief Templated version of length returning a vector of sizes.
     * Meant to work with std::vector<X> and std::string.
     * X must be a standard layout type.
     *
     * @tparam K Key type.
     * @param db Database instance.
     * @param keys Vector of keys.
     *
     * @return Vector of corresponding value sizes.
     */
    template<typename K>
    inline std::vector<hg_size_t> length_multi(const database& db, const std::vector<K>& keys) const {
        std::vector<hg_size_t> vsizes(keys.size());
        length_multi(db, keys, vsizes);
        return vsizes;
    }

    //////////////////////////
    // GET methods
    //////////////////////////

    /**
     * @brief Equivalent of sdskv_get. Will throw an exception if the key doesn't exist,
     * or if the buffer allocated for the value is too small.
     *
     * @param db Database instance.
     * @param key Key.
     * @param ksize Size of the key.
     * @param value Pointer to a buffer allocated for the value.
     * @param vsize Size of the value buffer.
     */
    bool get(const database& db,
             const void* key, hg_size_t ksize,
             void* value, hg_size_t* vsize) const;

    /**
     * @brief Templated version of get, meant to be used with std::vector<X> and std::string.
     * X must be a standard layout type.
     *
     * @tparam K Key type.
     * @tparam V Value type.
     * @param db Database instance.
     * @param key Key.
     * @param value Value.
     */
    template<typename K, typename V>
    inline bool get(const database& db,
             const K& key, V& value) const {
        hg_size_t s = value.size();
        if(s == 0) {
            s = length(db, key);
            object_resize(value, s);
        }
        try {
            get(db, object_data(key), object_size(key), object_data(value), &s);
        } catch(exception& ex) {
            if(ex.error() == SDSKV_ERR_SIZE) {
                object_resize(value, s);
                get(db, object_data(key), object_size(key), object_data(value), &s);
            } else {
                throw;
            }
        }
        object_resize(value, s);
        return true;
    }

    /**
     * @brief Get method returning its value instead of using an argument.
     * Meant to work with K and V being std::string or std::vector<X, with
     * X a standard layout type.
     *
     * @tparam K Key type.
     * @tparam V Value type.
     * @param db Database instance.
     * @param key Key.
     *
     * @return The value associated with the provided key.
     */
    template<typename K, typename V>
    inline V get(const database& db,
          const K& key) const {
        V value;
        get(db, key, value);
        return value;
    }

    //////////////////////////
    // GET_MULTI methods
    //////////////////////////

    /**
     * @brief Equivalent to sdskv_get_multi.
     *
     * @param db Database instance.
     * @param count Number of key/val pairs.
     * @param keys Array of keys.
     * @param ksizes Array of key sizes.
     * @param values Array of value buffers.
     * @param vsizes Array of sizes of value buffers.
     */
    bool get_multi(const database& db,
             hg_size_t count, const void* const* keys, const hg_size_t* ksizes,
             void** values, hg_size_t *vsizes) const;

    /**
     * @brief Get multiple key/val pairs using std::vector of addresses.
     *
     * @param db Database instance.
     * @param keys Vector of key addresses.
     * @param ksizes Vector of key sizes.
     * @param values Vector of value addresses.
     * @param vsizes Vector of value sizes.
     */
    inline bool get_multi(const database& db,
             const std::vector<const void*>& keys, const std::vector<hg_size_t>& ksizes,
             std::vector<void*>& values, std::vector<hg_size_t>& vsizes) const {
        if(keys.size() != ksizes.size()
        || keys.size() != values.size()
        || keys.size() != vsizes.size()) {
            throw std::length_error("Provided vectors should have the same size");
        }
        return get_multi(db, keys.size(), keys.data(),  ksizes.data(), values.data(), vsizes.data());
    }

    /**
     * @brief Get multiple key/val pairs using templated keys and values.
     * Meant to work with std::string and std::vector<X> where X is a standard
     * layout type.
     *
     * @tparam K Key type.
     * @tparam V Value type.
     * @param db Database instance.
     * @param keys Vector of keys.
     * @param values Vector of values.
     */
    template<typename K, typename V>
    inline bool get_multi(const database& db,
             const std::vector<K>& keys, std::vector<V>& values) const {
        if(keys.size() != values.size()) {
            throw std::length_error("Provided vectors should have the same size");
        }
        std::vector<const void*> kdata; kdata.reserve(keys.size());
        std::vector<void*> vdata; vdata.reserve(values.size());
        std::vector<hg_size_t> ksizes; ksizes.reserve(keys.size());
        std::vector<hg_size_t> vsizes; vsizes.reserve(values.size());
        for(const auto& k : keys) {
            ksizes.push_back(object_size(k));
            kdata.push_back(object_data(k));
        }
        for(auto& v : values) {
            vsizes.push_back(object_size(v));
            vdata.push_back(object_data(v));
        }
        get_multi(db, kdata, ksizes, vdata, vsizes);
        for(unsigned i=0; i < values.size(); i++) {
            object_resize(values[i], vsizes[i]);
        }
        return true;
    }

    /**
     * @brief Get method using iterator to templated keys and values.
     * Meant to work with std::string and std::vector<X> with X a standard
     * layout type.
     *
     * @tparam IK Key iterator type.
     * @tparam IV Value iterator type.
     * @param db Database instance.
     * @param kbegin Beginning iterator for keys.
     * @param kend End iterator for keys.
     * @param vbegin Beginning iterator for values.
     * @param vend End iterator for values.
     */
    template<typename IK, typename IV>
    inline bool get_multi(const database& db,
             const IK& kbegin, const IK& kend,
             const IV& vbegin, const IV& vend) const {
        hg_size_t count = std::distance(kbegin, kend);
        if(count != std::distance(vbegin, vend)) {
            throw std::length_error("Provided iterators should point to the same number of objects");
        }
        std::vector<const void*> kdata; kdata.reserve(count);
        std::vector<void*> vdata; vdata.reserve(count);
        std::vector<hg_size_t> ksizes; ksizes.reserve(count);
        std::vector<hg_size_t> vsizes; vsizes.reserve(count);
        for(auto it = kbegin; it != kend; it++) {
            ksizes.push_back(object_size(*it));
            kdata.push_back(object_data(*it));
        }
        for(auto it = vbegin; it != vend; it++) {
            vsizes.push_back(object_size(*it));
            vdata.push_back(object_data(*it));
        }
        return get_multi(db, kdata, ksizes, vdata, vsizes);
    }

    /**
     * @brief Get method that returns an a vector of values.
     * Meant to work with std::string and std::vector<X> where X
     * is a standard layout type.
     *
     * @tparam K Key type.
     * @tparam V Value type.
     * @param db Database instance.
     * @param keys Keys.
     *
     * @return the resulting std::vector of values.
     */
    template<typename K, typename V>
    inline std::vector<V> get_multi(
            const database& db,
            const std::vector<K>& keys) {
        hg_size_t num = keys.size();
        std::vector<hg_size_t> vsizes(num);
        length_multi(db, keys, vsizes);
        std::vector<V> values(num);
        for(unsigned i=0 ; i < num; i++) {
            object_resize(values[i], vsizes[i]);
        }
        get_multi(db, keys, values);
        return values;
    }

    //////////////////////////
    // ERASE methods
    //////////////////////////
    
    /**
     * @brief Equivalent to sdskv_erase.
     *
     * @param db Database instance.
     * @param key Key.
     * @param ksize Size of the key.
     */
    void erase(const database& db,
               const void* key,
               hg_size_t ksize) const;

    /**
     * @brief Templated erase method, meant to be used
     * with keys of type std::string or std::vector<X> where X is a
     * standard layout type.
     *
     * @tparam K Key type.
     * @param db Database instance.
     * @param key Key.
     */
    template<typename K>
    inline void erase(const database& db,
               const K& key) const {
        erase(db, object_data(key), object_size(key));
    }

    //////////////////////////
    // ERASE_MULTI methods
    //////////////////////////

    /**
     * @brief Equivalent to sdskv_erase_multi.
     *
     * @param db Database instance.
     * @param num Number of key/value pairs to erase.
     * @param keys Array of keys.
     * @param ksizes Array of key sizes.
     */
    void erase_multi(const database& db,
            hg_size_t num, const void* const* keys,
            const hg_size_t* ksizes) const;

    /**
     * @brief Erase a vector of keys. The key type K should be
     * std::string or std::vector<X> with X being a standard layout
     * type.
     *
     * @tparam K Type of 
     * @param db Database instance.
     * @param keys Vector of keys to erase.
     */
    template<typename K>
    inline void erase_multi(const database& db,
            const std::vector<K>& keys) const {
        std::vector<const void*> kdata; kdata.reserve(keys.size());
        std::vector<hg_size_t> ksizes; ksizes.reserve(keys.size());
        for(const auto& k : keys) {
            kdata.push_back(object_data(k));
            ksizes.push_back(object_size(k));
        }
        return erase_multi(db, keys.size(), kdata.data(), ksizes.data());
    }

    //////////////////////////
    // LIST_KEYS methods
    //////////////////////////
    
    /**
     * @brief Equivalent to sdskv_list_keys.
     *
     * @param db Database instance.
     * @param start_key Starting key (excluded from results).
     * @param start_ksize Starting key size.
     * @param prefix Prefix.
     * @param prefix_size Prefix size.
     * @param keys Resulting key buffers.
     * @param ksizes Resulting key buffer sizes.
     * @param max_keys Max number of keys.
     */
    void list_keys(const database& db,
            const void *start_key, hg_size_t start_ksize,
            const void *prefix, hg_size_t prefix_size,
            void** keys, hg_size_t* ksizes, hg_size_t* max_keys) const;

    inline void list_keys(const database& db,
            const void *start_key, hg_size_t start_ksize,
            void** keys, hg_size_t* ksizes, hg_size_t* max_keys) const {
        list_keys(db, start_key, start_ksize, NULL, 0, keys, ksizes, max_keys);
    }

    /**
     * @brief List keys starting from a given key (excluded).
     * K must be std::string or std::vector<X> with X a standard layout type.
     *
     * @tparam K Key type.
     * @param db Database instance.
     * @param start_key Start key.
     * @param keys Resulting keys.
     */
    template<typename K>
    inline void list_keys(const database& db,
                const K& start_key,
                std::vector<K>& keys) {
        hg_size_t max_keys = keys.size();
        if(max_keys == 0) return;
        std::vector<void*> kdata; kdata.reserve(keys.size());
        std::vector<hg_size_t> ksizes; ksizes.reserve(keys.size());
        for(auto& k : keys) {
            kdata.push_back(object_data(k));
            ksizes.push_back(object_size(k));
        }
        try {
            list_keys(db, object_data(start_key), object_size(start_key),
                kdata.data(), ksizes.data(), &max_keys);
        } catch(exception& e) {
            if(e.error() == SDSKV_ERR_SIZE && keys[0].size() == 0) {
                for(unsigned i=0; i < max_keys; i++) {
                    object_resize(keys[i], ksizes[i]);
                    kdata[i] = object_data(keys[i]);
                }
                list_keys(db, object_data(start_key), object_size(start_key),
                        kdata.data(), ksizes.data(), &max_keys);
            } else {
                throw;
            }
        }
        for(unsigned i=0; i < max_keys; i++) {
            object_resize(keys[i], ksizes[i]);
        }
        keys.resize(max_keys);
    }

    /**
     * @brief List keys with a prefix.
     *
     * @tparam K Key type.
     * @param db Database instance.
     * @param start_key Start key (excluded from results).
     * @param prefix Prefix.
     * @param keys Resulting keys.
     */
    template<typename K>
    inline void list_keys(const database& db,
                const K& start_key,
                const K& prefix,
                std::vector<K>& keys) {
        hg_size_t max_keys = keys.size();
        std::vector<void*> kdata; kdata.reserve(keys.size());
        std::vector<hg_size_t> ksizes; ksizes.reserve(keys.size());
        for(auto& k : keys) {
            kdata.push_back(object_data(k));
            ksizes.push_back(object_size(k));
        }
        try {
            list_keys(db, object_data(start_key), object_size(start_key),
                object_data(prefix), object_size(prefix),
                kdata.data(), ksizes.data(), &max_keys);
        } catch(exception& e) {
            if(e.error() == SDSKV_ERR_SIZE && object_size(keys[0]) == 0) {
                for(unsigned i=0; i < max_keys; i++) {
                    object_resize(keys[i], ksizes[i]);
                    kdata[i] = object_data(keys[i]);
                }
                list_keys(db, object_data(start_key), object_size(start_key),
                        object_data(prefix), object_size(prefix),
                        kdata.data(), ksizes.data(), &max_keys);
            } else {
                throw;
            }
        }
        for(unsigned i=0; i < max_keys; i++) {
            object_resize(keys[i], ksizes[i]);
        }
        keys.resize(max_keys);
    }

    //////////////////////////
    // LIST_KEYVALS methods
    //////////////////////////

    /**
     * @brief Same as list_keys but also returns the values.
     */
    void list_keyvals(const database& db,
            const void *start_key, hg_size_t start_ksize,
            const void *prefix, hg_size_t prefix_size,
            void** keys, hg_size_t* ksizes, 
            void** values, hg_size_t* vsizes,
            hg_size_t* max_items) const;

    /**
     * @brief Same as list_keys but also returns the values.
     */
    inline void list_keyvals(const database& db,
            const void *start_key, hg_size_t start_ksize,
            void** keys, hg_size_t* ksizes,
            void** values, hg_size_t* vsizes,
            hg_size_t* max_items) const {
        list_keyvals(db, start_key, start_ksize,
                NULL, 0, keys, ksizes, values, vsizes, max_items);
    }

    /**
     * @brief Same as list_keys but also returns the values.
     */
    template<typename K, typename V>
    inline void list_keyvals(const database& db,
                const K& start_key,
                std::vector<K>& keys,
                std::vector<V>& values) const {

        hg_size_t max_keys = std::min(keys.size(), values.size());
        std::vector<void*> kdata; kdata.reserve(keys.size());
        std::vector<hg_size_t> ksizes; ksizes.reserve(keys.size());
        std::vector<void*> vdata; vdata.reserve(values.size());
        std::vector<hg_size_t> vsizes; vsizes.reserve(values.size());
        for(auto& k : keys) {
            kdata.push_back(object_data(k));
            ksizes.push_back(object_size(k));
        }
        for(auto& v : values) {
            vdata.push_back(object_data(v));
            vsizes.push_back(object_size(v));
        }
        try {
            list_keyvals(db, object_data(start_key), object_size(start_key),
                kdata.data(), ksizes.data(), 
                vdata.data(), vsizes.data(), &max_keys);
        } catch(exception& e) {
            if(e.error() == SDSKV_ERR_SIZE) {
                for(unsigned i=0; i < max_keys; i++) {
                    object_resize(keys[i], ksizes[i]);
                    kdata[i] = object_data(keys[i]);
                    object_resize(values[i], vsizes[i]);
                    vdata[i] = object_data(values[i]);
                }
                list_keyvals(db, object_data(start_key), object_size(start_key),
                    kdata.data(), ksizes.data(), 
                    vdata.data(), vsizes.data(), &max_keys);
            } else {
                throw;
            }
        }
        for(unsigned i=0; i < max_keys; i++) {
            object_resize(keys[i], ksizes[i]);
            object_resize(values[i], vsizes[i]);
        }
        keys.resize(max_keys);
        values.resize(max_keys);
    }

    /**
     * @brief Same as list_keys but also returns the values.
     */
    template<typename K, typename V>
    inline void list_keyvals(const database& db,
                const K& start_key,
                const K& prefix,
                std::vector<K>& keys,
                std::vector<V>& values) const {

        hg_size_t max_keys = std::min(keys.size(), values.size());
        std::vector<void*> kdata; kdata.reserve(keys.size());
        std::vector<hg_size_t> ksizes; ksizes.reserve(keys.size());
        std::vector<void*> vdata; vdata.reserve(values.size());
        std::vector<hg_size_t> vsizes; vsizes.reserve(values.size());
        for(auto& k : keys) {
            kdata.push_back(object_data(k));
            ksizes.push_back(object_size(k));
        }
        for(auto& v : values) {
            vdata.push_back(object_data(v));
            vsizes.push_back(object_size(v));
        }
        try {
            list_keyvals(db, object_data(start_key), object_size(start_key),
                object_data(prefix), object_size(prefix),
                kdata.data(), ksizes.data(), 
                vdata.data(), vsizes.data(), &max_keys);
        } catch(exception& e) {
            if(e.error() == SDSKV_ERR_SIZE) {
                for(unsigned i=0; i < max_keys; i++) {
                    object_resize(keys[i], ksizes[i]);
                    kdata[i] = object_data(keys[i]);
                    object_resize(values[i], vsizes[i]);
                    vdata[i] = object_data(values[i]);
                }
                list_keyvals(db, object_data(start_key), object_size(start_key),
                        object_data(prefix), object_size(prefix),
                        kdata.data(), ksizes.data(), 
                        vdata.data(), vsizes.data(), &max_keys);
            } else {
                throw;
            }
        }
        for(unsigned i=0; i < max_keys; i++) {
            object_resize(keys[i], ksizes[i]);
            object_resize(values[i], vsizes[i]);
        }
        keys.resize(max_keys);
        values.resize(max_keys);
    }

    //////////////////////////
    // MIGRATE_KEYS methods
    //////////////////////////
    
    /**
     * @brief Migrates a given set of keys from a source to a destination database.
     *
     * @param source_db Source database.
     * @param dest_db Destination database.
     * @param num_items Number of items
     * @param keys Array of keys.
     * @param key_sizes Array of key sizes.
     * @param flag SDSKV_KEEP_ORIGINAL or SDSKV_REMOVE_ORIGINAL.
     */
    void migrate(const database& source_db, const database& dest_db,
            hg_size_t num_items, const void* const* keys, const hg_size_t* key_sizes,
            int flag = SDSKV_KEEP_ORIGINAL) const;

    /**
     * @brief Migrates a given set of keys from a source to a destination database.
     *
     * @param source_db Source database.
     * @param dest_db Destination database.
     * @param keys Array of keys.
     * @param ksizes Array of key sizes.
     * @param flag SDSKV_KEEP_ORIGINAL or SDSKV_REMOVE_ORIGINAL.
     */
    inline void migrate(const database& source_db, const database& dest_db,
            const std::vector<const void*> keys, 
            const std::vector<hg_size_t> ksizes,
            int flag = SDSKV_KEEP_ORIGINAL) const {
        if(keys.size() != ksizes.size()) {
            throw std::length_error("Provided vectors should have the same size");
        }
        migrate(source_db, dest_db, keys.size(), keys.data(), ksizes.data(), flag);
    }

    /**
     * @brief Migrate a set of keys from a source to a destination database.
     *
     * @tparam K Key type.
     * @param source_db Source database.
     * @param dest_db Destination database.
     * @param keys Vector of keys.
     * @param flag SDSKV_KEEP_ORIGINAL or SDSKV_REMOVE_ORIGINAL.
     */
    template<typename K>
    inline void migrate(const database& source_db, const database& dest_db,
            const std::vector<K> keys, int flag = SDSKV_KEEP_ORIGINAL) const {
        std::vector<hg_size_t> ksizes; ksizes.reserve(keys.size());
        std::vector<const void*> kdata; kdata.reserve(keys.size());
        for(const auto& k : keys) {
            ksizes.push_back(object_size(k));
            kdata.push_back(object_data(k));
        }
        migrate(source_db, dest_db,
                kdata, ksizes, flag);
    }

    /**
     * @brief Migrate a range of keys.
     *
     * @param source_db Source database.
     * @param dest_db Destination database.
     * @param key_range[2] Key range ] lb; ub [
     * @param key_sizes[2] Key sizes
     * @param flag SDSKV_KEEP_ORIGINAL or SDSKV_REMOVE_ORIGINAL.
     */
    void migrate(const database& source_db, const database& dest_db,
            const void* key_range[2], const hg_size_t key_sizes[2],
            int flag = SDSKV_KEEP_ORIGINAL) const;

    /**
     * @brief Migrate a range of keys.
     *
     * @tparam K Key type.
     * @param source_db Source database.
     * @param dest_db Destination database.
     * @param key_range Key range ] lb; ub [.
     * @param flag SDSKV_KEEP_ORIGINAL or SDSKV_REMOVE_ORIGINAL.
     */
    template<typename K>
    inline void migrate(const database& source_db, const database& dest_db,
                 const std::pair<K,K>& key_range,
                 int flag = SDSKV_KEEP_ORIGINAL) const {
        const void* key_range_arr[2] = { object_data(key_range.first), object_data(key_range.second) };
        const hg_size_t key_sizes_arr[2] = { object_size(key_range.first), object_size(key_range.second) };
        migrate(source_db, dest_db, key_range_arr, key_sizes_arr, flag);
    }

    /**
     * @brief Migrate keys with a prefix.
     *
     * @param source_db Source database.
     * @param dest_db Destination database.
     * @param prefix Prefix.
     * @param prefix_size Prefix size.
     * @param flag SDSKV_KEEP_ORIGINAL or SDSKV_REMOVE_ORIGINAL.
     */
    void migrate(const database& source_db, const database& dest_db,
            const void* prefix, hg_size_t prefix_size,
            int flag = SDSKV_KEEP_ORIGINAL) const;

    /**
     * @brief Migrate keys with a prefix.
     *
     * @tparam K Key type.
     * @param source_db Source database.
     * @param dest_db Destination database.
     * @param prefix Prefix.
     * @param flag SDSKV_KEEP_ORIGINAL or SDSKV_REMOVE_ORIGINAL.
     */
    template<typename K>
    inline void migrate(const database& source_db, const database& dest_db,
                 const K& prefix, int flag = SDSKV_KEEP_ORIGINAL) const {
        migrate(source_db, dest_db, object_data(prefix), object_size(prefix), flag);
    }

    /**
     * @brief Migrate all keys from a database to another.
     *
     * @param source_db Source database.
     * @param dest_db Destination database.
     * @param flag SDSKV_KEEP_ORIGINAL or SDSKV_REMOVE_ORIGINAL.
     */
    void migrate(const database& source_db, const database& dest_db, int flag = SDSKV_KEEP_ORIGINAL) const;

    //////////////////////////
    // MIGRATE_DB method
    //////////////////////////

    /**
     * @brief Migrates an entire database to another destination provider.
     *
     * @param source_db Source database (will be modified to point to the new provider).
     * @param dest_provider_addr Destination provider address.
     * @param dest_provider_id Destination provider id.
     * @param dest_root Path to the database in the new provider.
     * @param flag SDSKV_KEEP_ORIGINAL or SDSKV_REMOVE_ORIGINAL.
     */
    void migrate(database& source_db,
            const std::string& dest_provider_addr,
            uint16_t dest_provider_id,
            const std::string& dest_root,
            int flag = SDSKV_KEEP_ORIGINAL) const;

    //////////////////////////
    // SHUTDOWN method
    //////////////////////////
    
    /**
     * @brief Shuts down a Margo instance in a remote node.
     *
     * @param addr Mercury address of the Margo instance to shut down.
     */
    void shutdown(hg_addr_t addr) const {
        int ret = sdskv_shutdown_service(m_client, addr);
        _CHECK_RET(ret);
    }
};

/**
 * @brief The provider_handle class wraps and sdskv_provider_handle_t C handle.
 */
class provider_handle {

    friend class client;
    friend class database;

    sdskv_provider_handle_t m_ph = SDSKV_PROVIDER_HANDLE_NULL;
    client*                 m_client;

    public:

    /**
     * @brief Default constructor produces an invalid provider_handle.
     */
    provider_handle() = default;

    /**
     * @brief Creates a provider handle from a client, using an address
     * and a provider id.
     *
     * @param c Client.
     * @param addr Mercury address.
     * @param provider_id Provider id.
     */
    provider_handle(
            client& c,
            hg_addr_t addr,
            uint16_t provider_id=0)
    : m_client(&c)
    {
        int ret = sdskv_provider_handle_create(c.m_client, addr, provider_id, &m_ph);
        _CHECK_RET(ret);
    }

    /**
     * @brief Copy constructor.
     */
    provider_handle(const provider_handle& other)
    : m_client(other.m_client)
    , m_ph(other.m_ph) {
        if(m_ph != SDSKV_PROVIDER_HANDLE_NULL) {
            int ret = sdskv_provider_handle_ref_incr(m_ph);
            _CHECK_RET(ret);
        }
    }

    /**
     * @brief Move constructor.
     */
    provider_handle(provider_handle&& other)
    : m_client(other.m_client)
    , m_ph(other.m_ph) {
        other.m_ph = SDSKV_PROVIDER_HANDLE_NULL;
    }

    /**
     * @brief Copy assignment operator.
     */
    provider_handle& operator=(const provider_handle& other) {
        if(this == &other) return *this;
        if(m_ph != SDSKV_PROVIDER_HANDLE_NULL) {
            int ret = sdskv_provider_handle_release(m_ph);
            _CHECK_RET(ret);
        }
        m_ph = other.m_ph;
        m_client = other.m_client;
        int ret = sdskv_provider_handle_ref_incr(m_ph);
        _CHECK_RET(ret);
        return *this;
    }

    /**
     * @brief Move assignment operator.
     */
    provider_handle& operator=(provider_handle& other) {
        if(this == &other) return *this;
        if(m_ph != SDSKV_PROVIDER_HANDLE_NULL) {
            int ret = sdskv_provider_handle_release(m_ph);
            _CHECK_RET(ret);
        }
        m_ph = other.m_ph;
        m_client = other.m_client;
        other.m_ph = SDSKV_PROVIDER_HANDLE_NULL;
        other.m_client = nullptr;
        return *this;
    }

    /**
     * @brief Destructor.
     */
    ~provider_handle() {
        if(m_ph != SDSKV_PROVIDER_HANDLE_NULL)
            sdskv_provider_handle_release(m_ph);
    }

    /**
     * @brief Cast operator to sdskv_provider_handle_t.
     *
     * @return The underlying sdskv_provider_handle_t.
     */
    operator sdskv_provider_handle_t() const {
        return m_ph;
    }

};

/**
 * @brief The database class wraps a sdskv_database_id_t C handle
 * and provides object-oriented access to the underlying provider's database.
 */
class database {
    
    friend class client;
    friend class provider_handle;

    provider_handle     m_ph;
    sdskv_database_id_t m_db_id;

    /**
     * @brief Private constructor used by client::open.
     *
     * @param ph Provider handle.
     * @param db_id Database id.
     */
    database(const provider_handle& ph, sdskv_database_id_t db_id)
    : m_ph(ph)
    , m_db_id(db_id) {}

    public:

    /**
     * @brief Default constructor.
     */
    database() = default;

    /**
     * @brief Default copy constructor.
     */
    database(const database& other) = default;

    /**
     * @brief Default move constructor.
     */
    database(database&& other) = default;

    /**
     * @brief Default copy assignment operator.
     */
    database& operator=(const database& other) = default;

    /**
     * @brief Default move assignment operator.
     */
    database& operator=(database&& other) = default;

    /**
     * @brief Default destructor.
     */
    ~database() = default;

    /**
     * @brief Cast operator to underlying sdskv_database_id_t.
     *
     * @return The underlying sdskv_database_id_t.
     */
    operator sdskv_database_id_t() const {
        return m_db_id;
    }

    /**
     * @brief @see client::put.
     */
    template<typename ... T>
    void put(T&& ... args) const {
        m_ph.m_client->put(*this, std::forward<T>(args)...);
    }

    /**
     * @brief @see client::put_multi.
     */
    template<typename ... T>
    void put_multi(T&& ... args) const {
        m_ph.m_client->put_multi(*this, std::forward<T>(args)...);
    }

    /**
     * @brief @see client::length.
     */
    template<typename ... T>
    decltype(auto) length(T&& ... args) const {
        return m_ph.m_client->length(*this, std::forward<T>(args)...);
    }

    /**
     * @brief @see client::length_multi.
     */
    template<typename ... T>
    decltype(auto) length_multi(T&& ... args) const {
        return m_ph.m_client->length_multi(*this, std::forward<T>(args)...);
    }

    /**
     * @brief @see client::get.
     */
    template<typename ... T>
    decltype(auto) get(T&& ... args) const {
        return m_ph.m_client->get(*this, std::forward<T>(args)...);
    }

    /**
     * @brief @see client::get_multi.
     */
    template<typename ... T>
    decltype(auto) get_multi(T&& ... args) const {
        return m_ph.m_client->get_multi(*this, std::forward<T>(args)...);
    }

    /**
     * @brief @see client::exists
     */
    template<typename ... T>
    decltype(auto) exists(T&& ... args) const {
        return m_ph.m_client->exists(*this, std::forward<T>(args)...);
    }

    /**
     * @brief @see client::erase.
     */
    template<typename ... T>
    void erase(T&& ... args) const {
        m_ph.m_client->erase(*this, std::forward<T>(args)...);
    }

    /**
     * @brief @see client::erase_multi.
     */
    template<typename ... T>
    void erase_multi(T&& ... args) const {
        m_ph.m_client->erase_multi(*this, std::forward<T>(args)...);
    }

    /**
     * @brief @see client::list_keys.
     */
    template<typename ... T>
    decltype(auto) list_keys(T&& ... args) const {
        return m_ph.m_client->list_keys(*this, std::forward<T>(args)...);
    }

    /**
     * @brief @see client::list_keyvals.
     */
    template<typename ... T>
    decltype(auto) list_keyvals(T&& ... args) const {
        return m_ph.m_client->list_keyvals(*this, std::forward<T>(args)...);
    }

    /**
     * @brief @see client::migrate.
     */
    template<typename ... T>
    void migrate(const database& dest_db, T&& ... args) const {
        m_ph.m_client->migrate(*this, dest_db, std::forward<T>(args)...);
    }

    /**
     * @brief @see client::migrate.
     */
    template<typename ... T>
    void migrate(const std::string& dest_provider_addr, uint16_t dest_provider_id, T&& ... args) {
        m_ph.m_client->migrate(*this, dest_provider_addr, dest_provider_id, std::forward<T>(args)...);
    }

};

inline database client::open(const provider_handle& ph, const std::string& db_name) const {
    sdskv_database_id_t db_id;
    int ret = sdskv_open(ph.m_ph, db_name.c_str(), &db_id);
    _CHECK_RET(ret);
    return database(ph, db_id);
}

inline std::vector<database> client::open(const provider_handle& ph) const {
    std::vector<sdskv_database_id_t> ids;
    std::vector<char*> names; 
    hg_size_t count;
    int ret = sdskv_count_databases(ph.m_ph, &count);
    ids.resize(count);
    names.resize(count);
    _CHECK_RET(ret);
    ret = sdskv_list_databases(ph.m_ph, &count, names.data(), ids.data());
    _CHECK_RET(ret);
    for(unsigned i=0; i < count; i++) {
        free(names[i]);
    }
    std::vector<database> dbs; dbs.reserve(count);
    for(unsigned i=0; i < count; i++) {
        dbs.push_back(database(ph, ids[i]));
    }
    return dbs;
}

inline void client::put(const database& db,
        const void *key, hg_size_t ksize,
        const void *value, hg_size_t vsize) const {
    int ret = sdskv_put(db.m_ph.m_ph, db.m_db_id,
            key, ksize, value, vsize);
    _CHECK_RET(ret);
}

inline void client::put_multi(const database& db,
        hg_size_t count, const void* const* keys, const hg_size_t* ksizes,
        const void* const* values, const hg_size_t *vsizes) const {
    int ret = sdskv_put_multi(db.m_ph.m_ph, db.m_db_id,
            count, keys, ksizes, values, vsizes);
     _CHECK_RET(ret);
}

inline hg_size_t client::length(const database& db,
        const void* key, hg_size_t ksize) const {
    hg_size_t vsize;
    int ret = sdskv_length(db.m_ph.m_ph, db.m_db_id,
            key, ksize, &vsize);
    _CHECK_RET(ret);
    return vsize;
}

inline bool client::exists(const database& db, const void* key, hg_size_t ksize) const {
    int flag;
    int ret = sdskv_exists(db.m_ph.m_ph, db.m_db_id, key, ksize, &flag);
    _CHECK_RET(ret);
    return flag;
}

inline bool client::length_multi(const database& db,
        hg_size_t num, const void* const* keys,
        const hg_size_t* ksizes, hg_size_t* vsizes) const {
    int ret = sdskv_length_multi(db.m_ph.m_ph, db.m_db_id,
            num, keys, ksizes, vsizes);
    _CHECK_RET(ret);
    return true;
}

inline bool client::get(const database& db,
        const void* key, hg_size_t ksize,
        void* value, hg_size_t* vsize) const {
    int ret = sdskv_get(db.m_ph.m_ph, db.m_db_id,
            key, ksize, value, vsize);
    _CHECK_RET(ret);
    return true;
}

inline bool client::get_multi(const database& db,
        hg_size_t count, const void* const* keys, const hg_size_t* ksizes,
        void** values, hg_size_t *vsizes) const {
    int ret = sdskv_get_multi(db.m_ph.m_ph, db.m_db_id,
            count, keys, ksizes, values, vsizes);
    _CHECK_RET(ret);
    return true;
}

inline void client::erase(const database& db,
        const void* key,
        hg_size_t ksize) const {
    int ret = sdskv_erase(db.m_ph.m_ph, db.m_db_id,
            key, ksize);
    _CHECK_RET(ret);
}

inline void client::erase_multi(const database& db,
        hg_size_t num, const void* const* keys,
        const hg_size_t* ksizes) const {
    int ret = sdskv_erase_multi(db.m_ph.m_ph, db.m_db_id,
            num, keys, ksizes);
    _CHECK_RET(ret);
}

inline void client::list_keys(const database& db,
        const void *start_key, hg_size_t start_ksize,
        const void *prefix, hg_size_t prefix_size,
        void** keys, hg_size_t* ksizes, hg_size_t* max_keys) const {
    int ret = sdskv_list_keys_with_prefix(db.m_ph.m_ph, db.m_db_id,
            start_key, start_ksize, prefix, prefix_size,
            keys, ksizes, max_keys);
    _CHECK_RET(ret);
}

inline void client::list_keyvals(const database& db,
        const void *start_key, hg_size_t start_ksize,
        const void *prefix, hg_size_t prefix_size,
        void** keys, hg_size_t* ksizes,
        void** values, hg_size_t* vsizes,
        hg_size_t* max_items) const {
    int ret = sdskv_list_keyvals_with_prefix(db.m_ph.m_ph, db.m_db_id,
            start_key, start_ksize, prefix, prefix_size,
            keys, ksizes,
            values, vsizes,
            max_items);
    _CHECK_RET(ret);
}

inline void client::migrate(const database& source_db, const database& dest_db,
        hg_size_t num_items, const void* const* keys, const hg_size_t* key_sizes,
        int flag) const {
    hg_addr_t addr;
    uint16_t target_pr_id;
    sdskv_provider_handle_get_info(dest_db.m_ph.m_ph, NULL, &addr, &target_pr_id);

    margo_instance_id mid = dest_db.m_ph.m_client->m_mid;

    std::vector<char> target_addr_str(256);
    hg_size_t addr_size = 256;
    hg_return_t hret = margo_addr_to_string(mid, target_addr_str.data(), &addr_size, addr);

    if(hret != 0) throw std::runtime_error("margo_addr_to_string failed in client::migrate");

    int ret = sdskv_migrate_keys(source_db.m_ph.m_ph, source_db.m_db_id, 
            target_addr_str.data(), target_pr_id, dest_db.m_db_id, 
            num_items, keys, key_sizes, flag);
    _CHECK_RET(ret);
}

inline void client::migrate(const database& source_db, const database& dest_db,
        const void* key_range[2], const hg_size_t key_sizes[2],
        int flag) const {
    hg_addr_t addr;
    uint16_t target_pr_id;
    sdskv_provider_handle_get_info(dest_db.m_ph.m_ph, NULL, &addr, &target_pr_id);

    margo_instance_id mid = dest_db.m_ph.m_client->m_mid;

    std::vector<char> target_addr_str(256);
    hg_size_t addr_size = 256;
    hg_return_t hret = margo_addr_to_string(mid, target_addr_str.data(), &addr_size, addr);

    if(hret != 0) throw std::runtime_error("margo_addr_to_string failed in client::migrate");

    int ret = sdskv_migrate_key_range(source_db.m_ph.m_ph, source_db.m_db_id, 
            target_addr_str.data(), target_pr_id, 
            dest_db.m_db_id, key_range, key_sizes, flag);
    _CHECK_RET(ret);
}

inline void client::migrate(const database& source_db, const database& dest_db,
        const void* prefix, hg_size_t prefix_size,
        int flag) const {
    hg_addr_t addr;
    uint16_t target_pr_id;
    sdskv_provider_handle_get_info(dest_db.m_ph.m_ph, NULL, &addr, &target_pr_id);

    margo_instance_id mid = dest_db.m_ph.m_client->m_mid;

    std::vector<char> target_addr_str(256);
    hg_size_t addr_size = 256;
    hg_return_t hret = margo_addr_to_string(mid, target_addr_str.data(), &addr_size, addr);

    if(hret != 0) throw std::runtime_error("margo_addr_to_string failed in client::migrate");

    int ret = sdskv_migrate_keys_prefixed(source_db.m_ph.m_ph, source_db.m_db_id, 
            target_addr_str.data(), target_pr_id, dest_db.m_db_id, prefix, prefix_size, flag);
    _CHECK_RET(ret);
}

inline void client::migrate(const database& source_db, const database& dest_db, int flag) const {
    hg_addr_t addr;
    uint16_t target_pr_id;
    sdskv_provider_handle_get_info(dest_db.m_ph.m_ph, NULL, &addr, &target_pr_id);

    margo_instance_id mid = dest_db.m_ph.m_client->m_mid;

    std::vector<char> target_addr_str(256);
    hg_size_t addr_size = 256;
    hg_return_t hret = margo_addr_to_string(mid, target_addr_str.data(), &addr_size, addr);

    if(hret != HG_SUCCESS) throw std::runtime_error("margo_addr_to_string failed in client::migrate");

    int ret = sdskv_migrate_all_keys(source_db.m_ph.m_ph, source_db.m_db_id, 
            target_addr_str.data(), target_pr_id, dest_db.m_db_id, flag);
    _CHECK_RET(ret);
}

inline void client::migrate(database& db,
        const std::string& dest_provider_addr,
        uint16_t dest_provider_id,
        const std::string& dest_root,
        int flag) const {
    hg_addr_t addr;
    margo_instance_id mid = db.m_ph.m_client->m_mid;
    hg_return_t hret = margo_addr_lookup(mid, dest_provider_addr.c_str(), &addr);
    if(hret != HG_SUCCESS) throw std::runtime_error("margo_addr_lookup failed in client::migrate");

    provider_handle dest_ph(*(db.m_ph.m_client), addr, dest_provider_id);

    margo_addr_free(mid, addr);

    int ret = sdskv_migrate_database(db.m_ph.m_ph, db.m_db_id,
                dest_provider_addr.c_str(),
                dest_provider_id,
                dest_root.c_str(),
                flag);
    _CHECK_RET(ret);

    db.m_ph = std::move(dest_ph);
}

} // namespace sdskv

#undef _CHECK_RET

#endif
