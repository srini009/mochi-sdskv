#include <iostream>
#include <iomanip>
#include <cmath>
#include <algorithm>
#include <numeric>
#include <fstream>
#include <map>
#include <functional>
#include <memory>
#include <mpi.h>
#include <json/json.h>
#include <sdskv-client.hpp>
#include <sdskv-server.hpp>

using RemoteDatabase = sdskv::database;

/**
 * Helper function to generate random strings of a certain length.
 * These strings are readable.
 */
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

template<typename T>
class BenchmarkRegistration;

/**
 * The AbstractBenchmark class describes an interface that a benchmark object
 * needs to satisfy. This interface has a setup, execute, and teardown
 * methods. AbstractBenchmark also acts as a factory to create concrete instances.
 */
class AbstractBenchmark {

    MPI_Comm        m_comm;      // communicator gathering all clients
    RemoteDatabase& m_remote_db; // remote database

    template<typename T>
    friend class BenchmarkRegistration;

    using benchmark_factory_function = std::function<
        std::unique_ptr<AbstractBenchmark>(Json::Value&, MPI_Comm, RemoteDatabase&)>;
    static std::map<std::string, benchmark_factory_function> s_benchmark_factories;

    protected:

    RemoteDatabase& remoteDatabase() { return m_remote_db; }
    const RemoteDatabase& remoteDatabase() const { return m_remote_db; }
    MPI_Comm comm() const { return m_comm; }

    public:

    AbstractBenchmark(MPI_Comm c, RemoteDatabase& rdb)
    : m_comm(c)
    , m_remote_db(rdb) {}

    virtual ~AbstractBenchmark() = default;
    virtual void setup()    = 0;
    virtual void execute()  = 0;
    virtual void teardown() = 0;

    /**
     * @brief Factory function used to create benchmark instances.
     */
    template<typename ... T>
    static std::unique_ptr<AbstractBenchmark> create(const std::string& type, T&& ... args) {
        auto it = s_benchmark_factories.find(type);
        if(it == s_benchmark_factories.end())
            throw std::invalid_argument(type+" benchmark type unknown");
        return (it->second)(std::forward<T>(args)...);
    }
};

/**
 * @brief The mechanism bellow is used to provide the REGISTER_BENCHMARK macro,
 * which registers a child class of AbstractBenchmark and allows AbstractBenchmark::create("type", ...)
 * to return an instance of this concrete child class.
 */
template<typename T>
class BenchmarkRegistration {
    public:
    BenchmarkRegistration(const std::string& type) {
        AbstractBenchmark::s_benchmark_factories[type] = 
            [](Json::Value& config, MPI_Comm comm, RemoteDatabase& rdb) {
                return std::make_unique<T>(config, comm, rdb);
        };
    }
};

std::map<std::string, AbstractBenchmark::benchmark_factory_function> AbstractBenchmark::s_benchmark_factories;

#define REGISTER_BENCHMARK(__name, __class) \
    static BenchmarkRegistration<__class> __class##_registration(__name)

class AbstractAccessBenchmark : public AbstractBenchmark {
    
    protected:

    uint64_t                  m_num_entries = 0;
    std::pair<size_t, size_t> m_key_size_range;
    std::pair<size_t, size_t> m_val_size_range;
    bool                      m_erase_on_teardown;

    public:

    template<typename ... T>
    AbstractAccessBenchmark(Json::Value& config, T&& ... args)
    : AbstractBenchmark(std::forward<T>(args)...) {
        // read the configuration
        m_num_entries = config["num-entries"].asUInt64();
        if(config["key-sizes"].isIntegral()) {
            auto x = config["key-sizes"].asUInt64();
            m_key_size_range = { x, x+1 };
        } else if(config["key-sizes"].isArray() && config["key-sizes"].size() == 2) {
            auto x = config["key-sizes"][0].asUInt64();
            auto y = config["key-sizes"][1].asUInt64();
            if(x > y) throw std::range_error("invalid key-sizes range");
            m_key_size_range = { x, y };
        } else {
            throw std::range_error("invalid key-sizes range or value");
        }
        if(config["val-sizes"].isIntegral()) {
            auto x = config["val-sizes"].asUInt64();
            m_val_size_range = { x, x+1 };
        } else if(config["val-sizes"].isArray() && config["val-sizes"].size() == 2) {
            auto x = config["val-sizes"][0].asUInt64();
            auto y = config["val-sizes"][1].asUInt64();
            if(x >= y) throw std::range_error("invalid val-sizes range");
            m_val_size_range = { x, y };
        } else {
            throw std::range_error("invalid val-sizes range or value");
        }
        m_erase_on_teardown = config.get("erase-on-teardown", true).asBool();
    }
};

/**
 * PutBenchmark executes a series of PUT operations and measures their duration.
 */
class PutBenchmark : public AbstractAccessBenchmark {

    protected:

    std::vector<std::string>  m_keys;
    std::vector<std::string>  m_vals;

    public:

    template<typename ... T>
    PutBenchmark(T&& ... args)
    : AbstractAccessBenchmark(std::forward<T>(args)...) { }

    virtual void setup() override {
        // generate key/value pairs and store them in the local
        m_keys.reserve(m_num_entries);
        m_vals.reserve(m_num_entries);
        for(unsigned i=0; i < m_num_entries; i++) {
            size_t ksize = m_key_size_range.first + (rand() % (m_key_size_range.second - m_key_size_range.first));
            m_keys.push_back(gen_random_string(ksize));
            size_t vsize = m_val_size_range.first + (rand() % (m_val_size_range.second - m_val_size_range.first));
            m_vals.push_back(gen_random_string(vsize));
        }
    }

    virtual void execute() override {
        // execute PUT operations
        auto& db = remoteDatabase();
        for(unsigned i=0; i < m_num_entries; i++) {
            auto& key = m_keys[i];
            auto& val = m_vals[i];
            db.put(key, val);
        }
    }

    virtual void teardown() override {
        if(m_erase_on_teardown) {
            // erase all the keys from the database
            auto& db = remoteDatabase();
            db.erase_multi(m_keys);
        }
        // erase keys and values from the local vectors
        m_keys.resize(0); m_keys.shrink_to_fit();
        m_vals.resize(0); m_vals.shrink_to_fit();
    }
};
REGISTER_BENCHMARK("put", PutBenchmark);

/**
 * PutMultiBenchmark inherites from PutBenchmark and does the same but
 * executes a PUT-MULTI instead of a PUT.
 */
class PutMultiBenchmark : public PutBenchmark {
    
    protected:

    size_t m_batch_size;
    std::vector<hg_size_t>   m_ksizes;
    std::vector<const void*> m_kptrs;
    std::vector<hg_size_t>   m_vsizes;
    std::vector<const void*> m_vptrs;

    public:

    template<typename ... T>
    PutMultiBenchmark(Json::Value& config, T&& ... args)
    : PutBenchmark(config, std::forward<T>(args)...) {
        m_batch_size = config.get("batch-size", m_num_entries).asUInt();
    }

    virtual void setup() override {
        PutBenchmark::setup();
        m_ksizes.resize(m_batch_size);
        m_kptrs.resize(m_batch_size);
        m_vsizes.resize(m_batch_size);
        m_vptrs.resize(m_batch_size);
    }

    virtual void execute() override {
        auto& db = remoteDatabase();
        size_t remaining = m_num_entries;
        unsigned j = 0;
        while(remaining != 0) {
            size_t count = std::min<size_t>(remaining, m_batch_size);
            for(unsigned i=0; i<count; i++) {
                m_ksizes[i] = m_keys[i+j].size();
                m_kptrs[i]  = m_keys[i+j].data();
                m_vsizes[i] = m_vals[i+j].size();
                m_vptrs[i]  = m_vals[i+j].data();
            }
            if(count != m_batch_size) {
                m_ksizes.resize(count);
                m_kptrs.resize(count);
                m_vsizes.resize(count);
                m_vptrs.resize(count);
            }
            db.put(m_kptrs, m_ksizes, m_vptrs, m_vsizes);
            remaining -= count;
        }
        db.put(m_keys, m_vals);
    }

    virtual void teardown() override {
        PutBenchmark::teardown();
        m_ksizes.resize(0); m_ksizes.shrink_to_fit();
        m_kptrs.resize(0);  m_kptrs.shrink_to_fit();
        m_vsizes.resize(0); m_vsizes.shrink_to_fit();
        m_vptrs.resize(0);  m_vptrs.shrink_to_fit();
    }
};
REGISTER_BENCHMARK("put-multi", PutMultiBenchmark);

/**
 * GetBenchmark executes a series of GET operations and measures their duration.
 */
class GetBenchmark : public AbstractAccessBenchmark {

    protected:

    std::vector<std::string>  m_keys;
    std::vector<std::string>  m_vals_buffer;
    bool                      m_reuse_buffer = false;

    public:

    template<typename ... T>
    GetBenchmark(Json::Value& config, T&& ... args)
    : AbstractAccessBenchmark(config, std::forward<T>(args)...) {
        if(config.isMember("reuse-buffer")) {
            m_reuse_buffer = config["reuse-buffer"].asBool();
        }
    }

    virtual void setup() override {
        // generate key/value pairs and store them in the local
        m_keys.reserve(m_num_entries);
        std::vector<std::string> vals;
        vals.reserve(m_num_entries);
        for(unsigned i=0; i < m_num_entries; i++) {
            size_t ksize = m_key_size_range.first + (rand() % (m_key_size_range.second - m_key_size_range.first));
            m_keys.push_back(gen_random_string(ksize));
            size_t vsize = m_val_size_range.first + (rand() % (m_val_size_range.second - m_val_size_range.first));
            vals.push_back(gen_random_string(vsize));
        }
        // execute PUT operations (not part of the measure)
        auto& db = remoteDatabase();
        for(unsigned i=0; i < m_num_entries; i++) {
            auto& key = m_keys[i];
            auto& val = vals[i];
            db.put(key, val);
        }
        // make a copy of the keys so we don't reuse the same memory
        auto keys_cpy = m_keys;
        m_keys = std::move(keys_cpy);
        // prepare buffer to get values
        if(m_reuse_buffer)
            m_vals_buffer.resize(1, std::string(m_val_size_range.second-1, 0));
        else
            m_vals_buffer.resize(m_num_entries, std::string(m_val_size_range.second-1, 0));
    }

    virtual void execute() override {
        // execute GET operations
        auto& db = remoteDatabase();
        unsigned j = 0;
        for(unsigned i=0; i < m_num_entries; i++) {
            auto& key = m_keys[i];
            auto& val = m_vals_buffer[j];
            hg_size_t vsize = m_val_size_range.second-1;
            db.get((const void*)key.data(), key.size(), (void*)val.data(), &vsize);
            if(!m_reuse_buffer)
                j += 1;
        }
    }

    virtual void teardown() override {
        if(m_erase_on_teardown) {
            // erase all the keys from the database
            auto& db = remoteDatabase();
            for(unsigned i=0; i < m_num_entries; i++) {
                db.erase(m_keys[i]);
            }
        }
        // erase keys and values from the local vectors
        m_keys.resize(0); m_keys.shrink_to_fit();
        m_vals_buffer.resize(0); m_vals_buffer.shrink_to_fit();
    }
};
REGISTER_BENCHMARK("get", GetBenchmark);

/**
 * GetMultiBenchmark inherites from GetBenchmark and does the same but
 * executes a GET-MULTI instead of a GET.
 */
class GetMultiBenchmark : public GetBenchmark {
    
    protected:

    size_t m_batch_size;
    std::vector<hg_size_t>   m_ksizes;
    std::vector<const void*> m_kptrs;
    std::vector<hg_size_t>   m_vsizes;
    std::vector<void*>       m_vptrs;

    public:

    template<typename ... T>
    GetMultiBenchmark(Json::Value& config, T&& ... args)
    : GetBenchmark(config, std::forward<T>(args)...) {
        m_batch_size = config.get("batch-size", m_num_entries).asUInt();
    }

    virtual void setup() override {
        GetBenchmark::setup();
        m_ksizes.resize(m_batch_size);
        m_kptrs.resize(m_batch_size);
        m_vsizes.resize(m_batch_size);
        m_vptrs.resize(m_batch_size);
        if(m_vals_buffer.size() != m_batch_size && m_reuse_buffer) {
            m_vals_buffer.resize(m_batch_size, std::string(m_val_size_range.second-1, 0));
        }
    }

    virtual void execute() override {
        auto& db = remoteDatabase();
        size_t remaining = m_num_entries;
        unsigned j = 0, k = 0;
        while(remaining != 0) {
            size_t count = std::min<size_t>(remaining, m_batch_size);
            for(unsigned i=0; i < count; i++) {
                m_ksizes[i] = m_keys[j+i].size();
                m_kptrs[i]  = (const void*)m_keys[i+j].data();
                m_vsizes[i] = m_vals_buffer[i+k].size();
                m_vptrs[i]  = (void*)m_vals_buffer[i+k].data();
            }
            if(count != m_batch_size) {
                m_ksizes.resize(count);
                m_kptrs.resize(count);
                m_vsizes.resize(count);
                m_vptrs.resize(count);
            }
            db.get(m_kptrs, m_ksizes, m_vptrs, m_vsizes);
            if(!m_reuse_buffer)
                k += count;
            j += count;
            remaining -= count;
        }
    }

    virtual void teardown() override {
        GetBenchmark::teardown();
        m_ksizes.resize(0); m_ksizes.shrink_to_fit();
        m_kptrs.resize(0);  m_kptrs.shrink_to_fit();
        m_vsizes.resize(0); m_vsizes.shrink_to_fit();
        m_vptrs.resize(0);  m_vptrs.shrink_to_fit();
    }
};
REGISTER_BENCHMARK("get-multi", GetMultiBenchmark);

/**
 * LengthBenchmark executes a series of LENGTH operations and measures their duration.
 */
class LengthBenchmark : public GetBenchmark {

    public:

    template<typename ... T>
    LengthBenchmark(T&& ... args)
    : GetBenchmark(std::forward<T>(args)...) {}

    virtual void execute() override {
        // execute LENGTH operations
        auto& db = remoteDatabase();
        for(unsigned i=0; i < m_num_entries; i++) {
            auto& key = m_keys[i];
            db.length(key);
        }
    }
};
REGISTER_BENCHMARK("length", LengthBenchmark);

/**
 * LengthMultiBenchmark inherites from LengthBenchmark and does the same but
 * executes a LENGTH-MULTI instead of a LENGTH.
 */
class LengthMultiBenchmark : public LengthBenchmark {
    

    protected:

    size_t                   m_batch_size;
    std::vector<hg_size_t>   m_ksizes;
    std::vector<const void*> m_kptrs;
    std::vector<hg_size_t>   m_vsizes;

    public:

    template<typename ... T>
    LengthMultiBenchmark(Json::Value& config, T&& ... args)
    : LengthBenchmark(config, std::forward<T>(args)...) {
         m_batch_size = config.get("batch-size", m_num_entries).asUInt();
    }

    virtual void setup() override {
        LengthBenchmark::setup();
        if(m_reuse_buffer)
            m_vsizes.resize(m_batch_size);
        else
            m_vsizes.resize(m_num_entries);
        m_ksizes.resize(m_batch_size);
        m_kptrs.resize(m_batch_size);
    }

    virtual void execute() override {
        auto& db = remoteDatabase();
        size_t remaining = m_num_entries;
        unsigned j = 0, k = 0;
        while(remaining != 0) {
            size_t count = std::min<size_t>(remaining, m_batch_size);
            for(unsigned i=0; i < count; i++) {
                m_ksizes[i] = m_keys[i+j].size();
                m_kptrs[i] = (const void*)m_keys[i+j].data();
            }
            db.length(count, m_kptrs.data(), m_ksizes.data(), m_vsizes.data()+k);
            remaining -= count;
            j += count;
            if(!m_reuse_buffer)
                k += count;
        }
    }

    virtual void teardown() override {
        LengthBenchmark::teardown();
        m_ksizes.resize(0); m_ksizes.shrink_to_fit();
        m_kptrs.resize(0);  m_kptrs.shrink_to_fit();
        m_vsizes.resize(0); m_vsizes.shrink_to_fit();
    }
};
REGISTER_BENCHMARK("length-multi", LengthMultiBenchmark);

/**
 * EraseBenchmark executes a series of ERASE operations and measures their duration.
 */
class EraseBenchmark : public GetBenchmark {

    public:

    template<typename ... T>
    EraseBenchmark(T&& ... args)
    : GetBenchmark(std::forward<T>(args)...) {}

    virtual void setup() override {
        GetBenchmark::setup();
        m_vals_buffer.resize(0);
    }

    virtual void execute() override {
        // execute ERASE operations
        auto& db = remoteDatabase();
        for(unsigned i=0; i < m_num_entries; i++) {
            auto& key = m_keys[i];
            db.erase(key);
        }
    }

    virtual void teardown() override {
        // erase keys and values from the local vectors
        m_keys.resize(0); m_keys.shrink_to_fit();
        m_vals_buffer.resize(0); m_vals_buffer.shrink_to_fit(); 
    }
};
REGISTER_BENCHMARK("erase", EraseBenchmark);

/**
 * EraseMultiBenchmark inherites from EraseBenchmark and does the same but
 * executes a ERASE-MULTI instead of a ERASE.
 */
class EraseMultiBenchmark : public EraseBenchmark {
   
    protected:

    size_t                   m_batch_size;
    std::vector<hg_size_t>   m_ksizes;
    std::vector<const void*> m_kptrs;

    public:

    template<typename ... T>
    EraseMultiBenchmark(Json::Value& config, T&& ... args)
    : EraseBenchmark(config, std::forward<T>(args)...) {
         m_batch_size = config.get("batch-size", m_num_entries).asUInt();
    }

    virtual void setup() override {
        EraseBenchmark::setup();
        m_ksizes.resize(m_batch_size);
        m_kptrs.resize(m_batch_size);
    }

    virtual void execute() override {
        auto& db = remoteDatabase();
        size_t remaining = m_num_entries;
        unsigned j = 0;
        while(remaining != 0) {
            size_t count = std::min<size_t>(remaining, m_batch_size);
            for(unsigned i=0; i < count; i++) {
                m_ksizes[i] = m_keys[i+j].size();
                m_kptrs[i] = (const void*)m_keys[i+j].data();
            }
            db.erase_multi(count, m_kptrs.data(), m_ksizes.data());
            remaining -= count;
            j += count;
        }
    }

    virtual void teardown() override {
        EraseBenchmark::teardown();
        m_ksizes.resize(0); m_ksizes.shrink_to_fit();
        m_kptrs.resize(0);  m_kptrs.shrink_to_fit();
    }
};
REGISTER_BENCHMARK("erase-multi", EraseMultiBenchmark);

/**
 * ListKeysBenchmark executes a series of LIST KEYS operations and measures their duration.
 */
class ListKeysBenchmark : public GetBenchmark {

    protected:

    size_t                    m_batch_size;
    std::vector<std::string>  m_keys_buffer;
    std::vector<hg_size_t>    m_ksizes;
    std::vector<void*>        m_kptrs;

    public:

    template<typename ... T>
    ListKeysBenchmark(Json::Value& config, T&& ... args)
    : GetBenchmark(config, std::forward<T>(args)...) {
        m_batch_size = config.get("batch-size", 8).asUInt();
    }

    virtual void setup() override {
        GetBenchmark::setup();
        if(m_reuse_buffer)
            m_keys_buffer.resize(m_batch_size, std::string(m_key_size_range.second-1, 0));
        else
            m_keys_buffer.resize(m_num_entries, std::string(m_key_size_range.second-1, 0));
        m_ksizes.resize(m_batch_size);
        m_kptrs.resize(m_batch_size);
    }

    virtual void execute() override {
        auto& db = remoteDatabase();
        size_t remaining = m_num_entries;
        unsigned k = 0;
        std::string start_key = "";
        while(remaining != 0) {
            hg_size_t count = std::min<size_t>(remaining, m_batch_size);
            for(unsigned i=0; i < count; i++) {
                m_ksizes[i] = m_keys_buffer[i+k].size();
                m_kptrs[i]  = (void*)m_keys_buffer[i+k].data();
            }
            db.list_keys(start_key.data(), start_key.size(),
                    (void**)m_kptrs.data(), (hg_size_t*)m_ksizes.data(),
                    &count);
            if(count > 0) {
                start_key = std::string((const char*)m_kptrs[count-1], m_ksizes[count-1]);
            }
            if(!m_reuse_buffer)
                k += count;
            remaining -= count;
        }
    }

    virtual void teardown() override {
        GetBenchmark::teardown();
        m_keys_buffer.resize(0); m_keys_buffer.shrink_to_fit();
        m_ksizes.resize(0); m_ksizes.shrink_to_fit();
        m_kptrs.resize(0); m_kptrs.shrink_to_fit();
    }
};
REGISTER_BENCHMARK("list-keys", ListKeysBenchmark);

/**
 * ListKeysBenchmark executes a series of LIST KEYVALS operations and measures their duration.
 */
class ListKeyValsBenchmark : public ListKeysBenchmark {

    protected:

    std::vector<hg_size_t>    m_vsizes;
    std::vector<void*>        m_vptrs;

    public:

    template<typename ... T>
    ListKeyValsBenchmark(T&& ... args)
    : ListKeysBenchmark(std::forward<T>(args)...) {}

    virtual void setup() override {
        ListKeysBenchmark::setup();
        if(m_reuse_buffer)
            m_vals_buffer.resize(m_batch_size, std::string(m_val_size_range.second-1, 0));
        // if !m_reuse_buffer the m_vals_buffer vector already has the right size
        m_vsizes.resize(m_batch_size);
        m_vptrs.resize(m_batch_size);
    }

    virtual void execute() override {
        auto& db = remoteDatabase();
        size_t remaining = m_num_entries;
        unsigned k = 0;
        std::string start_key = "";
        while(remaining != 0) {
            hg_size_t count = std::min<size_t>(remaining, m_batch_size);
            for(unsigned i=0; i < count; i++) {
                m_ksizes[i] = m_keys_buffer[i+k].size();
                m_kptrs[i]  = (void*)m_keys_buffer[i+k].data();
                m_vsizes[i] = m_vals_buffer[i+k].size();
                m_vptrs[i]  = (void*)m_vals_buffer[i+k].data();
            }
            db.list_keyvals(start_key.data(), start_key.size(),
                    (void**)m_kptrs.data(), (hg_size_t*)m_ksizes.data(),
                    (void**)m_vptrs.data(), (hg_size_t*)m_vsizes.data(),
                    &count);
            if(count > 0) {
                start_key = std::string((const char*)m_kptrs[count-1], m_ksizes[count-1]);
            }
            if(!m_reuse_buffer)
                k += count;
            remaining -= count;
        }
    }

    virtual void teardown() override {
        ListKeysBenchmark::teardown();
        m_vsizes.resize(0); m_vsizes.shrink_to_fit();
        m_vptrs.resize(0);  m_vptrs.shrink_to_fit();
    }
};
REGISTER_BENCHMARK("list-keyvals", ListKeyValsBenchmark);

static void run_server(MPI_Comm comm, Json::Value& config);
static void run_client(MPI_Comm comm, Json::Value& config);
static void run_single_node(Json::Value& config);
static sdskv_db_type_t database_type_from_string(const std::string& type);

/**
 * @brief Main function.
 */
int main(int argc, char** argv) {

    MPI_Init(&argc, &argv);
    int rank, size;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    if(argc != 2) {
        if(rank == 0) {
            std::cerr << "Usage: " << argv[0] << " <config.json>" << std::endl;
            MPI_Abort(MPI_COMM_WORLD, -1);
        }
    }

    std::ifstream config_file(argv[1]);
    if(!config_file.good() && rank == 0) {
        std::cerr << "Could not read configuration file " << argv[1] << std::endl;
        MPI_Abort(MPI_COMM_WORLD, -1);
    }

    Json::Reader reader;
    Json::Value config;
    reader.parse(config_file, config);

    MPI_Comm comm = MPI_COMM_WORLD;
    bool single_node = (size == 1);
    if(!single_node) {
        MPI_Comm_split(MPI_COMM_WORLD, rank == 0 ? 0 : 1, rank, &comm);
        if(rank == 0) {
            run_server(comm, config);
        } else {
            run_client(comm, config);
        }
    } else {
        run_single_node(config);
    }

    MPI_Finalize();
    return 0;
}

static void run_server(MPI_Comm comm, Json::Value& config) {
    // initialize Margo
    margo_instance_id mid = MARGO_INSTANCE_NULL;
    std::string protocol = config["protocol"].asString();
    auto& server_config = config["server"];
    bool use_progress_thread = server_config["use-progress-thread"].asBool();
    int  rpc_thread_count = server_config["rpc-thread-count"].asInt();
    mid = margo_init(protocol.c_str(), MARGO_SERVER_MODE, use_progress_thread, rpc_thread_count);
    margo_enable_remote_shutdown(mid);
    // serialize server address
    std::vector<char> server_addr_str(256,0);
    hg_size_t buf_size = 256;
    hg_addr_t server_addr = HG_ADDR_NULL;
    margo_addr_self(mid, &server_addr);
    margo_addr_to_string(mid, server_addr_str.data(), &buf_size, server_addr);
    margo_addr_free(mid, server_addr);
    // send server address to client
    MPI_Bcast(&buf_size, sizeof(hg_size_t), MPI_BYTE, 0, MPI_COMM_WORLD);
    MPI_Bcast(server_addr_str.data(), buf_size, MPI_BYTE, 0, MPI_COMM_WORLD);
    // initialize sdskv provider
    auto provider = sdskv::provider::create(mid);
    // initialize poolset if present
    if(server_config["poolset"]) {
        auto& poolset_config = server_config["poolset"];
        int npools = poolset_config.get("pools",1).asInt();
        int nbufs = poolset_config.get("buffers",10).asInt();
        int min_size = poolset_config.get("min_size", 1024).asInt();
        int size_factor = poolset_config.get("size_factor", 2).asInt();
        provider->configure_bulk_poolset(npools, nbufs, min_size, size_factor);
    }
    // initialize database
    auto& database_config = server_config["database"];
    std::string db_name = database_config["name"].asString();
    std::string db_path = database_config["path"].asString();
    sdskv_db_type_t db_type = database_type_from_string(database_config["type"].asString());
    sdskv_config_t db_config = {
        .db_name = db_name.c_str(),
        .db_path = db_path.c_str(),
        .db_type = db_type,
        .db_comp_fn_name = nullptr,
        .db_no_overwrite = 0
    };
    provider->attach_database(db_config);
    // notify clients that the database is ready
    MPI_Barrier(MPI_COMM_WORLD);
    // wait for finalize
    margo_wait_for_finalize(mid);
}

static void run_client(MPI_Comm comm, Json::Value& config) {
    // get info from communicator
    int rank, num_clients;
    MPI_Comm_rank(comm, &rank);
    MPI_Comm_size(comm, &num_clients);
    Json::StyledStreamWriter styledStream;
    // initialize Margo
    margo_instance_id mid = MARGO_INSTANCE_NULL;
    std::string protocol = config["protocol"].asString();
    mid = margo_init(protocol.c_str(), MARGO_SERVER_MODE, 0, 0);
    // receive server address
    std::vector<char> server_addr_str;
    hg_size_t buf_size;
    hg_addr_t server_addr = HG_ADDR_NULL;
    MPI_Bcast(&buf_size, sizeof(hg_size_t), MPI_BYTE, 0, MPI_COMM_WORLD);
    server_addr_str.resize(buf_size, 0);
    MPI_Bcast(server_addr_str.data(), buf_size, MPI_BYTE, 0, MPI_COMM_WORLD);
    margo_addr_lookup(mid, server_addr_str.data(), &server_addr);
    // wait for server to have initialize the database
    MPI_Barrier(MPI_COMM_WORLD);
    {
        // create client
        sdskv::client client(mid);
        // setup poolset
        if(config["client"]) {
            auto& client_config = config["client"];
            if(client_config["poolset"]) {
                auto& poolset_config = client_config["poolset"];
                int npools = poolset_config.get("pools",1).asInt();
                int nbufs = poolset_config.get("buffers",10).asInt();
                int min_size = poolset_config.get("min_size", 1024).asInt();
                int size_factor = poolset_config.get("size_factor", 2).asInt();
                client.configure_bulk_poolset(npools, nbufs, min_size, size_factor);
            }
        }
        // open database
        sdskv::provider_handle ph(client, server_addr);
        std::string db_name = config["server"]["database"]["name"].asString();
        RemoteDatabase db = client.open(ph, db_name);
        // initialize the RNG seed
        int seed = config["seed"].asInt();
        // initialize benchmark instances
        std::vector<std::unique_ptr<AbstractBenchmark>> benchmarks;
        std::vector<unsigned> repetitions;
        std::vector<std::string> types;
        benchmarks.reserve(config["benchmarks"].size());
        repetitions.reserve(config["benchmarks"].size());
        types.reserve(config["benchmarks"].size());
        for(auto& bench_config : config["benchmarks"]) {
            std::string type = bench_config["type"].asString();
            types.push_back(type);
            benchmarks.push_back(AbstractBenchmark::create(type, bench_config, comm, db));
            repetitions.push_back(bench_config["repetitions"].asUInt());
        }
        // main execution loop
        for(unsigned i = 0; i < benchmarks.size(); i++) {
            auto& bench  = benchmarks[i];
            unsigned rep = repetitions[i];
            // reset the RNG
            srand(seed + rank*1789);
            std::vector<double> local_timings(rep);
            for(unsigned j = 0; j < rep; j++) {
                MPI_Barrier(comm);
                // benchmark setup
                bench->setup();
                MPI_Barrier(comm);
                // benchmark execution
                double t_start = MPI_Wtime();
                bench->execute();
                double t_end = MPI_Wtime();
                local_timings[j] = t_end - t_start;
                MPI_Barrier(comm);
                // teardown
                bench->teardown();
            }
            // exchange timings
            std::vector<double> global_timings(rep*num_clients);
            if(num_clients != 1) {
                MPI_Gather(local_timings.data(), local_timings.size(), MPI_DOUBLE,
                       global_timings.data(), local_timings.size(), MPI_DOUBLE, 0, comm);
            } else {
                std::copy(local_timings.begin(), local_timings.end(), global_timings.begin());
            }
            // print report
            if(rank == 0) {
                size_t n = global_timings.size();
                std::cout << "================ " << types[i] << " ================" << std::endl;
                styledStream.write(std::cout, config["benchmarks"][i]);
                std::cout << "-----------------" << std::string(types[i].size(),'-') << "-----------------" << std::endl;
                double average  = std::accumulate(global_timings.begin(), global_timings.end(), 0.0) / n;
                double variance = std::accumulate(global_timings.begin(), global_timings.end(), 0.0, [average](double acc, double x) {
                        return acc + std::pow((x - average),2);
                    });
                variance /= n;
                double stddev = std::sqrt(variance);
                std::sort(global_timings.begin(), global_timings.end());
                double min = global_timings[0];
                double max = global_timings[global_timings.size()-1];
                double median = (n % 2) ? global_timings[n/2] : ((global_timings[n/2] + global_timings[n/2 - 1])/2.0);
                double q1 = global_timings[n/4];
                double q3 = global_timings[(3*n)/4];
                std::cout << std::setprecision(9) << std::fixed;
                std::cout << "Samples         : " << n << std::endl;
                std::cout << "Average(sec)    : " << average << std::endl;
                std::cout << "Variance(sec^2) : " << variance << std::endl;
                std::cout << "StdDev(sec)     : " << stddev << std::endl;
                std::cout << "Minimum(sec)    : " << min << std::endl;
                std::cout << "Q1(sec)         : " << q1 << std::endl;
                std::cout << "Median(sec)     : " << median << std::endl;
                std::cout << "Q3(sec)         : " << q3 << std::endl;
                std::cout << "Maximum(sec)    : " << max << std::endl;
            }
        }
        // wait for all the clients to be done with their tasks
        MPI_Barrier(comm);
        // shutdown server and finalize margo
        if(rank == 0)
            margo_shutdown_remote_instance(mid, server_addr);
    }
    margo_addr_free(mid, server_addr);
    margo_finalize(mid);
}

static void run_single_node(Json::Value& config) {
    Json::StyledStreamWriter styledStream;
    // initialize Margo
    margo_instance_id mid = MARGO_INSTANCE_NULL;
    std::string protocol = config["protocol"].asString();
    auto& server_config = config["server"];
    bool use_progress_thread = server_config["use-progress-thread"].asBool();
    int  rpc_thread_count = server_config["rpc-thread-count"].asInt();
    mid = margo_init(protocol.c_str(), MARGO_SERVER_MODE, use_progress_thread, rpc_thread_count);
    // serialize server address
    hg_addr_t server_addr = HG_ADDR_NULL;
    margo_addr_self(mid, &server_addr);
    // initialize sdskv provider
    auto provider = sdskv::provider::create(mid);
    // initialize database
    auto& database_config = server_config["database"];
    std::string db_name = database_config["name"].asString();
    std::string db_path = database_config["path"].asString();
    sdskv_db_type_t db_type = database_type_from_string(database_config["type"].asString());
    sdskv_config_t db_config = {
        .db_name = db_name.c_str(),
        .db_path = db_path.c_str(),
        .db_type = db_type,
        .db_comp_fn_name = nullptr,
        .db_no_overwrite = 0
    };
    provider->attach_database(db_config);
    // initialize and start client
    {
        // open remote database
        sdskv::client client(mid);
        sdskv::provider_handle ph(client, server_addr);
        RemoteDatabase db = client.open(ph, db_name);
        // initialize the RNG seed
        int seed = config["seed"].asInt();
        // initialize benchmark instances
        std::vector<std::unique_ptr<AbstractBenchmark>> benchmarks;
        std::vector<unsigned> repetitions;
        std::vector<std::string> types;
        benchmarks.reserve(config["benchmarks"].size());
        repetitions.reserve(config["benchmarks"].size());
        types.reserve(config["benchmarks"].size());
        for(auto& bench_config : config["benchmarks"]) {
            std::string type = bench_config["type"].asString();
            types.push_back(type);
            benchmarks.push_back(AbstractBenchmark::create(type, bench_config, MPI_COMM_WORLD, db));
            repetitions.push_back(bench_config["repetitions"].asUInt());
        }
        // main execution loop
        for(unsigned i = 0; i < benchmarks.size(); i++) {
            auto& bench  = benchmarks[i];
            unsigned rep = repetitions[i];
            // reset the RNG
            srand(seed);
            std::vector<double> local_timings(rep);
            for(unsigned j = 0; j < rep; j++) {
                // benchmark setup
                bench->setup();
                // benchmark execution
                double t_start = MPI_Wtime();
                bench->execute();
                double t_end = MPI_Wtime();
                local_timings[j] = t_end - t_start;
                // teardown
                bench->teardown();
            }
            std::vector<double> global_timings(rep);
            std::copy(local_timings.begin(), local_timings.end(), global_timings.begin());
            // print report
            size_t n = global_timings.size();
            std::cout << "================ " << types[i] << " ================" << std::endl;
            styledStream.write(std::cout, config["benchmarks"][i]);
            std::cout << "-----------------" << std::string(types[i].size(),'-') << "-----------------" << std::endl;
            double average  = std::accumulate(global_timings.begin(), global_timings.end(), 0.0) / n;
            double variance = std::accumulate(global_timings.begin(), global_timings.end(), 0.0, [average](double acc, double x) {
                    return acc + std::pow((x - average),2);
                    });
            variance /= n;
            double stddev = std::sqrt(variance);
            std::sort(global_timings.begin(), global_timings.end());
            double min = global_timings[0];
            double max = global_timings[global_timings.size()-1];
            double median = (n % 2) ? global_timings[n/2] : ((global_timings[n/2] + global_timings[n/2 - 1])/2.0);
            double q1 = global_timings[n/4];
            double q3 = global_timings[(3*n)/4];
            std::cout << std::setprecision(9) << std::fixed;
            std::cout << "Samples         : " << n << std::endl;
            std::cout << "Average(sec)    : " << average << std::endl;
            std::cout << "Variance(sec^2) : " << variance << std::endl;
            std::cout << "StdDev(sec)     : " << stddev << std::endl;
            std::cout << "Minimum(sec)    : " << min << std::endl;
            std::cout << "Q1(sec)         : " << q1 << std::endl;
            std::cout << "Median(sec)     : " << median << std::endl;
            std::cout << "Q3(sec)         : " << q3 << std::endl;
            std::cout << "Maximum(sec)    : " << max << std::endl;
        }
    }
    margo_addr_free(mid, server_addr);
    margo_finalize(mid);
}
static sdskv_db_type_t database_type_from_string(const std::string& type) {
    if(type == "map") {
        return KVDB_MAP;
    } else if(type == "leveldb" || type == "ldb") {
        return KVDB_LEVELDB;
    } else if(type == "berkeleydb" || type == "bdb") {
        return KVDB_BERKELEYDB;
    }
    throw std::runtime_error(std::string("Unknown database type \"") + type + "\"");
}
