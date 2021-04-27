// Copyright (c) 2017, Los Alamos National Security, LLC.
// All rights reserved.
#ifndef bulk_h
#define bulk_h

#include <stddef.h>
#include "kv-config.h"
//#include <boost/functional/hash.hpp>
#include <vector>
#include <string>

// implementation is std::vector<char> specific
// typedef is for convenience
typedef std::vector<char> ds_bulk_t;

struct ds_bulk_hash {
    size_t operator()(const ds_bulk_t& v) const
    {
        size_t hash = 0;
        hash        = std::hash<std::string>()(std::string(v.begin(), v.end()));
        // boost::hash_range(hash, v.begin(), v.end());
        return hash;
    }
};

struct ds_bulk_equal {
    bool operator()(const ds_bulk_t& v1, const ds_bulk_t& v2) const
    {
        return (v1 == v2);
    }
};

struct ds_bulk_less {
    bool operator()(const ds_bulk_t& v1, const ds_bulk_t& v2) const
    {
        return (v1 < v2);
    }
};

#endif // bulk_h
