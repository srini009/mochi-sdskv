// Copyright (c) 2017, Los Alamos National Security, LLC.
// All rights reserved.
#ifndef bulk_h
#define bulk_h

#include <stddef.h>
#include "kv-config.h"
//#include <boost/functional/hash.hpp>
#include "fnv1a.h"
#include <vector>
#include <string>

class ds_bulk_t {
    
    bool   _owns_data = false;
    size_t _size      = 0;
    char*  _data      = nullptr;

    public:

    ds_bulk_t() = default;

    ds_bulk_t(const char* begin, const char* end)
    : _size(std::distance(begin, end))
    , _data(const_cast<char*>(begin)) {}

    ds_bulk_t(const char* data, size_t size)
    : _size(size)
    , _data(const_cast<char*>(data)) {}

    ds_bulk_t(size_t size)
    : _owns_data(true)
    , _size(size)
    , _data((char*)malloc(size)) {}

    ~ds_bulk_t() {
        if(_owns_data) free(_data);
    }

    ds_bulk_t(const ds_bulk_t& other)
    : _owns_data(true)
    , _size(other._size) {
        _data = (char*)malloc(_size);
        memcpy(_data, other._data, _size);
    }

    ds_bulk_t(ds_bulk_t&& other)
    : _owns_data(other._owns_data)
    , _size(other._size)
    , _data(other._data) {
        other._owns_data = false;
        other._size = 0;
        other._data = nullptr;
    }

    ds_bulk_t& operator=(const ds_bulk_t& other) {
        if(&other == this) return *this;
        if(_owns_data) free(_data);
        _owns_data = true;
        _size = other._size;
        _data = (char*)malloc(_size);
        memcpy(_data, other._data, _size);
        return *this;
    }

    ds_bulk_t& operator=(ds_bulk_t&& other) {
        if(&other == this) return *this;
        if(_owns_data) free(_data);
        _owns_data = other._owns_data;
        _size = other._size;
        _data = other._data;
        other._owns_data = false;
        other._size = 0;
        other._data = nullptr;
        return *this;
    }

    const char* data() const {
        return _data;
    }

    char* data() {
        return _data;
    }

    size_t size() const {
        return _size;
    }

    bool operator==(const ds_bulk_t& other) const {
        if(_size != other._size) return false;
        if(_data == other._data) return true;
        return 0 == memcmp(_data, other._data, _size);
    }

    bool operator!=(const ds_bulk_t& other) const {
        return !(*this == other);
    }

    bool operator<(const ds_bulk_t& other) const {
        if(_size == other._size) {
            if(_data == other._data) return false;
            int r = memcmp(_data, other._data, _size);
            return r < 0;
        } else {
            auto s = std::min(_size, other._size);
            if(_data == other._data)
                return _size < other._size;
            int r = memcmp(_data, other._data, _size);
            if(r < 0) return true;
            if(r > 0) return false;
            return _size < other._size;
        }
    }
};

struct ds_bulk_hash {
    size_t operator()(const ds_bulk_t &v) const {
        auto hashfn = fnv1a_t<8 * sizeof(std::size_t)> {};
        hashfn.update(v.data(), v.size());
        return hashfn.digest();
    }
};

struct ds_bulk_equal {
    bool operator()(const ds_bulk_t &v1, const ds_bulk_t &v2) const {
        return (v1 == v2);
    }
};

struct ds_bulk_less {
    bool operator()(const ds_bulk_t &v1, const ds_bulk_t &v2) const {
        return (v1 < v2);
    }
};

#endif // bulk_h
