#ifndef _data_slice_h
#define _data_slice_h

#include <stddef.h>
#include "kv-config.h"
#include "fnv1a.h"
#include <vector>
#include <string>

class data_slice {
    
    bool   _owns_data = false;
    size_t _size      = 0;
    char*  _data      = nullptr;

    public:

    data_slice() = default;

    data_slice(const char* begin, const char* end)
    : _size(std::distance(begin, end))
    , _data(const_cast<char*>(begin)) {}

    data_slice(const char* data, size_t size)
    : _size(size)
    , _data(const_cast<char*>(data)) {}

    data_slice(size_t size)
    : _owns_data(true)
    , _size(size)
    , _data((char*)malloc(size)) {}

    ~data_slice() {
        if(_owns_data) free(_data);
    }

    data_slice(const data_slice& other)
    : _owns_data(true)
    , _size(other._size) {
        _data = (char*)malloc(_size);
        memcpy(_data, other._data, _size);
    }

    data_slice(data_slice&& other)
    : _owns_data(other._owns_data)
    , _size(other._size)
    , _data(other._data) {
        other._owns_data = false;
        other._size = 0;
        other._data = nullptr;
    }

    data_slice& operator=(const data_slice& other) {
        if(&other == this) return *this;
        if(_owns_data) free(_data);
        _owns_data = true;
        _size = other._size;
        _data = (char*)malloc(_size);
        memcpy(_data, other._data, _size);
        return *this;
    }

    data_slice& operator=(data_slice&& other) {
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

    bool operator==(const data_slice& other) const {
        if(_size != other._size) return false;
        if(_data == other._data) return true;
        return 0 == memcmp(_data, other._data, _size);
    }

    bool operator!=(const data_slice& other) const {
        return !(*this == other);
    }

    bool operator<(const data_slice& other) const {
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

struct data_slice_hash {
    size_t operator()(const data_slice &v) const {
        auto hashfn = fnv1a_t<8 * sizeof(std::size_t)> {};
        hashfn.update(v.data(), v.size());
        return hashfn.digest();
    }
};

struct data_slice_equal {
    bool operator()(const data_slice &v1, const data_slice &v2) const {
        return (v1 == v2);
    }
};

struct data_slice_less {
    bool operator()(const data_slice &v1, const data_slice &v2) const {
        return (v1 < v2);
    }
};

#endif // bulk_h
