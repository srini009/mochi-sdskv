// Copyright (c) 2017, Los Alamos National Security, LLC.
// All rights reserved.
#include "datastore.h"
#include "kv-config.h"
#include <chrono>
#include <iostream>

using namespace std::chrono;

AbstractDataStore::AbstractDataStore()
{
    _eraseOnGet = false;
    _debug      = false;
    _in_memory  = false;
};

AbstractDataStore::AbstractDataStore(bool eraseOnGet, bool debug)
{
    _eraseOnGet = eraseOnGet;
    _debug      = debug;
    _in_memory  = false;
};

AbstractDataStore::~AbstractDataStore(){};
