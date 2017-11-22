ACLOCAL_AMFLAGS="-Im4"

bin_PROGRAMS = test/bench-client \
	       test/test-client \
	       test/test-server
test_bench_client_SOURCES = test/bench-client.cc
test_bench_client_CXXFLAGS = -pthread -std=c++11 -g -Wall -mcx16 -Wno-invalid-offsetof
test_bench_client_CPPFLAGS = ${CPPFLAGS} -I${srcdir}/src
test_bench_client_DEPENDENCIES = lib/libkvclient.la
test_bench_client_LDFLAGS = -Llib -lkvclient
test_bench_client_LDADD = ${LIBS} -lkvclient

lib_LTLIBRARIES = lib/libkvclient.la \
                  lib/libkvserver.la \
                  lib/libkvgroupclient.la \
                  lib/libkvgroupserver.la

lib_libkvclient_la_SOURCES = src/kv-client.c

lib_libkvgroupclient_la_SOURCES = src/kvgroup-client.cc
lib_libkvgroupclient_la_CXXFLAGS = -pthread -std=c++11 -g -Wall -mcx16 -Wno-invalid-offsetof
lib_libkvgroupclient_la_CPPFLAGS = ${CPPFLAGS}

lib_libkvserver_la_SOURCES = src/kv-server.cc \
			 src/datastore.cc \
			 src/BwTree/src/bwtree.cpp

include_HEADERS = src/sds-keyval.h \
		  src/sds-keyval-group.h

noinst_HEADERS = src/datastore.h \
		 src/BwTree/src/bwtree.h \
		 src/BwTree/src/atomic_stack.h\
		 src/BwTree/src/bloom_filter.h \
		 src/BwTree/src/sorted_small_set.h

lib_libkvserver_la_CXXFLAGS = -pthread -std=c++11 -g -Wall -mcx16 -Wno-invalid-offsetof
lib_libkvserver_la_CPPFLAGS = ${CPPFLAGS} -I${srcdir}/src/BwTree/src

lib_libkvgroupserver_la_SOURCES = src/kvgroup-server.cc
lib_libkvgroupserver_la_CXXFLAGS = -pthread -std=c++11 -g -Wall -mcx16 -Wno-invalid-offsetof
lib_libkvgroupserver_la_CPPFLAGS = ${CPPFLAGS}


check_PROGRAMS = test/test-client \
		test/test-server \
		test/bench-client \
		test/test-mpi \
		test/test-mpi-group


TESTS = test/test-client \
        test/test-server \
	test/bench-client \
	test/test-mpi \
	test/test-mpi-group

test_test_client_SOURCES = test/test-client.cc
test_test_client_CXXFLAGS = -pthread -std=c++11 -g -Wall -mcx16 -Wno-invalid-offsetof
test_test_client_CPPFLAGS = ${CPPFLAGS} -I${srcdir}/src
test_test_client_DEPENDENCIES = lib/libkvclient.la
test_test_client_LDFLAGS = -Llib -lkvclient

test_test_server_SOURCES = test/test-server.cc
test_test_server_CXXFLAGS = -pthread -std=c++11 -g -Wall -mcx16 -Wno-invalid-offsetof
test_test_server_CPPFLAGS = ${CPPFLAGS} -I${srcdir}/src
test_test_server_DEPENDENCIES = lib/libkvserver.la
test_test_server_LDFLAGS = -Llib -lkvserver -lssg -lboost_filesystem -lboost_system

test_test_mpi_SOURCES = test/test-mpi.cc

test_test_mpi_CXXFLAGS = -pthread -std=c++11 -g -Wall -mcx16 -Wno-invalid-offsetof
test_test_mpi_CPPFLAGS = ${CPPFLAGS} -I${srcdir}/src


test_test_mpi_DEPENDENCIES = lib/libkvserver.la lib/libkvclient.la
test_test_mpi_LDFLAGS = -Llib -lkvclient -lkvserver -lssg -lboost_filesystem -lboost_system

test_test_mpi_group_SOURCES = test/test-mpi-group.cc
test_test_mpi_group_CXXFLAGS = -pthread -std=c++11 -g -Wall -mcx16 -Wno-invalid-offsetof
test_test_mpi_group_CPPFLAGS = ${CPPFLAGS} -I${srcdir}/src
test_test_mpi_group_DEPENDENCIES = lib/libkvgroupserver.la lib/libkvgroupclient.la lib/libkvserver.la lib/libkvclient.la
test_test_mpi_group_LDFLAGS = -Llib -lkvgroupserver -lkvgroupclient -lkvclient -lkvserver -lssg -lch-placement -lleveldb -ldb_cxx -lboost_filesystem -lboost_system

pkgconfigdir = $(libdir)/pkgconfig
pkgconfig_DATA = maint/kv-server.pc \
		 maint/kv-client.pc



