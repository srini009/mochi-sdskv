#
# General test script utilities
#

if [ -z "$TIMEOUT" ] ; then
    echo expected TIMEOUT variable defined to its respective command
    exit 1
fi

if [ -z "$MKTEMP" ] ; then
    echo expected MKTEMP variable defined to its respective command
    exit 1
fi

TMPDIR=/dev/shm
export TMPDIR
mkdir -p $TMPDIR
TMPBASE=$(${MKTEMP} --tmpdir -d test-XXXXXX)

if [ ! -d $TMPBASE ];
then
  echo "Temp directory doesn't exist: $TMPBASE"
  exit 3
fi

function run_to ()
{
    maxtime=${1}s
    shift
    ${TIMEOUT} --signal=9 $maxtime "$@"
}

function test_start_server ()
{
    startwait=${1:-15}
    maxtime=${2:-120}

    if [[ -z "${SDSKV_TEST_LOG_LEVEL}" ]]; then
      LOG_LEVEL="critical"
    else
      LOG_LEVEL="${SDSKV_TEST_LOG_LEVEL}"
    fi

    run_to ${maxtime} bin/sdskv-server-daemon -v ${LOG_LEVEL} -f $TMPBASE/sdskv.addr ${SDSKV_TEST_TRANSPORT:-"na+sm"} ${@:3} &
    # wait for server to start
    sleep ${startwait}

    svr_addr=`cat $TMPBASE/sdskv.addr`
}

function test_start_custom_server ()
{
    startwait=${1:-15}
    maxtime=${2:-120}

    run_to ${maxtime} test/sdskv-custom-server-daemon -f $TMPBASE/sdskv.addr ${SDSKV_TEST_TRANSPORT:-"na+sm"} ${@:3} &
    # wait for server to start
    sleep ${startwait}

    svr_addr=`cat $TMPBASE/sdskv.addr`
}

function find_db_name ()
{
    test_db_name=${SDSKV_TEST_DB_NAME:-"sdskv-test-db"}
    test_db_type=${SDSKV_TEST_DB_TYPE:-"map"}
    test_db_full="${TMPBASE}/${test_db_name}:${test_db_type}"
}
