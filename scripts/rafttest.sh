#!/usr/bin/env bash

BASE_PATH=$(cd "$(dirname "$0")/.." && pwd)
SCRIPT_PATH=$BASE_PATH/scripts
BIN_PATH=$BASE_PATH/bin
LOG_DIR=$BASE_PATH/logs

# Create logs directory
mkdir -p "$LOG_DIR"

# ---------- coloring helper ----------
colorize() {
    awk '
    {
        if ($0 ~ /FAIL/) {
            printf "\033[1;31m%s\033[0m\n", $0
        } else if ($0 ~ /PASS/) {
            printf "\033[1;32m%s\033[0m\n", $0
        } else if (tolower($0) ~ /running/) {
            printf "\033[1;33m%s\033[0m\n", $0
        } else if ($0 ~ /Test/) {
            printf "\033[1;33m%s\033[0m\n", $0
        } else if (tolower($0) ~ /dropped/) {
            printf "\033[1;34m%s\033[0m\n", $0
        } else if ($0 ~ /WON/) {
            printf "\033[1;35m%s\033[0m\n", $0
        } else {
            print $0
        }
        fflush()
    }'
}

run_test() {
    local name=$1
    local num=$2
    local logfile="$LOG_DIR/test_${num}_${name}.log"

    printf "\033[1;33mTest %s starts (%s):\033[0m\n" "$num" "$logfile"

    # Capture raw output to file, show colored version on console
    "$SCRIPT_PATH/rafttest_single.sh" "$name" ${ALL_PORTS} ${ALL_PROXY_PORTS} ${TESTER_PORT} \
        2>&1 | tee "$logfile" | colorize

    echo ""
    echo ""
    sleep 3
}

printf "\033[1;33mPreparing tests:\033[0m\n"

# ---------- compile & build ----------
echo "Compiling yourCode"
cd "$BASE_PATH/yourCode" || exit
sh "$BASE_PATH/yourCode/compile.sh"

echo "Build proxy runner"
cd "$BASE_PATH/tests" || exit
go mod tidy
go build -buildvcs=false -o "$BIN_PATH/raftproxyrunner" "$BASE_PATH/tests/raftproxyrunner"
errorMsg=$?
if [ $errorMsg -ne 0 ]; then
    echo "FAIL: code does not compile"
    exit $errorMsg
fi

echo "Build test binary"
go build -buildvcs=false -o "$BIN_PATH/rafttest" "$BASE_PATH/tests/rafttest"
errorMsg=$?
if [ $errorMsg -ne 0 ]; then
    echo "FAIL: code does not compile"
    exit $errorMsg
fi

cd "$BASE_PATH" || exit
rm -f "$BASE_PATH/rafttest.log"

# ---------- generate ports ----------
i=0
while ((i < 11)); do
    N=$(((RANDOM % 10000) + 10000))
    echo "${A[*]}" | grep -q "$N" && continue
    A[$i]=$N
    ((i++))
done

NODE_PORT0=${A[0]}
NODE_PORT1=${A[1]}
NODE_PORT2=${A[2]}
NODE_PORT3=${A[3]}
NODE_PORT4=${A[4]}
PROXY_NODE_PORT0=${A[5]}
PROXY_NODE_PORT1=${A[6]}
PROXY_NODE_PORT2=${A[7]}
PROXY_NODE_PORT3=${A[8]}
PROXY_NODE_PORT4=${A[9]}
TESTER_PORT=${A[10]}

ALL_PORTS=" ${NODE_PORT0} ${NODE_PORT1} ${NODE_PORT2} ${NODE_PORT3} ${NODE_PORT4}"
ALL_PROXY_PORTS=" ${PROXY_NODE_PORT0} ${PROXY_NODE_PORT1} ${PROXY_NODE_PORT2} ${PROXY_NODE_PORT3} ${PROXY_NODE_PORT4}"

echo "All real ports:  ${ALL_PORTS}"
echo "All proxy ports: ${ALL_PROXY_PORTS}"
echo ""
echo ""
sleep 2

echo "The following 6 tests should be passed before working on the remaining 4."
echo "In between each test, a 3 second wait will be present."
echo ""
echo ""
sleep 2

# ---------- run tests ----------
run_test testOneCandidateOneRoundElection 1
run_test testOneCandidateStartTwoElection 2
run_test testTwoCandidateForElection     3
run_test testSplitVote                   4
run_test testAllForElection              5
run_test testLeaderRevertToFollower      6

echo "Ignore the following tests if you have failed any of the 6 tests above."
echo ""
echo ""
sleep 2

run_test testOneSimplePut        7
run_test testOneSimpleUpdate     8
run_test testOneSimpleDelete     9
run_test testDeleteNonExistKey  10

# ---------- final log ----------
cat "$BASE_PATH/rafttest.log" | tee "$LOG_DIR/test_result.log" | colorize
rm -f "$BASE_PATH/rafttest.log"
