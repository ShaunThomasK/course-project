#!/usr/bin/env bash
# Builds everything and runs every test file. As root (sudo) it also runs
# the privilege-dropping tests in test_sudo.sh.
#
#   tests/run_all.sh [name-filter]       e.g. tests/run_all.sh cache
#   sudo tests/run_all.sh                 everything, including test_sudo.sh
cd "$(dirname "$0")/.." || exit 2

if [ "$(id -u)" -ne 0 ]; then
    make -s all tests || exit 2
elif [ ! -x ./scheduler ] || [ ! -x tests/programs/bin/hello ]; then
    echo "Build as yourself first (make all tests), then run with sudo." >&2
    exit 2
fi

files=(tests/test_m1_core.sh tests/test_m2_resilience.sh tests/test_m3_scheduling.sh
       tests/test_m3_cache.sh tests/test_m4_checkpoint.sh)
[ "$(id -u)" -eq 0 ] && files+=(tests/test_sudo.sh)

failed=()
for f in "${files[@]}"; do
    echo
    echo "################ $f"
    bash "$f" "$@" || failed+=("$f")
done

echo
if [ ${#failed[@]} -eq 0 ]; then
    echo "All test files passed."
else
    echo "Failed: ${failed[*]}"
fi
[ "$(id -u)" -ne 0 ] && echo "(Privilege-dropping tests need root: sudo tests/run_all.sh, or sudo tests/test_sudo.sh)"
[ ${#failed[@]} -eq 0 ]
