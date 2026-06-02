#!/bin/bash
# Run cold start experiment end-to-end.
#
# Usage:
#   ./conf/run_cold.sh <ssh_key> <priv0> <priv1> <priv2> <priv3> <pub0> <pub1> <pub2> <pub3>
#
# Example:
#   ./conf/run_cold.sh ~/.ssh/id_rsa 10.0.0.1 10.0.0.2 10.0.0.3 10.0.0.4 \
#       54.1.2.3 54.1.2.4 54.1.2.5 54.1.2.6

set -e

KEY=$1
PRIV=("$2" "$3" "$4" "$5")
PUB=("$6" "$7" "$8" "$9")

SSH="ssh -i $KEY -o StrictHostKeyChecking=no"

echo "=== [1/4] Updating config.json for cold start ==="
python3 conf/gen_cold_config.py "${PRIV[@]}"

echo "=== [1.5/4] Killing existing distributor processes ==="
for i in 0 1 2 3; do
    ssh -i "$KEY" -o StrictHostKeyChecking=no "ubuntu@${PUB[$i]}" "kill -9 \$(pgrep -x distributor) 2>/dev/null; sleep 1; true" &
done
wait

echo "=== [2/4] Building and deploying binaries ==="
bash conf/deploy.sh "$KEY" "${PUB[@]}"

echo "=== [3/4] Starting experiment on all nodes ==="
echo "  Starting leader (node 0) on ${PUB[0]}"
$SSH "ubuntu@${PUB[0]}" "./distributor -id 0 -f config.json 2>log0.jsonl" &
sleep 3

for i in 1 2 3; do
    echo "  Starting node $i on ${PUB[$i]}"
    $SSH "ubuntu@${PUB[$i]}" "./distributor -id $i -f config.json -v 2>log${i}.jsonl" &
done

echo "  Waiting for all nodes to finish..."
wait
echo "  All nodes done."

echo "=== [4/4] Collecting logs ==="
bash conf/collect_logs.sh "$KEY" "${PUB[@]}"

echo "=== Done ==="
