#!/bin/bash

NODES=("$@")
OUTDIR="$(date +%Y%m%d_%H%M%S)"

mkdir -p "logs/$OUTDIR"

for i in "${!NODES[@]}"; do
    ip="${NODES[i]}"
    scp "ubuntu@$ip:~/log${i}.jsonl" "logs/$OUTDIR/log${i}.jsonl" &
done
wait

# merge

cat logs/$OUTDIR/log*.jsonl |jq -s 'sort_by(.time)[]' -c > "logs/$OUTDIR/merged.jsonl"
jq -s -c '(.[] | select(.message == "timer start") | .time) as $start | .[] | .time = ((.time - $start) / 1000)' "logs/$OUTDIR/merged.jsonl" > "logs/$OUTDIR/merged_elapsed.jsonl"

echo "saved and merged"