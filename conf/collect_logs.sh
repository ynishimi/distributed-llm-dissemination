#!/bin/bash

KEY=$1
NODES=("${@:2}")
OUTDIR="$(date +%Y%m%d_%H%M%S)"

mkdir -p "logs/$OUTDIR"

for i in "${!NODES[@]}"; do
    ip="${NODES[i]}"
    scp -i "$KEY" -o StrictHostKeyChecking=no "ubuntu@$ip:~/log${i}.jsonl" "logs/$OUTDIR/log${i}.jsonl" &
done
wait

# merge

cat logs/$OUTDIR/log*.jsonl |jq -s 'sort_by(.time)[]' -c > "logs/$OUTDIR/merged.jsonl"
jq -s -c '(.[] | select(.message == "timer start") | .time) as $start | .[] | .time = ((.time - $start) / 1000)' "logs/$OUTDIR/merged.jsonl" > "logs/$OUTDIR/merged_elapsed.jsonl"

echo "saved and merged"