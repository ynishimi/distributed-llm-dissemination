#!/bin/bash

KEY=$1
RECEIVERS=("${@:2}")

# build
GOOS=linux GOARCH=amd64 go build -o bin/distributor ./cmd
GOOS=linux GOARCH=amd64 go build -o bin/diskspeed ./diskspeed

# distribute
for receiver in "${RECEIVERS[@]}"; do
    scp -i "$KEY" -o StrictHostKeyChecking=no bin/distributor bin/diskspeed conf/config.json conf/init.sh "ubuntu@$receiver":~/ &
done
wait

echo "done"