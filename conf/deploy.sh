#!/bin/bash

RECEIVERS=("$@")

# build
GOOS=linux GOARCH=amd64 go build -o bin/distributor ./cmd
GOOS=linux GOARCH=amd64 go build -o bin/diskspeed ./diskspeed

# distribute
for receiver in "${RECEIVERS[@]}"; do
    scp bin/distributor bin/diskspeed conf/config.json conf/init.sh conf/exe.sh "ubuntu@$receiver":~/ &
done
wait

echo "done"