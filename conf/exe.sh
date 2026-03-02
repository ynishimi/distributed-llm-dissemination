#!/bin/bash
ID=$1
MODE=$2
IS_DISK=$3
IS_SETUP=$4

if [ "$IS_DISK" -eq 1 ]; then
    SSD_FLAG="-s /mnt/ssd"
else
    SSD_FLAG=""
fi

if [ "$IS_SETUP" -eq 1 ]; then
    ./distributor -id $ID -f config.json $SSD_FLAG -m $MODE -l -v
fi

sudo sh -c 'echo 1 > /proc/sys/vm/drop_caches'
./distributor -id $ID -f config.json $SSD_FLAG -m $MODE -v