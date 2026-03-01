#!/bin/bash

DEV=/dev/$1
SSD=/mnt/ssd

sudo mkfs.ext4 $DEV && sudo mkdir $SSD && sudo mount $DEV $SSD && sudo chown ubuntu:ubuntu $SSD
