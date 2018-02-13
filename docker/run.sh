#!/bin/bash

/etc/init.d/redis-server start

cd /opt/workspace/reproredis
make clean
make -e install

