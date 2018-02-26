#!/bin/bash
set -e

cd /usr/local/src/
git clone https://github.com/littlemole/$1.git

SKIPTESTS=true /usr/local/bin/build.sh $1
