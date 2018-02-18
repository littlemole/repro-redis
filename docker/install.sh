#!/bin/bash

cd /usr/local/src/
git clone https://github.com/littlemole/$1.git

/usr/local/bin/build.sh $1
