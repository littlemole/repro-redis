# This is a comment
FROM littlemole/devenv_gpp_make
MAINTAINER me <little.mole@oha7.org>

# std dependencies
RUN DEBIAN_FRONTEND=noninteractive apt-get install -y redis-server

ARG CXX=g++
ENV CXX=${CXX}

ARG BACKEND=libevent
ENV BACKEND=${BACKEND}

ARG BUILDCHAIN=make
ENV BUILDCHAIN=${BUILDCHAIN}

RUN /usr/local/bin/install.sh repro 
RUN /usr/local/bin/install.sh prio 

RUN mkdir -p /usr/local/src/repro-redis
ADD . /usr/local/src/repro-redis

RUN sed -i 's/bind 127\.0\.0\.1 ::1/bind 127.0.0.1/' /etc/redis/redis.conf

RUN /etc/init.d/redis-server start && SKIPTESTS=false /usr/local/bin/build.sh repro-redis 
