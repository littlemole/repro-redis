CXX = g++
BACKEND=libevent
DESTDIR=/
PREFIX=/usr/local

LIBNAME = reproredis
LIB = ./lib$(LIBNAME).a
LIBINC = ./include/$(LIBNAME)

PWD=$(shell pwd)

BUILDCHAIN = make
CONTAINER = $(shell echo "$(LIBNAME)_$(CXX)_$(BACKEND)_$(BUILDCHAIN)" | sed 's/++/pp/')
IMAGE = littlemole/$(CONTAINER)

BASE_CONTAINER = $(shell echo "devenv_$(CXX)_$(BUILDCHAIN)" | sed 's/++/pp/')
BASE_IMAGE = littlemole\/$(BASE_CONTAINER)

#################################################
# rule to compile all (default rule)
#################################################

all: ## just compiles and links the library
	cd src && make -e -f Makefile 
	
test-build: ## makes the test binaries
	cd t && make -e -f Makefile 
			
#################################################
# make clean
#################################################

clean: ## cleans up build artefacts
	cd t && make -f Makefile clean
	-find -name "*~" -exec rm {} \;
	cd src && make -f Makefile clean
	-rm -rf ./build
		
#################################################
# make test runs the unit tests
#################################################	

run-tests:
	bash -c 'BINS=$$(ls t/build/*.bin); for i in $$BINS; do $$i; if [ "$$?" != "0" ]; then echo "testrunner FAILED"; exit 1; fi; done; echo "testrunner OK";'
	
test: all test-build ## runs unit tests
	make run-tests

build: ## copy artefacts to ./build
	mkdir -p ./build/include
	mkdir -p ./build/lib/pkgconfig
	cp -r include/* ./build/include/
	cp src/build/*.a ./build/lib/
	cp $(LIBNAME).pc ./build/lib/pkgconfig

#################################################
# make install copies the lib to system folders
#################################################

install: remove ## installs lib to $(DESTDIR)/$(PREFIX) defaults to /usr/local
	-rm -rf $(DESTDIR)/$(PREFIX)/include/$(LIBNAME)
	cd src && make clean && make -e -f Makefile 
	cd t && make clean && make -e -f Makefile 
	if [ "$$SKIPTESTS" != "true" ]; then make run-tests; fi
	make build
	cp -r ./build/* $(DESTDIR)/$(PREFIX)	
	cd src && make clean && make release -e -f Makefile 
	cd t && make clean && make release -e -f Makefile 
	if [ "$$SKIPTESTS" != "true" ]; then make run-tests; fi
	make build
	cp -r ./build/* $(DESTDIR)/$(PREFIX)	

remove: ## remove lib from $(DESTDIR)/$(PREFIX) defaults to /usr/local
	-rm -rf $(DESTDIR)/$(PREFIX)/include/$(LIBNAME)
	-rm $(DESTDIR)/$(PREFIX)/lib/lib$(LIBNAME)*.a
	-rm $(DESTDIR)/$(PREFIX)/lib/pkgconfig/$(LIBNAME).pc
	

# docker stable testing environment

update-dockerfile:
	/bin/sed -i "s/FROM .*/FROM ${BASE_IMAGE}/" Dockerfile

image: update-dockerfile## build docker test image
	docker build -t $(IMAGE) . -fDockerfile  --build-arg CXX=$(CXX) --build-arg BACKEND=$(BACKEND) --build-arg BUILDCHAIN=$(BUILDCHAIN)

clean-image: update-dockerfile ## rebuild the docker test image from scratch
	docker build -t $(IMAGE) . --no-cache -fDockerfile --build-arg CXX=$(CXX) --build-arg BACKEND=$(BACKEND) --build-arg BUILDCHAIN=$(BUILDCHAIN)
		
bash: rmc image ## run the docker image and open a shell
	docker run --name $(CONTAINER) -ti $(IMAGE) bash

stop: ## stop running docker image, if any
	-docker stop $(CONTAINER)
	
rmc: stop ## remove docker container, if any
	-docker rm $(CONTAINER)

rmi : ## remove existing docker image, if any
	-docker rmi $(IMAGE)


# self documenting makefile, see 
# https://marmelab.com/blog/2016/02/29/auto-documented-makefile.html

help:
	@echo "common build flags for all targets:"
	@echo "\tCXX set to g++|clang++ to set compiler (defaults to g++)"
	@echo "\tBACKEND set to libevent|boost_asio (defaults to libevent)"
	@echo "example calls:"
	@echo "\t make test BACKEND=boost_asio "
	@echo "\t sudo make install BACKEND=boost_asio "
	@echo "\t sudo make run CXX=clang++ "
	@echo "\t sudo make run CXX=clang++ BACKEND=boost_asio "
	@echo "available targets:"
	@grep -E -h '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

.PHONY: build help rmi rmc stop bash image clean-image release remove install test clean test-build


