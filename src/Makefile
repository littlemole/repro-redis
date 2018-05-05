
DEBUG = -g -DMOL_PROMISE_DEBUG
release : override DEBUG = -O3

DEFAULT_OPTS = -std=c++14

ifneq ($(CXX),g++)
DEFAULT_OPTS = -std=c++14 -stdlib=libc++ -fcoroutines-ts -D_RESUMABLE_FUNCTIONS_SUPPORTED 
endif

OPTIONS = -std=c++14 -pthread -D_REENTRANT $(DEFAULT_OPTS)

PROMISE_IMPL = -DPROMISE_USE_LIBEVENT
ifeq ($(BACKEND),boost_asio)
        PROMISE_IMPL = -DPROMISE_USE_BOOST_ASIO
endif

# compile flags
CPPFLAGS = -Wall -I../include $(DEBUG) $(INCLUDE) $(OPTIONS) $(PROMISE_IMPL)


#################################################
# source and build path information
#################################################

# the  lib
LIB = ./build/libreproredisd.a
release : override LIB = ./build/libreproredis.a

LIB_SRC = .
LIB_BUILD = ./build


# Objects for the static library
LIB_SRCFILESABS = $(shell ls $(LIB_SRC)/*.cpp)
LIB_SRCFILES =  $(notdir $(LIB_SRCFILESABS))
LIB_OBJFILES = $(LIB_SRCFILES:%.cpp=$(LIB_BUILD)/%.o)

#################################################
# usually no need to change anything below
#################################################

.SUFFIXES: .cpp

#################################################
# rule to compile all (default rule)
#################################################

all: $(LIB) 

#################################################
# release rule 
#################################################

release: all

#################################################
# actual build rules
#################################################
	
#################################################
# rules to compile .o files from .cpp source
#################################################

$(LIB_BUILD)/%.o: $(LIB_SRC)/%.cpp 
	-mkdir -p $(LIB_BUILD)
	$(CXX) -c $^ -o $@ $(CPPFLAGS)

#################################################
# rule to compile the lib file from .o files
#################################################

$(LIB): $(LIB_OBJFILES)
	ar rcs $(LIB) $(LIB_OBJFILES)
	ranlib $(LIB)

		
#################################################
# make clean
#################################################

clean:
	-find -name "*~" -exec rm {} \;
	-rm -f $(LIB_BUILD)/*.a
	-rm -f $(LIB_BUILD)/*.o
	
