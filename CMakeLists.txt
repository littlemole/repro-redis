############################################
cmake_minimum_required(VERSION 2.8.9)
project(reproredis)
include(CTest)

set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

############################################
# macros
############################################

macro(AddCompilerFlags target flags)
    get_target_property(CF ${target} COMPILE_FLAGS)
    if(CF STREQUAL "CF-NOTFOUND")
        SET(CF "") # set to empty string
    else()
        SET(CF "${CF} ") # a space to cleanly separate from existing content
    endif()

    SET(CF "${CF} ${flags}" )
    set_target_properties(${target} PROPERTIES COMPILE_FLAGS ${CF} )
endmacro()


############################################
# clang support
############################################

set(LINKERFLAGS "")
set(STDLIB "")
set(FLAGS "")

if ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "Clang")
    set(STDLIB "c++abi")
    set(LINKERFLAGS "-stdlib=libc++ -fcoroutines-ts  -std=c++14")
    set(FLAGS "-stdlib=libc++ -fcoroutines-ts -D_RESUMABLE_FUNCTIONS_SUPPORTED")
endif()

set( CMAKE_EXE_LINKER_FLAGS  "${CMAKE_EXE_LINKER_FLAGS} ${LINKERFLAGS}" )


############################################
# OS specific dependencies
############################################

find_package(GTest REQUIRED)
find_package(OpenSSL REQUIRED)

IF (WIN32)

	set(OSLIBS "Ws2_32" "Rpcrt4")

	find_library(PRIOCPP_LIBRARY NAMES priocppd)
	find_library(REDIS_LIBRARY NAMES hiredis)
	find_library(INTEROP_LIBRARY NAMES Win32_Interop)
	find_library(LIBEVENT_LIBRARY NAMES event)

	set(BUILDFLAGS "-await ${FLAGS}")
	set(DEBUGFLAGS "-DMOL_PROMISE_DEBUG")

	set(LIBS ${PRIOCPP_LIBRARY} ${CRYPTONEAT_LIBRARY} ${GTEST_LIBRARIES} ${OPENSSL_LIBRARIES} ${REDIS_LIBRARY} ${INTEROP_LIBRARY} ${STDLIB} )

 ELSEIF(UNIX)

    set(OSLIBS "pthread")

    find_package(PkgConfig)
    pkg_check_modules(HIREDIS REQUIRED hiredis)
    pkg_check_modules(PRIOCPP REQUIRED priocpp)
    pkg_check_modules(LIBEVENT REQUIRED libevent_pthreads)

	set(LIBEVENT_LIBRARY ${LIBEVENT_LIBRARIES})

    set(BUILDFLAGS " ${FLAGS}")
	set(DEBUGFLAGS "-g -DMOL_PROMISE_DEBUG")

	set(LIBS ${PRIOCPP_LIBRARIES} ${GTEST_LIBRARIES} ${OPENSSL_LIBRARIES} ${HIREDIS_LIBRARIES} ${STDLIB} )
ENDIF ()


############################################
# backend: libevent or boost_asio
############################################

set(BACKEND ${LIBEVENT_LIBRARY})

option(WITH_LIBEVENT "use libevent (default:on)" ON)
if(WITH_LIBEVENT)
    add_definitions(-DPROMISE_USE_LIBEVENT)
else()
    add_definitions(-DPROMISE_USE_BOOST_ASIO)
    set(BACKEND "boost_system")
endif()


############################################
# sources
############################################

include_directories(include)
include_directories(${OPENSSL_INCLUDE_DIR})
include_directories(${PRIOCPP_INCLUDE_DIRS})

file(GLOB SOURCES "src/*.cpp")
file(GLOB HEADERS "include/reproredis/*.h")

############################################
# targets
############################################

add_library(reproredis  STATIC ${SOURCES})
add_library(reproredisd STATIC ${SOURCES})

add_executable(Tests t/test.cpp)

AddCompilerFlags(reproredis ${BUILDFLAGS})

AddCompilerFlags(reproredisd ${BUILDFLAGS})
AddCompilerFlags(reproredisd ${DEBUGFLAGS})

AddCompilerFlags(Tests ${BUILDFLAGS})
AddCompilerFlags(Tests ${DEBUGFLAGS})


############################################
# libs
############################################

target_link_libraries(Tests reproredisd ${LIBS} ${BACKEND} ${OSLIBS} )


############################################
# tests
############################################

if(WIN32)
	message("skip tests on win32")
else()
	add_test(NAME AllTests COMMAND Tests)
endif()
 
############################################
# install rules
############################################
 
install(TARGETS reproredis DESTINATION lib)
install(TARGETS reproredisd DESTINATION lib)

install(FILES ${HEADERS} DESTINATION include/reproredis)

install(FILES reproredis.pc DESTINATION lib/pkgconfig)
