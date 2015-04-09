cmake_minimum_required (VERSION 2.6)

project(zapps)

# Most of the stuff was copied from Zero's cmake

MESSAGE (STATUS)
MESSAGE (STATUS "========================================")
MESSAGE (STATUS "CMAKE_SYSTEM=${CMAKE_SYSTEM}")
MESSAGE (STATUS "CMAKE_SYSTEM_NAME=${CMAKE_SYSTEM_NAME}")
MESSAGE (STATUS "CMAKE_SYSTEM_PROCESSOR=${CMAKE_SYSTEM_PROCESSOR}")
MESSAGE (STATUS "CMAKE_C_COMPILER=${CMAKE_C_COMPILER}")
MESSAGE (STATUS "CMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}")
MESSAGE (STATUS "========================================")
MESSAGE (STATUS)

# Require 64-bit CPU
IF(${CMAKE_SYSTEM_PROCESSOR} MATCHES ".*64.*")
    MESSAGE (STATUS "Yes, you are on 64bit OS/CPU!")
    # Used in some checks in Shore-MT headers
    SET (ARCH_LP64  "true") # so far, always assume 64 bits
ELSE()
    MESSAGE (FATAL_ERROR
        "Zapps can only be built on 64bit OS/CPU. Your system seems 
        to be '${CMAKE_SYSTEM_PROCESSOR}'")
    RETURN()
ENDIF()

# Verbose gcc output when compiling (i.e., shows the whole command)
SET(CMAKE_VERBOSE_MAKEFILE OFF)

#############################################################
# Debug builds and coompilation flags
#############################################################

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/cmake/Modules/")

IF(NOT CMAKE_BUILD_TYPE)
  SET(CMAKE_BUILD_TYPE Release CACHE STRING
      "Default=Release build. You can choose the type of build, options are: Debug Release RelWithDebInfo Trace"
      FORCE)
ENDIF(NOT CMAKE_BUILD_TYPE)

# compiler options. ported from Makefile.generic of original ShoreMT.
# W_DEBUG_LEVEL enables various check logics and asserts. higher is more verbose.
SET(CMAKE_CXX_FLAGS_TRACE  "-Og -g -DDEBUG -DW_DEBUG_LEVEL=5 ${LINK_GOOGLEPROFILER}")
SET(CMAKE_CXX_FLAGS_DEBUG  "-O0 -g -DDEBUG -DW_DEBUG_LEVEL=3 ${LINK_GOOGLEPROFILER}")
##  SET(CMAKE_CXX_FLAGS_RELWITHDEBINFO "-O2 -g -DW_DEBUG_LEVEL=1 ${LINK_GOOGLEPROFILER}")
## Below is better for performance analysis; above is better for debugging
SET(CMAKE_CXX_FLAGS_RELWITHDEBINFO "-O2 -g -DW_DEBUG_LEVEL=0 -DNDEBUG -fPIC ${LINK_GOOGLEPROFILER} ${COMPILE_MUTRACE}")
#  SET(CMAKE_CXX_FLAGS_RELWITHDEBINFO "-O0 -g -DW_DEBUG_LEVEL=3 -DDEBUG ${LINK_GOOGLEPROFILER}")
SET(CMAKE_CXX_FLAGS_RELEASE "-O3 -DNDEBUG -fexpensive-optimizations -finline-functions -DW_DEBUG_LEVEL=0 ${LINK_GOOGLEPROFILER}")

# -pedantic requires -Wno-long-long
#
SET (ALWAYS_FLAGS "-std=c++11 -Wall -Wextra -Woverloaded-virtual -fno-strict-aliasing -Wpointer-arith -Wwrite-strings -pthread")
# NO_INLINE_FLAGS =  -fno-inline
SET (NO_INLINE_FLAGS "")
SET (YES_INLINE_FLAGS "-finline-functions")

## enabled by config option (default)
## Now try to find out the compiler arguments needed
SET(TUNE_FLAGS "-mtune=nocona")
# TUNE_FLAGS = -mtune=generic
SET(TARGET_FLAGS "-m64")
## Assembler flags
SET(AM_LDFLAGS "-m64")

SET(ALL_FLAGS "${PEDANTIC} ${TUNE_FLAGS} ${DEBUGFLAGS} ${W_WARNINGS} ${OPTFLAGS} ${MANDATORY_FLAGS} ${ALWAYS_FLAGS} ${TARGET_FLAGS} ${TEMPLATEFLAGS}")
ADD_DEFINITIONS(${ALL_FLAGS})


#############################################################
# Options to be passed to code via pre-processor flags
#############################################################

# options and configurations
option (USE_SHORE 
    "Compile with Shore-MT instead of Zero" OFF) 

CONFIGURE_FILE( ${CMAKE_SOURCE_DIR}/config/zapps-config.h.in
    ${CMAKE_BINARY_DIR}/config/zapps-config.h )


#############################################################
# Library dependencies of Zero/Shore-MT
#############################################################

IF(NOT USE_SHORE)
    IF(NOT SM_BINARY_DIR)
        SET(SM_BINARY_DIR ${CMAKE_BINARY_DIR}/../zero CACHE PATH
            "Path to Zero binary folder, where static libraries are found")
        MESSAGE("SM_BINARY_DIR not found -- set to ${SM_BINARY_DIR}")
    ENDIF()
    IF(NOT SM_SOURCE_DIR)
        SET(SM_SOURCE_DIR ${CMAKE_SOURCE_DIR}/../zero CACHE PATH
            "Path to Zero source folder, where headers are found")
        MESSAGE("SM_SOURCE_DIR not found -- set to ${SM_SOURCE_DIR}")
    ENDIF()
ELSE()
    # In Shore-MT, binaries and source are on the same path
    # Make checks for consistency of paramters given, if any
    IF(SM_SOURCE_DIR AND SM_BINARY_DIR)
        IF(NOT (SM_SOURCE_DIR STREQUAL SM_BINARY_DIR))
            MESSAGE(FATAL_ERROR 
                "SM_BINARY_DIR and SM_SOURCE_DIR must be equal with USE_SHORE!")
            MESSAGE(FATAL_ERROR 
                "Only one of the options is required though.")
            RETURN()
        ENDIF()
    ELSEIF (SM_SOURCE_DIR AND (NOT SM_BINARY_DIR))
        SET(SM_BINARY_DIR ${SM_SOURCE_DIR})
    ELSEIF ((NOT SM_SOURCE_DIR) AND SM_BINARY_DIR)
        SET(SM_SOURCE_DIR ${SM_BINARY_DIR})
    ELSE()
        SET(SM_SOURCE_DIR ${CMAKE_SOURCE_DIR}/../shore-mt CACHE PATH
            "Path to Shore-MT source folder, where headers and libraries are found")
        MESSAGE("SM_SOURCE_DIR not found -- set to ${SM_SOURCE_DIR}")
        SET(SM_BINARY_DIR ${SM_SOURCE_DIR})
    ENDIF()
ENDIF()

add_library(libsm STATIC IMPORTED)
set_target_properties(libsm PROPERTIES
    IMPORTED_LOCATION ${SM_BINARY_DIR}/src/sm/libsm.a)

add_library(libcommon STATIC IMPORTED)
set_target_properties(libcommon PROPERTIES
    IMPORTED_LOCATION ${SM_BINARY_DIR}/src/common/libcommon.a)

add_library(libfc STATIC IMPORTED)
set_target_properties(libfc PROPERTIES
    IMPORTED_LOCATION ${SM_BINARY_DIR}/src/fc/libfc.a)

add_library(libsthread STATIC IMPORTED)
set_target_properties(libsthread PROPERTIES
    IMPORTED_LOCATION ${SM_BINARY_DIR}/src/sthread/libsthread.a)

# libatomic_ops was removed in Zero
IF(USE_SHORE)
    add_library(libatomic_ops STATIC IMPORTED)
    set_target_properties(libatomic_ops PROPERTIES
        IMPORTED_LOCATION ${SM_BINARY_DIR}/src/atomic_ops/libatomic_ops.a)
ENDIF()

#############################################################
# Sub-directories
#############################################################

add_subdirectory(src)
