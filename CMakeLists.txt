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
SET(CMAKE_CXX_FLAGS_DEBUG5  "-O0 -g -DDEBUG -DW_DEBUG_LEVEL=5 -femit-class-debug-always ${LINK_GOOGLEPROFILER}")
SET(CMAKE_CXX_FLAGS_DEBUG3  "-O0 -g -DDEBUG -DW_DEBUG_LEVEL=3 -femit-class-debug-always ${LINK_GOOGLEPROFILER}")
SET(CMAKE_CXX_FLAGS_DEBUG1  "-Og -g -DDEBUG -DW_DEBUG_LEVEL=1 -femit-class-debug-always ${LINK_GOOGLEPROFILER}")
SET(CMAKE_CXX_FLAGS_DEBUG  "${CMAKE_CXX_FLAGS_DEBUG1}")
## Below is better for performance analysis; above is better for debugging
SET(CMAKE_CXX_FLAGS_PROFILE "-O3 -g -pg -DW_DEBUG_LEVEL=0 -DNDEBUG -fPIC ${LINK_GOOGLEPROFILER} ${COMPILE_MUTRACE}")
SET(CMAKE_CXX_FLAGS_RELEASE "-O3 -g -DNDEBUG -fexpensive-optimizations -finline-functions -DW_DEBUG_LEVEL=0 ${LINK_GOOGLEPROFILER}")

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
#CONFIGURE_FILE( ${CMAKE_SOURCE_DIR}/config/zapps-config.h.in
#    ${CMAKE_BINARY_DIR}/config/zapps-config.h )


#############################################################
# Library dependencies of Zero
#############################################################

IF(NOT SM_BINARY_DIR)
    SET(SM_BINARY_DIR ${CMAKE_BINARY_DIR}/../../zero/build CACHE PATH
        "Path to Zero binary folder, where static libraries are found")
    MESSAGE("SM_BINARY_DIR not found -- set to ${SM_BINARY_DIR}")
ENDIF()
IF(NOT SM_SOURCE_DIR)
    SET(SM_SOURCE_DIR ${CMAKE_SOURCE_DIR}/../zero CACHE PATH
        "Path to Zero source folder, where headers are found")
    MESSAGE("SM_SOURCE_DIR not found -- set to ${SM_SOURCE_DIR}")
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

#############################################################
# Sub-directories
#############################################################

add_subdirectory(src)
