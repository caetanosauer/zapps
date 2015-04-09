cmake_minimum_required (VERSION 2.6)

project(zapps)

MESSAGE (STATUS)
MESSAGE (STATUS "========================================")
MESSAGE (STATUS "CMAKE_SYSTEM=${CMAKE_SYSTEM}")
MESSAGE (STATUS "CMAKE_SYSTEM_NAME=${CMAKE_SYSTEM_NAME}")
MESSAGE (STATUS "CMAKE_SYSTEM_PROCESSOR=${CMAKE_SYSTEM_PROCESSOR}")
MESSAGE (STATUS "CMAKE_C_COMPILER=${CMAKE_C_COMPILER}")
MESSAGE (STATUS "CMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}")
MESSAGE (STATUS "========================================")
MESSAGE (STATUS)

IF(${CMAKE_SYSTEM_PROCESSOR} MATCHES ".*64.*")
  MESSAGE (STATUS "Yes, you are on 64bit OS/CPU!")
ELSE()
  MESSAGE (FATAL_ERROR "Foster B-tree can only be built on 64bit OS/CPU. Your system seems '${CMAKE_SYSTEM_PROCESSOR}'")
  RETURN()
ENDIF()

#if you don't want the full compiler output, remove the following line
SET(CMAKE_VERBOSE_MAKEFILE ON)
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

CONFIGURE_FILE( ${CMAKE_SOURCE_DIR}/config/zeroapps-config.h.in ${CMAKE_BINARY_DIR}/config/zeroapps-config.h )

# add library dependency on libsm of Zero
IF(NOT ZERO_BINARY_DIR)
    SET(ZERO_BINARY_DIR ${CMAKE_BINARY_DIR}/../zero CACHE PATH
        "Path to Zero binary folder, where static libraries are found")
    MESSAGE("ZERO_BINARY_DIR not found -- set to ${ZERO_BINARY_DIR}")
ENDIF(NOT ZERO_BINARY_DIR)

IF(NOT ZERO_SOURCE_DIR)
    SET(ZERO_SOURCE_DIR ${CMAKE_SOURCE_DIR}/../zero CACHE PATH
        "Path to Zero source folder, where headers are found")
    MESSAGE("ZERO_SOURCE_DIR not found -- set to ${ZERO_SOURCE_DIR}")
ENDIF(NOT ZERO_SOURCE_DIR)

add_library(libsm STATIC IMPORTED)
set_target_properties(libsm PROPERTIES
    IMPORTED_LOCATION ${ZERO_BINARY_DIR}/src/sm/libsm.a)

add_library(libcommon STATIC IMPORTED)
set_target_properties(libcommon PROPERTIES
    IMPORTED_LOCATION ${ZERO_BINARY_DIR}/src/common/libcommon.a)

add_library(libfc STATIC IMPORTED)
set_target_properties(libfc PROPERTIES
    IMPORTED_LOCATION ${ZERO_BINARY_DIR}/src/fc/libfc.a)

add_library(libsthread STATIC IMPORTED)
set_target_properties(libsthread PROPERTIES
    IMPORTED_LOCATION ${ZERO_BINARY_DIR}/src/sthread/libsthread.a)

add_subdirectory(src)
