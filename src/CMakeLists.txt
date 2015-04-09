INCLUDE_DIRECTORIES (
  ${CMAKE_SOURCE_DIR}/src/base
  ${SM_SOURCE_DIR}/config
  ${SM_SOURCE_DIR}/src/sm
  ${SM_SOURCE_DIR}/src/common
  ${SM_SOURCE_DIR}/src/sthread
  ${SM_SOURCE_DIR}/src/fc
  ${SM_SOURCE_DIR}/src/Lintel
)
# to look for generated headers
INCLUDE_DIRECTORIES (
  ${CMAKE_BINARY_DIR}/config
  ${SM_BINARY_DIR}/config
  ${SM_BINARY_DIR}/src/sm
  ${SM_BINARY_DIR}/src/sthread
)

if(USE_SHORE)
    include_directories(
        ${SM_BINARY_DIR}/src/atomic_ops
    )
endif()

add_subdirectory(base)

add_executable(zapps main.cpp)

set(zapps_LIBS
    # zapps components
    base
    # Zero/Shore libraries
    libsm
    libsthread
    libcommon
    libfc
    # third-party dependencies
    pthread
    boost_program_options
)

if(USE_SHORE)
    # Additional libs required by Shore-MT
    set (zapps_LIBS ${zapps_LIBS} libatomic_ops)
endif()

target_link_libraries(zapps ${zapps_LIBS})
