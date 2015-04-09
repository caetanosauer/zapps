INCLUDE_DIRECTORIES (
  ${CMAKE_SOURCE_DIR}/src/base
  ${ZERO_SOURCE_DIR}/config
  ${ZERO_SOURCE_DIR}/src/sm
  ${ZERO_SOURCE_DIR}/src/common
  ${ZERO_SOURCE_DIR}/src/sthread
  ${ZERO_SOURCE_DIR}/src/fc
  ${ZERO_SOURCE_DIR}/src/Lintel
)
# to look for generated headers
INCLUDE_DIRECTORIES (
  ${CMAKE_BINARY_DIR}/config
  ${ZERO_BINARY_DIR}/config
  ${ZERO_BINARY_DIR}/src/sm
  ${ZERO_BINARY_DIR}/src/sthread
)

add_subdirectory(base)

add_executable(zapps main.cpp)

target_link_libraries(zapps
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
