set(kits_SRCS
    ${CMAKE_CURRENT_SOURCE_DIR}/field.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/file_desc.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/index_desc.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/row.cpp
    #${CMAKE_CURRENT_SOURCE_DIR}/table_desc.cpp
    #${CMAKE_CURRENT_SOURCE_DIR}/table_man.cpp
    )

add_library (kits ${kits_SRCS})
