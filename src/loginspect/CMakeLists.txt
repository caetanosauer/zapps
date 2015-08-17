set(loginspect_SRCS
    ${CMAKE_CURRENT_SOURCE_DIR}/logcat.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/agglog.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/verifylog.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/logstats.cpp
    )

add_library (loginspect ${loginspect_SRCS})
