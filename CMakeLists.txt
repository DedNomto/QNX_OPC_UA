cmake_minimum_required(VERSION 3.5)

project(QNX_OPC_UA LANGUAGES C)

set(CMAKE_C_STANDARD 99)
set(CMAKE_C_STANDARD_REQUIRED ON)
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wall -D_QNX_SOURCE")

if(CMAKE_BUILD_TYPE STREQUAL "Debug")
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -g -O0 -DDEBUG")
else()
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -O2 -DNDEBUG")
endif()

add_executable(QNX_OPC_UA
    main.c
    main.h
)

target_include_directories(QNX_OPC_UA PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/include/mqueue
    ${CMAKE_CURRENT_SOURCE_DIR}/include/open62541
    ${CMAKE_CURRENT_SOURCE_DIR}/include/plugin
    ${CMAKE_CURRENT_SOURCE_DIR}/include/
    ${CMAKE_CURRENT_SOURCE_DIR}
)

target_link_libraries(QNX_OPC_UA
    ${CMAKE_CURRENT_SOURCE_DIR}/mqueue_lib.a
    ${CMAKE_CURRENT_SOURCE_DIR}/libopen62541.a
    socket
)

if(CMAKE_SYSTEM_NAME STREQUAL "KPDA")
    set(INSTALL_DESTDIR "/tmp")

    install(TARGETS QNX_OPC_UA
        RUNTIME DESTINATION "${INSTALL_DESTDIR}"
        BUNDLE DESTINATION "${INSTALL_DESTDIR}"
        LIBRARY DESTINATION "${INSTALL_DESTDIR}"
    )
endif()
