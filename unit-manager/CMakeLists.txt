cmake_minimum_required(VERSION 3.12)
project(unit_manager VERSION 1.6)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_BUILD_TYPE Debug)

# 头文件搜索路径
include_directories(
    ${CMAKE_SOURCE_DIR}/include
    ../hybrid-comm/include
    ../infra-controller/include
    ../utils
    ../network/include
    /usr/local/include
    include
    .
)

# 源文件列表
file(GLOB_RECURSE SRCS
    src/main.cpp
    src/remote_server.cpp
    src/config.cpp
    src/tcp_comm.cpp
    src/remote_action.cpp
    src/unit_data.cpp
    src/zmq_bus.cpp
    ${CMAKE_SOURCE_DIR}/../network/src/*.cc
)

# 尝试用 find_package 找 simdjson
find_package(simdjson QUIET)

# 只定义一次可执行文件
add_executable(${PROJECT_NAME} ${SRCS})

# 链接常规依赖库
target_link_libraries(${PROJECT_NAME} PRIVATE
    pthread
    util
    glog
    pgm
    sodium
    norm
    bsd
    gssapi_krb5
    zmq
    ${CMAKE_SOURCE_DIR}/../install/lib/libstackflow.a
)

# simdjson 链接策略：
# 1) 如果找到了 CMake 的 simdjson 包，就用它
# 2) 否则退回到系统动态库 .so（apt 安装 libsimdjson-dev 后一般在这个路径）
if(simdjson_FOUND)
    message(STATUS "Found simdjson package")
    target_link_libraries(${PROJECT_NAME} PRIVATE simdjson)
else()
    message(WARNING "simdjson package not found, linking manually")
    target_link_libraries(${PROJECT_NAME} PRIVATE /usr/lib/aarch64-linux-gnu/libsimdjson.so)
endif()

# 安装规则
install(TARGETS ${PROJECT_NAME}
    ARCHIVE DESTINATION lib
    LIBRARY DESTINATION lib
    RUNTIME DESTINATION bin
)

install(FILES "sys_config.json"
    DESTINATION share/${PROJECT_NAME}
)
