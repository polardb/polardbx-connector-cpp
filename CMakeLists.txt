cmake_minimum_required(VERSION 3.20)

# 项目名称
project(PolardbxDriver)

set(POLARDBX_VERSION_MAJOR 1)
set(POLARDBX_VERSION_MINOR 0)
set(POLARDBX_VERSION_PATCH 0)

# 设置C++标准
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED True)

# 确保编译和链接都使用 -pthread (仅在非Windows平台)
if(NOT WIN32)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -pthread")
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -pthread")
endif()

# 设置rpath以解决动态库加载问题
if(APPLE)
    set(CMAKE_INSTALL_RPATH "@executable_path/../lib;@executable_path;${CMAKE_INSTALL_RPATH}")
    set(CMAKE_BUILD_WITH_INSTALL_RPATH TRUE)
elseif(UNIX)
    set(CMAKE_INSTALL_RPATH "$ORIGIN/../lib:$ORIGIN:")
endif()

configure_file(
    include/version_info.h.cmake
    ${CMAKE_BINARY_DIR}/version_info.h
    @ONLY
)
include_directories(${CMAKE_BINARY_DIR})

find_package(GTest REQUIRED)
include_directories(${GTEST_INCLUDE_DIRS})

# MySQL Connector/C++ 路径配置
set(MYSQLCPPCONN_ROOT_DIR "/usr/local/mysql-connector-c++-8.0.32")
set(MYSQLCPPCONN_INCLUDE_DIR "${MYSQLCPPCONN_ROOT_DIR}/include")

# 查找MySQL Connector/C++库文件
find_library(MYSQLCPPCONN_LIBRARY
    NAMES mysqlcppconn.9.dylib libmysqlcppconn.9.dylib mysqlcppconn libmysqlcppconn
    PATHS ${MYSQLCPPCONN_ROOT_DIR}/lib64
)

find_library(MYSQLCPPCONN8_LIBRARY
    NAMES mysqlcppconn8.2.dylib libmysqlcppconn8.2.dylib mysqlcppconn8 libmysqlcppconn8
    PATHS ${MYSQLCPPCONN_ROOT_DIR}/lib64
)

# 查找依赖的SSL库
if(NOT WIN32)
    find_library(SSL_LIBRARY
        NAMES ssl.1.1.dylib libssl.1.1.dylib ssl.3.dylib libssl.3.dylib ssl libssl
        PATHS ${MYSQLCPPCONN_ROOT_DIR}/lib64 /opt/homebrew/lib
    )
    
    find_library(CRYPTO_LIBRARY
        NAMES crypto.1.1.dylib libcrypto.1.1.dylib crypto.3.dylib libcrypto.3.dylib crypto libcrypto
        PATHS ${MYSQLCPPCONN_ROOT_DIR}/lib64 /opt/homebrew/lib
    )
endif()

# 如果未找到，使用默认路径
if(NOT MYSQLCPPCONN_INCLUDE_DIR)
    set(MYSQLCPPCONN_INCLUDE_DIR "/usr/local/mysql-connector-c++-8.0.32/include")
endif()

if(NOT MYSQLCPPCONN_LIBRARY)
    set(MYSQLCPPCONN_LIBRARY "/usr/local/mysql-connector-c++-8.0.32/lib64/libmysqlcppconn.9.dylib")
endif()

if(NOT MYSQLCPPCONN8_LIBRARY)
    set(MYSQLCPPCONN8_LIBRARY "/usr/local/mysql-connector-c++-8.0.32/lib64/libmysqlcppconn8.2.dylib")
endif()

if(NOT SSL_LIBRARY)
    if(APPLE AND EXISTS "/opt/homebrew/opt/openssl@3/lib/libssl.3.dylib")
        set(SSL_LIBRARY "/opt/homebrew/opt/openssl@3/lib/libssl.3.dylib")
    else()
        set(SSL_LIBRARY "/usr/local/mysql-connector-c++-8.0.32/lib64/libssl.1.1.dylib")
    endif()
endif()

if(NOT CRYPTO_LIBRARY)
    if(APPLE AND EXISTS "/opt/homebrew/opt/openssl@3/lib/libcrypto.3.dylib")
        set(CRYPTO_LIBRARY "/opt/homebrew/opt/openssl@3/lib/libcrypto.3.dylib")
    else()
        set(CRYPTO_LIBRARY "/usr/local/mysql-connector-c++-8.0.32/lib64/libcrypto.1.1.dylib")
    endif()
endif()

# 包含项目头文件目录
include_directories(include)
include_directories(third_party/json/single_include)
include_directories(${MYSQLCPPCONN_INCLUDE_DIR})

# 添加源文件
file(GLOB SOURCES "src/*.cpp")

# 创建静态或动态库
add_library(polardbxdriver ${SOURCES})

# 添加测试可执行文件
add_executable(driver_test tests/driver_test.cpp)
add_executable(unit_test tests/unit_tests.cpp)
add_executable(concurrency_test tests/concurrency_test.cpp)
add_executable(lb_test tests/loadbalance_test.cpp)

# 链接库到测试可执行文件
set(MYSQL_LIBRARIES ${MYSQLCPPCONN_LIBRARY} ${MYSQLCPPCONN8_LIBRARY} ${SSL_LIBRARY} ${CRYPTO_LIBRARY})
target_link_libraries(driver_test polardbxdriver ${MYSQL_LIBRARIES})
target_link_libraries(unit_test polardbxdriver ${MYSQL_LIBRARIES} ${GTEST_MAIN_LIBRARIES})
target_link_libraries(concurrency_test polardbxdriver ${MYSQL_LIBRARIES} ${GTEST_MAIN_LIBRARIES})
target_link_libraries(lb_test polardbxdriver ${MYSQL_LIBRARIES} ${GTEST_MAIN_LIBRARIES})

# 设置链接属性以解决动态库依赖问题
if(APPLE)
    set_target_properties(driver_test unit_test concurrency_test lb_test PROPERTIES
        BUILD_RPATH "${MYSQLCPPCONN_ROOT_DIR}/lib64"
        INSTALL_RPATH "${MYSQLCPPCONN_ROOT_DIR}/lib64"
    )
    
    # 修改MySQL库文件的依赖路径
    add_custom_command(TARGET driver_test POST_BUILD
        COMMAND ${CMAKE_INSTALL_NAME_TOOL} -change libssl.1.1.dylib "${MYSQLCPPCONN_ROOT_DIR}/lib64/libssl.1.1.dylib" $<TARGET_FILE:driver_test>
        COMMAND ${CMAKE_INSTALL_NAME_TOOL} -change libcrypto.1.1.dylib "${MYSQLCPPCONN_ROOT_DIR}/lib64/libcrypto.1.1.dylib" $<TARGET_FILE:driver_test>
        COMMENT "Fixing SSL library paths for driver_test"
    )
    
    add_custom_command(TARGET unit_test POST_BUILD
        COMMAND ${CMAKE_INSTALL_NAME_TOOL} -change libssl.1.1.dylib "${MYSQLCPPCONN_ROOT_DIR}/lib64/libssl.1.1.dylib" $<TARGET_FILE:unit_test>
        COMMAND ${CMAKE_INSTALL_NAME_TOOL} -change libcrypto.1.1.dylib "${MYSQLCPPCONN_ROOT_DIR}/lib64/libcrypto.1.1.dylib" $<TARGET_FILE:unit_test>
        COMMENT "Fixing SSL library paths for unit_test"
    )
    
    add_custom_command(TARGET concurrency_test POST_BUILD
        COMMAND ${CMAKE_INSTALL_NAME_TOOL} -change libssl.1.1.dylib "${MYSQLCPPCONN_ROOT_DIR}/lib64/libssl.1.1.dylib" $<TARGET_FILE:concurrency_test>
        COMMAND ${CMAKE_INSTALL_NAME_TOOL} -change libcrypto.1.1.dylib "${MYSQLCPPCONN_ROOT_DIR}/lib64/libcrypto.1.1.dylib" $<TARGET_FILE:concurrency_test>
        COMMENT "Fixing SSL library paths for concurrency_test"
    )
    
    add_custom_command(TARGET lb_test POST_BUILD
        COMMAND ${CMAKE_INSTALL_NAME_TOOL} -change libssl.1.1.dylib "${MYSQLCPPCONN_ROOT_DIR}/lib64/libssl.1.1.dylib" $<TARGET_FILE:lb_test>
        COMMAND ${CMAKE_INSTALL_NAME_TOOL} -change libcrypto.1.1.dylib "${MYSQLCPPCONN_ROOT_DIR}/lib64/libcrypto.1.1.dylib" $<TARGET_FILE:lb_test>
        COMMENT "Fixing SSL library paths for lb_test"
    )
endif()

target_include_directories(unit_test PRIVATE ${GTEST_INCLUDE_DIRS})
target_include_directories(concurrency_test PRIVATE ${GTEST_INCLUDE_DIRS})
target_include_directories(lb_test PRIVATE ${GTEST_INCLUDE_DIRS})

# 安装规则
include(GNUInstallDirs)

# 安装头文件
install(DIRECTORY include/
    DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/polardbx-driver
    FILES_MATCHING PATTERN "*.h"
    PATTERN "version_info.h.cmake" EXCLUDE
)

# 安装库文件
install(TARGETS polardbxdriver
    EXPORT PolardbxDriverTargets
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
    RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
)

# 创建并安装配置文件
install(EXPORT PolardbxDriverTargets
    FILE PolardbxDriverConfig.cmake
    NAMESPACE Polardbx::
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/PolardbxDriver
)

# 创建并安装版本文件
include(CMakePackageConfigHelpers)
write_basic_package_version_file(
    PolardbxDriverConfigVersion.cmake
    VERSION ${POLARDBX_VERSION_MAJOR}.${POLARDBX_VERSION_MINOR}.${POLARDBX_VERSION_PATCH}
    COMPATIBILITY SameMajorVersion
)

install(FILES ${CMAKE_CURRENT_BINARY_DIR}/PolardbxDriverConfigVersion.cmake
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/PolardbxDriver
)

# 生成pkg-config文件
configure_file(
    cmake/polardbx-driver.pc.in
    ${CMAKE_CURRENT_BINARY_DIR}/polardbx-driver.pc
    @ONLY
)

install(FILES ${CMAKE_CURRENT_BINARY_DIR}/polardbx-driver.pc
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/pkgconfig
)

# 生成配置文件
configure_file(
    cmake/PolardbxDriverConfig.cmake.in
    ${CMAKE_CURRENT_BINARY_DIR}/PolardbxDriverConfig.cmake
    @ONLY
)

install(FILES ${CMAKE_CURRENT_BINARY_DIR}/PolardbxDriverConfig.cmake
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/PolardbxDriver
)