# polardbx-connector-cpp
This is a high-availability C++ driver for PolarDB-X. The features implemented so far are as follows:

- [x] Automatically reconnects to the new leader node after switching
- [x] Supports read-write separation
- [ ] Supports CoreDNS
- [ ] Supports transparent switching
- [x] Supports `COM_PING` for HA checks
- [x] Supports Load Balancing (random or leastConn)

## Installation
### Install mysql-connector-cpp 8.0.32
[install](https://downloads.mysql.com/archives/c-cpp/)

### Install polardbx-connector-cpp

```bash
mkdir build
cd build
cmake ..
make -j16
sudo make install
```

By default, this installs:
- Header files to `/usr/local/include/polardbx-driver/`
- Library files to `/usr/local/lib/`
- CMake config files to `/usr/local/lib/cmake/PolardbxDriver/`
- pkg-config file to `/usr/local/lib/pkgconfig/`

You can customize the installation prefix by passing `-DCMAKE_INSTALL_PREFIX=/your/custom/path` to cmake.

## Usage Methods

There are several ways to use the installed library in your project:

### Method 1: Using CMake (Recommended)

In your project's `CMakeLists.txt`:

```cmake
cmake_minimum_required(VERSION 3.20)
project(MyProject)

find_package(PolardbxDriver REQUIRED)

add_executable(myapp main.cpp)
target_link_libraries(myapp Polardbx::polardbxdriver)
```

Then configure and build your project:

```bash
mkdir build
cd build
cmake .. -DCMAKE_PREFIX_PATH=/usr/local  # Or your custom installation path
make
```

### Method 2: Using pkg-config

Compile your application using pkg-config:

```bash
g++ main.cpp -o myapp $(pkg-config --cflags --libs polardbx-driver)
```

Or with CMake:

```cmake
cmake_minimum_required(VERSION 3.20)
project(MyProject)

# Find the library using pkg-config
find_package(PkgConfig REQUIRED)
pkg_check_modules(POLARDBX_DRIVER REQUIRED polardbx-driver)

add_executable(myapp main.cpp)
target_include_directories(myapp PRIVATE ${POLARDBX_DRIVER_INCLUDE_DIRS})
target_link_libraries(myapp ${POLARDBX_DRIVER_LIBRARIES})
```

### Method 3: Manual Compilation

If you know where the library is installed:

```bash
g++ main.cpp -o myapp -I/usr/local/include/polardbx-driver -L/usr/local/lib -lpolardbxdriver
```

## Example Application

Here's a simple example showing how to use the driver:

```cpp
#include <iostream>
#include <polardbx_driver.h>
#include <polardbx_connection.h>

int main() {
    try {
        // Get driver instance
        sql::polardbx::PolarDBX_Driver* driver = sql::polardbx::get_driver_instance();
        
        // Connect to database
        std::unique_ptr<sql::Connection> con(driver->connect("tcp://127.0.0.1:3306", "user", "password"));
        
        if (con->isValid()) {
            std::cout << "Connected successfully!" << std::endl;
        }
        
        // Use connection...
        // con->...
        
    } catch (sql::SQLException &e) {
        std::cerr << "SQL Error: " << e.what() << std::endl;
        return 1;
    }
    
    return 0;
}
```

## Notes

1. Make sure the runtime linker can find the shared library. On Linux, you might need to:
    - Add the library path to `LD_LIBRARY_PATH`
    - Configure ldconfig
    - Use rpath during linking

2. When distributing applications that use this library, ensure the target systems have compatible versions of dependencies like OpenSSL.


## Development
### Build
```zsh
mkdir build
cd build
cmake ..
make -j16
```

### Run tests
```zsh
./driver_test {ip} {port} {user} {password}
./unit_test --DNHOST={dn_ip} --DNPORT={dn_port} --DNUSER={dn_user} --DNPASSWD={dn_password} --CNHOST={cn_ip} --CNPORT={cn_port} --CNUSER={cn_user} --CNPASSWD={cn_password} --CLUSTERID={cluster_id}
./concurrency_test --DNHOST={dn_ip} --DNPORT={dn_port} --DNUSER={dn_user}--DNPASSWD={dn_password} --CNHOST={cn_ip} --CNPORT={cn_port} --CNUSER={cn_user} --CNPASSWD={cn_password} --DBNAME={db_name}
./lb_test --HOST={ip} --PORT={port} --USER={user} --PASSWD={password} --LOADBALANCE=least_connection --READTHREADS=10 --WRITETHREADS=10
```