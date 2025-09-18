#include <gtest/gtest.h>
#include <iostream>
#include <memory>
#include <thread>
#include <mutex>
#include <atomic>
#include <map>
#include <random>
#include <chrono>
#include <functional>
#include <future>
#include <cstring>

#include "config.h"
#include "polardbx_driver.h"
#include "polardbx_connection.h"
#include <jdbc/cppconn/driver.h>
#include <jdbc/cppconn/exception.h>
#include <jdbc/cppconn/resultset.h>
#include <jdbc/cppconn/statement.h>
#include <jdbc/cppconn/prepared_statement.h>

using namespace std;
using namespace sql;


std::string USERNAME = "";
std::string PASSWORD = "";
std::string HOSTNAME = "";
int PORT = -1;
std::string DB_NAME = "ha_test_db";
std::string TABLE_NAME = "ha_test_tb";
const int INIT_DATA_COUNT = 1000;
int READ_THREADS = 0;
int WRITE_THREADS = 0;
std::string LOADBALANCE = ""; 

// 全局计数器（原子）
atomic<int64_t> readTransactions{0};
atomic<int64_t> readErrors{0};
atomic<int64_t> writeTransactions{0};
atomic<int64_t> writeErrors{0};

// 连接分布统计
map<string, int> urlCnt;
mutex mu;

// 随机数生成器
random_device rd;
mt19937 gen(rd());
uniform_int_distribution<> dis_value(1, 100000);
uniform_int_distribution<> dis_id(1, INIT_DATA_COUNT);

string getMySQLServerIP(Connection* conn) {
    auto polardbx_conn = dynamic_cast<sql::polardbx::PolarDBX_Connection*>(conn);
    return polardbx_conn -> getConnectionAddr();
}

// 初始化数据库和表
void initDB(const string & addr) {
    try {
        Driver* driver = sql::mysql::get_driver_instance();
        unique_ptr<Connection> conn(driver->connect(addr, USERNAME, PASSWORD));
        unique_ptr<Statement> stmt(conn->createStatement());

        stmt->execute("CREATE DATABASE IF NOT EXISTS " + std::string(DB_NAME));
        conn->setSchema(DB_NAME);

        string create_table = "CREATE TABLE IF NOT EXISTS " + std::string(TABLE_NAME) +
            " (id INT AUTO_INCREMENT PRIMARY KEY, value INT, tid INT)";
        stmt->execute(create_table);

        for (int i = 1; i <= INIT_DATA_COUNT; ++i) {
            stmt->execute("INSERT INTO " + std::string(TABLE_NAME) + " (value, tid) VALUES (123, 0)");
        }

    } catch (SQLException& e) {
        cerr << "initDB error: " << e.what() << endl;
        exit(1);
    }
}

// 读线程函数
void buildReads(Driver* driver, std::map< sql::SQLString, sql::ConnectPropertyVal > options) {
    try {
        unique_ptr<Connection> conn(driver->connect(options));
        conn->setSchema(DB_NAME);
        unique_ptr<Statement> stmt(conn->createStatement());

        // 获取读节点 IP
        string readNode = getMySQLServerIP(conn.get());

        {
            lock_guard<mutex> lock(mu);
            urlCnt[readNode]++;
        }

        while (true) {
            try {
                int id = dis_id(gen);
                string sql = "SELECT * FROM " + std::string(DB_NAME) + "." + std::string(TABLE_NAME)+ " WHERE id = " + to_string(id);
                ResultSet* res = stmt->executeQuery(sql);
                while (res->next()) {
                    // 可选：读取数据
                }
                delete res;
                readTransactions.fetch_add(1);
            } catch (SQLException& e) {
                readErrors.fetch_add(1);
                this_thread::sleep_for(chrono::milliseconds(100));
                continue;
            }
        }
        conn->close();
    } catch (SQLException& e) {
        cerr << "buildReads connection error: " << e.what() << endl;
    }
}

// 写线程函数
void buildWrites(Driver* driver, std::map< sql::SQLString, sql::ConnectPropertyVal > options, int threadID) {
    try {
       unique_ptr<Connection> conn(driver->connect(options));
        conn->setSchema(DB_NAME);
        conn->setAutoCommit(false);

        unique_ptr<Statement> stmt(conn->createStatement());

        // 获取写节点 IP
        string writeNode = getMySQLServerIP(conn.get());
        {
            lock_guard<mutex> lock(mu);
            urlCnt[writeNode]++;
        }

        while (true) {
            try {
                int value = dis_value(gen);
                string insert_sql = "INSERT INTO " + std::string(DB_NAME) + "." + std::string(TABLE_NAME) +
                    " (value, tid) VALUES (" + to_string(value) + ", " + to_string(threadID) + ")";
                stmt->execute(insert_sql);

                ResultSet* res = stmt->executeQuery("SELECT LAST_INSERT_ID()");
                int id = 0;
                if (res->next()) {
                    id = res->getInt(1);
                }
                delete res;

                if (id == 0) {
                    conn->rollback();
                    writeErrors.fetch_add(1);
                    this_thread::sleep_for(chrono::milliseconds(100));
                    continue;
                }

                string delete_sql = "DELETE FROM " + std::string(DB_NAME) + "." + std::string(TABLE_NAME) + " WHERE id = " + to_string(id);
                stmt->execute(delete_sql);

                conn->commit();
                writeTransactions.fetch_add(1);
            } catch (SQLException& e) {
                try {
                    conn->rollback();
                } catch (...) {}
                writeErrors.fetch_add(1);
                this_thread::sleep_for(chrono::milliseconds(100));
                continue;
            }
        }
    } catch (SQLException& e) {
        cerr << "buildWrites connection error: " << e.what() << endl;
    }
}

// 定期打印统计信息
void startStatsMonitor() {
    int64_t lastReadT = 0, lastReadE = 0, lastWriteT = 0, lastWriteE = 0;
    while (true) {
        this_thread::sleep_for(chrono::seconds(1));
        cout << "========================================" << endl;

        int64_t curReadT = readTransactions.load();
        int64_t curReadE = readErrors.load();
        int64_t curWriteT = writeTransactions.load();
        int64_t curWriteE = writeErrors.load();

        cout << "Read: " << (curReadT - lastReadT)
             << " (" << (curReadE - lastReadE) << " errors), "
             << "Write: " << (curWriteT - lastWriteT)
             << " (" << (curWriteE - lastWriteE) << " errors)" << endl;

        cout << "Connection Distribution:" << endl;
        lock_guard<mutex> lock(mu);
        for (const auto& p : urlCnt) {
            cout << "  " << p.first << ": " << p.second << endl;
        }

        lastReadT = curReadT;
        lastReadE = curReadE;
        lastWriteT = curWriteT;
        lastWriteE = curWriteE;
    }
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);

    // Check if any required environment variables are missing
    if (argc <= 7) {
        std::cerr << "arg count is wrong" << std::endl;
        std::cerr << "Usage: ./lb_test --HOST=<host> --PORT=<port> --USER=<user> --PASSWD=<passwd> --LOADBALANCE=<algorithm> --READTHREADS=<count> --WRITETHREADS=<count>" << std::endl;
        return EXIT_FAILURE;
    }

    string port_str = "";
    string read_thread_str = "";
    string write_thread_str = "";

    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg.find("--HOST=") == 0) {
            HOSTNAME = arg.substr(strlen("--HOST="));
        } else if (arg.find("--PORT=") == 0) {
            port_str = arg.substr(strlen("--PORT="));
        } else if (arg.find("--USER=") == 0) {
            USERNAME = arg.substr(strlen("--USER="));
        } else if (arg.find("--PASSWD=") == 0) {
            PASSWORD = arg.substr(strlen("--PASSWD="));
        } else if (arg.find("--LOADBALANCE=") == 0) {
            LOADBALANCE = arg.substr(strlen("--LOADBALANCE="));
        } else if (arg.find("--READTHREADS=") == 0) {
            read_thread_str = arg.substr(strlen("--READTHREADS="));
        } else if (arg.find("--WRITETHREADS=") == 0) {
            write_thread_str = arg.substr(strlen("--WRITETHREADS="));
        }
    }

    // Check if any required environment variables are missing
    if (HOSTNAME.empty() || 
        port_str.empty() || 
        USERNAME.empty() || 
        PASSWORD.empty() || 
        LOADBALANCE.empty() || 
        read_thread_str.empty() || 
        write_thread_str.empty()) {
        std::cerr << "Usage: ./lb_test --HOST=<host> --PORT=<port> --USER=<user> --PASSWD=<passwd> --LOADBALANCE=<algorithm> --READTHREADS=<count> --WRITETHREADS=<count>" << std::endl;
        return EXIT_FAILURE;
    }

    PORT = stoi(port_str);
    READ_THREADS = stoi(read_thread_str);
    WRITE_THREADS = stoi(write_thread_str);

    return RUN_ALL_TESTS();
}

TEST(LoadBalanceTest, DoLoadBalanceTest) {
    string addr = std::string(HOSTNAME) + ":" + std::to_string(PORT);

    // 创建驱动
    Driver* driver = sql::polardbx::get_driver_instance();

    // 初始化数据库
    cout << "Init database..." << endl;
    initDB(addr);

    std::map< sql::SQLString, sql::ConnectPropertyVal > options = {
        {OPT_USERNAME, USERNAME},
        {OPT_PASSWORD, PASSWORD},
        {OPT_HOSTNAME, HOSTNAME},
        {OPT_PORT, PORT},
        {OPT_LOAD_BALANCE_ALGORITHM, LOADBALANCE}
    };

    // 启动统计监控线程
    thread statsThread(startStatsMonitor);

    // 主循环：每隔 2 秒启动一批读写线程
    while (true) {
        this_thread::sleep_for(chrono::seconds(2));
        cout << "Build " << READ_THREADS << " read threads, " << WRITE_THREADS << " write threads" << endl;

        // 启动读线程
        for (int i = 0; i < READ_THREADS; ++i) {
            thread t(buildReads, driver, options);
            t.detach();
        }

        // 启动写线程
        for (int i = 0; i < WRITE_THREADS; ++i) {
            thread t(buildWrites, driver, options, i + 1);
            t.detach();
        }
    }
}

