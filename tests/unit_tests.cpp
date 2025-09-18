#include <gtest/gtest.h>
#include "config.h"
#include "ha_manager.h"
#include "polardbx_connection.h"
#include "polardbx_driver.h"
#include "const.hpp"

std::string dn_host;
int dn_port = 0;
std::string dn_username;
std::string dn_password;
std::string cn_host;
int cn_port = 0;
std::string cn_username;
std::string cn_password;
int cluster_id = 0;

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);

    std::cout << argv[1] << std::endl;

    // Check if any required environment variables are missing
    if (argc <= 9) {
        std::cerr << "arg count is wrong" << std::endl;
        std::cerr << "Usage: ./unit_test --DNHOST=<host> --DNPORT=<port> --DNUSER=<user> --DNPASSWD=<passwd> --CNHOST=<host> --CNPORT=<port> --CNUSER=<user> --CNPASSWD=<passwd> --CLUSTERID=<id>" << std::endl;
        return EXIT_FAILURE;
    }

    std::string dn_port_str;
    std::string cn_port_str;
    std::string cluster_id_str;

    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg.find("--DNHOST=") == 0) {
            dn_host = arg.substr(strlen("--DNHOST="));
        } else if (arg.find("--DNPORT=") == 0) {
            dn_port_str = arg.substr(strlen("--DNPORT="));
        } else if (arg.find("--DNUSER=") == 0) {
            dn_username = arg.substr(strlen("--DNUSER="));
        } else if (arg.find("--DNPASSWD=") == 0) {
            dn_password = arg.substr(strlen("--DNPASSWD="));
        } else if (arg.find("--CNHOST=") == 0) {
            cn_host = arg.substr(strlen("--CNHOST="));
        } else if (arg.find("--CNPORT=") == 0) {
            cn_port_str = arg.substr(strlen("--CNPORT="));
        } else if (arg.find("--CNUSER=") == 0) {
            cn_username = arg.substr(strlen("--CNUSER="));
        } else if (arg.find("--CNPASSWD=") == 0) {
            cn_password = arg.substr(strlen("--CNPASSWD="));
        } else if (arg.find("--CLUSTERID=") == 0) {
            cluster_id_str = arg.substr(strlen("--CLUSTERID="));
        }
    }

    // Check if any required environment variables are missing
    if (dn_host.empty() || dn_port_str.empty() || dn_username.empty() || dn_password.empty() ||
        cn_host.empty() || cn_port_str.empty() || cn_username.empty() || cn_password.empty() || cluster_id_str.empty()) {
        std::cerr << "Usage: ./unit_test --DNHOST=<host> --DNPORT=<port> --DNUSER=<user> --DNPASSWD=<passwd> --CNHOST=<host> --CNPORT=<port> --CNUSER=<user> --CNPASSWD=<passwd> --CLUSTERID=<id>" << std::endl;
        return EXIT_FAILURE;
    }


    dn_port = stoi(dn_port_str);
    cn_port = stoi(cn_port_str);
    cluster_id = stoi(cluster_id_str);

    return RUN_ALL_TESTS();
}

bool query_by_template(std::map< sql::SQLString, sql::ConnectPropertyVal >& options) {
    try {
        sql::Driver* driver = sql::polardbx::get_driver_instance();
        std::unique_ptr<sql::Connection> conn(driver->connect(options));
        std::unique_ptr<sql::Statement> statement(conn->createStatement());
        std::unique_ptr<sql::ResultSet> result(statement->executeQuery("SELECT 1"));
        conn->close();
    } catch (sql::SQLException& e) {
        std::cout << e.what() << std::endl;
        return false;
    }
    return true;
}

// 测试 config.cpp
TEST(ConfigTest, ConstructorDestructor) {
    sql::polardbx::PolarDBXConfig config;
    EXPECT_EQ(config.ClusterID, -1);
    EXPECT_EQ(config.HaCheckConnectTimeoutMillis, 3000);
    // 更多期望值
}

TEST(ConfigTest, SetAddr) {
    sql::polardbx::PolarDBXConfig config;
    config.set_addr("localhost", 3306);
    EXPECT_EQ(config.Addr, "localhost:3306");
    // 更多测试用例
}

TEST(ConfigTest, SetAddrEmptyInput) {
    sql::polardbx::PolarDBXConfig config;
    config.set_addr("", -1);
    EXPECT_EQ(config.Addr, "");
}

TEST(ConfigTest, SetAddrIllegalCharacters) {
    sql::polardbx::PolarDBXConfig config;
    config.set_addr("localhost!", 3306);
    EXPECT_EQ(config.Addr, "localhost!:3306");
}

TEST(ConfigTest, SetAddrNegativePort) {
    sql::polardbx::PolarDBXConfig config;
    config.set_addr("localhost", -1);
    EXPECT_EQ(config.Addr, "localhost:3306");
}

TEST(ConfigTest, SetConnProps) {
    sql::polardbx::PolarDBXConfig config;
    sql::ConnectOptionsMap props;
    props["username"] = "root";
    props[OPT_PASSWORD] = OPT_PASSWORD;
    config.set_conn_props(props);
    EXPECT_EQ(config.conn_properties_.size(), 2);
    // 更多测试用例
}

TEST(ConfigTest, SetConnPropsEmptyMap) {
    sql::polardbx::PolarDBXConfig config;
    sql::ConnectOptionsMap props;
    config.set_conn_props(props);
    EXPECT_EQ(config.conn_properties_.size(), 0);
}

// 测试 ha_manager.cpp
TEST(HaManagerTest, GetManager) {
    std::shared_ptr<sql::polardbx::PolarDBXConfig> config = std::make_shared<sql::polardbx::PolarDBXConfig>();
    std::map< sql::SQLString, sql::ConnectPropertyVal > options;
    options[OPT_USERNAME] = dn_username;
    options[OPT_PASSWORD] = dn_password;
    config->set_addr(dn_host, dn_port);
    config->set_conn_props(options);
    auto manager = sql::polardbx::HaManager::get_manager(config);
    ASSERT_NE(manager, nullptr);
}

TEST(HaManagerTest, GetManagerEmptyConfig) {
    std::shared_ptr<sql::polardbx::PolarDBXConfig> config = nullptr;
    auto manager = sql::polardbx::HaManager::get_manager(config);
    ASSERT_EQ(manager, nullptr);
}

TEST(HaManagerTest, GetManagerIllegalAddress) {
    std::shared_ptr<sql::polardbx::PolarDBXConfig> config = std::make_shared<sql::polardbx::PolarDBXConfig>();
    config->set_addr("!@#$%^&*", -1);
    try {
        auto manager = sql::polardbx::HaManager::get_manager(config);
    } catch (sql::SQLException& e) {
        EXPECT_STREQ(e.what(), "Unable to connect to localhost");
        return;
    }
    FAIL(); 
}

TEST(HaManagerTest, GetAvailableDNWithWait) {
    std::shared_ptr<sql::polardbx::PolarDBXConfig> config = std::make_shared<sql::polardbx::PolarDBXConfig>();
    std::map< sql::SQLString, sql::ConnectPropertyVal > options;
    options[OPT_USERNAME] = dn_username;
    options[OPT_PASSWORD] = dn_password;
    config->set_addr(dn_host, dn_port);
    config->set_conn_props(options);
    auto manager = sql::polardbx::HaManager::get_manager(config);
    auto [dn, ok] = manager->get_available_dn_with_wait(5000, false, 1, 1, "random");
    EXPECT_TRUE(ok);
    // 更多测试用例
}

TEST(HaManagerTest, GetAvailableDNWithWaitNegativeTimeout) {
    std::shared_ptr<sql::polardbx::PolarDBXConfig> config = std::make_shared<sql::polardbx::PolarDBXConfig>();
    std::map< sql::SQLString, sql::ConnectPropertyVal > options;
    options[OPT_USERNAME] = dn_username;
    options[OPT_PASSWORD] = dn_password;
    config->set_addr(dn_host, dn_port);
    config->set_conn_props(options);
    auto manager = sql::polardbx::HaManager::get_manager(config);
    auto [dn, ok] = manager->get_available_dn_with_wait(-5000, false, 1, 1, "random");
    EXPECT_TRUE(ok);
}

TEST(PolardbxConnectionTest, Constructor) {
    sql::Driver* driver = sql::polardbx::get_driver_instance();
    sql::polardbx::PolarDBX_Connection * connection = new sql::polardbx::PolarDBX_Connection(driver, dn_host + std::string(":") + std::to_string(dn_port), dn_username, dn_password);
    ASSERT_NE(connection, nullptr);
    delete connection;
}

TEST(PolardbxConnectionTest, ConstructorEmptyUsernamePassword) {
    sql::Driver* driver = sql::polardbx::get_driver_instance();
    try {
        sql::polardbx::PolarDBX_Connection * connection = new sql::polardbx::PolarDBX_Connection(driver, dn_host + std::string(":") + std::to_string(dn_port), "", "");
    } catch (sql::SQLException& e) {
        return;
    }
    FAIL();
}

// test params
TEST(MultiIpPort, Basic) {
    std::map<sql::SQLString, sql::ConnectPropertyVal> options = {
        {OPT_USERNAME, dn_username},
        {OPT_PASSWORD, dn_password},
        {OPT_HOSTNAME, dn_host},
        {OPT_PORT, dn_port}
    };
    bool result = query_by_template(options);
    EXPECT_TRUE(result);
}

TEST(ClusterId, WithClusterId) {
    std::map<sql::SQLString, sql::ConnectPropertyVal> options = {
        {OPT_USERNAME, dn_username},
        {OPT_PASSWORD, dn_password},
        {OPT_HOSTNAME, dn_host},
        {OPT_PORT, dn_port},
        {"clusterID", cluster_id}
    };
    bool result = query_by_template(options);
    EXPECT_TRUE(result);
}

TEST(HaTimeParams, TimeParams) {
    std::map<sql::SQLString, sql::ConnectPropertyVal> options = {
        {OPT_USERNAME, dn_username},
        {OPT_PASSWORD, dn_password},
        {OPT_HOSTNAME, dn_host},
        {OPT_PORT, dn_port},
        {"haCheckConnectTimeoutMillis", 3000},
        {"haCheckSocketTimeoutMillis", 3000},
        {"haCheckIntervalMillis", 100},
        {"checkLeaderTransferringIntervalMillis", 100},
        {"leaderTransferringWaitTimeoutMillis", 5000},
        {"connectTimeout", 5000}
    };
    bool result = query_by_template(options);
    EXPECT_TRUE(result);
}

TEST(RecordJdbcUrl, RecordUrl) {
    std::map<sql::SQLString, sql::ConnectPropertyVal> options = {
        {OPT_USERNAME, dn_username},
        {OPT_PASSWORD, dn_password},
        {OPT_HOSTNAME, dn_host},
        {OPT_PORT, dn_port},
        {"recordJdbcUrl", true}
    };
    bool result = query_by_template(options);
    EXPECT_TRUE(result);
}

TEST(DnSlaveRead, SlaveRead) {
    std::map<sql::SQLString, sql::ConnectPropertyVal> options = {
        {OPT_USERNAME, dn_username},
        {OPT_PASSWORD, dn_password},
        {OPT_HOSTNAME, dn_host},
        {OPT_PORT, dn_port},
        {"slaveRead", true}
    };
    bool result = query_by_template(options);
    EXPECT_TRUE(result);
}

TEST(SlaveWeight, WeightThreshold) {
    std::map<sql::SQLString, sql::ConnectPropertyVal> options = {
        {OPT_USERNAME, dn_username},
        {OPT_PASSWORD, dn_password},
        {OPT_HOSTNAME, dn_host},
        {OPT_PORT, dn_port},
        {"slaveRead", true},
        {"slaveWeightThreshold", 10}
    };
    bool result = query_by_template(options);
    EXPECT_FALSE(result);
}

TEST(ApplyDelay, DelayThreshold) {
    std::map<sql::SQLString, sql::ConnectPropertyVal> options = {
        {OPT_USERNAME, dn_username},
        {OPT_PASSWORD, dn_password},
        {OPT_HOSTNAME, dn_host},
        {OPT_PORT, dn_port},
        {"slaveRead", true},
        {"applyDelayThreshold", 3}
    };
    bool result = query_by_template(options);
    EXPECT_TRUE(result);
}

TEST(DirectMode, DirectMode) {
    std::map<sql::SQLString, sql::ConnectPropertyVal> options = {
        {OPT_USERNAME, dn_username},
        {OPT_PASSWORD, dn_password},
        {OPT_HOSTNAME, dn_host},
        {OPT_PORT, dn_port},
        {"directMode", true}
    };
    bool result = query_by_template(options);
    EXPECT_TRUE(result);
}

TEST(LoadBalance, LeastConnection) {
    std::map<sql::SQLString, sql::ConnectPropertyVal> options = {
        {OPT_USERNAME, dn_username},
        {OPT_PASSWORD, dn_password},
        {OPT_HOSTNAME, dn_host},
        {OPT_PORT, dn_port},
        {"slaveRead", true},
        {"loadBalanceAlgorithm", "least_connection"}
    };
    bool result = query_by_template(options);
    EXPECT_TRUE(result);
}

TEST(AllDnParams, AllParams) {
    std::map<sql::SQLString, sql::ConnectPropertyVal> options = {
        {OPT_USERNAME, dn_username},
        {OPT_PASSWORD, dn_password},
        {OPT_HOSTNAME, dn_host},
        {OPT_PORT, dn_port},
        {"clusterID", cluster_id},
        {"haCheckConnectTimeoutMillis", 3000},
        {"haCheckSocketTimeoutMillis", 3000},
        {"haCheckIntervalMillis", 100},
        {"checkLeaderTransferringIntervalMillis", 100},
        {"leaderTransferringWaitTimeoutMillis", 5000},
        {"connectTimeout", 5000},
        {"slaveRead", true},
        {"slaveWeightThreshold", 1},
        {"applyDelayThreshold", 3},
        {"loadBalanceAlgorithm", "least_connection"},
    };
    bool result = query_by_template(options);
    EXPECT_TRUE(result);
}

// cn
TEST(HaManagerTest, GetAvailableCNWithWaitPositiveTimeout) {
    std::shared_ptr<sql::polardbx::PolarDBXConfig> config = std::make_shared<sql::polardbx::PolarDBXConfig>();
    std::map< sql::SQLString, sql::ConnectPropertyVal > options;
    options[OPT_USERNAME] = cn_username;
    options[OPT_PASSWORD] = cn_password;
    config->set_addr(cn_host, cn_port);
    config->set_conn_props(options);
    
    auto manager = sql::polardbx::HaManager::get_manager(config);
    
    auto [cn, ok] = manager->get_available_cn_with_wait(5000, "polardbx-hz2", 2, "polardbx-hz1", false, "pxc-aar39vh9quig4y", "W", "random");
    
    EXPECT_TRUE(ok);
    EXPECT_FALSE(cn.empty());
}

TEST(HaManagerTest, GetAvailableCNWithWaitZeroTimeout) {
    std::shared_ptr<sql::polardbx::PolarDBXConfig> config = std::make_shared<sql::polardbx::PolarDBXConfig>();
    std::map< sql::SQLString, sql::ConnectPropertyVal > options;
    options[OPT_USERNAME] = cn_username;
    options[OPT_PASSWORD] = cn_password;
    config->set_addr(cn_host, cn_port);
    config->set_conn_props(options);
    
    auto manager = sql::polardbx::HaManager::get_manager(config);
    
    auto [cn, ok] = manager->get_available_cn_with_wait(0, "polardbx-hz2", 1, "polardbx-hz1", false, "pxc-aar39vh9quig4y", "W", "random");
    
    EXPECT_TRUE(ok);
    EXPECT_FALSE(cn.empty());
}

TEST(HaManagerTest, GetAvailableCNWithWaitNegativeTimeout) {
    std::shared_ptr<sql::polardbx::PolarDBXConfig> config = std::make_shared<sql::polardbx::PolarDBXConfig>();
    std::map< sql::SQLString, sql::ConnectPropertyVal > options;
    options[OPT_USERNAME] = cn_username;
    options[OPT_PASSWORD] = cn_password;
    config->set_addr(cn_host, cn_port);
    config->set_conn_props(options);
    
    auto manager = sql::polardbx::HaManager::get_manager(config);
    
    auto [cn, ok] = manager->get_available_cn_with_wait(-1, "polardbx-hz2", 1, "polardbx-hz1", false, "pxc-aar39vh9quig4y", "W", "random");
    
    EXPECT_TRUE(ok);
    EXPECT_FALSE(cn.empty());
}

TEST(HaManagerTest, GetAvailableCNWithWaitInvalidParameters) {
    std::shared_ptr<sql::polardbx::PolarDBXConfig> config = std::make_shared<sql::polardbx::PolarDBXConfig>();
    std::map< sql::SQLString, sql::ConnectPropertyVal > options;
    options[OPT_USERNAME] = cn_username;
    options[OPT_PASSWORD] = cn_password;
    config->set_addr(cn_host, cn_port);
    config->set_conn_props(options);
    
    auto manager = sql::polardbx::HaManager::get_manager(config);
    
    auto [cn, ok] = manager->get_available_cn_with_wait(5000, "invalid", 0, "invalid", false, "invalid", "invalid", "invalid");
    
    EXPECT_FALSE(ok);
}

TEST(CNParamTest, ZoneName) {
    std::map<sql::SQLString, sql::ConnectPropertyVal> options = {
        {OPT_USERNAME, cn_username},
        {OPT_PASSWORD, cn_password},
        {OPT_HOSTNAME, cn_host},
        {OPT_PORT, cn_port},
        {"zoneName", "polardbx-hz2"}
    };
    bool result = query_by_template(options);
    EXPECT_TRUE(result);
}

TEST(CNParamTest, MinZoneNodes) {
    std::map<sql::SQLString, sql::ConnectPropertyVal> options = {
        {OPT_USERNAME, cn_username},
        {OPT_PASSWORD, cn_password},
        {OPT_HOSTNAME, cn_host},
        {OPT_PORT, cn_port},
        {"zoneName", "polardbx-hz2"},
        {"minZoneNodes", 1}
    };
    bool result = query_by_template(options);
    EXPECT_TRUE(result);
    
    options["minZoneNodes"] = 2;
    result = query_by_template(options);
    EXPECT_FALSE(result); // 假设 minZoneNodes 设置为 2 应该导致失败
}

TEST(CNParamTest, BackupZoneName) {
    std::map<sql::SQLString, sql::ConnectPropertyVal> options = {
        {OPT_USERNAME, cn_username},
        {OPT_PASSWORD, cn_password},
        {OPT_HOSTNAME, cn_host},
        {OPT_PORT, cn_port},
        {"zoneName", "polardbx-hz2"},
        {"minZoneNodes", 2},
        {"backupZoneName", "polardbx-hz1"}
    };
    bool result = query_by_template(options);
    EXPECT_TRUE(result);
}

TEST(CNParamTest, CnSlaveRead) {
    std::map<sql::SQLString, sql::ConnectPropertyVal> options = {
        {OPT_USERNAME, cn_username},
        {OPT_PASSWORD, cn_password},
        {OPT_HOSTNAME, cn_host},
        {OPT_PORT, cn_port},
        {"slaveRead", false}
    };
    bool result = query_by_template(options);
    EXPECT_TRUE(result);
}

TEST(CNParamTest, InstanceName) {
    std::map<sql::SQLString, sql::ConnectPropertyVal> options = {
        {OPT_USERNAME, cn_username},
        {OPT_PASSWORD, cn_password},
        {OPT_HOSTNAME, cn_host},
        {OPT_PORT, cn_port},
        {"instanceName", "pxc-aar39vh9quig4y"}
    };
    bool result = query_by_template(options);
    EXPECT_TRUE(result);
}

TEST(CNParamTest, W) {
    std::map<sql::SQLString, sql::ConnectPropertyVal> options = {
        {OPT_USERNAME, cn_username},
        {OPT_PASSWORD, cn_password},
        {OPT_HOSTNAME, cn_host},
        {OPT_PORT, cn_port},
        {"mppRole", "W"}
    };
    bool result = query_by_template(options);
    EXPECT_TRUE(result);
}

TEST(CNParamTest, EnableFollowerRead0) {
    std::map<sql::SQLString, sql::ConnectPropertyVal> options = {
        {OPT_USERNAME, cn_username},
        {OPT_PASSWORD, cn_password},
        {OPT_HOSTNAME, cn_host},
        {OPT_PORT, cn_port},
        {"slaveRead", false},
        {"enableFollowerRead", 0}
    };
    bool result = query_by_template(options);
    EXPECT_TRUE(result);
}

TEST(CNParamTest, EnableFollowerRead0_1) {
    std::map<sql::SQLString, sql::ConnectPropertyVal> options = {
        {OPT_USERNAME, cn_username},
        {OPT_PASSWORD, cn_password},
        {OPT_HOSTNAME, cn_host},
        {OPT_PORT, cn_port},
        {"slaveRead", true},
        {"enableFollowerRead", 0}
    };
    bool result = query_by_template(options);
    EXPECT_TRUE(result);
}

TEST(CNParamTest, EnableFollowerRead1) {
    std::map<sql::SQLString, sql::ConnectPropertyVal> options = {
        {OPT_USERNAME, cn_username},
        {OPT_PASSWORD, cn_password},
        {OPT_HOSTNAME, cn_host},
        {OPT_PORT, cn_port},
        {"slaveRead", false},
        {"enableFollowerRead", 1}
    };
    bool result = query_by_template(options);
    EXPECT_TRUE(result);
}

TEST(CNParamTest, EnableFollowerRead1_1) {
    std::map<sql::SQLString, sql::ConnectPropertyVal> options = {
        {OPT_USERNAME, cn_username},
        {OPT_PASSWORD, cn_password},
        {OPT_HOSTNAME, cn_host},
        {OPT_PORT, cn_port},
        {"slaveRead", true},
        {"enableFollowerRead", 1}
    };
    bool result = query_by_template(options);
    EXPECT_TRUE(result);
}

TEST(CNParamTest, EnableFollowerRead2) {
    std::map<sql::SQLString, sql::ConnectPropertyVal> options = {
        {OPT_USERNAME, cn_username},
        {OPT_PASSWORD, cn_password},
        {OPT_HOSTNAME, cn_host},
        {OPT_PORT, cn_port},
        {"slaveRead", false},
        {"enableFollowerRead", 2}
    };
    bool result = query_by_template(options);
    EXPECT_TRUE(result);
}

TEST(CNParamTest, EnableFollowerRead2_2) {
    std::map<sql::SQLString, sql::ConnectPropertyVal> options = {
        {OPT_USERNAME, cn_username},
        {OPT_PASSWORD, cn_password},
        {OPT_HOSTNAME, cn_host},
        {OPT_PORT, cn_port},
        {"slaveRead", true},
        {"enableFollowerRead", 2}
    };
    bool result = query_by_template(options);
    EXPECT_TRUE(result);
}

TEST(CNParamTest, AllCnParams) {
    std::map<sql::SQLString, sql::ConnectPropertyVal> options = {
        {OPT_USERNAME, cn_username},
        {OPT_PASSWORD, cn_password},
        {OPT_HOSTNAME, cn_host},
        {OPT_PORT, cn_port},
        {"haCheckConnectTimeoutMillis", 3000},
        {"haCheckSocketTimeoutMillis", 3000},
        {"haCheckIntervalMillis", 100},
        {"checkLeaderTransferringIntervalMillis", 100},
        {"leaderTransferringWaitTimeoutMillis", 5000},
        {"connectTimeout", 5000},
        {"slaveRead", false},
        {"loadBalanceAlgorithm", "least_connection"},
        {"instanceName", "pxc-aar39vh9quig4y"},
        {"zoneName", "polardbx-hz2"},
        {"minZoneNodes", 2},
        {"backupZoneName", "polardbx-hz1"},
        {"mppRole", "W"}
    };
    bool result = query_by_template(options);
    EXPECT_TRUE(result);
    
    options["slaveRead"] = true;
    options["loadBalanceAlgorithm"] = "random";
    options["instanceName"] = "pxr-aaroo6j270u013";
    options["mppRole"] = "R";
    result = query_by_template(options);
    EXPECT_TRUE(result);
}