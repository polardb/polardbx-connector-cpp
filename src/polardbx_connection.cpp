#include "polardbx_connection.h"
#include "polardbx_driver.h"
#include "ha_manager.h"
#include "const.hpp"

#include <jdbc/mysql_connection.h>
#include <jdbc/mysql_driver.h>
#include <jdbc/cppconn/exception.h>
#include <jdbc/cppconn/statement.h>
#include <jdbc/cppconn/prepared_statement.h>
#include <jdbc/cppconn/metadata.h>
#include <jdbc/cppconn/resultset.h>
#include <jdbc/cppconn/warning.h>
#include <jdbc/cppconn/sqlstring.h>

namespace sql {
namespace polardbx {

PolarDBX_Connection::PolarDBX_Connection(Driver * _driver,
        const sql::SQLString& hostName,
        const sql::SQLString& userName,
        const sql::SQLString& password)
    : driver(_driver)
{
    auto p_cfg = std::make_shared<PolarDBXConfig>();
    auto c_cfg = std::make_shared<ConnectionConfig>();

    p_cfg->set_addr(hostName, 3306);
    std::map< sql::SQLString, sql::ConnectPropertyVal > options;
    options[OPT_USERNAME] = userName;
    options[OPT_PASSWORD] = password;
    p_cfg->set_conn_props(options);

    ha_manager_ = HaManager::get_manager(p_cfg);
    if (!ha_manager_) {
        throw sql::SQLException("failed to get ha manager, configuration is nullptr");
    }
    bool ok = false;
    if (ha_manager_->is_dn()) {
        auto [conn_addr, is_ok] = ha_manager_->get_available_dn_with_wait(c_cfg->ConnectTimeoutMillis, c_cfg->SlaveOnly, 
        c_cfg->ApplyDelayThreshold, c_cfg->SlaveWeightThreshold, c_cfg->LoadBalanceAlgorithm);
        ok = is_ok;
        conn_addr_ = conn_addr;
    } else {
        auto [conn_addr, is_ok] = ha_manager_->get_available_cn_with_wait(c_cfg->ConnectTimeoutMillis, c_cfg->ZoneName, 
        c_cfg->MinZoneNodes, c_cfg->BackupZoneName, c_cfg->SlaveOnly, c_cfg->InstanceName, c_cfg->MppRole, c_cfg->LoadBalanceAlgorithm);
        ok = is_ok;
        conn_addr_ = conn_addr;
    }

    if (!ok) {
        ha_manager_->drop_conn_count(conn_addr_);
        throw sql::SQLException("No available dn/cn");
    } else {
        Driver * driver = sql::mysql::get_driver_instance();
        real_conn = driver->connect(conn_addr_, userName, password);
    }
}

PolarDBX_Connection::PolarDBX_Connection(Driver * _driver,
        std::map< sql::SQLString, sql::ConnectPropertyVal > & options)
    : driver(_driver)
{
    if (options.find(OPT_DIRECT_MODE) != options.end()) {
        try {
            auto direct_mode = options[OPT_DIRECT_MODE].get<bool>();
            if (direct_mode) {
                real_conn = sql::mysql::get_driver_instance()->connect(options);
                return;
            }
        } catch (sql::InvalidArgumentException&) {
            throw sql::InvalidArgumentException("Wrong type passed for directMode expected bool");
        }
    }

    auto p_cfg = std::make_shared<PolarDBXConfig>();
    auto c_cfg = std::make_shared<ConnectionConfig>();

    bool record_jdbc_url = false;
    std::string jdbc_url = "";

    for (auto it = options.begin(); it != options.end(); it++) {
        if (!it->first.compare(OPT_CLUSTERID)) {
            try {
                auto cluster_id = it->second.get<int>();
                p_cfg->ClusterID = *cluster_id;
                jdbc_url += OPT_CLUSTERID;
                jdbc_url += "=";
                jdbc_url += std::to_string(p_cfg->ClusterID);
            } catch (sql::InvalidArgumentException&) {
                throw sql::InvalidArgumentException("Wrong type passed for clusterId expected int64_t");
            }
        } else if (!it->first.compare(OPT_HA_CHECK_CONNECT_TIMEOUT)) {
            try {
                auto val = it->second.get<int32_t>();
                p_cfg->HaCheckConnectTimeoutMillis = *val;
                jdbc_url += OPT_HA_CHECK_CONNECT_TIMEOUT;
                jdbc_url += "=";
                jdbc_url += std::to_string(p_cfg->HaCheckConnectTimeoutMillis);
            } catch (sql::InvalidArgumentException&) {
                throw sql::InvalidArgumentException("Wrong type passed for haCheckConnectTimeout expected int32_t");
            }
        } else if (!it->first.compare(OPT_HA_CHECK_SOCKET_TIMEOUT)) {
            try {
                auto val = it->second.get<int32_t>();
                p_cfg->HaCheckSocketTimeoutMillis = *val;
                jdbc_url += OPT_HA_CHECK_SOCKET_TIMEOUT;
                jdbc_url += "=";
                jdbc_url += std::to_string(p_cfg->HaCheckSocketTimeoutMillis);
            } catch (sql::InvalidArgumentException&) {
                throw sql::InvalidArgumentException("Wrong type passed for haCheckSocketTimeout expected int32_t");
            }
        } else if (!it->first.compare(OPT_HA_CHECK_INTERVAL)) {
            try {
                auto val = it->second.get<int32_t>();
                p_cfg->HaCheckIntervalMillis = *val;
                jdbc_url += OPT_HA_CHECK_INTERVAL;
                jdbc_url += "=";
                jdbc_url += std::to_string(p_cfg->HaCheckIntervalMillis);
            } catch (sql::InvalidArgumentException&) {
                throw sql::InvalidArgumentException("Wrong type passed for haCheckInterval expected int32_t");
            }
        } else if (!it->first.compare(OPT_CHECK_LEADER_TRANSFERRING_INTERVAL)) {
            try {
                auto val = it->second.get<int32_t>();
                p_cfg->CheckLeaderTransferringIntervalMillis = *val;
                jdbc_url += OPT_CHECK_LEADER_TRANSFERRING_INTERVAL;
                jdbc_url += "=";
                jdbc_url += std::to_string(p_cfg->CheckLeaderTransferringIntervalMillis);
            } catch (sql::InvalidArgumentException&) {
                throw sql::InvalidArgumentException("Wrong type passed for checkLeaderTransferringInterval expected int32_t");
            }
        } else if (!it->first.compare(OPT_LEADER_TRANSFERRING_WAIT_TIMEOUT)) {
            try {
                auto val = it->second.get<int32_t>();
                p_cfg->LeaderTransferringWaitTimeoutMillis = *val;
                jdbc_url += OPT_LEADER_TRANSFERRING_WAIT_TIMEOUT;
                jdbc_url += "=";
                jdbc_url += std::to_string(p_cfg->LeaderTransferringWaitTimeoutMillis);
            } catch (sql::InvalidArgumentException&) {
                throw sql::InvalidArgumentException("Wrong type passed for leaderTransferringWaitTimeout expected int32_t");
            }
        } else if (!it->first.compare(OPT_SMOOTH_SWITCHOVER)) {
            try {
                auto val = it->second.get<bool>();
                p_cfg->SmoothSwitchover = *val;
                jdbc_url += OPT_SMOOTH_SWITCHOVER;
                jdbc_url += "=";
                jdbc_url += std::to_string(p_cfg->SmoothSwitchover);
            } catch (sql::InvalidArgumentException&) {
                throw sql::InvalidArgumentException("Wrong type passed for smoothSwitchover expected bool");
            }
        } else if (!it->first.compare(OPT_RECORD_JDBC_URL)) {
            try {
                auto val = it->second.get<bool>();
                record_jdbc_url = *val;
            } catch (sql::InvalidArgumentException&) {
                throw sql::InvalidArgumentException("Wrong type passed for recordJdbcUrl expected bool");
            }
        } else if (!it->first.compare(OPT_IGNORE_VIP)) {
            try {
                auto val = it->second.get<bool>();
                p_cfg->IgnoreVip.store(*val);
                jdbc_url += OPT_IGNORE_VIP;
                jdbc_url += "=";
                jdbc_url += std::to_string(p_cfg->IgnoreVip.load());
            } catch (sql::InvalidArgumentException&) {
                throw sql::InvalidArgumentException("Wrong type passed for ignoreVip expected bool");
            }
        } else if (!it->first.compare(OPT_JSON_FILE)) {
            try {
                auto val = it->second.get<std::string>();
                p_cfg->JsonFile = *val;
                jdbc_url += OPT_JSON_FILE;
                jdbc_url += "=";
                jdbc_url += p_cfg->JsonFile;
            } catch (sql::InvalidArgumentException&) {
                throw sql::InvalidArgumentException("Wrong type passed for jsonFile expected std::string");
            }
        } else if (!it->first.compare(OPT_ENABLE_LOG)) {
            try {
                auto val = it->second.get<bool>();
                p_cfg->EnableLog = *val;
                jdbc_url += OPT_ENABLE_LOG;
                jdbc_url += "=";
                jdbc_url += std::to_string(p_cfg->EnableLog);
            } catch (sql::InvalidArgumentException&) {
                throw sql::InvalidArgumentException("Wrong type passed for enableLog expected bool");
            }
        } else if (!it->first.compare(OPT_SLAVE_ONLY)) {
            try {
                auto val = it->second.get<bool>();
                c_cfg->SlaveOnly = *val;
                jdbc_url += OPT_SLAVE_ONLY;
                jdbc_url += "=";
                jdbc_url += std::to_string(c_cfg->SlaveOnly);
            } catch (sql::InvalidArgumentException&) {
                throw sql::InvalidArgumentException("Wrong type passed for slaveOnly expected bool");
            }
        } else if (!it->first.compare(OPT_SLAVE_WEIGHT_THRESHOLD)) {
            try {
                auto val = it->second.get<int32_t>();
                c_cfg->SlaveWeightThreshold = *val;
                jdbc_url += OPT_SLAVE_WEIGHT_THRESHOLD;
                jdbc_url += "=";
                jdbc_url += std::to_string(c_cfg->SlaveWeightThreshold);
            } catch (sql::InvalidArgumentException&) {
                throw sql::InvalidArgumentException("Wrong type passed for slaveWeightThreshold expected int32_t");
            }
        } else if (!it->first.compare(OPT_APPLY_DELAY_THRESHOLD)) {
            try {
                auto val = it->second.get<int32_t>();
                c_cfg->ApplyDelayThreshold = *val;
                jdbc_url += OPT_APPLY_DELAY_THRESHOLD;
                jdbc_url += "=";
                jdbc_url += std::to_string(c_cfg->ApplyDelayThreshold);
            } catch (sql::InvalidArgumentException&) {
                throw sql::InvalidArgumentException("Wrong type passed for applyDelayThreshold expected int32_t");
            }
        } else if (!it->first.compare(OPT_LOAD_BALANCE_ALGORITHM)) {
            try {
                auto val = it->second.get<sql::SQLString>();
                c_cfg->LoadBalanceAlgorithm = std::string(*val);
                jdbc_url += OPT_LOAD_BALANCE_ALGORITHM;
                jdbc_url += "=";
                jdbc_url += c_cfg->LoadBalanceAlgorithm;
            } catch (sql::InvalidArgumentException&) {
                throw sql::InvalidArgumentException("Wrong type passed for loadBalanceAlgorithm expected string (SQLString)");
            }
        } else if (!it->first.compare(OPT_ZONE_NAME)) {
            try {
                auto val = it->second.get<sql::SQLString>();
                c_cfg->ZoneName = std::string(*val);
                jdbc_url += OPT_ZONE_NAME;
                jdbc_url += "=";
                jdbc_url += c_cfg->ZoneName;
            } catch (sql::InvalidArgumentException&) {
                throw sql::InvalidArgumentException("Wrong type passed for zoneName expected string (SQLString)");
            }
        } else if (!it->first.compare(OPT_MIN_ZONE_NODES)) {
            try {
                auto val = it->second.get<int32_t>();
                c_cfg->MinZoneNodes = *val;
                jdbc_url += OPT_MIN_ZONE_NODES;
                jdbc_url += "=";
                jdbc_url += std::to_string(c_cfg->MinZoneNodes);
            } catch (sql::InvalidArgumentException&) {
                throw sql::InvalidArgumentException("Wrong type passed for minZoneNodes expected int32_t");
            }
        } else if (!it->first.compare(OPT_BACKUP_ZONE_NAME)) {
            try {
                auto val = it->second.get<sql::SQLString>();
                c_cfg->BackupZoneName = std::string(*val);
                jdbc_url += OPT_BACKUP_ZONE_NAME;
                jdbc_url += "=";
                jdbc_url += c_cfg->BackupZoneName;
            } catch (sql::InvalidArgumentException&) {
                throw sql::InvalidArgumentException("Wrong type passed for backupZoneName expected string (SQLString)");
            }
        } else if (!it->first.compare(OPT_INSTANCE_NAME)) {
            try {
                auto val = it->second.get<sql::SQLString>();
                c_cfg->InstanceName = std::string(*val);
                jdbc_url += OPT_INSTANCE_NAME;
                jdbc_url += "=";
                jdbc_url += c_cfg->InstanceName;
            } catch (sql::InvalidArgumentException&) {
                throw sql::InvalidArgumentException("Wrong type passed for instanceName expected string (SQLString)");
            }
        } else if (!it->first.compare(OPT_MPP_ROLE)) {
            try {
                auto val = it->second.get<sql::SQLString>();
                c_cfg->MppRole = std::string(*val);
                jdbc_url += OPT_MPP_ROLE;
                jdbc_url += "=";
                jdbc_url += c_cfg->MppRole;
            } catch (sql::InvalidArgumentException&) {
                throw sql::InvalidArgumentException("Wrong type passed for mppRole expected string (SQLString)");
            }
        } else if (!it->first.compare(OPT_ENABLE_FOLLOWER_READ)) {
            try {
                auto val = it->second.get<int32_t>();
                c_cfg->EnableFollowerRead = *val;
                jdbc_url += OPT_ENABLE_FOLLOWER_READ;
                jdbc_url += "=";
                jdbc_url += std::to_string(c_cfg->EnableFollowerRead);
            } catch (sql::InvalidArgumentException&) {
                throw sql::InvalidArgumentException("Wrong type passed for enableFollowerRead expected int32_t");
            }
        } else if (!it->first.compare(OPT_POLARDBX_CONNECT_TIMEOUT)) {
            try {
                auto connect_timeout = it->second.get<int32_t>();
                c_cfg->ConnectTimeoutMillis = *connect_timeout;
                jdbc_url += OPT_POLARDBX_CONNECT_TIMEOUT;
                jdbc_url += "=";
                jdbc_url += std::to_string(c_cfg->ConnectTimeoutMillis);
            } catch (sql::InvalidArgumentException&) {
                throw sql::InvalidArgumentException("Wrong type passed for connectTimeout expected int32_t");
            }
        }
    }

    std::string host_name = "127.0.0.1";
    int port = 3306;
    if (options.find("hostName") != options.end()) {
        try {
            host_name = *(options["hostName"].get<sql::SQLString>());
        } catch (sql::InvalidArgumentException&) {
            throw sql::InvalidArgumentException("Wrong type passed for hostName expected sql::SQLString");
        }
    }

    if (options.find("port") != options.end()) {
        try {
            port = *(options["port"].get< int >());
        } catch (sql::InvalidArgumentException&) {
            throw sql::InvalidArgumentException("Wrong type passed for port expected int");
        }
    }
    p_cfg->set_addr(host_name, port);
    p_cfg->set_conn_props(options);

    jdbc_url += OPT_HOSTNAME;
    jdbc_url += "=";
    jdbc_url += p_cfg->Addr;

    ha_manager_ = HaManager::get_manager(p_cfg);
    if (!ha_manager_) {
        throw sql::SQLException("failed to get ha manager, configuration is nullptr");
    }

    bool ok = false;
    if (ha_manager_->is_dn()) {
        auto [conn_addr, is_ok] = ha_manager_->get_available_dn_with_wait(c_cfg->ConnectTimeoutMillis, c_cfg->SlaveOnly, 
        c_cfg->ApplyDelayThreshold, c_cfg->SlaveWeightThreshold, c_cfg->LoadBalanceAlgorithm);
        ok = is_ok;
        conn_addr_ = conn_addr;
    } else {
        auto [conn_addr, is_ok] = ha_manager_->get_available_cn_with_wait(c_cfg->ConnectTimeoutMillis, c_cfg->ZoneName, 
        c_cfg->MinZoneNodes, c_cfg->BackupZoneName, c_cfg->SlaveOnly, c_cfg->InstanceName, c_cfg->MppRole, c_cfg->LoadBalanceAlgorithm);
        ok = is_ok;
        conn_addr_ = conn_addr;
    }

    if (!ok) {
        ha_manager_->drop_conn_count(conn_addr_);
        throw sql::SQLException("No available dn/cn");
    } else {
        options["hostName"] = conn_addr_;
        Driver * driver = sql::mysql::get_driver_instance();
        real_conn = driver->connect(options);

        if (record_jdbc_url) {
            recordJDBCURL(jdbc_url, real_conn);
        }
        if (!ha_manager_->is_dn() && !c_cfg->SlaveOnly) {
            enableFollowerRead(c_cfg->EnableFollowerRead, real_conn);
        }
    }
}

// [&param1=value1&param2=value2]
void PolarDBX_Connection::recordJDBCURL(const std::string & jdbc_url, sql::Connection * conn) {
    size_t max_size = strlen(RECORD_DSN_QUERY.c_str()) + jdbc_url.size() + 1;
    char buffer[max_size];
    snprintf(buffer, max_size, RECORD_DSN_QUERY.c_str(), jdbc_url.c_str());

    std::unique_ptr<sql::Statement> stmt(conn->createStatement());
    stmt->execute(buffer);
}

void PolarDBX_Connection::enableFollowerRead(int followerReadState, sql::Connection * conn) {
    std::unique_ptr<sql::Statement> stmt(conn->createStatement());
    switch (followerReadState) {
            case -1:
                // do nothing
                break;
            case 0:
                stmt->execute(SET_FOLLOWER_READ_FALSE);
                break;
            case 1:
                stmt->execute(SET_FOLLOWER_READ_TRUE);
                stmt->execute(SET_READ_WEIGHT);
                stmt->execute(ENABLE_CONSISTENT_READ_FALSE);
                break;
            case 2:
                stmt->execute(SET_FOLLOWER_READ_TRUE);
                stmt->execute(SET_READ_WEIGHT);
                stmt->execute(ENABLE_CONSISTENT_READ_TRUE);
                break;
            default:
                throw std::invalid_argument("Invalid enableFollowerRead state");
        }
}

PolarDBX_Connection::~PolarDBX_Connection()
{
    delete real_conn;
}

void PolarDBX_Connection::clearWarnings()
{
    real_conn->clearWarnings();
}

void PolarDBX_Connection::close()
{
    real_conn->close();
    if (ha_manager_ != nullptr) ha_manager_->drop_conn_count(conn_addr_);
}

void PolarDBX_Connection::commit()
{
    real_conn->commit();
}

sql::Statement * PolarDBX_Connection::createStatement()
{
    return real_conn->createStatement();
}

sql::SQLString PolarDBX_Connection::escapeString(const sql::SQLString & s)
{
    return dynamic_cast<sql::mysql::MySQL_Connection *>(real_conn)->escapeString(s);
}

bool PolarDBX_Connection::getAutoCommit()
{
    return real_conn->getAutoCommit();
}

sql::SQLString PolarDBX_Connection::getCatalog()
{
    return real_conn->getCatalog();
}

Driver * PolarDBX_Connection::getDriver()
{
    return driver;
}

sql::SQLString PolarDBX_Connection::getSchema()
{
    return real_conn->getSchema();
}

sql::SQLString PolarDBX_Connection::getClientInfo()
{
    return real_conn->getClientInfo();
}

void PolarDBX_Connection::getClientOption(const sql::SQLString & optionName, void * optionValue)
{
    real_conn->getClientOption(optionName, optionValue);
}

sql::SQLString PolarDBX_Connection::getClientOption(const sql::SQLString & optionName)
{
    return real_conn->getClientOption(optionName);
}

sql::DatabaseMetaData * PolarDBX_Connection::getMetaData()
{
    return real_conn->getMetaData();
}

enum_transaction_isolation PolarDBX_Connection::getTransactionIsolation()
{
    return real_conn->getTransactionIsolation();
}

const sql::SQLWarning * PolarDBX_Connection::getWarnings()
{
    return real_conn->getWarnings();
}

bool PolarDBX_Connection::isClosed()
{
    return real_conn->isClosed();
}

bool PolarDBX_Connection::isReadOnly()
{
    return real_conn->isReadOnly();
}

bool PolarDBX_Connection::isValid()
{
    return real_conn->isValid();
}

bool PolarDBX_Connection::reconnect()
{
    return real_conn->reconnect();
}

sql::SQLString PolarDBX_Connection::nativeSQL(const sql::SQLString& sql)
{
    return real_conn->nativeSQL(sql);
}

sql::PreparedStatement * PolarDBX_Connection::prepareStatement(const sql::SQLString& sql)
{
    return real_conn->prepareStatement(sql);
}

sql::PreparedStatement * PolarDBX_Connection::prepareStatement(const sql::SQLString& sql, int autoGeneratedKeys)
{
    return real_conn->prepareStatement(sql, autoGeneratedKeys);
}

sql::PreparedStatement * PolarDBX_Connection::prepareStatement(const sql::SQLString& sql, int columnIndexes[])
{
    return real_conn->prepareStatement(sql, columnIndexes);
}

sql::PreparedStatement * PolarDBX_Connection::prepareStatement(const sql::SQLString& sql, int resultSetType, int resultSetConcurrency)
{
    return real_conn->prepareStatement(sql, resultSetType, resultSetConcurrency);
}

sql::PreparedStatement * PolarDBX_Connection::prepareStatement(const sql::SQLString& sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability)
{
    return real_conn->prepareStatement(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
}

sql::PreparedStatement * PolarDBX_Connection::prepareStatement(const sql::SQLString& sql, sql::SQLString columnNames[])
{
    return real_conn->prepareStatement(sql, columnNames);
}

void PolarDBX_Connection::releaseSavepoint(sql::Savepoint * savepoint)
{
    real_conn->releaseSavepoint(savepoint);
}

void PolarDBX_Connection::rollback()
{
    real_conn->rollback();
}

void PolarDBX_Connection::rollback(sql::Savepoint * savepoint)
{
    real_conn->rollback(savepoint);
}

void PolarDBX_Connection::setAutoCommit(bool autoCommit)
{
    real_conn->setAutoCommit(autoCommit);
}

void PolarDBX_Connection::setCatalog(const sql::SQLString& catalog)
{
    real_conn->setCatalog(catalog);
}

void PolarDBX_Connection::setSchema(const sql::SQLString& schema)
{
    real_conn->setSchema(schema);
}

sql::Connection * PolarDBX_Connection::setClientOption(const sql::SQLString & optionName, const void * optionValue)
{
    return real_conn->setClientOption(optionName, optionValue);
}

sql::Connection * PolarDBX_Connection::setClientOption(const sql::SQLString & optionName, const sql::SQLString & optionValue)
{
    return real_conn->setClientOption(optionName, optionValue);
}

void PolarDBX_Connection::setHoldability(int holdability)
{
    real_conn->setHoldability(holdability);
}

void PolarDBX_Connection::setReadOnly(bool readOnly)
{
    real_conn->setReadOnly(readOnly);
}

sql::Savepoint * PolarDBX_Connection::setSavepoint()
{
    return real_conn->setSavepoint();
}

sql::Savepoint * PolarDBX_Connection::setSavepoint(const sql::SQLString& name)
{
    return real_conn->setSavepoint(name);
}

void PolarDBX_Connection::setTransactionIsolation(enum_transaction_isolation level)
{
    real_conn->setTransactionIsolation(level);
}

sql::SQLString PolarDBX_Connection::getSessionVariable(const sql::SQLString & varname)
{
    return dynamic_cast<sql::mysql::MySQL_Connection *>(real_conn)->getSessionVariable(varname);
}

void PolarDBX_Connection::setSessionVariable(const sql::SQLString & varname, const sql::SQLString & value)
{
    dynamic_cast<sql::mysql::MySQL_Connection *>(real_conn)->setSessionVariable(varname, value);
}

void PolarDBX_Connection::setSessionVariable(const sql::SQLString & varname, unsigned int value)
{
    dynamic_cast<sql::mysql::MySQL_Connection *>(real_conn)->setSessionVariable(varname, value);
}

sql::SQLString PolarDBX_Connection::getLastStatementInfo()
{
    return dynamic_cast<sql::mysql::MySQL_Connection *>(real_conn)->getLastStatementInfo();
}

std::string PolarDBX_Connection::getConnectionAddr() {
    return conn_addr_;
}

} // namespace sql
} // namespace polardbx