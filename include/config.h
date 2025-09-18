#ifndef CONFIG_H
#define CONFIG_H

#include <string>
#include <unordered_map>
#include <vector>
#include <set>
#include <atomic>
#include <nlohmann/json.hpp>
#include <jdbc/cppconn/connection.h>

// HA related
#define OPT_CLUSTERID                     "clusterID"
#define OPT_HA_CHECK_CONNECT_TIMEOUT      "haCheckConnectTimeout"
#define OPT_HA_CHECK_SOCKET_TIMEOUT       "haCheckSocketTimeout"
#define OPT_HA_CHECK_INTERVAL             "haCheckInterval"
#define OPT_CHECK_LEADER_TRANSFERRING_INTERVAL  "checkLeaderTransferringInterval"
#define OPT_LEADER_TRANSFERRING_WAIT_TIMEOUT    "leaderTransferringWaitTimeout"
#define OPT_SMOOTH_SWITCHOVER             "smoothSwitchover"
#define OPT_RECORD_JDBC_URL               "recordJdbcUrl"
#define OPT_DIRECT_MODE                   "directMode"
#define OPT_IGNORE_VIP                    "ignoreVip"
#define OPT_JSON_FILE                     "jsonFile"
#define OPT_ENABLE_LOG                    "enableLog"

// Connect related
#define OPT_POLARDBX_CONNECT_TIMEOUT        "connectTimeout"
#define OPT_SLAVE_ONLY                      "slaveRead"
#define OPT_SLAVE_WEIGHT_THRESHOLD          "slaveWeightThreshold"
#define OPT_APPLY_DELAY_THRESHOLD           "applyDelayThreshold"
#define OPT_LOAD_BALANCE_ALGORITHM          "loadBalanceAlgorithm"
#define OPT_ZONE_NAME                       "zoneName"
#define OPT_MIN_ZONE_NODES                  "minZoneNodes"
#define OPT_BACKUP_ZONE_NAME                "backupZoneName"
#define OPT_INSTANCE_NAME                   "instanceName"
#define OPT_MPP_ROLE                        "mppRole"
#define OPT_ENABLE_FOLLOWER_READ            "enableFollowerRead"

namespace sql {
namespace polardbx {

class PolarDBXConfig {
public:
    PolarDBXConfig();
    ~PolarDBXConfig();

    void set_addr(const std::string& hostName, int port);
    void set_conn_props(sql::ConnectOptionsMap conn_props);

    std::string Addr;

    int ClusterID;
    int32_t HaCheckConnectTimeoutMillis;
    int32_t HaCheckSocketTimeoutMillis;
    int32_t HaCheckIntervalMillis;
    int32_t CheckLeaderTransferringIntervalMillis;
    int32_t LeaderTransferringWaitTimeoutMillis;
    bool SmoothSwitchover;
    std::atomic<bool> IgnoreVip;
    std::string JsonFile;
    bool EnableLog;

    sql::ConnectOptionsMap conn_properties_;
};

class ConnectionConfig {
public:
    ConnectionConfig();
    ~ConnectionConfig();
    int32_t ConnectTimeoutMillis;
    bool SlaveOnly;
    int32_t SlaveWeightThreshold;
    int32_t ApplyDelayThreshold;
    std::string LoadBalanceAlgorithm;

    std::string ZoneName;
    int32_t MinZoneNodes;
    std::string BackupZoneName;
    std::string InstanceName;
    std::string MppRole;
    int32_t EnableFollowerRead;
};

} // namespace polardbx
} // namespace sql

#endif // CONFIG_H