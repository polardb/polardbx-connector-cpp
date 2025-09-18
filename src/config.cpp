#include "config.h"
#include "utils.hpp"
#include <algorithm>
#include <cassert>
#include <cstring>
#include <iomanip>
#include <sstream>
#include <string>
#include <utility>
#include <vector>
#include <nlohmann/json.hpp>

namespace sql {
namespace polardbx {

PolarDBXConfig::PolarDBXConfig()
    : Addr(""),
      ClusterID(-1),
      HaCheckConnectTimeoutMillis(3000),
      HaCheckSocketTimeoutMillis(3000),
      HaCheckIntervalMillis(5000),
      CheckLeaderTransferringIntervalMillis(100),
      LeaderTransferringWaitTimeoutMillis(5000),
      SmoothSwitchover(false),
      IgnoreVip(true),
      JsonFile(""),
      EnableLog(false)
{
}

PolarDBXConfig::~PolarDBXConfig() {}

void PolarDBXConfig::set_addr(const std::string& host_name, int port) {
    if (port < 0)   port = 3306;

    std::string multi_addr = "";
    std::istringstream iss(host_name);
    std::string addr;

    while (getline(iss, addr, ',')) {
        addr = addr.find(':') == std::string::npos ? addr + ":" + std::to_string(port) : addr;
        addr = get_address_without_protocol(addr);
        multi_addr += multi_addr.empty() ? addr : "," + addr;
    }
    Addr = multi_addr;
}

void PolarDBXConfig::set_conn_props(sql::ConnectOptionsMap conn_props) {
    conn_properties_ = conn_props;
    conn_properties_.erase(OPT_RECONNECT);
    conn_properties_.erase(OPT_RETRY_COUNT);
    conn_properties_.erase(OPT_CONNECT_TIMEOUT);
    conn_properties_.erase(OPT_READ_TIMEOUT);
    conn_properties_.erase(OPT_WRITE_TIMEOUT);
    conn_properties_.erase("hostName");
    conn_properties_.erase("port");
}

ConnectionConfig::ConnectionConfig() 
    : ConnectTimeoutMillis(5000), 
      SlaveOnly(false), 
      SlaveWeightThreshold(1), 
      ApplyDelayThreshold(3), 
      LoadBalanceAlgorithm("random"),
      ZoneName(""),
      MinZoneNodes(0),
      BackupZoneName(""),
      InstanceName(""),
      MppRole(""),
      EnableFollowerRead(-1)
{
};

ConnectionConfig::~ConnectionConfig() {};

} // namespace polardbx
} // namespace sql