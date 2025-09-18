#ifndef CONST_HPP
#define CONST_HPP

#include <string>
namespace sql {
namespace polardbx {

const std::string BASIC_INFO_QUERY {"/* PolarDB-X-Driver HAMANAGER */ select version(), @@cluster_id, @@port;"};
const std::string CLUSTER_LOCAL_QUERY {"/* PolarDB-X-Driver HAMANAGER */ select CURRENT_LEADER, ROLE from information_schema.alisql_cluster_local limit 1;"};
const std::string CLUSTER_GLOBAL_QUERY {"/* PolarDB-X-Driver HAMANAGER */ select ROLE, IP_PORT from information_schema.alisql_cluster_global;"};
const std::string CHECK_LEADER_TRANSFER_QUERY {"/* PolarDB-X-Driver HAMANAGER */ show global status like 'consensus_in_leader_transfer';"};
const std::string SET_PING_MODE {"/* PolarDB-X-Driver HAMANAGER */ set session ping_mode='IS_LEADER,NOT_IN_LEADER_TRANSFER,NO_CLUSTER_CHANGED';"};
const std::string SHOW_MPP_QUERY {"/* PolarDB-X-HA-Driver HAMANAGER */ show mpp;"};
const std::string RECORD_DSN_QUERY {"/* PolarDB-X-Driver HAMANAGER */ call dbms_conn.comment_connection('%s');"};
const std::string CLUSTER_HEALTH_QUERY {"/* PolarDB-X-Driver HAMANAGER */ select a.Role, a.IP_PORT from information_schema.alisql_cluster_health a join information_schema.alisql_cluster_global b on a.IP_PORT=b.IP_PORT where a.APPLY_RUNNING='Yes' and a.APPLY_DELAY_SECONDS <= %d and b.ELECTION_WEIGHT > %d"};
const std::string SET_FOLLOWER_READ_TRUE {"/* PolarDB-X-Driver HAMANAGER */ set session enable_in_memory_follower_read = true;"};
const std::string SET_FOLLOWER_READ_FALSE {"/* PolarDB-X-Driver HAMANAGER */ set session enable_in_memory_follower_read = false;"};
const std::string SET_READ_WEIGHT {"/* PolarDB-X-Driver HAMANAGER */ set session FOLLOWER_READ_WEIGHT = 100;"};
const std::string ENABLE_CONSISTENT_READ_TRUE {"/* PolarDB-X-Driver HAMANAGER */ set session ENABLE_CONSISTENT_REPLICA_READ = true;"};
const std::string ENABLE_CONSISTENT_READ_FALSE {"/* PolarDB-X-Driver HAMANAGER */ set session ENABLE_CONSISTENT_REPLICA_READ = false;"};

const std::string RESET {"\033[0m"};
const std::string RED {"\033[31m"};
const std::string GREEN {"\033[32m"};
const std::string YELLOW {"\033[33m"};
const std::string BLUE {"\033[34m"};
const std::string MAGENTA {"\033[35m"};
const std::string CYAN {"\033[36m"};

enum DNState { LEADER_ALIVE = 0, LEADER_TRANSFERRING = 1, LEADER_TRANSFERRED = 2, LEADER_LOST = 3 };
enum CNState { CN_ALIVE = 0, CN_LOST = 1 };

constexpr std::string_view MYSQL_NATIVE {"mysqlNative"};
constexpr std::string_view LEADER_ONLY {"leaderOnly"};
constexpr std::string_view SLAVE_ONLY {"slaveOnly"};
constexpr std::string_view MIX {"mix"};
constexpr std::string_view MYSQL_WO {"mysqlWo"};
constexpr std::string_view LEADER_ONLY_WO {"leaderOnlyWo"};
constexpr std::string_view SLAVE_ONLY_WO {"slaveOnlyWo"};
constexpr std::string_view MIX_WO {"mixWo"};

constexpr std::string_view RANDOM {"random"};
constexpr std::string_view LEAST_CONN {"least_connection"};

const std::string W {"W"};
const std::string R {"R"};
const std::string CR {"CR"};

constexpr int DO_NOTHING {-1};
constexpr int READ_LEADER {0};
constexpr int READ_FOLLOWER {1};
constexpr int READ_CONSISTENT_FOLLOWER {2};

} // namespace polardbx

} // namespace sql

#endif // CONST_HPP