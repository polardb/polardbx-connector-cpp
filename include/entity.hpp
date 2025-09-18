#ifndef ENTITY_HPP
#define ENTITY_HPP

#include <string>
#include <vector>
#include <memory>
#include <optional>
#include <atomic>
#include <nlohmann/json.hpp>
#include <jdbc/cppconn/connection.h>

namespace sql {
namespace polardbx {

struct LeaderTransferInfo {
    std::string tag; // ip:port
    int64_t nanos; // timestamp

    LeaderTransferInfo() {};
    LeaderTransferInfo(const std::string& tag, int64_t nanos) : tag(tag), nanos(nanos) {};
};

struct XClusterNodeBasic {
    std::string Tag;
    std::string Host;
    int32_t Port;
    std::string Role;
    std::vector<std::shared_ptr<XClusterNodeBasic>> Peers;
    std::string UpdateTime;

    XClusterNodeBasic() {};

    XClusterNodeBasic(const std::string& tag,
                      const std::string& host,
                      int32_t port,
                      const std::string& role,
                      const std::vector<std::shared_ptr<XClusterNodeBasic>>& peers,
                      const std::string& updateTime)
        : Tag(tag), Host(host), Port(port), Role(role), Peers(peers), UpdateTime(updateTime) {};

    nlohmann::json to_json() const {
        return {
            {"tag", Tag},
            {"host", Host},
            {"port", Port},
            {"role", Role},
            {"peers", nlohmann::json::array()},
            {"update_time", UpdateTime}
        };
    }

    static XClusterNodeBasic from_json(const nlohmann::json &j) {
        XClusterNodeBasic node;
        node.Tag = j["tag"];
        node.Host = j["host"];
        node.Port = j["port"];
        node.Role = j["role"];
        node.UpdateTime = j["update_time"];
        return node;
    }
};

struct XClusterInfo {
    std::shared_ptr<XClusterNodeBasic> LeaderInfo;
    std::shared_ptr<LeaderTransferInfo> leader_transfer_info;
    std::atomic<int32_t> GlobalPortGap;
    std::shared_ptr<sql::Connection> LongConnection;

    XClusterInfo() : GlobalPortGap(-8000), LeaderInfo(nullptr), leader_transfer_info(nullptr), LongConnection(nullptr)  {};
    ~XClusterInfo() {};
    XClusterInfo(const std::shared_ptr<XClusterNodeBasic>& leader_info, const std::shared_ptr<LeaderTransferInfo>& leader_transfer_info, std::shared_ptr<sql::Connection> long_connection)
        : LeaderInfo(leader_info), leader_transfer_info(leader_transfer_info), LongConnection(long_connection) {};
};

struct MppInfo {
    std::string Tag; // NODE(ip:port)
    std::string Role;
    std::string InstanceName; // ID
    std::vector<std::string> ZoneList; // sub_cluster
    std::string IsLeader;

    MppInfo() {};
    MppInfo(const std::string& tag, const std::string& role, const std::string& instance_name, const std::vector<std::string>& zone_list, const std::string& is_leader)
        : Tag(tag), Role(role), InstanceName(instance_name), ZoneList(zone_list), IsLeader(is_leader) {};

    nlohmann::json to_json() const {
        return {
            {"tag", Tag},
            {"role", Role},
            {"instance_name", InstanceName},
            {"zone_list", ZoneList},
            {"is_leader", IsLeader}
        };
    }

    static MppInfo from_json(const nlohmann::json &j) {
        MppInfo info;
        info.Tag = j["tag"];
        info.Role = j["role"];
        info.InstanceName = j["instance_name"];
        info.ZoneList = j["zone_list"];
        info.IsLeader = j["is_leader"];
        return info;
    }
};

} // namespace polardbx

} // namespace sql

#endif // ENTITY_HPP