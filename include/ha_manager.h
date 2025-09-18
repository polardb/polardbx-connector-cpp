#ifndef HA_MANAGER_H_
#define HA_MANAGER_H_

#include <shared_mutex>
#include <unordered_map>
#include <thread>
#include <condition_variable>
#include "entity.hpp"
#include "config.h"
#include "logger.h"
#include "const.hpp"
#include "utils.hpp"
#include "jdbc/cppconn/statement.h"
#include "jdbc/cppconn/connection.h"
#include "jdbc/cppconn/resultset.h"
#include "jdbc/mysql_driver.h"
#include "jdbc/mysql_connection.h"

namespace sql {
namespace polardbx {

class HaManager {
public:
    explicit HaManager(
        std::atomic<bool> is_dn,
        bool use_ipv6,
        uint32_t version,
        std::shared_ptr<PolarDBXConfig> p_cfg)
        : is_dn_(is_dn), use_ipv6_(use_ipv6), version_(version), p_cfg_(p_cfg), dn_cluster_info_(std::make_shared<XClusterInfo>()), stop_flag_(false) {
            driver_logger_ = std::make_shared<Logger>("driver", BLUE);
            monitor_logger_ = std::make_shared<Logger>("monitor", GREEN);
            driver_logger_->setEnabled(p_cfg->EnableLog);
            monitor_logger_->setEnabled(p_cfg->EnableLog);
        }

    ~HaManager(){
        stop_flag_ = true;
        if (checker_thread_ && checker_thread_->joinable()) {
            checker_thread_->join();
        }
    };

    static std::tuple<int, std::string, bool> get_cluster_id_and_version(std::shared_ptr<PolarDBXConfig> p_cfg);
    static std::shared_ptr<HaManager> get_manager(std::shared_ptr<PolarDBXConfig> p_cfg);

    static std::unordered_map<std::string, std::shared_ptr<HaManager>> managers_;
    static std::shared_mutex managers_rw_mutex_;

    std::pair<std::string, bool> get_available_dn_with_wait(int32_t timeoutMs, bool slaveOnly, 
        int32_t applyDelayThreshold, int32_t slaveWeightThreshold, const std::string& loadBalanceAlgorithm);

    std::pair<std::string, bool> get_available_cn_with_wait(int32_t timeoutMs, const std::string& zoneName, 
        int32_t minZoneNodes, const std::string& backupZoneName, bool slaveRead, const std::string& instanceName,
        const std::string& mppRole, const std::string& loadBalanceAlgorithm);

    void add_conn_count(const std::string& addr);
    void drop_conn_count(const std::string& addr);
    bool is_dn() {return is_dn_;};

private:
    std::shared_mutex rw_mutex_;
    std::mutex mutex_;
    static std::mutex driver_mutex_;
    std::condition_variable conn_req_;

    bool is_dn_;
    bool use_ipv6_;
    uint32_t version_;

    std::shared_ptr<PolarDBXConfig> p_cfg_;
    std::shared_ptr<XClusterInfo> dn_cluster_info_;
    std::vector<std::shared_ptr<MppInfo>> cn_cluster_info_;
    std::vector<std::string> connection_addresses_;
    std::unordered_map<std::string, std::atomic<int64_t>> conn_cnt_;
    std::atomic<bool> stop_flag_;
    std::shared_ptr<std::thread> checker_thread_;

    std::shared_ptr<Logger> driver_logger_;
    std::shared_ptr<Logger> monitor_logger_;

    void dn_ha_checker();
    void cn_ha_checker();
    int32_t ping_leader(const std::shared_ptr<XClusterNodeBasic> &leader, std::shared_ptr<sql::Connection> conn);
    int32_t fully_check();
    std::vector<std::shared_ptr<MppInfo>> get_mpp_info(const std::string &addr) noexcept;
    std::vector<std::string> get_zone_list(const std::string& zone_names);
    bool probe_and_update_leader();
    void update_connection_addresses();
    std::unordered_map<std::string, std::shared_ptr<XClusterNodeBasic>> get_all_dn_info_concurrent(const std::vector<std::string> &addresses);
    std::shared_ptr<XClusterNodeBasic> get_dn_info(const std::string &addr) noexcept;
    std::pair<std::shared_ptr<XClusterNodeBasic>, bool> check_leader_exist(std::unordered_map<std::string, std::shared_ptr<XClusterNodeBasic>> &dn_infos);
    std::pair<std::vector<std::shared_ptr<XClusterNodeBasic>>, bool> load_dn_from_file(const std::string& filepath) noexcept;
    bool save_dn_to_file(const std::vector<std::shared_ptr<XClusterNodeBasic>>& nodes, const std::string& filename) noexcept;
    bool save_mpp_to_file(const std::vector<std::shared_ptr<MppInfo>>& mpp, const std::string& filename) noexcept;
    std::pair<std::vector<std::shared_ptr<MppInfo>>, bool> load_mpp_from_file(const std::string& filename) noexcept;

    std::pair<std::string, bool> get_available_dn_internal(bool slaveOnly, int32_t applyDelayThreshold, 
        int32_t slaveWeightThreshold, const std::string& loadBalanceAlgorithm);

    std::pair<std::string, bool> get_available_cn_internal(const std::string& zoneName, int32_t minZoneNodes,
        const std::string& backupZoneName, bool slaveRead, const std::string& instanceName,
        const std::string& mppRole, const std::string& loadBalanceAlgorithm);
    
    std::string get_dn_follower(const std::string& leader, int32_t applyDelayThreshold, int32_t slaveWeightThreshold, const std::string& loadBalanceAlgorithm);
    std::string get_node_with_load_balance(const std::set<std::string>& candidates, const std::string& loadBalanceAlgorithm);
};

inline std::string gen_cluster_tag(int cluster_id, const std::string& addr) {
    if (cluster_id == -1) {
		return addr + "#";
	}
	return std::to_string(cluster_id);
}

inline std::set<std::string> get_zone_set(const std::string& zone_names) {
    std::set<std::string> zone_set;
    if (!zone_names.empty()) {
        std::istringstream iss(zone_names);
        std::string token;
        while (getline(iss, token, ',')) {
            zone_set.insert(trim(token));
        }
    }
    return zone_set;
}

inline bool is_overlapped(const std::set<std::string>& set, const std::vector<std::string>& list) {
    for (const auto& item : list) {
        if (set.find(item) != set.end()) {
            return true;
        }
    }
    return false;
}

} // namespace polardbx
} // namespace sql

#endif // HA_MANAGER_H_