#include "ha_manager.h"
#include "utils.hpp"
#include "const.hpp"
#include <iostream>
#include <fstream>
#include <filesystem>
#include <chrono>
#include <thread>
#include <map>
#include <functional>
#include <nlohmann/json.hpp>
#include <random>
#include <jdbc/cppconn/connection.h>

namespace fs = std::filesystem;

namespace sql {
namespace polardbx {
std::unordered_map<std::string, std::shared_ptr<HaManager>> HaManager::managers_;
std::shared_mutex HaManager::managers_rw_mutex_;
std::mutex HaManager::driver_mutex_;

std::shared_ptr<HaManager> HaManager::get_manager(std::shared_ptr<PolarDBXConfig> p_cfg) {
    if (p_cfg == nullptr) {
        return nullptr;
    }
    auto tag = gen_cluster_tag(p_cfg->ClusterID, p_cfg->Addr);

    {
        std::shared_lock<std::shared_mutex> lk(managers_rw_mutex_);
        auto it = managers_.find(tag);
        if (it != managers_.end()) {
            return it->second;
        }
    }

    auto tmp_dir = fs::temp_directory_path();
    auto [cluster_id, version, is_dn] = get_cluster_id_and_version(p_cfg);
    bool use_ipv6 = isIPv6(p_cfg->Addr);
    std::string json_file;

    if (p_cfg->JsonFile.empty()) {
        if (is_dn) {
            if (use_ipv6) {
                json_file = tmp_dir / ("XCluster-" + std::to_string(cluster_id) + "-IPv6.json");
            } else {
                json_file = tmp_dir / ("XCluster-" + std::to_string(cluster_id) + "-IPv4.json");
            }
        } else {
            if (use_ipv6) {
                json_file = tmp_dir / ("XCluster-" + p_cfg->Addr + "-IPv6.json");
            } else {
                json_file = tmp_dir / ("XCluster-" + p_cfg->Addr + "-IPv4.json");
            }
        }
        p_cfg->JsonFile = json_file;
    }

    std::ifstream file(json_file);
    if (!file.is_open()) {
        // create new file
        std::ofstream n_file(json_file);
        if (!n_file.is_open()) {
            throw std::runtime_error("Failed to create file: " + json_file);
        }
        n_file.close();
    }

    tag = gen_cluster_tag(cluster_id, p_cfg->Addr);

    {
        std::unique_lock<std::shared_mutex> lk(managers_rw_mutex_);
        if (managers_.find(tag) == managers_.end()) {
            auto manager = std::make_shared<HaManager>(
                is_dn,
                use_ipv6,
                versionString2Int32(version),
                p_cfg
            );

            manager->checker_thread_ = std::make_shared<std::thread>([manager]() {
                if (manager->is_dn_) {
                    manager->dn_ha_checker();
                } else {
                    manager->cn_ha_checker();
                }
            });

            managers_[tag] = manager;
            return manager;
        } else {
            return managers_[tag];
        }

    }
}

std::pair<std::string, bool> HaManager::get_available_dn_with_wait(
    int32_t timeoutMs,
    bool slaveOnly,
    int32_t applyDelayThreshold,
    int32_t slaveWeightThreshold,
    const std::string& loadBalanceAlgorithm)
{
    using namespace std::chrono;

    auto deadlineNs = high_resolution_clock::now().time_since_epoch().count() +
                     static_cast<int64_t>(timeoutMs) * 1000000LL;

    while (true) {
        auto nowNs = high_resolution_clock::now().time_since_epoch().count();

        if (nowNs >= deadlineNs) {
            driver_logger_->info("get_available_dn_with_wait last try");
            return get_available_dn_internal(slaveOnly, applyDelayThreshold, slaveWeightThreshold, loadBalanceAlgorithm);
        }
        driver_logger_->info("get_available_dn_with_wait try");
        auto [dn, ok] = get_available_dn_internal(slaveOnly, applyDelayThreshold, slaveWeightThreshold, loadBalanceAlgorithm);
        driver_logger_->debug("get_available_dn_with_wait: " + std::to_string(ok) + ", dn:" + dn);
       
        if (ok && !dn.empty()) {
            return {dn, ok};
        }

        nowNs = high_resolution_clock::now().time_since_epoch().count();
        int64_t sleepDurationMs = std::max<int64_t>(0, (deadlineNs - nowNs) / 1000000);
        {
            driver_logger_->info("get_available_dn failed, wait to be notified, " + std::to_string(sleepDurationMs) + "ms");
            std::unique_lock<std::mutex> lk(mutex_); 
            conn_req_.wait_for(lk, std::chrono::milliseconds(sleepDurationMs));
        }
    }
}

std::pair<std::string, bool> HaManager::get_available_dn_internal(
    bool slaveOnly, 
    int32_t applyDelayThreshold, 
    int32_t slaveWeightThreshold, 
    const std::string& loadBalanceAlgorithm) 
{
    std::string leader = "";
    {
        std::shared_lock<std::shared_mutex> lk(rw_mutex_);
        if (dn_cluster_info_->LeaderInfo != nullptr) {
            leader = dn_cluster_info_->LeaderInfo->Tag;
        }
    }

    if (leader.empty()) {
        return {"", false};
    }

    if (!slaveOnly) {
        return {leader, true};
    }

    std::string follower = get_dn_follower(leader, applyDelayThreshold, slaveWeightThreshold, loadBalanceAlgorithm);
    if (!follower.empty()) {
        return {follower, true};
    }

    return {"", false};
}

std::string HaManager::get_dn_follower(
    const std::string& leader,
    int32_t applyDelayThreshold,
    int32_t slaveWeightThreshold,
    const std::string& loadBalanceAlgorithm)
{
    std::set<std::string> followers;
    try {
        sql::Driver* driver;
        {
            std::lock_guard<std::mutex> lock(driver_mutex_);
            driver = sql::mysql::get_driver_instance();
        }
        sql::ConnectOptionsMap conn_props = p_cfg_->conn_properties_;
        conn_props["hostName"] = leader;
        conn_props[OPT_CONNECT_TIMEOUT] = 2;
        std::unique_ptr<sql::Connection> conn(driver->connect(conn_props));

        char query_buffer[512];
        snprintf(query_buffer, sizeof(query_buffer), CLUSTER_HEALTH_QUERY.c_str(),
                applyDelayThreshold, slaveWeightThreshold);

        std::unique_ptr<sql::Statement> stmt(conn->createStatement());
        std::unique_ptr<sql::ResultSet> res(stmt->executeQuery(query_buffer));
        while (res->next()) {
            std::string role = res->getString(1); // ROLE
            std::string addr = res->getString(2); // IP_PORT

            if (!caseInsensitiveEqual(role, "Follower")) {
                continue;
            }

            auto [host, paxos_port] = parseHostPort(addr);
            auto port = paxos_port + dn_cluster_info_->GlobalPortGap;
            followers.insert(mergeHostPort(host, port));
        }

        conn->close();
    } catch (sql::SQLException &e) {
        driver_logger_->error(std::string("get_dn_follower failed: ") + e.what());
        return "";
    }

    if (followers.empty()) {
        return "";
    }

    return get_node_with_load_balance(followers, loadBalanceAlgorithm);
}

std::string HaManager::get_node_with_load_balance(const std::set<std::string>& candidates, const std::string& loadBalanceAlgorithm) {
    if (candidates.empty()) {
        return "";
    }

    std::unique_lock<std::shared_mutex> lk(rw_mutex_);
    std::string conn_node;

    if (caseInsensitiveEqual(loadBalanceAlgorithm, "random")) {
        static thread_local std::mt19937 rng(static_cast<unsigned int>(
            std::chrono::high_resolution_clock::now().time_since_epoch().count()
        ));
        std::vector<std::string> candidate_list(candidates.begin(), candidates.end());
        std::uniform_int_distribution<size_t> dist(0, candidate_list.size() - 1);

        conn_node = candidate_list[dist(rng)];

    } else if (caseInsensitiveEqual(loadBalanceAlgorithm, "least_connection") || caseInsensitiveEqual(loadBalanceAlgorithm, "least_conn")) {
        int64_t leastCnt = INT64_MAX;
        for (const auto& node : candidates) {
            auto it = conn_cnt_.find(node);
            if (it == conn_cnt_.end()) {
                conn_cnt_[node] = 0;
                conn_node = node;
                break;
            }

            int64_t cnt = it->second.load();
            if (cnt < leastCnt) {
                leastCnt = cnt;
                conn_node = node;
            }
        }

    } else {
        conn_node = *candidates.begin();
    }

    if (!conn_node.empty()) {
        add_conn_count(conn_node);
    }

    return conn_node;
}

std::pair<std::string, bool> HaManager::get_available_cn_with_wait(int32_t timeoutMs, const std::string& zoneName, 
    int32_t minZoneNodes, const std::string& backupZoneName, bool slaveRead, const std::string& instanceName,
    const std::string& mppRole, const std::string& loadBalanceAlgorithm) {
    using namespace std::chrono;
    auto deadlineNs = high_resolution_clock::now().time_since_epoch().count() +
                     static_cast<int64_t>(timeoutMs) * 1000000LL;

    while (true) {
        auto nowNanos = high_resolution_clock::now().time_since_epoch().count();

        if (nowNanos >= deadlineNs) {
            // last try
            driver_logger_->info("get_available_cn_with_wait last try");
            return get_available_cn_internal(zoneName, minZoneNodes, backupZoneName, slaveRead, instanceName, mppRole, loadBalanceAlgorithm);
        }

        driver_logger_->info("get_available_cn_with_wait try");
        auto [cn, ok] = get_available_cn_internal(zoneName, minZoneNodes, backupZoneName, slaveRead, instanceName, mppRole, loadBalanceAlgorithm);
        driver_logger_->debug("get_available_cn_with_wait: " + std::to_string(ok) + ", cn:" + cn);
        if (ok && !cn.empty()) {
            return {cn, ok};
        }

        nowNanos = high_resolution_clock::now().time_since_epoch().count();
        int64_t sleepDurationMs = std::max<int64_t>(0, (deadlineNs - nowNanos) / 1000000);
        {
            driver_logger_->info("get_available_cn failed, wait to be notified, " + std::to_string(sleepDurationMs) + "ms");
            std::unique_lock<std::mutex> lk(mutex_); 
            conn_req_.wait_for(lk, std::chrono::milliseconds(sleepDurationMs));
        }
    }
}

std::pair<std::string, bool> HaManager::get_available_cn_internal(const std::string& zoneName, int32_t minZoneNodes,
    const std::string& backupZoneName, bool slaveRead, const std::string& instanceName,
    const std::string& mppRole, const std::string& loadBalanceAlgorithm) {
    driver_logger_->debug("try to get valid cn: " + zoneName + ", minZoneNodes: " + std::to_string(minZoneNodes) + ", instanceName: " + instanceName);

    auto zoneSet = get_zone_set(zoneName);
    auto backupZoneSet = get_zone_set(backupZoneName);
    std::set<std::string> validCn;
    std::set<std::string> backupCn;

    {
        std::shared_lock<std::shared_mutex> lk(rw_mutex_);
        for (const auto& cn : cn_cluster_info_) {
            if ((instanceName.empty() || instanceName == cn->InstanceName) &&
                ((slaveRead && (!caseInsensitiveEqual(mppRole, W) && !caseInsensitiveEqual(cn->Role, W))) ||
                    (!slaveRead && (caseInsensitiveEqual(mppRole, W) || mppRole.empty()) && caseInsensitiveEqual(cn->Role, W)))) {

                if (zoneSet.empty() || is_overlapped(zoneSet, cn->ZoneList)) {
                    validCn.insert(cn->Tag);
                }
                if (is_overlapped(backupZoneSet, cn->ZoneList)) {
                    backupCn.insert(cn->Tag);
                }
            }
        }
    }

    std::string conn_cn = "";
    if (validCn.size() >= minZoneNodes) {
        conn_cn = get_node_with_load_balance(validCn, loadBalanceAlgorithm);
    } else if (!backupCn.empty()) {
        conn_cn = get_node_with_load_balance(backupCn, loadBalanceAlgorithm);
    }
    return {conn_cn, !conn_cn.empty()};
}

void HaManager::cn_ha_checker() {
    while (!stop_flag_) {
        std::unordered_map<std::string, std::shared_ptr<MppInfo>> cn_map;

        if (connection_addresses_.empty()) {
            update_connection_addresses();
        }

        for (const auto& addr : connection_addresses_) {
            auto mppInfo = get_mpp_info(addr);
            for (auto& mpp : mppInfo) {
                cn_map[mpp->Tag] = mpp;
            }
        }

        std::vector<std::shared_ptr<MppInfo>> cn_cluster_info;
        for (auto& pair : cn_map) {
            cn_cluster_info.push_back(pair.second);
        }

        if (!cn_cluster_info.empty()) {
            save_mpp_to_file(cn_cluster_info, p_cfg_->JsonFile);
        }

        int32_t cluster_state = cn_cluster_info.empty() ? CN_LOST : CN_ALIVE;
        if (cluster_state == CN_ALIVE) {
            monitor_logger_->debug("Cn cluster size is " + std::to_string(cn_cluster_info.size()));
            std::unique_lock<std::shared_mutex> lk(rw_mutex_);
            cn_cluster_info_ = cn_cluster_info;
            conn_req_.notify_all();
        } else {
            cluster_state = CN_LOST;
        }
        
        auto interval = cluster_state == CN_ALIVE ? p_cfg_->HaCheckIntervalMillis : std::min(500, p_cfg_->HaCheckIntervalMillis);

        std::this_thread::sleep_for(std::chrono::milliseconds(interval));
    }
}

std::vector<std::shared_ptr<MppInfo>> HaManager::get_mpp_info(const std::string &addr) noexcept {
    std::vector<std::shared_ptr<MppInfo>> mpp_infos;
    try {
        sql::Driver* driver;
        {
            std::lock_guard<std::mutex> lock(driver_mutex_);
            driver = sql::mysql::get_driver_instance();
        }
        sql::ConnectOptionsMap conn_props = p_cfg_->conn_properties_;
        conn_props["hostName"] = addr;
        conn_props[OPT_CONNECT_TIMEOUT] = 2;
        std::unique_ptr<sql::Connection> conn(driver->connect(conn_props));
        std::unique_ptr<sql::Statement> stmt(conn->createStatement());
        std::unique_ptr<sql::ResultSet> res(stmt->executeQuery(SHOW_MPP_QUERY));

        while (res->next()) {
            auto instance_name = res->getString(1);
            auto tag = res->getString(2);
            auto role = res->getString(3);
            auto is_leader = res->getString(4);
            auto zone_names = res->getString(5);
            std::vector<std::string> zone_list = get_zone_list(zone_names);

            monitor_logger_->debug("instanceName: " + instance_name + ", tag: " + tag);

            mpp_infos.push_back(std::make_shared<MppInfo>(tag, role, instance_name, zone_list, is_leader));
        }

        conn->close();
    } catch (sql::SQLException& e) {
        monitor_logger_->error(std::string("Failed to get mpp info: ") + addr + ", error: " + e.what());
    }
    return mpp_infos;
}

std::vector<std::string> HaManager::get_zone_list(const std::string& zone_names) {
    std::vector<std::string> zone_list;

    if (zone_names.empty()) {
        return zone_list;
    }

    std::stringstream ss(zone_names);
    std::string token;
    while (getline(ss, token, ',')) {
        zone_list.push_back(trim(token));
    }

    return zone_list;
}

bool HaManager::save_mpp_to_file(const std::vector<std::shared_ptr<MppInfo>>& mpp, const std::string& filename) noexcept {
    try {
        nlohmann::json jArray;
        for (const auto& info : mpp) {
            auto item = info->to_json();
            jArray.push_back(item);
        }

        std::ofstream file(filename);
        if (!file.is_open()) {
            monitor_logger_->info("Failed to open mpp file: " + filename);
            return false;
        }

        file << std::setw(4) << jArray << std::endl;
        file.close();
    } catch (std::exception &e) {
        monitor_logger_->error(std::string("Failed to save mpp file: ") + filename + ", error: " + e.what());
        return false;
    }
    return true;
}

std::pair<std::vector<std::shared_ptr<MppInfo>>, bool> HaManager::load_mpp_from_file(const std::string& filename) noexcept {
    std::vector<std::shared_ptr<MppInfo>> mpp;
    try {
        std::ifstream file(filename);
        if (!file.is_open()) {
            monitor_logger_->info("Failed to open file: " + filename);
            return {{}, false};
        }

        nlohmann::json jArray;
        file >> jArray;
        file.close();

        if (jArray.is_null()) {
            return {{}, false};
        }

        for (const auto& item : jArray) {
            auto info = std::make_shared<MppInfo>(MppInfo::from_json(item));
            mpp.push_back(info);
        }
    } catch (std::exception &e) {
        monitor_logger_->error(std::string("Failed to parse file: ") + filename + ", error: " + e.what());
        return {{}, false};
    }

    return {mpp, true};
}

void HaManager::dn_ha_checker() {
    while (!stop_flag_) {
        {
            std::unique_lock<std::shared_mutex> lk(rw_mutex_);
            if (dn_cluster_info_->leader_transfer_info != nullptr) {
                auto now = std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch()).count();
                auto timeoutNanos = static_cast<int64_t>(p_cfg_->LeaderTransferringWaitTimeoutMillis) * 1000000LL; // ms to ns
                if (now - dn_cluster_info_->leader_transfer_info->nanos > timeoutNanos) {
                    dn_cluster_info_->leader_transfer_info.reset();
                }
            }
        }

        int32_t clusterState = 0;
        auto leader = dn_cluster_info_->LeaderInfo;
        auto conn = dn_cluster_info_->LongConnection;
        if (leader != nullptr && conn != nullptr) {
            monitor_logger_->info("start ping leader");
            clusterState = ping_leader(leader, conn);
        } else {
            monitor_logger_->info("start full check");
            clusterState = fully_check();
        }

        int interval = 0;
        if (clusterState == LEADER_ALIVE) {
            // leader is alive, retry in (~, 100] ms
            interval = std::max(0, std::min(100, static_cast<int>(p_cfg_->HaCheckIntervalMillis)));
        } else if (clusterState == LEADER_LOST) {
            // leader is lost, retry in (~, 3000] ms
            interval = std::max(0, std::min(3000, static_cast<int>(p_cfg_->HaCheckIntervalMillis)));
        } else if (clusterState == LEADER_TRANSFERRING) {
            // leader is transferring, retry in (~, transfer_time_out] ms
            interval = std::max(0, static_cast<int>(p_cfg_->CheckLeaderTransferringIntervalMillis));
        } else if (clusterState == LEADER_TRANSFERRED) {
            // leader has transferrred, retry now
            interval = 0;
        }

        if (interval > 0) {
            std::this_thread::sleep_for(std::chrono::milliseconds(interval));
        }
    }
}

int32_t HaManager::ping_leader(const std::shared_ptr<XClusterNodeBasic> &leader, std::shared_ptr<sql::Connection> conn) {
    if (leader == nullptr || conn == nullptr) {
        return LEADER_LOST;
    }

    try {
        std::unique_ptr<sql::Statement> stmt(conn->createStatement());
        std::unique_ptr<sql::ResultSet> res1(stmt->executeQuery(CLUSTER_LOCAL_QUERY));
        while (res1->next()) {
            auto role = res1->getString(2);
            if (!caseInsensitiveEqual(role, "Leader")) {
                std::unique_lock<std::shared_mutex> lk(rw_mutex_);
                dn_cluster_info_->LeaderInfo.reset();
                return LEADER_TRANSFERRED;
            }
        }
    
        std::unique_ptr<sql::ResultSet> res2(stmt->executeQuery(CHECK_LEADER_TRANSFER_QUERY));
        while (res2->next()) {
            auto is_transferring = res2->getInt(2);
            if (is_transferring) {
                std::unique_lock<std::shared_mutex> lk(rw_mutex_);
                dn_cluster_info_->LeaderInfo.reset();
                dn_cluster_info_->leader_transfer_info = std::make_shared<LeaderTransferInfo>(leader->Tag, std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch()).count());
                return LEADER_TRANSFERRING;
            }
        }
        
    } catch (sql::SQLException &e) {
        monitor_logger_->error(std::string("ping_leader failed: ") + e.what());
        std::unique_lock<std::shared_mutex> lk(rw_mutex_);
        dn_cluster_info_->LeaderInfo.reset();
        return LEADER_LOST;
    }

    return LEADER_ALIVE;
}

int32_t HaManager::fully_check() {
    auto leader_exist = probe_and_update_leader();
    auto leader_transfer_info = dn_cluster_info_->leader_transfer_info;

    if (leader_exist) {
        return LEADER_ALIVE;
    }

    if (leader_transfer_info != nullptr) {
        return LEADER_TRANSFERRING;
    }

    return LEADER_LOST;
}

bool HaManager::probe_and_update_leader() {
    if (connection_addresses_.empty()) {
        update_connection_addresses();
    }

    monitor_logger_->debug("start to get all dn info concurrently");
    auto dn_info_map = get_all_dn_info_concurrent(connection_addresses_);

    auto [leader, leader_exist] = check_leader_exist(dn_info_map);
    if (!leader_exist) {
        return false;
    }

    std::vector<std::shared_ptr<XClusterNodeBasic>> dn_info_list;
    for (const auto& [addr, info] : dn_info_map) {
        if (info != nullptr) {
            dn_info_list.push_back(info);
        }
    }
    save_dn_to_file(dn_info_list, p_cfg_->JsonFile);
    
    try {
        sql::Driver* driver;
        {
            std::lock_guard<std::mutex> lock(driver_mutex_);
            driver = sql::mysql::get_driver_instance();
        }
        sql::ConnectOptionsMap conn_props = p_cfg_->conn_properties_;
        conn_props["hostName"] = leader->Tag;
        conn_props[OPT_CONNECT_TIMEOUT] = 2;
        std::shared_ptr<sql::Connection> conn(driver->connect(conn_props));
        std::unique_ptr<sql::Statement> stmt(conn->createStatement());
        std::unique_ptr<sql::ResultSet> res(stmt->executeQuery(CHECK_LEADER_TRANSFER_QUERY));
        while (res->next()) {
            auto is_transferring = res->getInt(2);
            if (is_transferring) {
                std::unique_lock<std::shared_mutex> lk(rw_mutex_);
                dn_cluster_info_->LeaderInfo.reset();
                if (dn_cluster_info_->LongConnection != nullptr && !dn_cluster_info_->LongConnection->isClosed()) {
                    dn_cluster_info_->LongConnection = nullptr;
                }
                dn_cluster_info_->LongConnection = nullptr;
                dn_cluster_info_->leader_transfer_info = std::make_shared<LeaderTransferInfo>(leader->Tag, std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch()).count());
                return false;
            }
        }

        {
            std::unique_lock<std::shared_mutex> lk(rw_mutex_);
            dn_cluster_info_->LeaderInfo = leader;
            dn_cluster_info_->leader_transfer_info.reset();
            if (dn_cluster_info_->LongConnection != nullptr && !dn_cluster_info_->LongConnection->isClosed()) {
                dn_cluster_info_->LongConnection->close();
            }
            dn_cluster_info_->LongConnection = conn;
            conn_req_.notify_all();
            return true;
        }
    } catch (sql::SQLException &e) {
        monitor_logger_->error(std::string("probe_and_update_leader failed: ") + e.what());
        return false;
    }
}

void HaManager::update_connection_addresses() {
    std::set<std::string> connection_addresses;

    std::string json_file, dsn_addrs;
    {
        std::shared_lock<std::shared_mutex> lk(rw_mutex_);
        json_file = p_cfg_->JsonFile;
        dsn_addrs = p_cfg_->Addr;
    }

    if (!json_file.empty()) {
        if (is_dn_) {
            auto [nodes, success] = load_dn_from_file(json_file);
            if (success) {
                for (const auto& node : nodes) {
                    if (node && (caseInsensitiveEqual(node->Role, "Leader") ||
                                 caseInsensitiveEqual(node->Role, "Follower"))) {
                        connection_addresses.insert(get_address_without_protocol(node->Tag));
                    }
                }
            }
        } else {
            auto [mppInfos, success] = load_mpp_from_file(json_file);
            if (success) {
                for (const auto& info : mppInfos) {
                    monitor_logger_->debug("get mpp from file " + p_cfg_->JsonFile + ": " + info->Tag);
                    connection_addresses.insert(info->Tag);
                }
            }
        }
    }

    if (!dsn_addrs.empty()) {
        std::istringstream iss(dsn_addrs);
        std::string token;
        while (getline(iss, token, ',')) {
            auto addr = trim(token);
            if (!addr.empty()) {
                monitor_logger_->debug("get mpp from dsn: " + addr);
                connection_addresses.insert(get_address_without_protocol(addr));
            }
        }
    }

    if (connection_addresses_.empty()) {
        std::unique_lock<std::shared_mutex> lk(rw_mutex_);
        connection_addresses_.assign(connection_addresses.begin(), connection_addresses.end());
    }
}

std::unordered_map<std::string, std::shared_ptr<XClusterNodeBasic>> HaManager::get_all_dn_info_concurrent(const std::vector<std::string> &addresses) {
    std::unordered_map<std::string, std::shared_ptr<XClusterNodeBasic>> dn_infos;
    std::mutex mu;
    std::vector<std::thread> threads;
    threads.reserve(addresses.size());
    monitor_logger_->debug(std::to_string(addresses.size()) + " addresses to be probed");

    for (const auto& addr : addresses) {
        threads.emplace_back([this, &dn_infos, &mu, addr]() {
            auto info = get_dn_info(addr);
            if (info != nullptr) {
                monitor_logger_->debug("get dn info: " + info->Tag);
                std::lock_guard<std::mutex> lock(mu);
                dn_infos[info->Tag] = info;
                for (const auto& peer : info->Peers) {
                    dn_infos[peer->Tag] = peer;
                }
            }
        });
    }

    for (auto& t : threads) {
        if (t.joinable()) {
            t.join();
        }
    }

    monitor_logger_->debug(std::to_string(dn_infos.size()) + " dn infos got");
    return dn_infos;
}

std::shared_ptr<XClusterNodeBasic> HaManager::get_dn_info(const std::string &addr) noexcept {
    std::shared_ptr<XClusterNodeBasic> dn_info;
    try {
        sql::Driver* driver;
        {
            std::lock_guard<std::mutex> lock(driver_mutex_);
            driver = sql::mysql::get_driver_instance();
        }
        sql::ConnectOptionsMap conn_props = p_cfg_->conn_properties_;
        conn_props["hostName"] = addr;
        conn_props[OPT_CONNECT_TIMEOUT] = 5;
        monitor_logger_->debug("try to connect " + addr);
        std::unique_ptr<sql::Connection> conn(driver->connect(conn_props));
        std::unique_ptr<sql::Statement> stmt(conn->createStatement());
        std::unique_ptr<sql::ResultSet> res1(stmt->executeQuery(CLUSTER_LOCAL_QUERY));

        std::string current_leader, role;
        while (res1->next()) {
            current_leader = res1->getString(1);
            role = res1->getString(2);
        }

        auto [host, port] = parseHostPort(addr);
        auto [leader_host, leader_paxos_port] = parseHostPort(current_leader);
        if (!caseInsensitiveEqual(role, "Leader")) {
            int32_t leader_port;
            {
                std::shared_lock<std::shared_mutex> lk(rw_mutex_);
                leader_port = leader_paxos_port + dn_cluster_info_->GlobalPortGap;
            }
            auto leader_peer = std::make_shared<XClusterNodeBasic>(
                mergeHostPort(leader_host, leader_port),
                leader_host,
                leader_port,
                "Leader",
                std::vector<std::shared_ptr<XClusterNodeBasic>>(),
                std::to_string(std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch()).count())
            );
            dn_info = std::make_shared<XClusterNodeBasic>(
                addr,
                host,
                port,
                role,
                std::vector<std::shared_ptr<XClusterNodeBasic>>({leader_peer}),
                std::to_string(std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch()).count())
            );
        } else {
            auto paxos_port_gap = port - leader_paxos_port;
            {
                std::unique_lock<std::shared_mutex> lk(rw_mutex_);
                dn_cluster_info_->GlobalPortGap = paxos_port_gap;
            }
            std::unique_ptr<sql::ResultSet> res2(stmt->executeQuery(CLUSTER_GLOBAL_QUERY));
            std::vector<std::shared_ptr<XClusterNodeBasic>> peers;
            while (res2->next()) {
                auto peer_role = res2->getString(1);
                auto peer_addr = res2->getString(2);
                auto [peer_host, peer_paxos_port] = parseHostPort(peer_addr);
                if (!caseInsensitiveEqual(peer_role, "Leader")) {
                    auto peer_port = peer_paxos_port + dn_cluster_info_->GlobalPortGap;
                    peers.push_back(std::make_shared<XClusterNodeBasic>(
                        mergeHostPort(peer_host, peer_port),
                        peer_host,
                        peer_port,
                        peer_role,
                        std::vector<std::shared_ptr<XClusterNodeBasic>>(),
                        std::to_string(std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch()).count())
                    ));
                }
            }

            dn_info = std::make_shared<XClusterNodeBasic>(
                addr,
                host,
                port,
                role,
                peers,
                std::to_string(std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch()).count())
            );
        }
        conn->close();

    } catch (sql::SQLException &e) {
        monitor_logger_->error(std::string("get dn info failed: ") + e.what());
        dn_info.reset();
    }
    return dn_info;
}

std::pair<std::shared_ptr<XClusterNodeBasic>, bool> HaManager::check_leader_exist(std::unordered_map<std::string, std::shared_ptr<XClusterNodeBasic>> &dn_infos) {
    std::shared_ptr<XClusterNodeBasic> leader;
    bool leader_exist = false;
    for (const auto& [addr, dn_info] : dn_infos) {
        if (caseInsensitiveEqual(dn_info->Role, "Leader")) {
            if (p_cfg_->IgnoreVip || dn_info->Tag == mergeHostPort(dn_info->Host, dn_info->Port)) {
                leader_exist = true;
                leader = dn_info;
                break;
            }
        }
    }
    return std::make_pair(leader, leader_exist);
}

bool HaManager::save_dn_to_file(const std::vector<std::shared_ptr<XClusterNodeBasic>>& nodes, const std::string& filename) noexcept {
    try {
        nlohmann::json arr;
        for (const auto& node : nodes) {
            if (node) {
                arr.push_back(node->to_json());
            }
        }

        std::string content = arr.dump(2);

        auto temp_filename = filename + ".tmp";

        std::ofstream ofs(temp_filename, std::ios::out | std::ios::trunc);
        if (!ofs.is_open()) {
            return false;
        }
        ofs << content << std::flush;
        ofs.close();

        if (!ofs.good()) {
            fs::remove(temp_filename);
            return false;
        }

        fs::rename(temp_filename, filename);

        return true;

    } catch (const std::exception& e) {
        monitor_logger_->error(std::string("save_dn_to_file failed: ") + e.what());
        return false;
    }
}

std::pair<std::vector<std::shared_ptr<XClusterNodeBasic>>, bool> HaManager::load_dn_from_file(const std::string& filepath) noexcept {
    std::ifstream file(filepath);
    if (!file.is_open()) {
        return {{}, false};
    }

    try {
        nlohmann::json j;
        file >> j;
        std::vector<std::shared_ptr<XClusterNodeBasic>> nodes;
        for (auto& item : j) {
            auto node = std::make_shared<XClusterNodeBasic>(XClusterNodeBasic::from_json(item));
            nodes.push_back(node);
        }
        return {nodes, true};
    } catch (std::exception& e) {
        monitor_logger_->error(std::string("load_dn_from_file failed: ") + e.what());
        return {{}, false};
    }
}

std::tuple<int, std::string, bool> HaManager::get_cluster_id_and_version(std::shared_ptr<PolarDBXConfig> p_cfg) {
    std::istringstream iss(p_cfg->Addr);
    std::string conn_addr;
    while (getline(iss, conn_addr, ',')) {
        conn_addr = trim(conn_addr);
        if (!conn_addr.empty()) {
            break;
        }
    }
    
    if (conn_addr.empty()) {
        throw sql::InvalidArgumentException("Invalid connection address");
    }

    int cluster_id = -1;
    std::string version = "";
    bool is_dn = false;;
    try {
        sql::Driver* driver;
        {
            std::lock_guard<std::mutex> lock(driver_mutex_);
            driver = sql::mysql::get_driver_instance();
        }
        sql::ConnectOptionsMap conn_props = p_cfg->conn_properties_;
        conn_props["hostName"] = conn_addr;
        conn_props[OPT_CONNECT_TIMEOUT] = 2;
        std::unique_ptr<sql::Connection> conn(driver->connect(conn_props));
        std::unique_ptr<sql::Statement> stmt(conn->createStatement());
        std::unique_ptr<sql::ResultSet> res(stmt->executeQuery(BASIC_INFO_QUERY));

        while (res->next()) {
            version = res->getString(1);
            cluster_id = res->getInt(2);
        }

        conn->close();

        if (version.find("-TDDL-") != std::string::npos) {
            cluster_id = -1;
            is_dn = false;
        } else {
            is_dn = true;
        }

        return {cluster_id, version, is_dn};
    } catch (sql::SQLException& e) {
        throw e;
    }
}

void HaManager::add_conn_count(const std::string& addr) {
    conn_cnt_[addr]++;
}

void HaManager::drop_conn_count(const std::string& addr) {
    std::unique_lock<std::shared_mutex> lk(rw_mutex_);
    conn_cnt_[addr]--;
}

} // namespace polardbx
} // namespace sql