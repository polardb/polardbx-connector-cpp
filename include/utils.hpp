#ifndef UTILS_HPP
#define UTILS_HPP

#include <string>
#include <sstream>
#include <vector>
#include <stdexcept>
#include <arpa/inet.h>

namespace sql {
namespace polardbx {

inline bool isIPv6(const std::string& address) {
    struct sockaddr_in sa4;
    struct sockaddr_in6 sa6;
    if (inet_pton(AF_INET, address.c_str(), &sa4.sin_addr) == 1) {
        return false;
    } else if (inet_pton(AF_INET6, address.c_str(), &sa6.sin6_addr) == 1) {
        return true;
    } else {
        return false;
    }
}

inline uint32_t versionString2Int32(const std::string& versionStr) {
    if (!versionStr.empty()) {
        size_t limit = 0;
        while (limit < versionStr.size()) {
            char ch = versionStr[limit];
            if ((ch < '0' || ch > '9') && ch != '.') {
                break;
            }
            ++limit;
        }
        std::string numOnly = versionStr.substr(0, limit);
        std::vector<std::string> parts;
        std::istringstream iss(numOnly);
        std::string part;
        while (getline(iss, part, '.')) {
            parts.push_back(part);
        }
        if (parts.size() >= 3) {
            int v1 = std::stoi(parts[0]);
            int v2 = std::stoi(parts[1]);
            int v3 = std::stoi(parts[2]);
            return static_cast<uint32_t>(10000 * v1 + 100 * v2 + v3);
        }
    }
    return 0;
}

inline std::string& ltrim(std::string& s) {
    s.erase(s.begin(), std::find_if(s.begin(), s.end(), [](unsigned char ch) {
        return !std::isspace(ch);
    }));
    return s;
}

inline std::string& rtrim(std::string& s) {
    s.erase(std::find_if(s.rbegin(), s.rend(), [](unsigned char ch) {
        return !std::isspace(ch);
    }).base(), s.end());
    return s;
}

inline std::string& trim(std::string& s) {
    return ltrim(rtrim(s));
}

inline bool containsIPv6Addresses(const std::string& addresses) {
    std::istringstream iss(addresses);
    std::string token;

    while (getline(iss, token, ',')) {
        token = trim(token);
        if (token.find_first_not_of(" \t\n\v\f\r") == std::string::npos || token.empty()) continue; // Skip empty or whitespace-only tokens
        if (isIPv6(token)) {
            return true;
        }
    }
    return false;
}

inline bool caseInsensitiveEqual(const std::string& a, const std::string& b) {
    if (a.length() != b.length()) return false;

    for (size_t i = 0; i < a.length(); ++i) {
        if (tolower(a[i]) != tolower(b[i])) {
            return false;
        }
    }
    return true;
}

inline std::tuple<std::string, uint32_t> parseHostPort(const std::string& addr) {
    std::string host;
    uint32_t port;

    std::string path = addr;
    auto pos = addr.find("://");
    if (pos != std::string::npos) {
        path = addr.substr(pos + 3);
    }
    pos = path.find(':');
    if (pos == std::string::npos) {
        host = path;
        port = 3306;
    } else {
        host = path.substr(0, pos);
        port = std::stoi(path.substr(pos + 1));
    }
    return {host, port};
}

inline std::string mergeHostPort(const std::string& host, uint32_t port) {
    auto addr = host + ":" + std::to_string(port);
    return addr;
}

inline std::string get_address_without_protocol(const std::string& addr) {
    auto [host, port] = parseHostPort(addr);
    return mergeHostPort(host, port);
}

} // namespace polardbx
} // namespace sql

#endif // UTILS_HPP