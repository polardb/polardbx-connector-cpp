#include "logger.h"
#include <iostream>
#include <sstream>
#include <ctime>
#include <iomanip>

namespace sql {
namespace polardbx {

std::mutex Logger::mutex;

Logger::Logger(const std::string& threadName, const std::string& colorCode, bool enabled)
    : threadName(threadName), colorCode(colorCode), enabled(enabled) {}

void Logger::info(const std::string& message) {
    log(INFO, message);
}

void Logger::debug(const std::string& message) {
    log(DEBUG, message);
}

void Logger::error(const std::string& message) {
    log(ERROR, message);
}

void Logger::setEnabled(bool enabled) {
    this->enabled = enabled;
}

void Logger::log(int level, const std::string& message) {
    if (!enabled) {
        return;
    }

    std::lock_guard<std::mutex> lock(mutex);
    std::time_t now = std::time(nullptr);
    std::tm tm = *std::localtime(&now);
    std::ostringstream oss;
    oss << "[" << std::put_time(&tm, "%Y-%m-%d %H:%M:%S") << "] "
        << getLevelString(level) << " [" << threadName << "] ";

    std::cout << colorCode << oss.str() << message << "\033[0m" << std::endl;
}

std::string Logger::getLevelString(int level) {
    switch (level) {
        case INFO: return "INFO";
        case DEBUG: return "DEBUG";
        case ERROR: return "ERROR";
        default: return "";
    }
}

}  // namespace polardbx
}  // namespace sql