#ifndef LOGGER_H
#define LOGGER_H

#include <mutex>
#include <string>

namespace sql {
namespace polardbx {

enum LogLevel { INFO, DEBUG, ERROR };

class Logger {
public:
    Logger(const std::string& threadName, const std::string& colorCode, bool enabled = true);
    void info(const std::string& message);
    void debug(const std::string& message);
    void error(const std::string& message);
    void setEnabled(bool enabled);

private:
    std::string threadName;
    std::string colorCode;
    bool enabled;
    static std::mutex mutex;

    void log(int level, const std::string& message);
    std::string getLevelString(int level);
};

}  // namespace polardbx
}  // namespace sql

#endif  // LOGGER_H