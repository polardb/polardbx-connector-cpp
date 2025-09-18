#include <gtest/gtest.h>
#include <thread>
#include <vector>
#include <atomic>
#include <chrono>
#include <mutex>
#include <iostream>
#include <fstream>
#include <jdbc/mysql_driver.h>
#include <jdbc/mysql_connection.h>
#include <jdbc/cppconn/statement.h>
#include <jdbc/cppconn/prepared_statement.h>
#include <jdbc/cppconn/resultset.h>
#include <jdbc/cppconn/exception.h>

#include "polardbx_connection.h"
#include "polardbx_driver.h"
#include "ha_manager.h"
#include "config.h"

using namespace std;
using namespace sql;

const char* updateSQL = R"(
	UPDATE history
	SET h_data = ?
	WHERE h_c_id = ? AND h_c_d_id = ? AND h_c_w_id = ?
)";

const chrono::seconds duration(20);

std::atomic<int> successCount = 0;
std::atomic<int> failCount = 0;

std::string dn_host;
int dn_port = 0;
std::string dn_username;
std::string dn_password;
std::string cn_host;
int cn_port = 0;
std::string cn_username;
std::string cn_password;
std::string dbname;

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);

    // Check if any required environment variables are missing
    if (argc <= 9) {
        std::cerr << "arg count is wrong" << std::endl;
        std::cerr << "Usage: ./concurrency_test --DNHOST=<host> --DNPORT=<port> --DNUSER=<user> --DNPASSWD=<passwd> --CNHOST=<host> --CNPORT=<port> --CNUSER=<user> --CNPASSWD=<passwd> --DBNAME=<dbname>" << std::endl;
        return EXIT_FAILURE;
    }

	std::string dn_port_str;
	std::string cn_port_str;

    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg.find("--DNHOST=") == 0) {
            dn_host = arg.substr(strlen("--DNHOST="));
        } else if (arg.find("--DNPORT=") == 0) {
            dn_port_str = arg.substr(strlen("--DNPORT="));
        } else if (arg.find("--DNUSER=") == 0) {
            dn_username = arg.substr(strlen("--DNUSER="));
        } else if (arg.find("--DNPASSWD=") == 0) {
            dn_password = arg.substr(strlen("--DNPASSWD="));
        } else if (arg.find("--CNHOST=") == 0) {
            cn_host = arg.substr(strlen("--CNHOST="));
        } else if (arg.find("--CNPORT=") == 0) {
            cn_port_str = arg.substr(strlen("--CNPORT="));
        } else if (arg.find("--CNUSER=") == 0) {
            cn_username = arg.substr(strlen("--CNUSER="));
        } else if (arg.find("--CNPASSWD=") == 0) {
            cn_password = arg.substr(strlen("--CNPASSWD="));
        } else if (arg.find("--DBNAME=") == 0) {
            dbname = arg.substr(strlen("--DBNAME="));
        }
    }

    // Check if any required environment variables are missing
    if (dn_host.empty() || dn_port_str.empty() || dn_username.empty() || dn_password.empty() ||
		cn_host.empty() || cn_port_str.empty() || cn_username.empty() || cn_password.empty() || dbname.empty()) {
		std::cerr << "Usage: ./concurrency_test --DNHOST=<host> --DNPORT=<port> --DNUSER=<user> --DNPASSWD=<passwd> --CNHOST=<host> --CNPORT=<port> --CNUSER=<user> --CNPASSWD=<passwd> --DBNAME=<dbname>" << std::endl;
		return EXIT_FAILURE;
	}


    dn_port = stoi(dn_port_str);
    cn_port = stoi(cn_port_str);

    return RUN_ALL_TESTS();
}

// void startQPSCounter(const string& filename, const atomic<bool>& isOver) {
// 	std::ofstream file;
//     file.open(filename, std::ios_base::out | std::ios_base::trunc);
//     if (!file.is_open()) {
//         throw std::runtime_error("Failed to open or create file: " + filename);
//     }

// 	file << "timestamp,success,fail,qps\n";

// 	long lastSuccess = successCount.load();
// 	long lastFail = failCount.load();

// 	while (!isOver.load()) {
// 		this_thread::sleep_for(chrono::seconds(1));
// 		long currentSuccess = successCount.load();
// 		long currentFail = failCount.load();

// 		long deltaSuccess = currentSuccess - lastSuccess;
// 		long deltaFail = currentFail - lastFail;

// 		double qps = static_cast<double>(deltaSuccess) / 1.0;

// 		file << chrono::system_clock::to_time_t(chrono::system_clock::now()) << "," << deltaSuccess << "," << deltaFail << "," << qps << "\n";

// 		lastSuccess = currentSuccess;
// 		lastFail = currentFail;
// 	}

// 	file.close();
// }

void startQPSCounter(const atomic<bool>& isOver) {
    cout << "timestamp,success,fail,qps\n";

    long lastSuccess = successCount.load();
    long lastFail = failCount.load();

    while (!isOver.load()) {
        this_thread::sleep_for(chrono::seconds(1));
        long currentSuccess = successCount.load();
        long currentFail = failCount.load();

        long deltaSuccess = currentSuccess - lastSuccess;
        long deltaFail = currentFail - lastFail;

        double qps = static_cast<double>(deltaSuccess) / 1.0;

        cout << chrono::system_clock::to_time_t(chrono::system_clock::now()) << "," << deltaSuccess << "," << deltaFail << "," << qps << "\n";

        lastSuccess = currentSuccess;
        lastFail = currentFail;
    }
}


bool executeUpdate(int id, std::string hostname, std::string user, std::string password, std::string dbname) {
	try {
		auto driver = sql::polardbx::get_driver_instance();
		std::unique_ptr<sql::Connection> conn(driver->connect(hostname, user, password));
		conn->setSchema(dbname);
		std::unique_ptr<sql::PreparedStatement>  pstmt(conn->prepareStatement(updateSQL));
		pstmt->setString(1, "New transaction data");
		pstmt->setInt(2, id);
		pstmt->setInt(3, id);
		pstmt->setInt(4, id);
		pstmt->executeUpdate();
		conn->close();
	} catch (sql::SQLException& e) {
		std::cout << "Failed to execute SQL: " << e.what() << std::endl;
		return false;
	}
	return true;
}

TEST(ConcurrentDNTest, DoConcurrentTest) {
	atomic<bool> isOver{false};
	thread record_thread(startQPSCounter, ref(isOver));
	vector<thread> workers;

    successCount = 0;
    failCount = 0;

	for (int i = 0; i < 100; ++i) {
		workers.emplace_back([&]() {
			auto startTime = chrono::steady_clock::now();
			while (chrono::duration_cast<chrono::seconds>(chrono::steady_clock::now() - startTime).count() < duration.count()) {
				this_thread::sleep_for(chrono::milliseconds(100));
				if (executeUpdate(i, std::string(dn_host) + ":" + std::to_string(dn_port), dn_username, dn_password, dbname)) {
					successCount++;
				}
			}
		});
	}

	for (auto& worker : workers) {
		if (worker.joinable()) {
			worker.join();
		}
	}

	isOver.store(true);
	record_thread.join();
}

TEST(ConcurrentCNTest, DoConcurrentTest) {
	atomic<bool> isOver{false};
	thread record_thread(startQPSCounter, ref(isOver));
	vector<thread> workers;

    successCount = 0;
    failCount = 0;

	for (int i = 0; i < 100; ++i) {
		workers.emplace_back([&]() {
			auto startTime = chrono::steady_clock::now();
			while (chrono::duration_cast<chrono::seconds>(chrono::steady_clock::now() - startTime).count() < duration.count()) {
				this_thread::sleep_for(chrono::milliseconds(100));
				if (executeUpdate(i, std::string(cn_host) + ":" + std::to_string(cn_port), cn_username, cn_password, dbname)) {
					successCount++;
				}
			}
		});
	}

	for (auto& worker : workers) {
		if (worker.joinable()) {
			worker.join();
		}
	}

	isOver.store(true);
	record_thread.join();
}