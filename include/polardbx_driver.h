/*
 * Copyright (c) 2025, Alibaba Cloud. All rights reserved.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License, version 2.0, as
 * published by the Free Software Foundation.
 *
 * This program is also distributed with certain software (including
 * but not limited to OpenSSL) that is licensed under separate terms,
 * as designated in a particular file or component or in included license
 * documentation. The authors of PolarDB-X hereby grant you an
 * additional permission to link the program and your derivative works
 * with the separately licensed software that they have included with
 * PolarDB-X.
 *
 * Without limiting anything contained in the foregoing, this file,
 * which is part of PolarDB-X Connector/C++, is also subject to the
 * Universal FOSS Exception, version 1.0, a copy of which can be found at
 * http://oss.oracle.com/licenses/universal-foss-exception.
 *
 * This program is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU General Public License, version 2.0, for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA
 */

#ifndef _POLARDBX_DRIVER_H_
#define _POLARDBX_DRIVER_H_

#ifndef CPPCONN_PUBLIC_FUNC
    #ifdef _WIN32
        #ifdef BUILDING_DLL
            #define CPPCONN_PUBLIC_FUNC __declspec(dllexport)
        #else
            #define CPPCONN_PUBLIC_FUNC __declspec(dllimport)
        #endif
    #else
        #define CPPCONN_PUBLIC_FUNC
    #endif
#endif

#include "jdbc/cppconn/driver.h"
#include "jdbc/cppconn/sqlstring.h"

#include <memory>
#include <mutex>

extern "C"
{
CPPCONN_PUBLIC_FUNC void * sql_polardbx_get_driver_instance();
}

namespace sql {
namespace polardbx {

class CPPCONN_PUBLIC_FUNC PolarDBX_Driver : public Driver
{

    Fido_Callback* fido_callback = nullptr;
    Fido_Callback fido_callback_store;

public:
    PolarDBX_Driver(const SQLString& clientLib = "");
    PolarDBX_Driver();
    virtual ~PolarDBX_Driver();

    Connection* connect(const SQLString& hostName,
                        const SQLString& userName,
                        const SQLString& password) override;

    Connection* connect(ConnectOptionsMap& properties) override;

    int getMajorVersion() override;

    int getMinorVersion() override;

    int getPatchVersion() override;

    const SQLString& getName() override;

    void setCallBack(Fido_Callback& cb) override;

    void setCallBack(Fido_Callback&& cb) override;

    void threadInit() override {};

    void threadEnd() override {};

private:
    PolarDBX_Driver(const PolarDBX_Driver&) = delete;
    void operator=(const PolarDBX_Driver&) = delete;

    static std::unique_ptr<PolarDBX_Driver> instance;
    static std::mutex init_mutex;
};

CPPCONN_PUBLIC_FUNC sql::polardbx::PolarDBX_Driver * _get_driver_instance_by_name(const char * const clientlib);

inline static PolarDBX_Driver * get_driver_instance_by_name(const char * const clientlib)
{
  check_lib();
  return sql::polardbx::_get_driver_instance_by_name(clientlib);
}

inline static PolarDBX_Driver * get_driver_instance() {
    return sql::polardbx::get_driver_instance_by_name("");
}

inline static PolarDBX_Driver * get_polardbx_driver_instance() {
    return get_driver_instance();
}

}} // namespace sql::polardbx

#endif /* _POLARDBX_DRIVER_H_ */

/*
 * Local variables:
 * tab-width: 4
 * c-basic-offset: 4
 * End:
 * vim600: noet sw=4 ts=4 fdm=marker
 * vim<600: noet sw=4 ts=4
 */