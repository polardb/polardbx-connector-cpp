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

#ifdef _WIN32
#include <Windows.h>
#endif

#include "polardbx_driver.h"
#include "polardbx_connection.h"

#include <jdbc/cppconn/exception.h>
#include "version_info.h"

extern "C"
{
CPPCONN_PUBLIC_FUNC void * sql_polardbx_get_driver_instance()
{
    void* ret = sql::polardbx::get_driver_instance();
    return ret;
}

CPPCONN_PUBLIC_FUNC sql::Driver * _get_driver_instance_by_name(const char* const clientlib)
{
    return sql::polardbx::_get_driver_instance_by_name(clientlib);
}

} // extern "C"


namespace sql {
namespace polardbx {

static const ::sql::SQLString emptyStr("");
std::mutex mu;

CPPCONN_PUBLIC_FUNC sql::polardbx::PolarDBX_Driver * _get_driver_instance_by_name(const char* const clientlib)
{
    SQLString dummy(clientlib);

    static std::map<sql::SQLString, std::shared_ptr<PolarDBX_Driver>> driver;
    
    std::map< sql::SQLString, std::shared_ptr< PolarDBX_Driver > >::const_iterator cit;

    std::lock_guard<std::mutex> lk(mu);
    if ((cit = driver.find(dummy)) != driver.end()) {
        return cit->second.get();
    } else {
        std::shared_ptr< PolarDBX_Driver > newDriver;

        newDriver.reset(new PolarDBX_Driver(dummy));
        driver[dummy] = newDriver;

        return newDriver.get();
    }
}

PolarDBX_Driver::PolarDBX_Driver()
{
}


PolarDBX_Driver::PolarDBX_Driver(const SQLString & clientLib)
{
}


PolarDBX_Driver::~PolarDBX_Driver()
{
}


Connection* PolarDBX_Driver::connect(const SQLString& hostName,
                                     const SQLString& userName,
                                     const SQLString& password)
{
    return new PolarDBX_Connection(this, hostName, userName, password);
}


Connection* PolarDBX_Driver::connect(ConnectOptionsMap& properties)
{
    return new PolarDBX_Connection(this, properties);
}


int PolarDBX_Driver::getMajorVersion()
{
    return POLARDBX_CPPCONN_MAJOR_VERSION;  // 在 version_info.h.cmake 中定义
}


int PolarDBX_Driver::getMinorVersion()
{
    return POLARDBX_CPPCONN_MINOR_VERSION;
}


int PolarDBX_Driver::getPatchVersion()
{
    return POLARDBX_CPPCONN_PATCH_VERSION;
}


const SQLString& PolarDBX_Driver::getName()
{
    static const SQLString name("PolarDB-X Connector/C++");
    return name;
}


void PolarDBX_Driver::setCallBack(Fido_Callback&& cb)
{
    fido_callback_store = std::move(cb);
    fido_callback = &fido_callback_store;
}


void PolarDBX_Driver::setCallBack(Fido_Callback& cb)
{
    fido_callback_store = Fido_Callback{};
    fido_callback = &cb;
}


} // namespace polardbx
} // namespace sql


/*
 * Local variables:
 * tab-width: 4
 * c-basic-offset: 4
 * End:
 * vim600: noet sw=4 ts=4 fdm=marker
 * vim<600: noet sw=4 ts=4
 */